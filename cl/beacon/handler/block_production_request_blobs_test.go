// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	network_services_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
)

type testBlob struct {
	blob       *cltypes.Blob
	commitment cltypes.KZGCommitment
	proofs     []cltypes.KZGProof
}

// newTestBlob returns a distinct valid blob with its commitment and the proofs a block request
// carries for it: one blob proof before Fulu, one proof per cell from Fulu on.
func newTestBlob(t *testing.T, seed byte, version clparams.StateVersion) testBlob {
	t.Helper()
	blob := &cltypes.Blob{}
	blob[31] = seed // last byte of the first field element keeps it below the modulus
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 0)
	require.NoError(t, err)
	if version >= clparams.FuluVersion {
		_, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
		require.NoError(t, err)
		return testBlob{blob: blob, commitment: cltypes.KZGCommitment(commitment), proofs: proofs}
	}
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(blob), commitment, 0)
	require.NoError(t, err)
	return testBlob{blob: blob, commitment: cltypes.KZGCommitment(commitment), proofs: []cltypes.KZGProof{cltypes.KZGProof(proof)}}
}

func newRequestBlock(cfg *clparams.BeaconChainConfig, version clparams.StateVersion, slot uint64, blobs ...testBlob) *cltypes.DenebSignedBeaconBlock {
	block := cltypes.NewDenebSignedBeaconBlock(cfg, version)
	block.SignedBlock.Block.Slot = slot
	for _, b := range blobs {
		commitment := b.commitment
		block.SignedBlock.Block.Body.BlobKzgCommitments.Append(&commitment)
		block.Blobs.Append(b.blob)
		for i := range b.proofs {
			block.KZGProofs.Append(&b.proofs[i])
		}
	}
	return block
}

// newRequestBlobsHandler returns a handler whose chain is at version from genesis.
func newRequestBlobsHandler(t *testing.T, version clparams.StateVersion) *ApiHandler {
	t.Helper()
	blobBundles, err := lru.New[common.Bytes48, BlobBundle]("test-request-blobs", minBlobBundleCacheSize)
	require.NoError(t, err)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0, 0, 0, 0
	cfg.FuluForkEpoch = math.MaxUint64
	if version >= clparams.FuluVersion {
		cfg.FuluForkEpoch = 0
	}
	return &ApiHandler{blobBundles: blobBundles, beaconChainCfg: &cfg}
}

func requireBundle(t *testing.T, bundles map[common.Bytes48]BlobBundle, b testBlob) {
	t.Helper()
	bundle, ok := bundles[common.Bytes48(b.commitment)]
	require.True(t, ok, "a bundle must be returned for every commitment")
	require.Equal(t, common.Bytes48(b.commitment), bundle.Commitment)
	require.Equal(t, *b.blob, *bundle.Blob)
	require.Len(t, bundle.KzgProofs, len(b.proofs))
	for i := range b.proofs {
		require.Equal(t, common.Bytes48(b.proofs[i]), bundle.KzgProofs[i])
	}
}

func requireNotCached(t *testing.T, h *ApiHandler, blobs ...testBlob) {
	t.Helper()
	for _, b := range blobs {
		_, ok := h.blobBundles.Get(common.Bytes48(b.commitment))
		require.False(t, ok, "request data must not be written to the blob bundle cache")
	}
}

func TestRequestBlobBundlesVerifiesRequestBlobs(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t, version)
			first, second := newTestBlob(t, 1, version), newTestBlob(t, 2, version)

			bundles, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, version, 0, first, second))

			require.NoError(t, err)
			require.Len(t, bundles, 2)
			requireBundle(t, bundles, first)
			requireBundle(t, bundles, second)
			requireNotCached(t, h, first, second)
			if version >= clparams.FuluVersion {
				for _, b := range []testBlob{first, second} {
					cells, err := das.ComputeCells(b.blob)
					require.NoError(t, err)
					require.Equal(t, cells, bundles[common.Bytes48(b.commitment)].Cells, "the verified cells must be kept for the column build")
				}
			}
		})
	}
}

// TestRequestBlobBundlesUsesCachedBundles proves the producing node uses the bundle it got from the
// execution layer: the request's copy of that blob is neither verified nor used.
func TestRequestBlobBundlesUsesCachedBundles(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.ElectraVersion)
	b := newTestBlob(t, 1, clparams.ElectraVersion)
	h.blobBundles.Add(common.Bytes48(b.commitment), BlobBundle{
		Commitment: common.Bytes48(b.commitment),
		Blob:       b.blob,
		KzgProofs:  []common.Bytes48{common.Bytes48(b.proofs[0])},
	})
	request := b
	request.proofs = []cltypes.KZGProof{{0xc0}} // would fail verification if it were checked

	bundles, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, request))

	require.NoError(t, err)
	requireBundle(t, bundles, b)
}

// TestRequestBlobBundlesVerifiesCachedBundleWithWrongProofCount proves a cached bundle whose proof
// count does not fit the block's fork is not used: the request's copy is verified and used instead,
// so the column build never indexes cell proofs the bundle does not have.
func TestRequestBlobBundlesVerifiesCachedBundleWithWrongProofCount(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.FuluVersion)
	electraBlob, fuluBlob := newTestBlob(t, 1, clparams.ElectraVersion), newTestBlob(t, 1, clparams.FuluVersion)
	h.blobBundles.Add(common.Bytes48(electraBlob.commitment), BlobBundle{
		Commitment: common.Bytes48(electraBlob.commitment),
		Blob:       electraBlob.blob,
		KzgProofs:  []common.Bytes48{common.Bytes48(electraBlob.proofs[0])},
	})

	bundles, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, clparams.FuluVersion, 0, fuluBlob))

	require.NoError(t, err)
	requireBundle(t, bundles, fuluBlob)
}

func TestRequestBlobBundlesRejectsForkThatDoesNotMatchSlot(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.FuluVersion)
	b := newTestBlob(t, 1, clparams.ElectraVersion)

	_, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, b))

	require.ErrorContains(t, err, "is labelled electra but its slot is in fulu")
}

// TestRequestBlobBundlesChecksAtForkAndBlobScheduleBoundaries pins the fork-label check and the
// blob limit at the slots where they change: the last Electra slot and the first Fulu slot, and the
// last slot before and the first slot of a blob schedule entry.
func TestRequestBlobBundlesChecksAtForkAndBlobScheduleBoundaries(t *testing.T) {
	const fuluEpoch, bpoEpoch = 2, 3
	h := newRequestBlobsHandler(t, clparams.ElectraVersion)
	h.beaconChainCfg.FuluForkEpoch = fuluEpoch
	h.beaconChainCfg.BlobSchedule = []clparams.BlobParameters{{Epoch: bpoEpoch, MaxBlobsPerBlock: 6}}
	slotsPerEpoch := h.beaconChainCfg.SlotsPerEpoch
	fuluBlobs := func(n int) []testBlob {
		blobs := make([]testBlob, n)
		for i := range blobs {
			blobs[i] = newTestBlob(t, byte(i+1), clparams.FuluVersion)
		}
		return blobs
	}
	electraBlob, fuluBlob := newTestBlob(t, 1, clparams.ElectraVersion), newTestBlob(t, 1, clparams.FuluVersion)

	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
		slot    uint64
		blobs   []testBlob
		wantErr string
	}{
		{"last electra slot labelled electra", clparams.ElectraVersion, fuluEpoch*slotsPerEpoch - 1, []testBlob{electraBlob}, ""},
		{"first fulu slot labelled electra", clparams.ElectraVersion, fuluEpoch * slotsPerEpoch, []testBlob{electraBlob}, "is labelled electra but its slot is in fulu"},
		{"last electra slot labelled fulu", clparams.FuluVersion, fuluEpoch*slotsPerEpoch - 1, []testBlob{fuluBlob}, "is labelled fulu but its slot is in electra"},
		{"7 blobs right before the schedule entry", clparams.FuluVersion, bpoEpoch*slotsPerEpoch - 1, fuluBlobs(7), ""},
		{"6 blobs at the schedule entry", clparams.FuluVersion, bpoEpoch * slotsPerEpoch, fuluBlobs(6), ""},
		{"7 blobs at the schedule entry", clparams.FuluVersion, bpoEpoch * slotsPerEpoch, fuluBlobs(7), "more than 6 blob commitments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundles, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, tc.version, tc.slot, tc.blobs...))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, bundles, len(tc.blobs))
		})
	}
}

func TestRequestBlobBundlesRejectsBlobsWithoutCommitmentList(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.ElectraVersion)
	block := newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, newTestBlob(t, 1, clparams.ElectraVersion))
	block.SignedBlock.Block.Body.BlobKzgCommitments = nil

	_, err := h.requestBlobBundles(block)

	require.ErrorContains(t, err, "request has blobs but the block has no blob_kzg_commitments")
}

// TestRequestBlobBundlesRejectsTooManyBlobsBeforeVerifying proves the gossip blob limit is checked
// before any KZG work: the proofs here are invalid, yet the limit is what the request fails on.
func TestRequestBlobBundlesRejectsTooManyBlobsBeforeVerifying(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.ElectraVersion)
	limit := int(h.beaconChainCfg.MaxBlobsPerBlockElectra)
	blobs := make([]testBlob, limit+1)
	for i := range blobs {
		blobs[i] = testBlob{blob: &cltypes.Blob{}, commitment: cltypes.KZGCommitment{byte(i + 1)}, proofs: []cltypes.KZGProof{{}}}
	}

	_, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, blobs...))

	require.ErrorContains(t, err, "more than 9 blob commitments")

	fulu := newRequestBlobsHandler(t, clparams.FuluVersion)
	fulu.beaconChainCfg.BlobSchedule = []clparams.BlobParameters{{Epoch: 0, MaxBlobsPerBlock: 3}}
	fuluBlobs := make([]testBlob, 4)
	for i := range fuluBlobs {
		fuluBlobs[i] = testBlob{blob: &cltypes.Blob{}, commitment: cltypes.KZGCommitment{byte(i + 1)}, proofs: make([]cltypes.KZGProof, goethkzg.CellsPerExtBlob)}
	}

	_, err = fulu.requestBlobBundles(newRequestBlock(fulu.beaconChainCfg, clparams.FuluVersion, 0, fuluBlobs...))

	require.ErrorContains(t, err, "more than 3 blob commitments", "from Fulu on the limit comes from the blob schedule")
}

func TestRequestBlobBundlesRejectsMismatchedCounts(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t, version)
			b := newTestBlob(t, 1, version)

			missingProof := newRequestBlock(h.beaconChainCfg, version, 0, b)
			missingProof.KZGProofs = cltypes.NewDenebSignedBeaconBlock(h.beaconChainCfg, version).KZGProofs
			for i := range len(b.proofs) - 1 {
				missingProof.KZGProofs.Append(&b.proofs[i])
			}
			_, err := h.requestBlobBundles(missingProof)
			require.ErrorContains(t, err, "do not match")

			missingBlob := newRequestBlock(h.beaconChainCfg, version, 0, b)
			extra := cltypes.KZGCommitment{0xc0}
			missingBlob.SignedBlock.Block.Body.BlobKzgCommitments.Append(&extra)
			_, err = h.requestBlobBundles(missingBlob)
			require.ErrorContains(t, err, "do not match")
		})
	}
}

func TestRequestBlobBundlesRejectsInvalidProof(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t, version)
			valid, invalid := newTestBlob(t, 1, version), newTestBlob(t, 2, version)
			invalid.proofs = append([]cltypes.KZGProof(nil), invalid.proofs...)
			invalid.proofs[len(invalid.proofs)-1] = valid.proofs[len(valid.proofs)-1]

			bundles, err := h.requestBlobBundles(newRequestBlock(h.beaconChainCfg, version, 0, valid, invalid))

			require.ErrorContains(t, err, "invalid blob kzg proofs")
			require.Nil(t, bundles)
		})
	}
}

// TestRequestBlobBundlesLeavesMissingBlockToValidation proves a request without a block, such as
// JSON with "signed_block": null, is left to block validation instead of dereferencing nil.
func TestRequestBlobBundlesLeavesMissingBlockToValidation(t *testing.T) {
	h := newRequestBlobsHandler(t, clparams.ElectraVersion)
	b := newTestBlob(t, 1, clparams.ElectraVersion)

	noSignedBlock := newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, b)
	noSignedBlock.SignedBlock = nil
	bundles, err := h.requestBlobBundles(noSignedBlock)
	require.NoError(t, err)
	require.Nil(t, bundles)

	noMessage := newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, 0, b)
	noMessage.SignedBlock.Block = nil
	bundles, err = h.requestBlobBundles(noMessage)
	require.NoError(t, err)
	require.Nil(t, bundles)
}

func TestCollectPublishedPayloadDataReusesCachedCells(t *testing.T) {
	commitment := cltypes.KZGCommitment{1}
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](1, 48)
	commitments.Append(&commitment)
	marker := make([]cltypes.Cell, goethkzg.CellsPerExtBlob)
	for i := range marker {
		marker[i][0] = 0xaa
	}
	bundle := BlobBundle{Commitment: common.Bytes48(commitment), Blob: &cltypes.Blob{}, KzgProofs: make([]common.Bytes48, goethkzg.CellsPerExtBlob), Cells: marker}

	cellsAndProofs, pending, err := collectPublishedPayloadData(commitments, false, func(common.Bytes48) (BlobBundle, bool) { return bundle, true })

	require.NoError(t, err)
	require.False(t, pending)
	require.Len(t, cellsAndProofs, 1)
	require.Equal(t, marker, cellsAndProofs[0].Blobs)
}

// TestBlobBundleCacheSizeHoldsTwoBlocksAtScheduledLimit proves the cache fits two full blocks at
// the highest blob limit in the schedule, so a block cannot evict its own bundles.
func TestBlobBundleCacheSizeHoldsTwoBlocksAtScheduledLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	require.Equal(t, minBlobBundleCacheSize, blobBundleCacheSize(&cfg))

	cfg.BlobSchedule = append(append([]clparams.BlobParameters(nil), cfg.BlobSchedule...), clparams.BlobParameters{Epoch: math.MaxUint64 - 1, MaxBlobsPerBlock: 72})
	require.Equal(t, 144, blobBundleCacheSize(&cfg))
}

type publishedGossip struct {
	mu     sync.Mutex
	topics map[string][]byte
}

func (p *publishedGossip) record(_ context.Context, topic string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics[topic] = data
	return nil
}

// newPublishingHandler returns a handler at forkVersion from epoch 1 whose block service answers
// validation with validationErr and whose gossip records every published message.
func newPublishingHandler(t *testing.T, forkVersion clparams.StateVersion, validationErr error) (*ApiHandler, *publishedGossip) {
	t.Helper()
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	if forkVersion >= clparams.FuluVersion {
		handler.beaconChainCfg.FuluForkEpoch = 1
		handler.beaconChainCfg.InitializeForkSchedule()
	}
	ctrl := gomock.NewController(t)
	blockService := network_services_mock.NewMockBlockService(ctrl)
	blockService.EXPECT().ValidateGossip(gomock.Any(), gomock.Any()).Return(validationErr).AnyTimes()
	blockService.EXPECT().CommitGossipReservation(gomock.Any()).AnyTimes()
	blockService.EXPECT().ReleaseGossipReservation(gomock.Any()).AnyTimes()
	blockService.EXPECT().SchedulePublishedBlockForLaterProcessing(gomock.Any(), gomock.Any()).Return(completedPublishedBlockJob{}).AnyTimes()
	handler.blockService = blockService
	published := &publishedGossip{topics: map[string][]byte{}}
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(published.record).AnyTimes()
	handler.gossipManager = gossipManager
	return handler, published
}

func postBlock(t *testing.T, handler *ApiHandler, block *cltypes.DenebSignedBeaconBlock, headerVersion clparams.StateVersion, asJSON bool) error {
	t.Helper()
	var body []byte
	var err error
	contentType := "application/octet-stream"
	if asJSON {
		body, err = json.Marshal(block)
		contentType = "application/json"
	} else {
		body, err = block.EncodeSSZ(nil)
	}
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Eth-Consensus-Version", headerVersion.String())
	_, err = handler.PostEthV2BeaconBlocks(httptest.NewRecorder(), req)
	return err
}

func requireStatus(t *testing.T, err error, status int) {
	t.Helper()
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, status, endpointErr.Code)
}

// TestPostEthV2BeaconBlocksPublishesRequestBlobsOnNonProducingNode proves a beacon node that did
// not produce a block, and so has no cached blob bundles for it, publishes the block's blob
// sidecars from the blobs sent with the request, in both request encodings.
func TestPostEthV2BeaconBlocksPublishesRequestBlobsOnNonProducingNode(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		t.Run(map[bool]string{false: "ssz", true: "json"}[asJSON], func(t *testing.T) {
			handler, published := newPublishingHandler(t, clparams.ElectraVersion, nil)
			b := newTestBlob(t, 1, clparams.ElectraVersion)
			block := newRequestBlock(handler.beaconChainCfg, clparams.ElectraVersion, handler.beaconChainCfg.SlotsPerEpoch, b)

			require.NoError(t, postBlock(t, handler, block, clparams.ElectraVersion, asJSON))

			require.Contains(t, published.topics, gossip.TopicNameBeaconBlock)
			var sidecar cltypes.BlobSidecar
			require.NoError(t, sidecar.DecodeSSZ(published.topics[gossip.TopicNameBlobSidecar(0)], int(clparams.ElectraVersion)))
			require.Equal(t, *b.blob, sidecar.Blob)
			require.Equal(t, common.Bytes48(b.commitment), sidecar.KzgCommitment)
			require.Equal(t, common.Bytes48(b.proofs[0]), sidecar.KzgProof)
			requireNotCached(t, handler, b)
		})
	}
}

// TestPostEthV2BeaconBlocksPublishesRequestColumnsOnNonProducingNode is the Fulu counterpart: the
// node builds and publishes every data column from the request's blobs and cell proofs.
func TestPostEthV2BeaconBlocksPublishesRequestColumnsOnNonProducingNode(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg)
	}
	handler, published := newPublishingHandler(t, clparams.FuluVersion, nil)
	b := newTestBlob(t, 1, clparams.FuluVersion)
	block := newRequestBlock(handler.beaconChainCfg, clparams.FuluVersion, handler.beaconChainCfg.SlotsPerEpoch, b)

	require.NoError(t, postBlock(t, handler, block, clparams.FuluVersion, false))

	columns := 0
	for topic, data := range published.topics {
		if !strings.HasPrefix(topic, "data_column_sidecar_") {
			continue
		}
		columns++
		sidecar := cltypes.NewDataColumnSidecar()
		require.NoError(t, sidecar.DecodeSSZ(data, int(clparams.FuluVersion)))
		require.True(t, das.VerifyDataColumnSidecarKZGProofs(sidecar), "column %d must carry valid cells and proofs", sidecar.Index)
		// Caplin's decoder tolerates a wrong fixed-part size, other clients do not: the published
		// bytes must be the canonical encoding, with a depth-4 commitments inclusion proof.
		canonical, err := sidecar.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, canonical, data, "column %d must use the spec SSZ layout", sidecar.Index)
	}
	require.Equal(t, int(handler.beaconChainCfg.NumberOfColumns), columns)
	requireNotCached(t, handler, b)
}

type gatedPublishedBlockJob struct {
	waiting chan<- struct{}
	stored  <-chan struct{}
}

func (j gatedPublishedBlockJob) Wait(ctx context.Context) error {
	j.waiting <- struct{}{}
	select {
	case <-j.stored:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestBroadcastGloasBlockPublishesColumnsAfterLocalStore(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	handler, published := newPublishingHandler(t, clparams.FuluVersion, nil)
	handler.beaconChainCfg.GloasForkEpoch = 1
	handler.beaconChainCfg.InitializeForkSchedule()
	waiting, stored := make(chan struct{}, 1), make(chan struct{})
	blockService := network_services_mock.NewMockBlockService(gomock.NewController(t))
	blockService.EXPECT().ValidateGossip(gomock.Any(), gomock.Any()).Return(nil)
	blockService.EXPECT().CommitGossipReservation(gomock.Any())
	blockService.EXPECT().SchedulePublishedBlockForLaterProcessing(gomock.Any(), gomock.Any()).Return(gatedPublishedBlockJob{waiting: waiting, stored: stored})
	handler.blockService = blockService
	b := newTestBlob(t, 1, clparams.FuluVersion)
	proofs := make([]common.Bytes48, len(b.proofs))
	for i := range b.proofs {
		proofs[i] = common.Bytes48(b.proofs[i])
	}
	handler.blobBundles.Add(common.Bytes48(b.commitment), BlobBundle{Commitment: common.Bytes48(b.commitment), Blob: b.blob, KzgProofs: proofs})
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	block.Block.Slot = handler.beaconChainCfg.SlotsPerEpoch
	bid := block.Block.Body.SignedExecutionPayloadBid.Message
	bid.BuilderIndex = clparams.BuilderIndexSelfBuild
	commitment := b.commitment
	bid.BlobKzgCommitments.Append(&commitment)

	done := make(chan error, 1)
	go func() { done <- handler.broadcastBlock(t.Context(), block, BlockPublishingValidationGossip) }()
	columnsPublished := func() int {
		published.mu.Lock()
		defer published.mu.Unlock()
		columns := 0
		for topic := range published.topics {
			if strings.HasPrefix(topic, "data_column_sidecar_") {
				columns++
			}
		}
		return columns
	}
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("broadcast finished without waiting for the local store: %v", err)
	}
	require.Zero(t, columnsPublished())

	close(stored)
	require.NoError(t, <-done)
	require.Equal(t, int(handler.beaconChainCfg.NumberOfColumns), columnsPublished())
}

// TestPostEthV2BeaconBlocksRejectedBlockLeavesNoBlobData proves blobs from a request whose block
// fails validation are neither published nor cached.
func TestPostEthV2BeaconBlocksRejectedBlockLeavesNoBlobData(t *testing.T) {
	handler, published := newPublishingHandler(t, clparams.ElectraVersion, errors.New("block is invalid"))
	b := newTestBlob(t, 1, clparams.ElectraVersion)
	block := newRequestBlock(handler.beaconChainCfg, clparams.ElectraVersion, handler.beaconChainCfg.SlotsPerEpoch, b)

	requireStatus(t, postBlock(t, handler, block, clparams.ElectraVersion, false), http.StatusBadRequest)

	require.Empty(t, published.topics)
	requireNotCached(t, handler, b)
}

// TestPostEthV2BeaconBlocksRejectsBlobsLabelledWithAnotherFork proves a Fulu-slot block labelled
// electra is rejected before any blob work, so its single blob proofs never reach the column build.
func TestPostEthV2BeaconBlocksRejectsBlobsLabelledWithAnotherFork(t *testing.T) {
	handler, published := newPublishingHandler(t, clparams.FuluVersion, nil)
	b := newTestBlob(t, 1, clparams.ElectraVersion)
	block := newRequestBlock(handler.beaconChainCfg, clparams.ElectraVersion, handler.beaconChainCfg.SlotsPerEpoch, b)

	requireStatus(t, postBlock(t, handler, block, clparams.ElectraVersion, false), http.StatusBadRequest)

	require.Empty(t, published.topics)
	requireNotCached(t, handler, b)
}
