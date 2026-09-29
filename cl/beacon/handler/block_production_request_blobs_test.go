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
	"net/http"
	"net/http/httptest"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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

func newRequestBlock(cfg *clparams.BeaconChainConfig, version clparams.StateVersion, blobs ...testBlob) *cltypes.DenebSignedBeaconBlock {
	block := cltypes.NewDenebSignedBeaconBlock(cfg, version)
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

func newRequestBlobsHandler(t *testing.T) *ApiHandler {
	t.Helper()
	blobBundles, err := lru.New[common.Bytes48, BlobBundle]("test-request-blobs", maxBlobBundleCacheSize)
	require.NoError(t, err)
	cfg := clparams.MainnetBeaconConfig
	return &ApiHandler{blobBundles: blobBundles, beaconChainCfg: &cfg}
}

func requireCachedBundle(t *testing.T, h *ApiHandler, b testBlob) {
	t.Helper()
	bundle, ok := h.blobBundles.Get(common.Bytes48(b.commitment))
	require.True(t, ok, "blob bundle must be cached")
	require.Equal(t, common.Bytes48(b.commitment), bundle.Commitment)
	require.Equal(t, *b.blob, *bundle.Blob)
	require.Len(t, bundle.KzgProofs, len(b.proofs))
	for i := range b.proofs {
		require.Equal(t, common.Bytes48(b.proofs[i]), bundle.KzgProofs[i])
	}
}

func TestAddRequestBlobBundlesCachesVerifiedBlobs(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t)
			first, second := newTestBlob(t, 1, version), newTestBlob(t, 2, version)

			require.NoError(t, h.addRequestBlobBundles(newRequestBlock(h.beaconChainCfg, version, first, second)))

			requireCachedBundle(t, h, first)
			requireCachedBundle(t, h, second)
		})
	}
}

// TestAddRequestBlobBundlesKeepsCachedBundles proves the producing node keeps the bundle it got
// from the execution layer: the request's copy of that blob is neither verified nor stored.
func TestAddRequestBlobBundlesKeepsCachedBundles(t *testing.T) {
	h := newRequestBlobsHandler(t)
	b := newTestBlob(t, 1, clparams.ElectraVersion)
	h.blobBundles.Add(common.Bytes48(b.commitment), BlobBundle{
		Commitment: common.Bytes48(b.commitment),
		Blob:       b.blob,
		KzgProofs:  []common.Bytes48{common.Bytes48(b.proofs[0])},
	})
	request := b
	request.proofs = []cltypes.KZGProof{{0xc0}} // would fail verification if it were checked

	require.NoError(t, h.addRequestBlobBundles(newRequestBlock(h.beaconChainCfg, clparams.ElectraVersion, request)))

	requireCachedBundle(t, h, b)
}

func TestAddRequestBlobBundlesRejectsMismatchedCounts(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t)
			b := newTestBlob(t, 1, version)

			missingProof := newRequestBlock(h.beaconChainCfg, version, b)
			missingProof.KZGProofs = cltypes.NewDenebSignedBeaconBlock(h.beaconChainCfg, version).KZGProofs
			for i := range len(b.proofs) - 1 {
				missingProof.KZGProofs.Append(&b.proofs[i])
			}
			require.Error(t, h.addRequestBlobBundles(missingProof))

			missingBlob := newRequestBlock(h.beaconChainCfg, version, b)
			extra := cltypes.KZGCommitment{0xc0}
			missingBlob.SignedBlock.Block.Body.BlobKzgCommitments.Append(&extra)
			require.Error(t, h.addRequestBlobBundles(missingBlob))

			_, ok := h.blobBundles.Get(common.Bytes48(b.commitment))
			require.False(t, ok, "nothing may be cached from a malformed request")
		})
	}
}

// TestAddRequestBlobBundlesRejectsInvalidProofWithoutCaching proves a request with one bad blob
// caches none of its blobs, including the valid ones.
func TestAddRequestBlobBundlesRejectsInvalidProofWithoutCaching(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			h := newRequestBlobsHandler(t)
			valid, invalid := newTestBlob(t, 1, version), newTestBlob(t, 2, version)
			invalid.proofs = append([]cltypes.KZGProof(nil), invalid.proofs...)
			invalid.proofs[len(invalid.proofs)-1] = valid.proofs[len(valid.proofs)-1]

			require.Error(t, h.addRequestBlobBundles(newRequestBlock(h.beaconChainCfg, version, valid, invalid)))

			for _, b := range []testBlob{valid, invalid} {
				_, ok := h.blobBundles.Get(common.Bytes48(b.commitment))
				require.False(t, ok, "nothing may be cached from a request with an invalid proof")
			}
		})
	}
}

// TestPostEthV2BeaconBlocksPublishesRequestBlobsOnNonProducingNode proves a beacon node that did
// not produce a block, and so has no cached blob bundles for it, publishes the block's blob
// sidecars from the blobs sent with the request.
func TestPostEthV2BeaconBlocksPublishesRequestBlobsOnNonProducingNode(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	b := newTestBlob(t, 1, clparams.ElectraVersion)
	block := newRequestBlock(handler.beaconChainCfg, clparams.ElectraVersion, b)
	block.SignedBlock.Block.Slot = 1
	body, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	blockService := network_services_mock.NewMockBlockService(ctrl)
	blockService.EXPECT().ValidateGossip(gomock.Any(), gomock.Any()).Return(nil)
	blockService.EXPECT().CommitGossipReservation(gomock.Any())
	blockService.EXPECT().SchedulePublishedBlockForLaterProcessing(gomock.Any(), gomock.Any()).Return(completedPublishedBlockJob{})
	blockService.EXPECT().ReleaseGossipReservation(gomock.Any()).AnyTimes()
	handler.blockService = blockService
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameBeaconBlock, gomock.Any()).Return(nil)
	var published cltypes.BlobSidecar
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameBlobSidecar(0), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, data []byte) error {
			return published.DecodeSSZ(data, int(clparams.ElectraVersion))
		},
	)
	handler.gossipManager = gossipManager

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Eth-Consensus-Version", clparams.ElectraVersion.String())

	_, err = handler.PostEthV2BeaconBlocks(httptest.NewRecorder(), req)
	require.NoError(t, err)
	require.Equal(t, *b.blob, published.Blob)
	require.Equal(t, common.Bytes48(b.commitment), published.KzgCommitment)
	require.Equal(t, common.Bytes48(b.proofs[0]), published.KzgProof)
}
