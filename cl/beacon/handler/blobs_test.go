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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type frozenBlobSnapshotReader struct {
	frozenBlobsExclusive uint64
	sidecars             []*cltypes.BlobSidecar
	err                  error
}

type blobBackfillStatusStub bool

func (s blobBackfillStatusStub) BlobBackfillPending(uint64) bool { return bool(s) }

type changingBlobBackfillStatus struct{ calls atomic.Int64 }

func (s *changingBlobBackfillStatus) BlobBackfillPending(uint64) bool {
	return s.calls.Add(1) > 1
}

type completingBlobBackfillStatus struct{ calls atomic.Int64 }

func (s *completingBlobBackfillStatus) BlobBackfillPending(uint64) bool {
	return s.calls.Add(1) == 1
}

type partialBlobStorage struct {
	blob_storage.BlobStorage
	sidecars []*cltypes.BlobSidecar
	complete bool
}

type completingBlobStorage struct {
	blob_storage.BlobStorage
	reads    atomic.Int64
	sidecars []*cltypes.BlobSidecar
}

func (s *completingBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	if s.reads.Add(1) == 1 {
		return nil, false, nil
	}
	return s.sidecars, true, nil
}

type advancingFrozenBlobSnapshots struct {
	frozen   atomic.Uint64
	sidecars []*cltypes.BlobSidecar
}

func (s *advancingFrozenBlobSnapshots) FrozenBlobs() uint64 { return s.frozen.Load() }
func (s *advancingFrozenBlobSnapshots) ReadBlobSidecars(uint64) ([]*cltypes.BlobSidecar, error) {
	return s.sidecars, nil
}

type freezingBlobStorage struct {
	blob_storage.BlobStorage
	snapshots       *advancingFrozenBlobSnapshots
	freezeAt        uint64
	storageSidecars []*cltypes.BlobSidecar
}

func (s freezingBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	s.snapshots.frozen.Store(s.freezeAt)
	return s.storageSidecars, false, nil
}

func (s partialBlobStorage) ReadBlobSidecars(context.Context, uint64, common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	return s.sidecars, s.complete, nil
}

type bodyOnlyBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
}

func (r bodyOnlyBlobBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return nil, errors.New("full block unavailable")
}

type unavailableBlobBodyReader struct {
	freezeblocks.BeaconSnapshotReader
}

func (r unavailableBlobBodyReader) ReadBeaconBlockBodyBySlot(context.Context, kv.Tx, uint64) (*cltypes.SignedBeaconBlock, error) {
	return nil, nil
}

type nilCommitmentsBlobBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	block *cltypes.SignedBeaconBlock
}

func (r nilCommitmentsBlobBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return r.block, nil
}

func (r nilCommitmentsBlobBlockReader) ReadBeaconBlockBodyBySlot(context.Context, kv.Tx, uint64) (*cltypes.SignedBeaconBlock, error) {
	return r.block, nil
}

func (r frozenBlobSnapshotReader) FrozenBlobs() uint64 { return r.frozenBlobsExclusive }
func (r frozenBlobSnapshotReader) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.sidecars, nil
}

type blobsTestFixture struct {
	handler       *ApiHandler
	fcu           *forkchoicemock.ForkChoiceStorageMock
	slot          uint64
	blockRoot     common.Hash
	versionedHash common.Hash
	sidecars      []*cltypes.BlobSidecar
}

func TestGetBlobsFromFrozenSnapshots(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars:             f.sidecars,
	}

	out := getBeaconBlobs(t, f)

	require.Len(t, out.Data, 1)
	require.True(t, strings.HasPrefix(out.Data[0], "0x02"))
}

func TestGetBlobsEmptyWhenFrozenSidecarsMissing(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	out := getBeaconBlobs(t, f)

	require.Empty(t, out.Data)
}

func TestGetBlobsUnavailableWhileBackfillInProgress(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp := requestBeaconBlobs(t, server.URL, f)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestGetBlobsUnavailableAtFirstUnfrozenSlotDuringBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot}

	statusCode := getBeaconBlobsStatus(t, f)

	require.Equal(t, http.StatusServiceUnavailable, statusCode)
}

func TestBlobSidecarsBackfillNotPendingForNonCanonicalBlock(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	pending := f.handler.blobBackfillPending(f.slot, f.blockRoot, common.HexToHash("0x01"))

	require.False(t, pending)
}

func TestGetBlobSidecarsUnavailableWhileBackfillInProgress(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var endpointError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&endpointError))
	require.Equal(t, http.StatusServiceUnavailable, endpointError.Code)
	require.Equal(t, "blob sidecars are still being backfilled", endpointError.Message)
}

func TestGetBlobSidecarsReturnsRequestedStoredIndexFromIncompleteBlock(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1]}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10) + "?indices=0")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var envelope struct {
		Data []struct {
			Index string `json:"index"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Len(t, envelope.Data, 1)
	require.Equal(t, "0", envelope.Data[0].Index)
}

func TestGetBlobSidecarsUnavailableForIncompleteCanonicalStorageAfterBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(false)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1]}

	for _, tc := range []struct {
		name       string
		query      string
		statusCode int
	}{
		{name: "all", statusCode: http.StatusServiceUnavailable},
		{name: "missing index", query: "?indices=1", statusCode: http.StatusServiceUnavailable},
		{name: "stored index", query: "?indices=0", statusCode: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.statusCode, getBlobSidecarsStatus(t, f, tc.query))
		})
	}
}

func TestGetBlobSidecarsDoesNotTrustTooSmallCompleteSet(t *testing.T) {
	testCases := []struct {
		name       string
		query      string
		statusCode int
	}{
		{name: "all", statusCode: http.StatusServiceUnavailable},
		{name: "missing index", query: "?indices=1", statusCode: http.StatusServiceUnavailable},
		{name: "available index", query: "?indices=0", statusCode: http.StatusOK},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := setupBlobsTest(t)
			f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
			f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1], complete: true}

			require.Equal(t, tc.statusCode, getBlobSidecarsStatus(t, f, tc.query))
		})
	}
}

func TestCanonicalCompleteStorageRejectsDuplicateBlobIndex(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(false)
	f.handler.blobStoage = partialBlobStorage{
		BlobStorage: f.handler.blobStoage,
		sidecars:    []*cltypes.BlobSidecar{f.sidecars[0], f.sidecars[0]},
		complete:    true,
	}

	require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, ""))
	require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, "?indices=1"))
	require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatusWithQuery(t, f, ""))
	require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatusWithQuery(t, f, "?versioned_hashes="+f.versionedHash.Hex()))
}

func TestCanonicalCompleteStorageRejectsWrongBlobIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*cltypes.BlobSidecar)
	}{
		{
			name: "block root",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				header := *sidecar.SignedBlockHeader.Header
				header.ParentRoot[0] ^= 0xff
				signedHeader := *sidecar.SignedBlockHeader
				signedHeader.Header = &header
				sidecar.SignedBlockHeader = &signedHeader
			},
		},
		{
			name: "commitment",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.KzgCommitment[0] ^= 0xff
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupBlobsTest(t)
			invalid := *f.sidecars[1]
			tc.mutate(&invalid)
			f.handler.blobBackfillStatus = blobBackfillStatusStub(false)
			f.handler.blobStoage = partialBlobStorage{
				BlobStorage: f.handler.blobStoage,
				sidecars:    []*cltypes.BlobSidecar{f.sidecars[0], &invalid},
				complete:    true,
			}

			require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, ""))
			require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, "?indices=1"))
			require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatusWithQuery(t, f, ""))
			require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatusWithQuery(t, f, "?versioned_hashes="+f.versionedHash.Hex()))
		})
	}
}

func TestCanonicalCompleteStorageRejectsInvalidBlobCryptography(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*cltypes.BlobSidecar)
	}{
		{
			name: "signature",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				signedHeader := *sidecar.SignedBlockHeader
				signedHeader.Signature[0] ^= 0xff
				sidecar.SignedBlockHeader = &signedHeader
			},
		},
		{
			name: "blob",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.Blob[0] ^= 0xff
			},
		},
		{
			name: "proof",
			mutate: func(sidecar *cltypes.BlobSidecar) {
				sidecar.KzgProof[0] ^= 0xff
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupBlobsTest(t)
			invalid := *f.sidecars[1]
			tc.mutate(&invalid)
			f.handler.blobBackfillStatus = blobBackfillStatusStub(false)
			f.handler.blobStoage = partialBlobStorage{
				BlobStorage: f.handler.blobStoage,
				sidecars:    []*cltypes.BlobSidecar{f.sidecars[0], &invalid},
				complete:    true,
			}

			require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, "?indices=1"))
			require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatusWithQuery(t, f, "?versioned_hashes="+f.versionedHash.Hex()))
		})
	}
}

func TestSideBranchCompleteStorageFiltersInvalidBlobSidecars(t *testing.T) {
	f := setupBlobsTest(t)
	invalid := *f.sidecars[1]
	signedHeader := *invalid.SignedBlockHeader
	signedHeader.Signature[0] ^= 0xff
	invalid.SignedBlockHeader = &signedHeader
	f.handler.blobStoage = partialBlobStorage{
		BlobStorage: f.handler.blobStoage,
		sidecars:    []*cltypes.BlobSidecar{f.sidecars[0], &invalid},
		complete:    true,
	}
	tx, err := f.handler.indiciesDB.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, f.slot, common.Hash{0xff}))
	require.NoError(t, tx.Commit())

	require.Equal(t, 1, getBlobSidecarsDataLenByID(t, f.handler, f.blockRoot.Hex()))
	require.Equal(t, 1, getBeaconBlobsDataLenByID(t, f.handler, f.blockRoot.Hex()))
}

func TestFrozenSnapshotFiltersDuplicateAndInvalidBlobSidecars(t *testing.T) {
	f := setupBlobsTest(t)
	invalid := *f.sidecars[1]
	signedHeader := *invalid.SignedBlockHeader
	signedHeader.Signature[0] ^= 0xff
	invalid.SignedBlockHeader = &signedHeader
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars:             []*cltypes.BlobSidecar{f.sidecars[0], f.sidecars[0], &invalid},
	}

	require.Equal(t, 1, getBlobSidecarsDataLenByID(t, f.handler, strconv.FormatUint(f.slot, 10)))
	require.Equal(t, 1, getBeaconBlobsDataLenByID(t, f.handler, strconv.FormatUint(f.slot, 10)))
}

func TestBlobStorageValidationSurvivesFrozenBoundaryAdvance(t *testing.T) {
	f := setupBlobsTest(t)
	invalid := *f.sidecars[1]
	signedHeader := *invalid.SignedBlockHeader
	signedHeader.Signature[0] ^= 0xff
	invalid.SignedBlockHeader = &signedHeader
	snapshots := &advancingFrozenBlobSnapshots{sidecars: []*cltypes.BlobSidecar{f.sidecars[0], &invalid}}
	snapshots.frozen.Store(f.slot)
	f.handler.caplinSnapshots = snapshots
	f.handler.blobStoage = freezingBlobStorage{BlobStorage: f.handler.blobStoage, snapshots: snapshots, freezeAt: f.slot + 1}

	require.Equal(t, 1, getBlobSidecarsDataLenByID(t, f.handler, strconv.FormatUint(f.slot, 10)))
}

func TestCanonicalGloasStorageAcceptsSidecarWithoutInclusionProof(t *testing.T) {
	blob := cltypes.Blob{1}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blob), commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = 100
	blockCommitment := cltypes.KZGCommitment(commitment)
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(&blockCommitment)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	sidecar := cltypes.NewBlobSidecar(
		0,
		&blob,
		common.Bytes48(commitment),
		common.Bytes48(proof),
		block.SignedBeaconBlockHeader(),
		solid.NewHashVector(cltypes.CommitmentBranchSize),
	)

	valid, complete, err := validateBlobSidecars(block, blockRoot, []*cltypes.BlobSidecar{sidecar})

	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []*cltypes.BlobSidecar{sidecar}, valid)
}

func TestPreDenebCanonicalBlockDoesNotServeStrayStoredSidecar(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	block := blocks[0]
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	slot := block.Block.Slot
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, blockRoot, slot))
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, blockRoot))
	require.NoError(t, tx.Commit())
	handler.blobStoage = partialBlobStorage{
		BlobStorage: handler.blobStoage,
		sidecars:    []*cltypes.BlobSidecar{{Index: 0, Blob: cltypes.Blob{1}}},
		complete:    true,
	}
	f := blobsTestFixture{handler: handler, fcu: fcu, slot: slot, blockRoot: blockRoot}

	require.Equal(t, http.StatusOK, getBlobSidecarsStatus(t, f, ""))
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(slot, 10))
	require.NoError(t, err)
	defer resp.Body.Close()
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.Empty(t, envelope.Data)
}

func getBlobSidecarsDataLenByID(t *testing.T, handler *ApiHandler, blockID string) int {
	t.Helper()
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + blockID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	return len(envelope.Data)
}

func getBeaconBlobsDataLenByID(t *testing.T, handler *ApiHandler, blockID string) int {
	t.Helper()
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blobs/" + blockID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var envelope beaconBlobsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	return len(envelope.Data)
}

func TestGetBlobSidecarsUnavailableForRequestedMissingIndex(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1]}

	require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, "?indices=1"))
}

func TestGetBlobSidecarsUnavailableWhenAnyRequestedIndexIsMissing(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1]}

	require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, "?indices=0&indices=1"))
}

func TestGetBlobSidecarsBackfillStatusDoesNotRequireFullBlock(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blockReader = bodyOnlyBlobBlockReader{BeaconSnapshotReader: f.handler.blockReader}

	require.Equal(t, http.StatusServiceUnavailable, getBlobSidecarsStatus(t, f, ""))
}

func TestGetBlobSidecarsErrorsWhenCanonicalBodyIsUnavailable(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blockReader = unavailableBlobBodyReader{BeaconSnapshotReader: f.handler.blockReader}

	require.Equal(t, http.StatusInternalServerError, getBlobSidecarsStatus(t, f, ""))
}

func TestRequestedBlobSidecarsMissingTreatsZeroCanonicalRootAsEmptySlot(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blockReader = unavailableBlobBodyReader{BeaconSnapshotReader: f.handler.blockReader}
	tx, err := f.handler.indiciesDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	missing, err := f.handler.requestedBlobSidecarsMissing(t.Context(), tx, f.slot+1, nil, true, nil)

	require.NoError(t, err)
	require.False(t, missing)
}

func TestGetBlobSidecarsEmptyForBlockWithoutCommitmentsDuringBackfill(t *testing.T) {
	f := setupBlobsTestWithoutCommitments(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	require.Equal(t, http.StatusOK, getBlobSidecarsStatus(t, f, ""))
}

func TestGetBlobSidecarsRejectsNilCommitmentsDuringBackfill(t *testing.T) {
	f := setupBlobsTestWithoutCommitments(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	block, err := f.handler.blockReader.ReadBlockByRoot(t.Context(), nil, f.blockRoot)
	require.NoError(t, err)
	block.Block.Body.BlobKzgCommitments = nil
	f.handler.blockReader = nilCommitmentsBlobBlockReader{BeaconSnapshotReader: f.handler.blockReader, block: block}

	require.Equal(t, http.StatusInternalServerError, getBlobSidecarsStatus(t, f, ""))
}

func TestGetBlobSidecarsEmptyForUnmatchedIndexDuringBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	require.Equal(t, http.StatusOK, getBlobSidecarsStatus(t, f, "?indices=99"))
}

func TestGetBlobSidecarsRejectsMalformedIndexWhileSidecarsMissing(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	require.Equal(t, http.StatusBadRequest, getBlobSidecarsStatus(t, f, "?indices=abc"))
}

func TestGetBlobSidecarsRejectsEmptyIndex(t *testing.T) {
	f := setupBlobsTest(t)

	require.Equal(t, http.StatusBadRequest, getBlobSidecarsStatus(t, f, "?indices="))
}

func TestGetBlobSidecarsRejectsDuplicateIndices(t *testing.T) {
	f := setupBlobsTest(t)

	require.Equal(t, http.StatusBadRequest, getBlobSidecarsStatus(t, f, "?indices=0&indices=0"))
}

func TestReadBlobSidecarsRechecksPendingAfterIncompleteReread(t *testing.T) {
	f := setupBlobsTest(t)
	status := &changingBlobBackfillStatus{}
	f.handler.blobBackfillStatus = status
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage}

	_, complete, pending, err := f.handler.readBlobSidecarsWithBackfillStatus(t.Context(), f.slot, f.blockRoot, f.blockRoot)
	require.NoError(t, err)
	require.False(t, complete)
	require.True(t, pending)
	require.Equal(t, int64(2), status.calls.Load())
}

func TestGetBlobSidecarsReturnsUnavailableWithoutRereadAfterBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	storage := &completingBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars}
	f.handler.blobStoage = storage
	f.handler.blobBackfillStatus = blobBackfillStatusStub(false)

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10) + "?indices=0")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, int64(1), storage.reads.Load())
}

func TestReadBlobSidecarsRereadsWhenBackfillCompletesDuringRead(t *testing.T) {
	f := setupBlobsTest(t)
	status := &completingBlobBackfillStatus{}
	storage := &completingBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars}
	f.handler.blobBackfillStatus = status
	f.handler.blobStoage = storage

	out, complete, pending, err := f.handler.readBlobSidecarsWithBackfillStatus(t.Context(), f.slot, f.blockRoot, f.blockRoot)

	require.NoError(t, err)
	require.True(t, complete)
	require.False(t, pending)
	require.Equal(t, f.sidecars, out)
	require.Equal(t, int64(2), storage.reads.Load())
}

func TestGetBlobSidecarsFrozenMissingDuringBackfillKeepsExistingResponse(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	require.Equal(t, http.StatusOK, getBlobSidecarsStatus(t, f, ""))
}

func TestGetBlobsUnavailableWhenStorageIsIncompleteAfterBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(false)

	require.Equal(t, http.StatusServiceUnavailable, getBeaconBlobsStatus(t, f))
}

func TestGetBlobsReturnsStoredDataDuringBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	require.NoError(t, f.handler.blobStoage.WriteBlobSidecars(t.Context(), f.blockRoot, f.sidecars))

	out := getBeaconBlobs(t, f)

	require.Len(t, out.Data, 1)
}

func TestGetBlobsReturnsRequestedStoredDataFromIncompleteBlock(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[1:]}

	out := getBeaconBlobs(t, f)

	require.Len(t, out.Data, 1)
}

func TestGetBlobsDoesNotReturnIncompleteCanonicalStorageAfterBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(false)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[:1]}
	storedHash, err := utils.KzgCommitmentToVersionedHash(f.sidecars[0].KzgCommitment)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		query      string
		statusCode int
	}{
		{name: "all", statusCode: http.StatusServiceUnavailable},
		{name: "missing hash", query: "?versioned_hashes=" + f.versionedHash.Hex(), statusCode: http.StatusServiceUnavailable},
		{name: "stored hash", query: "?versioned_hashes=" + storedHash.Hex(), statusCode: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.statusCode, getBeaconBlobsStatusWithQuery(t, f, tc.query))
		})
	}
}

func TestGetBlobsUnavailableWhenAnyRequestedHashIsMissing(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.handler.blobStoage = partialBlobStorage{BlobStorage: f.handler.blobStoage, sidecars: f.sidecars[1:]}
	firstHash, err := utils.KzgCommitmentToVersionedHash(f.sidecars[0].KzgCommitment)
	require.NoError(t, err)

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()
	url := server.URL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(f.slot, 10) + "?versioned_hashes=" + firstHash.Hex() + "&versioned_hashes=" + f.versionedHash.Hex()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestGetBlobsEmptyForUnmatchedHashDuringBackfill(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	f.versionedHash = common.HexToHash("0xffff")

	out := getBeaconBlobs(t, f)

	require.Empty(t, out.Data)
}

func TestGetBlobsRejectsMalformedVersionedHash(t *testing.T) {
	f := setupBlobsTest(t)

	require.Equal(t, http.StatusBadRequest, getBeaconBlobsStatusWithQuery(t, f, "?versioned_hashes=0x01"))
}

func TestGetBlobsRejectsDuplicateVersionedHash(t *testing.T) {
	f := setupBlobsTest(t)
	hash := f.versionedHash.Hex()

	require.Equal(t, http.StatusBadRequest, getBeaconBlobsStatusWithQuery(t, f, "?versioned_hashes="+hash+"&versioned_hashes="+hash))
}

func TestGetBlobsOrdersResponseByBlockCommitments(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1, sidecars: f.sidecars}
	firstHash, err := utils.KzgCommitmentToVersionedHash(f.sidecars[0].KzgCommitment)
	require.NoError(t, err)

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()
	query := "?versioned_hashes=" + f.versionedHash.Hex() + "&versioned_hashes=" + firstHash.Hex()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(f.slot, 10) + query)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out beaconBlobsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Data, 2)
	require.True(t, strings.HasPrefix(out.Data[0], "0x01"))
	require.True(t, strings.HasPrefix(out.Data[1], "0x02"))
}

func TestGetBlobsEmptyForBlockWithoutCommitmentsDuringBackfill(t *testing.T) {
	f := setupBlobsTestWithoutCommitments(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)

	out := getBeaconBlobs(t, f)

	require.Empty(t, out.Data)
}

func TestGetBlobsRejectsNilCommitmentsDuringBackfill(t *testing.T) {
	f := setupBlobsTestWithoutCommitments(t)
	f.handler.blobBackfillStatus = blobBackfillStatusStub(true)
	block, err := f.handler.blockReader.ReadBlockByRoot(t.Context(), nil, f.blockRoot)
	require.NoError(t, err)
	block.Block.Body.BlobKzgCommitments = nil
	f.handler.blockReader = nilCommitmentsBlobBlockReader{BeaconSnapshotReader: f.handler.blockReader, block: block}

	require.Equal(t, http.StatusInternalServerError, getBeaconBlobsStatus(t, f))
}

func TestGetBlobsErrorsWhenFrozenSnapshotReadFails(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		err:                  errors.New("snapshot read failed"),
	}

	statusCode := getBeaconBlobsStatus(t, f)

	require.Equal(t, http.StatusInternalServerError, statusCode)
}

func TestReadBlobSidecarsAtFirstUnfrozenSlotFromStorage(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot,
		sidecars:             []*cltypes.BlobSidecar{{Index: 99}},
	}
	require.NoError(t, f.handler.blobStoage.WriteBlobSidecars(t.Context(), f.blockRoot, f.sidecars))

	out, found, err := f.handler.readBlobSidecars(t.Context(), f.slot, f.blockRoot, f.blockRoot)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, f.sidecars, out)
}

func TestReadBlobSidecarsRetriesSnapshotWhenFrozenBoundaryAdvances(t *testing.T) {
	f := setupBlobsTest(t)
	snapshots := &advancingFrozenBlobSnapshots{sidecars: f.sidecars}
	snapshots.frozen.Store(f.slot)
	f.handler.caplinSnapshots = snapshots
	f.handler.blobStoage = freezingBlobStorage{BlobStorage: f.handler.blobStoage, snapshots: snapshots, freezeAt: f.slot + 1}

	out, found, err := f.handler.readBlobSidecars(t.Context(), f.slot, f.blockRoot, f.blockRoot)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, f.sidecars, out)
}

func TestReadBlobSidecarsRetainsPartialStorageWhenAdvancedSnapshotIsEmpty(t *testing.T) {
	f := setupBlobsTest(t)
	snapshots := &advancingFrozenBlobSnapshots{}
	snapshots.frozen.Store(f.slot)
	f.handler.caplinSnapshots = snapshots
	f.handler.blobStoage = freezingBlobStorage{
		BlobStorage:     f.handler.blobStoage,
		snapshots:       snapshots,
		freezeAt:        f.slot + 1,
		storageSidecars: f.sidecars[:1],
	}

	out, found, err := f.handler.readBlobSidecars(t.Context(), f.slot, f.blockRoot, f.blockRoot)

	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, f.sidecars[:1], out)
}

func TestGetBlobSidecarsRetainsPartialStorageWhenBackfillCompletesIntoEmptySnapshot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		query      string
		statusCode int
		dataLen    int
	}{
		{name: "stored member", query: "?indices=0", statusCode: http.StatusOK, dataLen: 1},
		{name: "missing member", query: "?indices=1", statusCode: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupBlobsTest(t)
			snapshots := &advancingFrozenBlobSnapshots{}
			snapshots.frozen.Store(f.slot)
			f.handler.caplinSnapshots = snapshots
			f.handler.blobBackfillStatus = &completingBlobBackfillStatus{}
			f.handler.blobStoage = freezingBlobStorage{
				BlobStorage:     f.handler.blobStoage,
				snapshots:       snapshots,
				freezeAt:        f.slot + 1,
				storageSidecars: f.sidecars[:1],
			}

			server := httptest.NewServer(f.handler.mux)
			defer server.Close()
			resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10) + tc.query)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.statusCode, resp.StatusCode)
			if tc.statusCode == http.StatusOK {
				var envelope struct {
					Data []json.RawMessage `json:"data"`
				}
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
				require.Len(t, envelope.Data, tc.dataLen)
			}
		})
	}
}

type beaconBlobsResponse struct {
	Data []string `json:"data"`
}

func getBeaconBlobs(t *testing.T, f blobsTestFixture) beaconBlobsResponse {
	t.Helper()

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp := requestBeaconBlobs(t, server.URL, f)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out beaconBlobsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func getBeaconBlobsStatus(t *testing.T, f blobsTestFixture) int {
	t.Helper()

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp := requestBeaconBlobs(t, server.URL, f)
	defer resp.Body.Close()
	return resp.StatusCode
}

func getBeaconBlobsStatusWithQuery(t *testing.T, f blobsTestFixture, query string) int {
	t.Helper()
	server := httptest.NewServer(f.handler.mux)
	defer server.Close()
	resp, err := http.Get(server.URL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(f.slot, 10) + query)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func requestBeaconBlobs(t *testing.T, baseURL string, f blobsTestFixture) *http.Response {
	t.Helper()

	resp, err := http.Get(baseURL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(f.slot, 10) + "?versioned_hashes=" + f.versionedHash.Hex())
	require.NoError(t, err)
	return resp
}

func getBlobSidecarsStatus(t *testing.T, f blobsTestFixture, query string) int {
	t.Helper()

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10) + query)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func setupBlobsTest(t *testing.T) blobsTestFixture {
	t.Helper()
	return setupBlobsTestWithCommitments(t, make([]cltypes.KZGCommitment, 2))
}

func setupBlobsTestWithoutCommitments(t *testing.T) blobsTestFixture {
	t.Helper()
	return setupBlobsTestWithCommitments(t, nil)
}

func setupBlobsTestWithCommitments(t *testing.T, commitments []cltypes.KZGCommitment) blobsTestFixture {
	t.Helper()

	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	block := blocks[0]
	slot := block.Block.Slot

	block.Block.Body.BlobKzgCommitments.Clear()
	blobs := make([]cltypes.Blob, len(commitments))
	proofs := make([]common.Bytes48, len(commitments))
	for i := range commitments {
		blobs[i][0] = byte(i + 1)
		commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blobs[i]), 0)
		require.NoError(t, err)
		proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blobs[i]), commitment, 0)
		require.NoError(t, err)
		commitments[i] = cltypes.KZGCommitment(commitment)
		proofs[i] = common.Bytes48(proof)
		block.Block.Body.BlobKzgCommitments.Append(&commitments[i])
	}
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	var versionedHash common.Hash
	if len(commitments) > 1 {
		versionedHash, err = utils.KzgCommitmentToVersionedHash(common.Bytes48(commitments[1]))
		require.NoError(t, err)
	}
	sidecars := make([]*cltypes.BlobSidecar, len(commitments))
	for i := range commitments {
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for branchIndex, hash := range branch {
			inclusionProof.Set(branchIndex, hash)
		}
		sidecars[i] = cltypes.NewBlobSidecar(
			uint64(i),
			&blobs[i],
			common.Bytes48(commitments[i]),
			proofs[i],
			block.SignedBeaconBlockHeader(),
			inclusionProof,
		)
	}

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, blockRoot, slot))
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, blockRoot))
	require.NoError(t, tx.Commit())

	return blobsTestFixture{
		handler:       handler,
		fcu:           fcu,
		slot:          slot,
		blockRoot:     blockRoot,
		versionedHash: versionedHash,
		sidecars:      sidecars,
	}
}

type blobSidecarsEnvelope struct {
	Version             *string         `json:"version"`
	ExecutionOptimistic *bool           `json:"execution_optimistic"`
	Finalized           *bool           `json:"finalized"`
	Data                json.RawMessage `json:"data"`
}

func TestBlobSidecarsResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	f.fcu.IsRootOptimisticVal = true
	f.fcu.FinalizedSlotVal = f.slot - 1

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars: []*cltypes.BlobSidecar{
			{Index: 0, Blob: cltypes.Blob{1}},
		},
	}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var envelope blobSidecarsEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))

	require.NotNil(t, envelope.Version, "response must include 'version'")
	require.NotNil(t, envelope.ExecutionOptimistic, "response must include 'execution_optimistic'")
	require.NotNil(t, envelope.Finalized, "response must include 'finalized'")
	require.NotNil(t, envelope.Data, "response must include 'data'")

	require.Equal(t, "electra", *envelope.Version)
	require.True(t, *envelope.ExecutionOptimistic, "execution_optimistic must be true")
	require.False(t, *envelope.Finalized, "finalized must be false")
}

func TestBlobSidecarsEmptyResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	f.fcu.IsRootOptimisticVal = false
	f.fcu.FinalizedSlotVal = f.slot + 1

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var envelope blobSidecarsEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))

	require.NotNil(t, envelope.Version, "empty response must include 'version'")
	require.NotNil(t, envelope.ExecutionOptimistic, "empty response must include 'execution_optimistic'")
	require.NotNil(t, envelope.Finalized, "empty response must include 'finalized'")
	require.NotNil(t, envelope.Data, "empty response must include 'data'")

	require.Equal(t, "electra", *envelope.Version)
	require.False(t, *envelope.ExecutionOptimistic, "execution_optimistic must be false")
	require.True(t, *envelope.Finalized, "finalized must be true")
}
