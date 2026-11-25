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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	forkchoicemock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type frozenBlobSnapshotReader struct {
	frozenBlobsExclusive uint64
	sidecars             []*cltypes.BlobSidecar
	err                  error
}

func (r frozenBlobSnapshotReader) FrozenBlobs() uint64 { return r.frozenBlobsExclusive }
func (r frozenBlobSnapshotReader) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.sidecars, nil
}

type blobsTestFixture struct {
	handler         *ApiHandler
	fcu             *forkchoicemock.ForkChoiceStorageMock
	slot            uint64
	versionedHashes []common.Hash
}

func TestGetBlobsFromFrozenSnapshots(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars: []*cltypes.BlobSidecar{
			{Index: 0, Blob: cltypes.Blob{1}},
			{Index: 1, Blob: cltypes.Blob{2}},
		},
	}

	out := getBeaconBlobs(t, f, "versioned_hashes="+f.versionedHashes[1].Hex())

	require.Len(t, out.Data, 1)
	require.True(t, strings.HasPrefix(out.Data[0], "0x02"))
}

func TestGetBlobsEmptyWhenFrozenSidecarsMissing(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	out := getBeaconBlobs(t, f, "versioned_hashes="+f.versionedHashes[1].Hex())

	require.Empty(t, out.Data)
}

func TestGetBlobsErrorsWhenFrozenSnapshotReadFails(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		err:                  errors.New("snapshot read failed"),
	}

	resp := requestBlobEndpoint(t, f, "blobs", "versioned_hashes="+f.versionedHashes[1].Hex())
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestGetBlobsReturnsCommitmentOrder(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars: []*cltypes.BlobSidecar{
			{Index: 0, Blob: cltypes.Blob{1}},
			{Index: 1, Blob: cltypes.Blob{2}},
			{Index: 2, Blob: cltypes.Blob{3}},
		},
	}

	out := getBeaconBlobs(t, f, "versioned_hashes="+f.versionedHashes[2].Hex()+","+f.versionedHashes[0].Hex())

	require.Len(t, out.Data, 2)
	require.True(t, strings.HasPrefix(out.Data[0], "0x01"))
	require.True(t, strings.HasPrefix(out.Data[1], "0x03"))
}

func TestGetBlobsIgnoresUnknownVersionedHash(t *testing.T) {
	f := setupBlobsTest(t)
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars:             []*cltypes.BlobSidecar{{Index: 0}, {Index: 1}, {Index: 2}},
	}

	out := getBeaconBlobs(t, f, "versioned_hashes="+common.Hash{0x42}.Hex())

	require.Empty(t, out.Data)
}

func TestBlobEndpointsRejectInvalidSelector(t *testing.T) {
	f := setupBlobsTest(t)
	hash := f.versionedHashes[0].Hex()

	for name, tc := range map[string]struct{ endpoint, query string }{
		"blobs duplicate hash":      {"blobs", "versioned_hashes=" + hash + "," + hash},
		"blobs duplicate hash case": {"blobs", "versioned_hashes=" + hash + ",0x" + strings.ToUpper(hash[2:])},
		"blobs empty hash":          {"blobs", "versioned_hashes="},
		"blobs short hash":          {"blobs", "versioned_hashes=" + hash[:len(hash)-2]},
		"blobs no 0x prefix":        {"blobs", "versioned_hashes=" + hash[2:]},
		"blobs 0X prefix":           {"blobs", "versioned_hashes=0X" + hash[2:]},
		"blobs non-hex hash":        {"blobs", "versioned_hashes=0x" + strings.Repeat("zz", 32)},
		"blobs long hash":           {"blobs", "versioned_hashes=" + hash + "00"},
		"blobs odd length hash":     {"blobs", "versioned_hashes=" + hash + "0"},
		"blobs one bad of two":      {"blobs", "versioned_hashes=" + hash + ",0x1234"},
		"sidecars duplicate index":  {"blob_sidecars", "indices=1,1"},
		"sidecars malformed index":  {"blob_sidecars", "indices=abc"},
		"sidecars negative index":   {"blob_sidecars", "indices=-1"},
		"sidecars empty index":      {"blob_sidecars", "indices="},
	} {
		for storage, sidecars := range map[string][]*cltypes.BlobSidecar{
			"no stored sidecars": nil,
			"stored sidecars":    {{Index: 0}, {Index: 1}, {Index: 2}},
		} {
			t.Run(name+"/"+storage, func(t *testing.T) {
				f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1, sidecars: sidecars}
				resp := requestBlobEndpoint(t, f, tc.endpoint, tc.query)
				defer resp.Body.Close()
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			})
		}
	}
}

func TestBlobEndpointsValidateSelectorBeforeBlockLookup(t *testing.T) {
	f := setupBlobsTest(t)
	unknownBlockRoot := common.Hash{0x42}.Hex()

	for endpoint, query := range map[string]string{
		"blobs":         "versioned_hashes=0x1234",
		"blob_sidecars": "indices=abc",
	} {
		t.Run(endpoint, func(t *testing.T) {
			resp := requestBlobEndpointForBlock(t, f, endpoint, unknownBlockRoot, query)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func requestBlobEndpoint(t *testing.T, f blobsTestFixture, endpoint, query string) *http.Response {
	t.Helper()
	return requestBlobEndpointForBlock(t, f, endpoint, strconv.FormatUint(f.slot, 10), query)
}

func requestBlobEndpointForBlock(t *testing.T, f blobsTestFixture, endpoint, blockID, query string) *http.Response {
	t.Helper()

	server := httptest.NewServer(f.handler.mux)
	t.Cleanup(server.Close)

	req, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/"+endpoint+"/"+blockID+"?"+query, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

type beaconBlobsResponse struct {
	Data []string `json:"data"`
}

func getBeaconBlobs(t *testing.T, f blobsTestFixture, query string) beaconBlobsResponse {
	t.Helper()

	resp := requestBlobEndpoint(t, f, "blobs", query)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out beaconBlobsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func setupBlobsTest(t *testing.T) blobsTestFixture {
	t.Helper()
	return setupBlobsTestWithCommitments(t, []cltypes.KZGCommitment{{69}, {1}, {2}})
}

func setupBlobsTestWithCommitments(t *testing.T, commitments []cltypes.KZGCommitment) blobsTestFixture {
	t.Helper()

	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	block := blocks[0]
	slot := block.Block.Slot

	block.Block.Body.BlobKzgCommitments.Clear()
	versionedHashes := make([]common.Hash, len(commitments))
	for i := range commitments {
		block.Block.Body.BlobKzgCommitments.Append(&commitments[i])
		var err error
		versionedHashes[i], err = utils.KzgCommitmentToVersionedHash(common.Bytes48(commitments[i]))
		require.NoError(t, err)
	}
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, blockRoot, slot))
	require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, blockRoot))
	require.NoError(t, tx.Commit())

	return blobsTestFixture{
		handler:         handler,
		fcu:             fcu,
		slot:            slot,
		versionedHashes: versionedHashes,
	}
}

// blobSidecarsEnvelope captures the full response shape for blob_sidecars.
type blobSidecarsEnvelope struct {
	Version             *string         `json:"version"`
	ExecutionOptimistic *bool           `json:"execution_optimistic"`
	Finalized           *bool           `json:"finalized"`
	Data                json.RawMessage `json:"data"`
}

// TestBlobSidecarsResponseEnvelope verifies that GET /eth/v1/beacon/blob_sidecars/{block_id}
// returns the required envelope fields (version, execution_optimistic, finalized) per the
// Beacon API specification. This test checks the non-finalized and optimistic state.
func TestBlobSidecarsResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	// Set up forkchoice mock to simulate non-finalized and optimistic block.
	f.fcu.IsRootOptimisticVal = true
	f.fcu.FinalizedSlotVal = f.slot - 1

	// Set up frozen snapshots with actual blob data.
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		sidecars: []*cltypes.BlobSidecar{
			{Index: 0, Blob: cltypes.Blob{1}},
		},
	}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	req, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/blob_sidecars/"+strconv.FormatUint(f.slot, 10), nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var envelope blobSidecarsEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))

	// All three envelope fields must be present (non-nil pointers).
	require.NotNil(t, envelope.Version, "response must include 'version'")
	require.NotNil(t, envelope.ExecutionOptimistic, "response must include 'execution_optimistic'")
	require.NotNil(t, envelope.Finalized, "response must include 'finalized'")
	require.NotNil(t, envelope.Data, "response must include 'data'")

	// Verify values.
	require.Equal(t, "electra", *envelope.Version)
	require.True(t, *envelope.ExecutionOptimistic, "execution_optimistic must be true")
	require.False(t, *envelope.Finalized, "finalized must be false")
}

// TestBlobSidecarsEmptyResponseEnvelope verifies that even when no blobs are found,
// the response envelope still includes all required fields. This test checks the finalized and non-optimistic state.
func TestBlobSidecarsEmptyResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	// Set up forkchoice mock to simulate finalized and non-optimistic block.
	f.fcu.IsRootOptimisticVal = false
	f.fcu.FinalizedSlotVal = f.slot + 1

	// No snapshots, no blob storage data for this block → empty response.
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	req, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/blob_sidecars/"+strconv.FormatUint(f.slot, 10), nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var envelope blobSidecarsEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))

	// All envelope fields must still be present even with empty data.
	require.NotNil(t, envelope.Version, "empty response must include 'version'")
	require.NotNil(t, envelope.ExecutionOptimistic, "empty response must include 'execution_optimistic'")
	require.NotNil(t, envelope.Finalized, "empty response must include 'finalized'")
	require.NotNil(t, envelope.Data, "empty response must include 'data'")

	// Verify values.
	require.Equal(t, "electra", *envelope.Version)
	require.False(t, *envelope.ExecutionOptimistic, "execution_optimistic must be false")
	require.True(t, *envelope.Finalized, "finalized must be true")
}
