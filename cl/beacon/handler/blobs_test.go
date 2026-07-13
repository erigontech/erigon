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
	handler       *ApiHandler
	slot          uint64
	versionedHash common.Hash
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

func TestGetBlobsErrorsWhenFrozenSnapshotReadFails(t *testing.T) {
	f := setupBlobsTest(t)

	f.handler.caplinSnapshots = frozenBlobSnapshotReader{
		frozenBlobsExclusive: f.slot + 1,
		err:                  errors.New("snapshot read failed"),
	}

	statusCode := getBeaconBlobsStatus(t, f)

	require.Equal(t, http.StatusInternalServerError, statusCode)
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

func requestBeaconBlobs(t *testing.T, baseURL string, f blobsTestFixture) *http.Response {
	t.Helper()

	resp, err := http.Get(baseURL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(f.slot, 10) + "?versioned_hashes=" + f.versionedHash.Hex())
	require.NoError(t, err)
	return resp
}

func setupBlobsTest(t *testing.T) blobsTestFixture {
	t.Helper()

	db, blocks, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	block := blocks[0]
	slot := block.Block.Slot

	commitments := []cltypes.KZGCommitment{{69}, {1}}
	block.Block.Body.BlobKzgCommitments.Clear()
	block.Block.Body.BlobKzgCommitments.Append(&commitments[0])
	block.Block.Body.BlobKzgCommitments.Append(&commitments[1])
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(commitments[1]))
	require.NoError(t, err)

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, blockRoot, slot))
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, blockRoot))
	require.NoError(t, tx.Commit())

	return blobsTestFixture{
		handler:       handler,
		slot:          slot,
		versionedHash: versionedHash,
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
// Beacon API specification. This is a regression test for the missing envelope fields bug.
func TestBlobSidecarsResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	// Set up frozen snapshots with actual blob data.
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

	// All three envelope fields must be present (non-nil pointers).
	require.NotNil(t, envelope.Version, "response must include 'version'")
	require.NotNil(t, envelope.ExecutionOptimistic, "response must include 'execution_optimistic'")
	require.NotNil(t, envelope.Finalized, "response must include 'finalized'")
	require.NotNil(t, envelope.Data, "response must include 'data'")

	// Verify values.
	require.Equal(t, "electra", *envelope.Version)
	require.False(t, *envelope.ExecutionOptimistic)
}

// TestBlobSidecarsEmptyResponseEnvelope verifies that even when no blobs are found,
// the response envelope still includes all required fields.
func TestBlobSidecarsEmptyResponseEnvelope(t *testing.T) {
	f := setupBlobsTest(t)

	// No snapshots, no blob storage data for this block → empty response.
	f.handler.caplinSnapshots = frozenBlobSnapshotReader{frozenBlobsExclusive: f.slot + 1}

	server := httptest.NewServer(f.handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blob_sidecars/" + strconv.FormatUint(f.slot, 10))
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

	require.Equal(t, "electra", *envelope.Version)
	require.False(t, *envelope.ExecutionOptimistic)
}
