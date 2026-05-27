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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type mockBlobSnapshots struct {
	frozenSlot uint64
	sidecars   []*cltypes.BlobSidecar
}

func (m mockBlobSnapshots) FrozenBlobs() uint64 { return m.frozenSlot }
func (m mockBlobSnapshots) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	return m.sidecars, nil
}

func TestGetBlobsFrozenSlot(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	block := blocks[0]
	slot := block.Block.Slot

	commitments := []cltypes.KZGCommitment{{69}, {1}}
	block.Block.Body.BlobKzgCommitments.Clear()
	block.Block.Body.BlobKzgCommitments.Append(&commitments[0])
	block.Block.Body.BlobKzgCommitments.Append(&commitments[1])
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteHeaderSlot(tx, blockRoot, slot))
	require.NoError(t, beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, blockRoot))
	require.NoError(t, tx.Commit())

	handler.caplinSnapshots = mockBlobSnapshots{
		frozenSlot: slot,
		sidecars: []*cltypes.BlobSidecar{
			{Index: 0, Blob: cltypes.Blob{1}},
			{Index: 1, Blob: cltypes.Blob{2}},
		},
	}

	vHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(commitments[1]))
	require.NoError(t, err)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/blobs/" + strconv.FormatUint(slot, 10) + "?versioned_hashes=" + vHash.Hex())
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Data []string `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Data, 1)
}
