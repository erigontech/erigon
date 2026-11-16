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

package blobfetch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// prooflessBlock returns a Deneb block with blobs blobs and its sidecars carrying the empty proof that some endpoints
// serve, so the fetch has to compute every proof.
func prooflessBlock(t *testing.T, blobs int) (*cltypes.SignedBeaconBlock, common.Hash, []*cltypes.BlobSidecar) {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	data := make([]goethkzg.Blob, blobs)
	commitments := make([]goethkzg.KZGCommitment, blobs)
	for i := range data {
		data[i][1] = byte(i + 1)
		var err error
		commitments[i], err = kzg.Ctx().BlobToKZGCommitment(&data[i], 0)
		require.NoError(t, err)
		block.Block.Body.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitments[i]))
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	sidecars := make([]*cltypes.BlobSidecar, blobs)
	for i := range data {
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j := range branch {
			inclusion.Set(j, common.Hash(branch[j]))
		}
		sidecars[i] = cltypes.NewBlobSidecar(uint64(i), (*cltypes.Blob)(&data[i]), common.Bytes48(commitments[i]),
			common.Bytes48(goethkzg.PointAtInfinity), block.SignedBeaconBlockHeader(), inclusion)
	}
	return block, root, sidecars
}

func TestFetchAndStoreComputesMissingProofsWithinACPUBudget(t *testing.T) {
	block, root, sidecars := prooflessBlock(t, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{"data": sidecars}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	store := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	budget := semaphore.NewWeighted(2)

	require.NoError(t, FetchAndStore(t.Context(), store, srv.URL, root, block, budget))

	stored, found, err := store.ReadBlobSidecars(t.Context(), block.Block.Slot, root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, stored, 4)
	require.NoError(t, blob_storage.VerifyBlobSidecars(stored, block.Version(), func(*cltypes.SignedBeaconBlockHeader) error { return nil }))
	require.True(t, budget.TryAcquire(2), "every budget slot is returned")
}
