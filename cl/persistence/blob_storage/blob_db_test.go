// Copyright 2024 The Erigon Authors
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

package blob_storage

import (
	"context"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func setupTestDB(t *testing.T) kv.RwDB {
	db := memdb.NewTestDB(t, kv.ChainDB)
	return db
}

func TestBlobDB(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	s1 := cltypes.NewBlobSidecar(0, &cltypes.Blob{1}, common.Bytes48{2}, common.Bytes48{3}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))
	s2 := cltypes.NewBlobSidecar(1, &cltypes.Blob{3}, common.Bytes48{5}, common.Bytes48{9}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))

	//
	bs := NewBlobStore(db, afero.NewMemMapFs(), 12, &clparams.MainnetBeaconConfig, nil)
	blockRoot := common.Hash{1}
	err := bs.WriteBlobSidecars(context.Background(), blockRoot, []*cltypes.BlobSidecar{s1, s2})
	require.NoError(t, err)

	sidecars, found, err := bs.ReadBlobSidecars(context.Background(), 1, blockRoot)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, sidecars, 2)

	require.Equal(t, s1.Blob, sidecars[0].Blob)
	require.Equal(t, s2.Blob, sidecars[1].Blob)
	require.Equal(t, s1.Index, sidecars[0].Index)
	require.Equal(t, s2.Index, sidecars[1].Index)
	require.Equal(t, s1.CommitmentInclusionProof, sidecars[0].CommitmentInclusionProof)
	require.Equal(t, s2.CommitmentInclusionProof, sidecars[1].CommitmentInclusionProof)
	require.Equal(t, s1.KzgCommitment, sidecars[0].KzgCommitment)
	require.Equal(t, s2.KzgCommitment, sidecars[1].KzgCommitment)
	require.Equal(t, s1.KzgProof, sidecars[0].KzgProof)
	require.Equal(t, s2.KzgProof, sidecars[1].KzgProof)
	require.Equal(t, s1.SignedBlockHeader, sidecars[0].SignedBlockHeader)
	require.Equal(t, s2.SignedBlockHeader, sidecars[1].SignedBlockHeader)
}
