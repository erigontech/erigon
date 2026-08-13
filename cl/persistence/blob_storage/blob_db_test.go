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

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestVerifyBlobSidecarsGloasDoesNotRequireInclusionProof(t *testing.T) {
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	sidecar := cltypes.NewBlobSidecar(
		0,
		(*cltypes.Blob)(&blob),
		common.Bytes48(commitment),
		common.Bytes48(proof),
		&cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}},
		solid.NewHashVector(cltypes.CommitmentBranchSize),
	)

	require.NoError(t, VerifyBlobSidecars([]*cltypes.BlobSidecar{sidecar}, clparams.GloasVersion, nil))
	require.Error(t, VerifyBlobSidecars([]*cltypes.BlobSidecar{sidecar}, clparams.FuluVersion, nil))
}

func setupTestDB(t *testing.T) kv.RwDB {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
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

func TestRemoveBlobSidecarsClearsMetadataWhenAFileIsMissing(t *testing.T) {
	db := setupTestDB(t)
	fs := afero.NewMemMapFs()
	bs := NewBlobStore(db, fs, 12, &clparams.MainnetBeaconConfig, nil)
	blockRoot := common.Hash{1}
	sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{1}, common.Bytes48{2}, common.Bytes48{3}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))
	require.NoError(t, bs.WriteBlobSidecars(t.Context(), blockRoot, []*cltypes.BlobSidecar{sidecar}))
	_, filePath := blobSidecarFilePath(1, 0, blockRoot)
	require.NoError(t, fs.Remove(filePath))

	require.NoError(t, bs.RemoveBlobSidecars(t.Context(), 1, blockRoot))
	count, err := bs.KzgCommitmentsCount(t.Context(), blockRoot)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestKzgCommitmentsCountHonorsCanceledContext(t *testing.T) {
	db := setupTestDB(t)
	bs := NewBlobStore(db, afero.NewMemMapFs(), 12, &clparams.MainnetBeaconConfig, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := bs.KzgCommitmentsCount(ctx, common.Hash{})
	require.ErrorIs(t, err, context.Canceled)
}
