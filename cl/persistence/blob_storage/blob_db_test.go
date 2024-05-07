package blob_storage

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) kv.RwDB {
	db := memdb.NewTestDB(t)
	return db
}

func TestBlobDB(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	s1 := cltypes.NewBlobSidecar(0, &cltypes.Blob{1}, libcommon.Bytes48{2}, libcommon.Bytes48{3}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))
	s2 := cltypes.NewBlobSidecar(1, &cltypes.Blob{3}, libcommon.Bytes48{5}, libcommon.Bytes48{9}, &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}, solid.NewHashVector(cltypes.CommitmentBranchSize))

	//
	bs := NewBlobStore(db, afero.NewMemMapFs(), 12, &clparams.MainnetBeaconConfig, nil)
	blockRoot := libcommon.Hash{1}
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
