package blob_storage

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) kv.RwDB {
	db := memdb.NewTestDB(t)
	return db
}

func TestBlobWriterAndReader(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	sidecar := &cltypes.BlobSidecar{
		Index: 1,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot: 56,
				Root: libcommon.Hash{0x01},
			},
		},
		Blob:                     cltypes.Blob{1},
		KzgCommitment:            libcommon.Bytes48{1},
		KzgProof:                 libcommon.Bytes48{1},
		CommitmentInclusionProof: solid.NewHashVector(17),
	}

	err := WriteBlob(tx, sidecar)
	require.NoError(t, err)

	encoded, _ := sidecar.EncodeSSZ(nil)

	// test ReadBlobByKzgCommitment
	readBlob, err := ReadBlobSidecarByKzgCommitment(tx, sidecar.KzgCommitment)
	require.NoError(t, err)

	decoded := cltypes.BlobSidecar{}
	decoded.DecodeSSZ(encoded, 0)
	require.Equal(t, decoded, readBlob)

	// test ReadKzgCommitmentsByBlockRoot
	blockRoot, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
	readCommitments, err := ReadKzgCommitmentsByBlockRoot(tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, []libcommon.Bytes48{sidecar.KzgCommitment}, readCommitments)
}

func TestRetrieveBlobsAndProofs(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	sidecar := &cltypes.BlobSidecar{
		Index: 1,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot: 56,
				Root: libcommon.Hash{0x01},
			},
		},
		Blob:                     cltypes.Blob{1},
		KzgCommitment:            libcommon.Bytes48{1},
		KzgProof:                 libcommon.Bytes48{1},
		CommitmentInclusionProof: solid.NewHashVector(17),
	}

	err := WriteBlob(tx, sidecar)
	require.NoError(t, err)

	// Call retrieveBlobsAndProofs with the block root of the stored sidecar
	blockRoot, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
	blobs, proofs, err := RetrieveBlobsAndProofs(tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, []cltypes.Blob{sidecar.Blob}, blobs)
	require.Equal(t, []libcommon.Bytes48{sidecar.KzgProof}, proofs)
}
