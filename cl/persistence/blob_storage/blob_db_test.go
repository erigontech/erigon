package blob_storage

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *aferoBlobStorage {
	fs := afero.NewMemMapFs()
	rawDB := NewBlobStorage(fs, &clparams.MainnetBeaconConfig)
	return rawDB
}

func TestBlobWriterAndReader(t *testing.T) {
	bs := setupTestDB(t)

	sidecar := &cltypes.BlobSidecar{
		Index: 1,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot: 56,
				Root: libcommon.Hash{0x01},
			},
		},
		Blob:                     cltypes.Blob{},
		KzgCommitment:            libcommon.Bytes48{},
		KzgProof:                 libcommon.Bytes48{},
		CommitmentInclusionProof: solid.NewHashVector(17),
	}

	err := bs.BlobWriter(sidecar)
	require.NoError(t, err)

	// Read the sidecar from the storage
	blockRoot, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
	readSidecar, err := bs.BlobReader(blockRoot)
	require.NoError(t, err)

	// Check that the read sidecar is the same as the written sidecar
	require.Equal(t, sidecar, readSidecar)
}

func TestRetrieveBlobsAndProofs(t *testing.T) {
	bs := setupTestDB(t)

	sidecar := &cltypes.BlobSidecar{
		Index: 1,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot: 56,
				Root: libcommon.Hash{0x01},
			},
		},
		Blob:                     cltypes.Blob{},
		KzgCommitment:            libcommon.Bytes48{},
		KzgProof:                 libcommon.Bytes48{},
		CommitmentInclusionProof: solid.NewHashVector(17),
	}
	blockRoot, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
	bs.blockRootToSidecar.Store(blockRoot, sidecar)

	// Call retrieveBlobsAndProofs with the block root of the stored sidecar
	blobs, proofs, err := bs.retrieveBlobsAndProofs(blockRoot)

	// Check that no error occurred
	require.NoError(t, err)

	// Check that the returned blobs and proofs match the blob and proof of the stored sidecar
	require.Equal(t, []cltypes.Blob{sidecar.Blob}, blobs)
	require.Equal(t, []libcommon.Bytes48{sidecar.KzgProof}, proofs)
}
