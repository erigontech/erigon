package blob_storage

import (
	"encoding/json"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func WriteBlob(tx kv.RwTx, blobSidecar *cltypes.BlobSidecar) error {
	// Encode the blobSidecar
	encodedBlobSidecar, err := blobSidecar.EncodeSSZ(nil)
	if err != nil {
		return err
	}

	// Write the blobSidecar to the KzgCommitmentToBlob table
	err = tx.Put(kv.KzgCommitmentToBlob, blobSidecar.KzgCommitment[:], encodedBlobSidecar)
	if err != nil {
		return err
	}

	// Read the existing kzgCommitments for BlockRoot
	blockRoot, _ := blobSidecar.SignedBlockHeader.Header.HashSSZ()
	data, _ := tx.GetOne(kv.BlockRootToKzgCommitments, blockRoot[:])
	var kzgCommitments []libcommon.Bytes48
	if len(data) > 0 {
		if err := json.Unmarshal(data, &kzgCommitments); err != nil {
			return err
		}
	}
	kzgCommitments = append(kzgCommitments, blobSidecar.KzgCommitment)
	data, err = json.Marshal(kzgCommitments)
	if err != nil {
		return err
	}
	// Write the updated kzgCommitments to the BlockRootToKzgCommitments table
	err = tx.Put(kv.BlockRootToKzgCommitments, blockRoot[:], data)
	if err != nil {
		return err
	}

	return nil
}

func ReadBlobSidecarByKzgCommitment(tx kv.Tx, kzgCommitment libcommon.Bytes48) (cltypes.BlobSidecar, error) {
	var blob cltypes.BlobSidecar
	data, err := tx.GetOne(kv.KzgCommitmentToBlob, kzgCommitment[:])
	if err != nil {
		return blob, err
	}
	// Decode the data into the blob
	if err := blob.DecodeSSZ(data, 0); err != nil {
		return blob, err
	}
	return blob, nil
}

func ReadKzgCommitmentsByBlockRoot(tx kv.Tx, blockRoot [32]byte) ([]libcommon.Bytes48, error) {
	var kzgCommitments []libcommon.Bytes48
	data, err := tx.GetOne(kv.BlockRootToKzgCommitments, blockRoot[:])
	if err != nil {
		return kzgCommitments, err
	}
	// Decode the data into the kzgCommitments
	if err := json.Unmarshal(data, &kzgCommitments); err != nil {
		return kzgCommitments, err
	}

	return kzgCommitments, nil
}

func RetrieveBlobsAndProofs(tx kv.Tx, beaconBlockRoot [32]byte) ([]cltypes.Blob, []libcommon.Bytes48, error) {
	kzgCommitments, err := ReadKzgCommitmentsByBlockRoot(tx, beaconBlockRoot)
	if err != nil {
		return nil, nil, err
	}

	blobs := make([]cltypes.Blob, len(kzgCommitments))
	proofs := make([]libcommon.Bytes48, len(kzgCommitments))

	for i, kzgCommitment := range kzgCommitments {
		blobSidecar, err := ReadBlobSidecarByKzgCommitment(tx, kzgCommitment)
		if err != nil {
			return nil, nil, err
		}
		blobs[i] = blobSidecar.Blob
		proofs[i] = blobSidecar.KzgProof
	}
	return blobs, kzgCommitments, nil
}
