package cltypes_test

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/assert"
)

func TestBlobSideCar_EncodeDecodeSSZ(t *testing.T) {
	blockRoot := cltypes.Root{1, 2, 3}
	index := uint64(123)
	slot := cltypes.Slot(456)
	blockParentRoot := cltypes.Root{4, 5, 6}
	proposerIndex := uint64(789)
	blob := cltypes.Blob{}
	kzgCommitment := cltypes.KZGCommitment{7, 8, 9}
	kzgProof := cltypes.KZGProof{10, 11, 12}

	sideCar := &cltypes.BlobSideCar{
		BlockRoot:       blockRoot,
		Index:           index,
		Slot:            slot,
		BlockParentRoot: blockParentRoot,
		ProposerIndex:   proposerIndex,
		Blob:            blob,
		KZGCommitment:   kzgCommitment,
		KZGProof:        kzgProof,
	}

	encoded, err := sideCar.EncodeSSZ([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	decoded := &cltypes.BlobSideCar{}
	err = decoded.DecodeSSZ(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.BlockRoot != blockRoot {
		t.Errorf("unexpected BlockRoot: got %v, want %v", decoded.BlockRoot, blockRoot)
	}
	if decoded.Index != index {
		t.Errorf("unexpected Index: got %d, want %d", decoded.Index, index)
	}
	if decoded.Slot != slot {
		t.Errorf("unexpected Slot: got %d, want %d", decoded.Slot, slot)
	}
	if decoded.BlockParentRoot != blockParentRoot {
		t.Errorf("unexpected BlockParentRoot: got %v, want %v", decoded.BlockParentRoot, blockParentRoot)
	}
	if decoded.ProposerIndex != proposerIndex {
		t.Errorf("unexpected ProposerIndex: got %d, want %d", decoded.ProposerIndex, proposerIndex)
	}
	if !bytes.Equal(decoded.Blob[:], blob[:]) {
		t.Errorf("unexpected Blob: got %v, want %v", decoded.Blob, blob)
	}
	if decoded.KZGCommitment != kzgCommitment {
		t.Errorf("unexpected KZGCommitment: got %v, want %v", decoded.KZGCommitment, kzgCommitment)
	}
	if decoded.KZGProof != kzgProof {
		t.Errorf("unexpected KZGProof: got %v, want %v", decoded.KZGProof, kzgProof)
	}
}

func TestSignedBlobSideCar(t *testing.T) {
	// Create a BlobSideCar to use as the message for SignedBlobSideCar
	blob := cltypes.Blob{1, 2, 3, 4, 5, 6, 7, 8}
	blobSideCar := cltypes.BlobSideCar{
		BlockRoot:       cltypes.Root{1},
		Index:           2,
		Slot:            3,
		BlockParentRoot: cltypes.Root{4},
		ProposerIndex:   5,
		Blob:            blob,
		KZGCommitment:   cltypes.KZGCommitment{6},
		KZGProof:        cltypes.KZGProof{7},
	}

	// Create a SignedBlobSideCar with the BlobSideCar and a signature
	signature := [96]byte{8}
	signedBlobSideCar := cltypes.SignedBlobSideCar{
		Message:   blobSideCar,
		Signature: signature,
	}

	// Encode the SignedBlobSideCar
	encoded, err := signedBlobSideCar.EncodeSSZ(nil)
	assert.NoError(t, err)

	// Decode the encoded SignedBlobSideCar
	decoded := cltypes.SignedBlobSideCar{}
	err = decoded.DecodeSSZ(encoded)
	assert.NoError(t, err)

	// Assert that the decoded SignedBlobSideCar is equal to the original SignedBlobSideCar
	assert.Equal(t, signedBlobSideCar.Message.BlockRoot, decoded.Message.BlockRoot)
	assert.Equal(t, signedBlobSideCar.Message.Index, decoded.Message.Index)
	assert.Equal(t, signedBlobSideCar.Message.Slot, decoded.Message.Slot)
	assert.Equal(t, signedBlobSideCar.Message.BlockParentRoot, decoded.Message.BlockParentRoot)
	assert.Equal(t, signedBlobSideCar.Message.ProposerIndex, decoded.Message.ProposerIndex)
	assert.True(t, bytes.Equal(signedBlobSideCar.Message.Blob[:], decoded.Message.Blob[:]))
	assert.Equal(t, signedBlobSideCar.Message.KZGCommitment, decoded.Message.KZGCommitment)
	assert.Equal(t, signedBlobSideCar.Message.KZGProof, decoded.Message.KZGProof)
	assert.Equal(t, signedBlobSideCar.Signature, decoded.Signature)
}

func TestBlobIdentifier_EncodeDecodeSSZ(t *testing.T) {
	blockRoot := cltypes.Root{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	index := uint64(123456)

	blobID := &cltypes.BlobIdentifier{
		BlockRoot: blockRoot,
		Index:     index,
	}

	// encode to SSZ
	encoded, err := blobID.EncodeSSZ(nil)
	if err != nil {
		t.Fatalf("EncodeSSZ failed: %v", err)
	}

	// decode from SSZ
	decoded := &cltypes.BlobIdentifier{}
	if err := decoded.DecodeSSZ(encoded); err != nil {
		t.Fatalf("DecodeSSZ failed: %v", err)
	}

	// compare original and decoded values
	if !bytes.Equal(blobID.BlockRoot[:], decoded.BlockRoot[:]) {
		t.Errorf("BlockRoot mismatch: expected %v, got %v", blobID.BlockRoot, decoded.BlockRoot)
	}
	if blobID.Index != decoded.Index {
		t.Errorf("Index mismatch: expected %v, got %v", blobID.Index, decoded.Index)
	}

	// check SSZ encoding size
	expectedSize := blobID.EncodingSizeSSZ()
	if len(encoded) != expectedSize {
		t.Errorf("Encoding size mismatch: expected %d, got %d", expectedSize, len(encoded))
	}
}
