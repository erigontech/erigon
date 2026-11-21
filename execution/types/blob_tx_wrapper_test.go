package types

import (
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/execution/protocol/params"
)

func TestConvertToV1(t *testing.T) {
	chainID := uint256.NewInt(5)
	v0 := MakeWrappedBlobTxn(chainID)

	require.Equal(t, byte(0), v0.WrapperVersion)
	require.Len(t, v0.Proofs, 2) // 1 proof per blob

	converted, err := v0.ConvertToV1()
	require.NoError(t, err)
	require.NotEmpty(t, converted)

	// Decode the converted bytes back.
	txn, err := DecodeWrappedTransaction(converted)
	require.NoError(t, err)

	v1, ok := txn.(*BlobTxWrapper)
	require.True(t, ok)

	assert.Equal(t, byte(1), v1.WrapperVersion)
	assert.Len(t, v1.Blobs, 2)
	assert.Len(t, v1.Commitments, 2)
	assert.Len(t, v1.Proofs, 2*int(params.CellsPerExtBlob)) // 128 cell proofs per blob

	// Blobs and commitments should be identical.
	assert.Equal(t, v0.Blobs, v1.Blobs)
	assert.Equal(t, v0.Commitments, v1.Commitments)

	// Transaction body should be identical.
	assert.Equal(t, v0.Tx.BlobVersionedHashes, v1.Tx.BlobVersionedHashes)
	assert.Equal(t, v0.Tx.Nonce, v1.Tx.Nonce)
	assert.Equal(t, v0.Tx.GasLimit, v1.Tx.GasLimit)

	// Verify the cell proofs are cryptographically valid.
	err = libkzg.VerifyCellProofBatch(v1.blobBytes(), v1.commitmentValues(), v1.proofValues())
	require.NoError(t, err)

	// Original wrapper should be unchanged.
	assert.Equal(t, byte(0), v0.WrapperVersion)
	assert.Len(t, v0.Proofs, 2)
}

// helpers to convert typed slices into the formats expected by VerifyCellProofBatch
func (txw *BlobTxWrapper) blobBytes() [][]byte {
	out := make([][]byte, len(txw.Blobs))
	for i := range txw.Blobs {
		out[i] = txw.Blobs[i][:]
	}
	return out
}

func (txw *BlobTxWrapper) commitmentValues() []goethkzg.KZGCommitment {
	out := make([]goethkzg.KZGCommitment, len(txw.Commitments))
	for i, c := range txw.Commitments {
		out[i] = goethkzg.KZGCommitment(c)
	}
	return out
}

func (txw *BlobTxWrapper) proofValues() []goethkzg.KZGProof {
	out := make([]goethkzg.KZGProof, len(txw.Proofs))
	for i, p := range txw.Proofs {
		out[i] = goethkzg.KZGProof(p)
	}
	return out
}
