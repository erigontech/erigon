package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/require"
)

func TestKZGCommitmentCopy(t *testing.T) {
	require := require.New(t)

	commitment := cltypes.KZGCommitment{}
	commitment[0] = 1
	commitmentCopy := commitment.Copy()

	// Modify the original commitment
	commitment[0] = 2

	require.EqualValues(commitmentCopy[0], 1, "KZGCommitment Copy did not create a separate copy")
}

func TestKZGCommitmentEncodeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := cltypes.KZGCommitment{}
	commitment[0] = 1

	encoded, err := commitment.EncodeSSZ([]byte{})
	require.NoError(err, "Error encoding KZGCommitment")

	expected := append([]byte{}, commitment[:]...)
	require.Equal(encoded, expected, "KZGCommitment EncodeSSZ did not produce the expected result")
}

func TestKZGCommitmentDecodeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := cltypes.KZGCommitment{}
	encoded := append([]byte{}, commitment[:]...)
	encoded[0] = 1

	err := commitment.DecodeSSZ(encoded, 0)
	require.NoError(err, "Error decoding KZGCommitment")

	expected := cltypes.KZGCommitment{}
	expected[0] = 1
	require.Equal(commitment, expected, "KZGCommitment DecodeSSZ did not produce the expected result")
}

func TestKZGCommitmentEncodingSizeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := cltypes.KZGCommitment{}
	encodingSize := commitment.EncodingSizeSSZ()

	require.Equal(encodingSize, 48, "KZGCommitment EncodingSizeSSZ did not return the expected size")
}

func TestKZGCommitmentHashSSZ(t *testing.T) {
	require := require.New(t)

	commitment := cltypes.KZGCommitment{}
	commitment[0] = 1

	hash, err := commitment.HashSSZ()
	require.NoError(err, "Error hashing KZGCommitment")

	// Calculate the expected hash using the same method as the HashSSZ function
	expected, err := merkle_tree.BytesRoot(commitment[:])
	require.NoError(err, "Error calculating expected hash")

	require.Equal(hash, expected, "KZGCommitment HashSSZ did not produce the expected result")
}
