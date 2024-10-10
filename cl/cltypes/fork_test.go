package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/stretchr/testify/require"
)

func TestForkStatic(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{}
	isStatic := fork.Static()

	require.True(isStatic, "Fork Static method did not return true")
}

func TestForkCopy(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{
		PreviousVersion: [4]byte{1, 2, 3, 4},
		CurrentVersion:  [4]byte{5, 6, 7, 8},
		Epoch:           123,
	}

	copy := fork.Copy()

	require.Equal(fork.PreviousVersion, copy.PreviousVersion, "Fork Copy did not create a separate copy")
	require.Equal(fork.CurrentVersion, copy.CurrentVersion, "Fork Copy did not create a separate copy")
	require.Equal(fork.Epoch, copy.Epoch, "Fork Copy did not create a separate copy")
}

func TestForkEncodeSSZ(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{
		PreviousVersion: [4]byte{1, 2, 3, 4},
		CurrentVersion:  [4]byte{5, 6, 7, 8},
		Epoch:           123,
	}

	encoded, err := fork.EncodeSSZ([]byte{})
	require.NoError(err, "Error encoding Fork")

	expected, err := ssz2.MarshalSSZ([]byte{}, fork.PreviousVersion[:], fork.CurrentVersion[:], fork.Epoch)
	require.NoError(err, "Error calculating expected encoded value")

	require.Equal(encoded, expected, "Fork EncodeSSZ did not produce the expected result")
}

func TestForkDecodeSSZ(t *testing.T) {
	require := require.New(t)

	fork := &cltypes.Fork{
		PreviousVersion: [4]byte{1, 2, 3, 4},
		CurrentVersion:  [4]byte{5, 6, 7, 8},
		Epoch:           123,
	}
	encoded, err := fork.EncodeSSZ([]byte{})
	require.NoError(err, "Error preparing encoded value")

	err = fork.DecodeSSZ(encoded, 0)
	require.NoError(err, "Error decoding Fork")

	expected := &cltypes.Fork{
		PreviousVersion: [4]byte{1, 2, 3, 4},
		CurrentVersion:  [4]byte{5, 6, 7, 8},
		Epoch:           123,
	}
	require.Equal(fork, expected, "Fork DecodeSSZ did not produce the expected result")
}

func TestForkEncodingSizeSSZ(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{}
	encodingSize := fork.EncodingSizeSSZ()

	require.Equal(encodingSize, 16, "Fork EncodingSizeSSZ did not return the expected size")
}

func TestForkHashSSZ(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{
		PreviousVersion: [4]byte{1, 2, 3, 4},
		CurrentVersion:  [4]byte{5, 6, 7, 8},
		Epoch:           123,
	}

	hash, err := fork.HashSSZ()
	require.NoError(err, "Error hashing Fork")

	expected, err := merkle_tree.HashTreeRoot(fork.PreviousVersion[:], fork.CurrentVersion[:], fork.Epoch)
	require.NoError(err, "Error calculating expected hash")

	require.Equal(hash, expected, "Fork HashSSZ did not produce the expected result")
}
