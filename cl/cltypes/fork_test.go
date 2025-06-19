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

package cltypes_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
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

	require.Equal(expected, encoded, "Fork EncodeSSZ did not produce the expected result")
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
	require.Equal(expected, fork, "Fork DecodeSSZ did not produce the expected result")
}

func TestForkEncodingSizeSSZ(t *testing.T) {
	require := require.New(t)

	fork := cltypes.Fork{}
	encodingSize := fork.EncodingSizeSSZ()

	require.Equal(16, encodingSize, "Fork EncodingSizeSSZ did not return the expected size")
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

	require.Equal(expected, hash, "Fork HashSSZ did not produce the expected result")
}
