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

package cltypes

import (
	"testing"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/require"
)

func TestKZGCommitmentCopy(t *testing.T) {
	require := require.New(t)

	commitment := KZGCommitment{}
	commitment[0] = 1
	commitmentCopy := commitment.Copy()

	// Modify the original commitment
	commitment[0] = 2

	require.EqualValues(1, commitmentCopy[0], "KZGCommitment Copy did not create a separate copy")
}

func TestKZGCommitmentEncodeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := KZGCommitment{}
	commitment[0] = 1

	encoded, err := commitment.EncodeSSZ([]byte{})
	require.NoError(err, "Error encoding KZGCommitment")

	expected := append([]byte{}, commitment[:]...)
	require.Equal(expected, encoded, "KZGCommitment EncodeSSZ did not produce the expected result")
}

func TestKZGCommitmentDecodeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := KZGCommitment{}
	encoded := append([]byte{}, commitment[:]...)
	encoded[0] = 1

	err := commitment.DecodeSSZ(encoded, 0)
	require.NoError(err, "Error decoding KZGCommitment")

	expected := KZGCommitment{}
	expected[0] = 1
	require.Equal(expected, commitment, "KZGCommitment DecodeSSZ did not produce the expected result")
}

func TestKZGCommitmentEncodingSizeSSZ(t *testing.T) {
	require := require.New(t)

	commitment := KZGCommitment{}
	encodingSize := commitment.EncodingSizeSSZ()

	require.Equal(48, encodingSize, "KZGCommitment EncodingSizeSSZ did not return the expected size")
}

func TestKZGCommitmentHashSSZ(t *testing.T) {
	require := require.New(t)

	commitment := KZGCommitment{}
	commitment[0] = 1

	hash, err := commitment.HashSSZ()
	require.NoError(err, "Error hashing KZGCommitment")

	// Calculate the expected hash using the same method as the HashSSZ function
	expected, err := merkle_tree.BytesRoot(commitment[:])
	require.NoError(err, "Error calculating expected hash")

	require.Equal(expected, hash, "KZGCommitment HashSSZ did not produce the expected result")
}
