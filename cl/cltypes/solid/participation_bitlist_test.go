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

package solid_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestParticipationBitListStatic(t *testing.T) {
	require := require.New(t)

	bitList := solid.ParticipationBitList{}
	isStatic := bitList.Static()

	require.False(isStatic, "BitList Static method did not return false")
}

func TestParticipationBitListClear(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(10, 20)
	bitList.Clear()

	require.Zero(bitList.Length(), "BitList Clear did not reset the length to zero")
}

func TestParticipationBitListCopyTo(t *testing.T) {
	require := require.New(t)

	source := solid.NewParticipationBitList(5, 10)
	target := solid.NewParticipationBitList(0, 0)

	source.CopyTo(target)

	expectedHash, err := source.HashSSZ()
	require.NoError(err)

	actualHash, err := target.HashSSZ()
	require.NoError(err)

	require.Equal(expectedHash, actualHash, "BitList CopyTo did not copy the bits correctly")
	require.Equal(source.Length(), target.Length(), "BitList CopyTo did not set the target length correctly")
}

func TestParticipationBitListPop(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	popped := bitList.Pop()

	require.Equal(byte(0), popped, "BitList Pop did not return the expected value")
	require.Equal(4, bitList.Length(), "BitList Pop did not decrement the length")
}

func TestParticipationBitListAppend(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bitList.Append(1)
	bitList.Append(0)

	require.Equal(7, bitList.Length(), "BitList Append did not increment the length correctly")
	require.Equal(byte(1), bitList.Get(5), "BitList Append did not append the bits correctly")
	require.Equal(byte(0), bitList.Get(6), "BitList Append did not append the bits correctly")
}

func TestParticipationBitListGet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bit := bitList.Get(2)

	require.Zero(bit, "BitList Get did not return the expected value")
}

func TestParticipationBitListSet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bitList.Set(2, 1)

	require.Equal(byte(1), bitList.Get(2), "BitList Set did not set the bit correctly")
}

func TestParticipationBitListLength(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	length := bitList.Length()

	require.Equal(5, length, "BitList Length did not return the expected value")
}

func TestParticipationBitListCap(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	capacity := bitList.Cap()

	require.Equal(10, capacity, "BitList Cap did not return the expected value")
}

// Add more tests as needed for other functions in the BitList struct.
