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

func TestBitListStatic(t *testing.T) {
	require := require.New(t)

	BitList := solid.BitList{}
	isStatic := BitList.Static()

	require.False(isStatic, "BitList Static method did not return false")
}

func TestBitListClear(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(10, 20)
	BitList.Clear()

	require.Zero(BitList.Length(), "BitList Clear did not reset the length to zero")
}

func TestBitListCopyTo(t *testing.T) {
	require := require.New(t)

	source := solid.NewBitList(5, 10)
	target := solid.NewBitList(0, 0)

	source.CopyTo(target)

	expectedHash, err := source.HashSSZ()
	require.NoError(err)

	actualHash, err := target.HashSSZ()
	require.NoError(err)

	require.Equal(expectedHash, actualHash, "BitList CopyTo did not copy the bits correctly")
	require.Equal(source.Length(), target.Length(), "BitList CopyTo did not set the target length correctly")
}

func TestBitListPop(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	popped := BitList.Pop()

	require.Equal(byte(0), popped, "BitList Pop did not return the expected value")
	require.Equal(4, BitList.Length(), "BitList Pop did not decrement the length")
}

func TestBitListAppend(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	BitList.Append(1)
	BitList.Append(0)

	require.Equal(7, BitList.Length(), "BitList Append did not increment the length correctly")
	require.Equal(byte(1), BitList.Get(5), "BitList Append did not append the bits correctly")
	require.Equal(byte(0), BitList.Get(6), "BitList Append did not append the bits correctly")
}

func TestBitListGet(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	bit := BitList.Get(2)

	require.Zero(bit, "BitList Get did not return the expected value")
}

func TestBitListSet(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	BitList.Set(2, 1)

	require.Equal(byte(1), BitList.Get(2), "BitList Set did not set the bit correctly")
}

func TestBitListLength(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	length := BitList.Length()

	require.Equal(5, length, "BitList Length did not return the expected value")
}

func TestBitListCap(t *testing.T) {
	require := require.New(t)

	BitList := solid.NewBitList(5, 10)

	capacity := BitList.Cap()

	require.Equal(10, capacity, "BitList Cap did not return the expected value")
}

// Add more tests as needed for other functions in the BitList struct.

func TestBitlistMerge(t *testing.T) {
	require := require.New(t)

	b1 := solid.BitlistFromBytes([]byte{0b11010000}, 10)
	b2 := solid.BitlistFromBytes([]byte{0b10000101}, 10)

	merged, err := b1.Merge(b2)
	require.NoError(err)

	require.Equal(7, merged.Bits(), "BitList Merge did not return the expected number of bits")
	require.Equal(1, merged.Length(), "BitList Union did not return the expected length")
	require.Equal(byte(0b11010101), merged.Get(0), "BitList Union did not return the expected value")
}

func TestBitSlice(t *testing.T) {
	require := require.New(t)

	bs := solid.NewBitSlice()

	bs.AppendBit(true)
	bs.AppendBit(false)
	bs.AppendBit(true)
	bs.AppendBit(false)

	bytes := bs.Bytes()

	require.Equal([]byte{0b00000101}, bytes, "BitSlice AppendBit did not append the bits correctly")
	require.Equal(4, bs.Length(), "BitSlice AppendBit did not increment the length correctly")
}
