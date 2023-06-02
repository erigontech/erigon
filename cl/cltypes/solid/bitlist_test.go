package solid_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestBitListStatic(t *testing.T) {
	require := require.New(t)

	bitList := solid.BitList{}
	isStatic := bitList.Static()

	require.False(isStatic, "BitList Static method did not return false")
}

func TestBitListClear(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(10, 20)
	bitList.Clear()

	require.Zero(bitList.Length(), "BitList Clear did not reset the length to zero")
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

	bitList := solid.NewBitList(5, 10)

	popped := bitList.Pop()

	require.Equal(byte(0), popped, "BitList Pop did not return the expected value")
	require.Equal(4, bitList.Length(), "BitList Pop did not decrement the length")
}

func TestBitListAppend(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(5, 10)

	bitList.Append(1)
	bitList.Append(0)

	require.Equal(7, bitList.Length(), "BitList Append did not increment the length correctly")
	require.Equal(byte(1), bitList.Get(5), "BitList Append did not append the bits correctly")
	require.Equal(byte(0), bitList.Get(6), "BitList Append did not append the bits correctly")
}

func TestBitListGet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(5, 10)

	bit := bitList.Get(2)

	require.Zero(bit, "BitList Get did not return the expected value")
}

func TestBitListSet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(5, 10)

	bitList.Set(2, 1)

	require.Equal(byte(1), bitList.Get(2), "BitList Set did not set the bit correctly")
}

func TestBitListLength(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(5, 10)

	length := bitList.Length()

	require.Equal(5, length, "BitList Length did not return the expected value")
}

func TestBitListCap(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewBitList(5, 10)

	capacity := bitList.Cap()

	require.Equal(10, capacity, "BitList Cap did not return the expected value")
}

// Add more tests as needed for other functions in the BitList struct.
