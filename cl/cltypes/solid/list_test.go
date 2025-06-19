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

package solid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
)

func TestHashVector(t *testing.T) {
	vectorSize := 10
	hashVector := NewHashVector(vectorSize)
	assert.NotNil(t, hashVector)

	// Test Cap and Length
	assert.Equal(t, vectorSize, hashVector.Cap())
	assert.Equal(t, vectorSize, hashVector.Length())

	// Test Set and Get
	hash := common.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	hashVector.Set(0, hash)
	assert.Equal(t, hash, hashVector.Get(0))

	// Test Encoding and Decoding
	buf, err := hashVector.EncodeSSZ(nil)
	require.NoError(t, err)
	newHashVector := NewHashVector(vectorSize)
	err = newHashVector.DecodeSSZ(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, hashVector, newHashVector)

	// Test HashSSZ
	hash, err = hashVector.HashSSZ()
	require.NoError(t, err)
	assert.NotNil(t, hash)

	// Test Clone
	assert.NotNil(t, hashVector.Clone())
}

func TestByteBasedUint64Slice(t *testing.T) {
	sliceSize := 10
	uint64Slice := NewUint64Slice(sliceSize)
	assert.NotNil(t, uint64Slice)

	// Test Cap and Length
	assert.Equal(t, sliceSize, uint64Slice.Cap())
	assert.Equal(t, 0, uint64Slice.Length())

	// Test Set, Get and Append
	var testValue uint64 = 123456789
	uint64Slice.Append(testValue)
	assert.Equal(t, testValue, uint64Slice.Get(0))

	// Test Encoding and Decoding
	buf, err := uint64Slice.EncodeSSZ(nil)
	require.NoError(t, err)
	newUint64Slice := NewUint64Slice(sliceSize)
	err = newUint64Slice.DecodeSSZ(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64Slice, newUint64Slice)

	// Test HashSSZ
	hash, err := uint64Slice.HashVectorSSZ()
	require.NoError(t, err)
	assert.NotNil(t, hash)

	// Test Pop
	popped := uint64Slice.Pop()
	assert.Equal(t, testValue, popped)
	assert.Equal(t, 0, uint64Slice.Length())
}

func TestNewHashList(t *testing.T) {
	h := NewHashList(10)
	assert.Equal(t, 0, h.Length(), "Newly created hashList should be empty")
}

func TestHashListAppend(t *testing.T) {
	h := NewHashList(10)
	hash := common.Hash{1, 2, 3}
	h.Append(hash)
	assert.Equal(t, 1, h.Length(), "HashList length should be 1 after appending a value")
	assert.Equal(t, hash, h.Get(0), "Value in HashList should match the appended value")
}

func TestHashListSetGet(t *testing.T) {
	h := NewHashList(10)
	hash := common.Hash{1, 2, 3}
	hashNew := common.Hash{4, 5, 6}
	h.Append(hash)
	h.Set(0, hashNew)
	assert.Equal(t, hashNew, h.Get(0), "Value in HashList should match the set value")
}

func TestHashListClear(t *testing.T) {
	h := NewHashList(10)
	hash := common.Hash{1, 2, 3}
	h.Append(hash)
	h.Clear()
	assert.Equal(t, 0, h.Length(), "HashList should be empty after clear")
}

func TestHashListEncodeDecodeSSZ(t *testing.T) {
	h := NewHashList(10)
	hash := common.Hash{1, 2, 3}
	h.Append(hash)

	encoded, err := h.EncodeSSZ([]byte{})
	assert.NoError(t, err, "EncodeSSZ should not return an error")

	hDecoded := NewHashList(10)
	err = hDecoded.DecodeSSZ(encoded, 0)
	assert.NoError(t, err, "DecodeSSZ should not return an error")

	assert.Equal(t, h.Length(), hDecoded.Length(), "Lengths should match after decoding")
	assert.Equal(t, h.Get(0), hDecoded.Get(0), "Values should match after decoding")
}

func TestNewUint64ListSSZ(t *testing.T) {
	h := NewUint64ListSSZ(10)
	assert.Equal(t, 0, h.Length(), "Newly created Uint64ListSSZ should be empty")
}

func TestUint64ListSSZAppend(t *testing.T) {
	h := NewUint64ListSSZ(10)
	h.Append(123)
	assert.Equal(t, 1, h.Length(), "Uint64ListSSZ length should be 1 after appending a value")
	assert.Equal(t, uint64(123), h.Get(0), "Value in Uint64ListSSZ should match the appended value")
}

func TestUint64ListSSZSetGet(t *testing.T) {
	h := NewUint64ListSSZ(10)
	h.Append(123)
	h.Set(0, 456)
	assert.Equal(t, uint64(456), h.Get(0), "Value in Uint64ListSSZ should match the set value")
}

func TestUint64ListSSZClear(t *testing.T) {
	h := NewUint64ListSSZ(10)
	h.Append(123)
	h.Clear()
	assert.Equal(t, 0, h.Length(), "Uint64ListSSZ should be empty after clear")
}

func TestUint64ListSSZEncodeDecodeSSZ(t *testing.T) {
	h := NewUint64ListSSZ(10)
	h.Append(123)

	encoded, err := h.EncodeSSZ([]byte{})
	assert.NoError(t, err, "EncodeSSZ should not return an error")

	hDecoded := NewUint64ListSSZ(10)
	err = hDecoded.DecodeSSZ(encoded, 0)
	assert.NoError(t, err, "DecodeSSZ should not return an error")

	assert.Equal(t, h.Length(), hDecoded.Length(), "Lengths should match after decoding")
	assert.Equal(t, h.Get(0), hDecoded.Get(0), "Values should match after decoding")
}

func TestIsUint64SortedSet(t *testing.T) {
	h := NewUint64ListSSZFromSlice(10, []uint64{1, 2, 3, 4, 5})
	assert.True(t, IsUint64SortedSet(h), "IsUint64SortedSet should return true for sorted set")

	h = NewUint64ListSSZFromSlice(10, []uint64{5, 4, 3, 2, 1})
	assert.False(t, IsUint64SortedSet(h), "IsUint64SortedSet should return false for unsorted set")
}

func TestIntersectionOfSortedSets(t *testing.T) {
	h1 := NewUint64ListSSZFromSlice(10, []uint64{1, 2, 3, 4, 5})
	h2 := NewUint64ListSSZFromSlice(10, []uint64{4, 5, 6, 7, 8})

	intersection := IntersectionOfSortedSets(h1, h2)
	expected := []uint64{4, 5}
	assert.Equal(t, expected, intersection, "IntersectionOfSortedSets should return correct intersection")
}

func TestTransactionsSSZ(t *testing.T) {
	// Create sample transactions
	transactions := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}

	// Test NewTransactionsSSZFromTransactions
	transactionsSSZ := NewTransactionsSSZFromTransactions(transactions)
	assert.NotNil(t, transactionsSSZ)
	assert.Equal(t, transactions, transactionsSSZ.underlying)

	// Test DecodeSSZ
	encodedData, err := transactionsSSZ.EncodeSSZ(nil)
	require.NoError(t, err)
	decodedTransactionsSSZ := &TransactionsSSZ{}
	err = decodedTransactionsSSZ.DecodeSSZ(encodedData, len(encodedData))
	require.NoError(t, err)
	assert.Equal(t, transactionsSSZ, decodedTransactionsSSZ)

	// Test EncodeSSZ
	encodedData, err = transactionsSSZ.EncodeSSZ(nil)
	require.NoError(t, err)
	assert.NotEmpty(t, encodedData)

	// Test HashSSZ
	expectedRoot := common.HexToHash("55b3a5969a59aaac27189b17dba3e6f17f64ff9b9f52734cafa9fd5d9010cb3b") // Example expected root
	root, err := transactionsSSZ.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedRoot, common.Hash(root))

	// Test EncodingSizeSSZ
	expectedEncodingSize := len(encodedData)
	encodingSize := transactionsSSZ.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test UnderlyngReference
	gotTransactions := transactionsSSZ.UnderlyngReference()
	assert.Equal(t, transactions, gotTransactions)

	// Test ForEach
	var visitedTransactions [][]byte
	transactionsSSZ.ForEach(func(tx []byte, idx int, total int) bool {
		visitedTransactions = append(visitedTransactions, tx)
		return true
	})
	assert.Equal(t, transactions, visitedTransactions)
}
