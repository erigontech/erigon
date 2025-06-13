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

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock data
var (
	mockRoot  = common.HexToHash("0x01")
	mockEpoch = uint64(12345)
)

func TestNewDynamicListSSZ(t *testing.T) {
	limit := 10
	dynamicList := NewDynamicListSSZ[Validator](limit)

	assert.Equal(t, 0, dynamicList.Len())
	assert.False(t, dynamicList.Static())
}

func TestNewStaticListSSZ(t *testing.T) {
	limit := 10
	bytesPerElement := 40
	staticList := NewStaticListSSZ[Validator](limit, bytesPerElement)

	assert.Equal(t, 0, staticList.Len())
	assert.False(t, staticList.Static())
}

func TestListSSZAppendAndClear(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Validator](limit)

	// create a new checkpoint
	list.Append(NewValidator())

	assert.Equal(t, 1, list.Len())

	list.Clear()
	assert.Equal(t, 0, list.Len())
}

func TestListSSZClone(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Validator](limit)

	// create a new checkpoint
	checkpoint := NewValidator()
	list.Append(checkpoint)

	clone := list.Clone().(*ListSSZ[Validator])
	assert.NotEqual(t, list.Len(), clone.Len())
}

func TestListSSZEncodeDecodeSSZ(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Validator](limit)

	// create a new checkpoint
	checkpoint := NewValidator()
	list.Append(checkpoint)

	encoded, err := list.EncodeSSZ(nil)
	require.NoError(t, err)

	decodedList := NewDynamicListSSZ[Validator](limit)
	err = decodedList.DecodeSSZ(encoded, 0)
	require.NoError(t, err)

	assert.Equal(t, list.Len(), decodedList.Len())
}

func TestUint64VectorSSZ(t *testing.T) {
	// Test NewUint64VectorSSZ
	size := 5
	arr := NewUint64VectorSSZ(size)
	assert.NotNil(t, arr)
	assert.Equal(t, size, arr.Length())
	// Test Static
	assert.True(t, arr.Static())

	// Test CopyTo
	otherArr := NewUint64VectorSSZ(size)
	arr.Set(0, 10)
	arr.Set(1, 20)
	arr.CopyTo(otherArr)
	assert.Equal(t, arr.Length(), otherArr.Length())
	assert.Equal(t, arr.Get(0), otherArr.Get(0))
	assert.Equal(t, arr.Get(1), otherArr.Get(1))

	// Test Range
	var result []uint64
	arr.Range(func(index int, value uint64, length int) bool {
		result = append(result, value)
		return true
	})
	expectedResult := []uint64{10, 20, 0x0, 0x0, 0x0}
	assert.Equal(t, expectedResult, result)

	// Test Get and Set
	arr.Set(2, 30)
	assert.Equal(t, uint64(30), arr.Get(2))

	// Test Length
	assert.Equal(t, size, arr.Length())

	// Test Cap
	assert.Equal(t, size, arr.Cap())

	// Test Clone
	clone := arr.Clone().(*uint64VectorSSZ)
	assert.Equal(t, arr.Length(), clone.Length())

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := arr.EncodeSSZ(nil)
	require.NoError(t, err)
	decodedArr := NewUint64VectorSSZ(size)
	err = decodedArr.DecodeSSZ(encodedData, 0)
	require.NoError(t, err)
	assert.Equal(t, arr.Length(), decodedArr.Length())
	assert.Equal(t, arr.Get(0), decodedArr.Get(0))
	assert.Equal(t, arr.Get(1), decodedArr.Get(1))

	// Test EncodingSizeSSZ
	expectedEncodingSize := arr.Length() * 8
	encodingSize := arr.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)
}
