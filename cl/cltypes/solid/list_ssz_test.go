package solid

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

// Mock data
var (
	mockRoot  = common.HexToHash("0x01")
	mockEpoch = uint64(12345)
)

func TestNewDynamicListSSZ(t *testing.T) {
	limit := 10
	dynamicList := NewDynamicListSSZ[Checkpoint](limit)

	assert.Equal(t, 0, dynamicList.Len())
	assert.Equal(t, false, dynamicList.Static())
}

func TestNewStaticListSSZ(t *testing.T) {
	limit := 10
	bytesPerElement := 40
	staticList := NewStaticListSSZ[Checkpoint](limit, bytesPerElement)

	assert.Equal(t, 0, staticList.Len())
	assert.Equal(t, false, staticList.Static())
}

func TestListSSZAppendAndClear(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Checkpoint](limit)

	// create a new checkpoint
	checkpoint := NewCheckpointFromParameters(mockRoot, mockEpoch)
	list.Append(checkpoint)

	assert.Equal(t, 1, list.Len())

	list.Clear()
	assert.Equal(t, 0, list.Len())
}

func TestListSSZClone(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Checkpoint](limit)

	// create a new checkpoint
	checkpoint := NewCheckpointFromParameters(mockRoot, mockEpoch)
	list.Append(checkpoint)

	clone := list.Clone().(*ListSSZ[Checkpoint])
	assert.NotEqual(t, list.Len(), clone.Len())
}

func TestListSSZEncodeDecodeSSZ(t *testing.T) {
	limit := 10
	list := NewDynamicListSSZ[Checkpoint](limit)

	// create a new checkpoint
	checkpoint := NewCheckpointFromParameters(mockRoot, mockEpoch)
	list.Append(checkpoint)

	encoded, err := list.EncodeSSZ(nil)
	assert.NoError(t, err)

	decodedList := NewDynamicListSSZ[Checkpoint](limit)
	err = decodedList.DecodeSSZ(encoded, 0)
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	decodedArr := NewUint64VectorSSZ(size)
	err = decodedArr.DecodeSSZ(encodedData, 0)
	assert.NoError(t, err)
	assert.Equal(t, arr.Length(), decodedArr.Length())
	assert.Equal(t, arr.Get(0), decodedArr.Get(0))
	assert.Equal(t, arr.Get(1), decodedArr.Get(1))

	// Test EncodingSizeSSZ
	expectedEncodingSize := arr.Length() * 8
	encodingSize := arr.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)
}
