package util

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestBitFlagCombine(t *testing.T) {
	assert := assert.New(t)

	three := BitFlag.Combine(1, 2)
	assert.Equal(3, three)
}

func TestBitFlagAny(t *testing.T) {
	assert := assert.New(t)

	var one uint64 = 1 << 0
	var two uint64 = 1 << 1
	var four uint64 = 1 << 2
	var eight uint64 = 1 << 3
	var sixteen uint64 = 1 << 4
	var invalid uint64 = 1 << 5

	masterFlag := BitFlag.Combine(one, two, four, eight)
	checkFlag := BitFlag.Combine(one, sixteen)
	assert.True(BitFlag.Any(masterFlag, checkFlag))
	assert.False(BitFlag.Any(masterFlag, invalid))
}

func TestBitFlagAll(t *testing.T) {
	assert := assert.New(t)

	var one uint64 = 1 << 0
	var two uint64 = 1 << 1
	var four uint64 = 1 << 2
	var eight uint64 = 1 << 3
	var sixteen uint64 = 1 << 4

	masterFlag := BitFlag.Combine(one, two, four, eight)
	checkValidFlag := BitFlag.Combine(one, two)
	checkInvalidFlag := BitFlag.Combine(one, sixteen)
	assert.True(BitFlag.All(masterFlag, checkValidFlag))
	assert.False(BitFlag.All(masterFlag, checkInvalidFlag))
}

func TestBitFlagSet(t *testing.T) {
	assert := assert.New(t)

	var zero uint64
	var four uint64 = 1 << 4
	assert.Equal(16, BitFlag.Set(zero, 1<<4))
	assert.Equal(16, BitFlag.Set(four, 1<<4))
}

func TestBitFlagZero(t *testing.T) {
	assert := assert.New(t)

	var zero uint64
	var one uint64 = 1 << 0
	var four uint64 = 1 << 4

	var flagSet = BitFlag.Combine(one, four)
	assert.Equal(0, BitFlag.Zero(zero, 1<<4))
	assert.Equal(0, BitFlag.Zero(four, 1<<4))
	assert.Equal(16, BitFlag.Zero(flagSet, one))
}
