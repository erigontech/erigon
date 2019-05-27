package util

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestSequenceInts(t *testing.T) {
	assert := assert.New(t)

	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	actual := Sequence.Ints(10)

	assert.Equal(len(actual), len(expected))
	for i := 0; i < len(expected); i++ {
		assert.Equal(expected[i], actual[i])
	}
}

func TestSequenceIntsFrom(t *testing.T) {
	assert := assert.New(t)

	expected := []int{5, 6, 7, 8, 9}
	actual := Sequence.IntsFrom(5, 10)

	assert.Equal(len(actual), len(expected))
	for i := 0; i < len(expected); i++ {
		assert.Equal(expected[i], actual[i])
	}
}

func TestSequenceIntsFromBy(t *testing.T) {
	assert := assert.New(t)

	expected := []int{0, 2, 4, 6, 8}
	actual := Sequence.IntsFromBy(0, 10, 2)

	assert.Equal(len(actual), len(expected))
	for i := 0; i < len(expected); i++ {
		assert.Equal(expected[i], actual[i])
	}
}
