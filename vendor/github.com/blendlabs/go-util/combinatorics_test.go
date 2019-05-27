package util

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestCombinationsOfInt(t *testing.T) {
	assert := assert.New(t)

	combinations := Combinatorics.CombinationsOfInt(1, 2, 3, 4)
	assert.Len(combinations, 15)
	assert.Len(combinations[0], 4)
}

func TestCombinationsOfFloat(t *testing.T) {
	assert := assert.New(t)

	combinations := Combinatorics.CombinationsOfFloat(1.0, 2.0, 3.0, 4.0)
	assert.Len(combinations, 15)
	assert.Len(combinations[0], 4)
}

func TestCombinationsOfString(t *testing.T) {
	assert := assert.New(t)

	combinations := Combinatorics.CombinationsOfString("a", "b", "c", "d")
	assert.Len(combinations, 15)
	assert.Len(combinations[0], 4)
}

func TestPermutationsOfInt(t *testing.T) {
	assert := assert.New(t)

	permutations := Combinatorics.PermutationsOfInt(123, 216, 4, 11)
	assert.Len(permutations, 24)
	assert.Len(permutations[0], 4)
}

func TestPermutationsOfFloat(t *testing.T) {
	assert := assert.New(t)

	permutations := Combinatorics.PermutationsOfFloat(3.14, 2.57, 1.0, 6.28)
	assert.Len(permutations, 24)
	assert.Len(permutations[0], 4)
}

func TestPermutationsOfString(t *testing.T) {
	assert := assert.New(t)

	permutations := Combinatorics.PermutationsOfString("a", "b", "c", "d")
	assert.Len(permutations, 24)
	assert.Len(permutations[0], 4)
}

func TestPermuteDistributions(t *testing.T) {
	assert := assert.New(t)

	permutations := Combinatorics.PermuteDistributions(4, 2)
	assert.Len(permutations, 5)

	assert.Len(permutations[0], 2)
}

func TestPairsOfInt(t *testing.T) {
	assert := assert.New(t)

	pairs := Combinatorics.PairsOfInt(1, 2, 3, 4, 5)
	assert.Len(pairs, 10)
	assert.Equal(4, pairs[9][0])
	assert.Equal(5, pairs[9][1])
}

func TestPairsOfFloat64(t *testing.T) {
	assert := assert.New(t)

	pairs := Combinatorics.PairsOfFloat64(1, 2, 3, 4, 5)
	assert.Len(pairs, 10)
	assert.Equal(4, pairs[9][0])
	assert.Equal(5, pairs[9][1])
}

type any = interface{}

func TestAnagrams(t *testing.T) {
	assert := assert.New(t)

	words := Combinatorics.Anagrams("abcde")
	assert.Len(words, 120)
	assert.Any(words, func(v any) bool {
		return v.(string) == "abcde"
	})
	assert.Any(words, func(v any) bool {
		return v.(string) == "ecdab"
	})
}
