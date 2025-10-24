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

package eth2shuffle_test

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/utils/eth2shuffle"
)

func getStandardHashFn() eth2shuffle.HashFn {
	hash := sha256.New()
	hashFn := func(in []byte) []byte {
		hash.Reset()
		hash.Write(in)
		return hash.Sum(nil)
	}
	return hashFn
}

func readEncodedListInput(input string, requiredLen int64, lineIndex int) ([]uint64, error) {
	var itemStrs []string
	if input != "" {
		itemStrs = strings.Split(input, ":")
	} else {
		itemStrs = make([]string, 0)
	}
	if int64(len(itemStrs)) != requiredLen {
		return nil, fmt.Errorf("expected outputs length does not match list size on line %d\n", lineIndex)
	}
	items := make([]uint64, len(itemStrs), len(itemStrs))
	for i, itemStr := range itemStrs {
		item, err := strconv.ParseInt(itemStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected list item on line %d, item %d cannot be parsed\n", lineIndex, i)
		}
		items[i] = uint64(item)
	}
	return items, nil
}

func TestAgainstSpec(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Open CSV file
	f, err := os.Open("spec/tests.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	// constant in spec
	rounds := uint8(90)

	// Loop through lines & turn into object
	for lineIndex, line := range lines {

		parsedSeed, err := hex.DecodeString(line[0])
		if err != nil {
			t.Fatalf("seed on line %d cannot be parsed\n", lineIndex)
		}
		listSize, err := strconv.ParseInt(line[1], 10, 32)
		if err != nil {
			t.Fatalf("list size on line %d cannot be parsed\n", lineIndex)
		}
		inputItems, err := readEncodedListInput(line[2], listSize, lineIndex)
		require.NoError(t, err)
		expectedItems, err := readEncodedListInput(line[3], listSize, lineIndex)
		require.NoError(t, err)

		t.Run("", func(listSize uint64, shuffleIn []uint64, shuffleOut []uint64) func(st *testing.T) {
			return func(st *testing.T) {
				seed := [32]byte{}
				copy(seed[:], parsedSeed)
				// run every test case in parallel. Input data is copied, for loop won't mess it up.
				st.Parallel()

				hashFn := getStandardHashFn()

				st.Run("PermuteIndex", func(it *testing.T) {
					for i := uint64(0); i < listSize; i++ {
						// calculate the permuted index. (i.e. shuffle single index)
						permuted := eth2shuffle.PermuteIndex(hashFn, rounds, i, listSize, seed)
						// compare with expectation
						if shuffleIn[i] != shuffleOut[permuted] {
							it.FailNow()
						}
					}
				})

				st.Run("UnpermuteIndex", func(it *testing.T) {
					// for each index, test un-permuting
					for i := uint64(0); i < listSize; i++ {
						// calculate the un-permuted index. (i.e. un-shuffle single index)
						unpermuted := eth2shuffle.UnpermuteIndex(hashFn, rounds, i, listSize, seed)
						// compare with expectation
						if shuffleOut[i] != shuffleIn[unpermuted] {
							it.FailNow()
						}
					}
				})

				st.Run("ShuffleList", func(it *testing.T) {
					// create input, this slice will be shuffled.
					testInput := make([]uint64, listSize, listSize)
					copy(testInput, shuffleIn)
					// shuffle!
					eth2shuffle.ShuffleList(hashFn, testInput, rounds, seed)
					// compare shuffled list to expected output
					for i := uint64(0); i < listSize; i++ {
						if testInput[i] != shuffleOut[i] {
							it.FailNow()
						}
					}
				})

				st.Run("UnshuffleList", func(it *testing.T) {
					// create input, this slice will be un-shuffled.
					testInput := make([]uint64, listSize, listSize)
					copy(testInput, shuffleOut)
					// un-shuffle!
					eth2shuffle.UnshuffleList(hashFn, testInput, rounds, seed)
					// compare shuffled list to original input
					for i := uint64(0); i < listSize; i++ {
						if testInput[i] != shuffleIn[i] {
							it.FailNow()
						}
					}
				})
			}
		}(uint64(listSize), inputItems, expectedItems))
	}
}
