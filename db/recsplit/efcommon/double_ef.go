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

package efcommon

import (
	"fmt"
	"math/bits"
)

// DeriveDoubleEFFields computes the derived fields for a DoubleEliasFano struct.
// It is shared between eliasfano16 and eliasfano32 which define identical
// DoubleEliasFano structs but differ in jumpSizeWords (due to different
// superQSize constants).
//
// Parameters:
//   - numBuckets, uCumKeys, uPosition: header values read from the struct
//   - data: the backing slice (may be nil for a fresh build; will be allocated/resliced)
//   - jumpSizeWords: result of the package-specific jumpSizeWords() call
//
// Returns the derived values and resliced sub-slices that the caller
// must assign back to the struct fields.
func DeriveDoubleEFFields(
	numBuckets, uCumKeys, uPosition uint64,
	data []uint64,
	jumpSizeWords int,
) (result DeriveResult) {
	if uPosition/(numBuckets+1) == 0 {
		result.LPosition = 0
	} else {
		result.LPosition = 63 ^ uint64(bits.LeadingZeros64(uPosition/(numBuckets+1)))
	}
	if uCumKeys/(numBuckets+1) == 0 {
		result.LCumKeys = 0
	} else {
		result.LCumKeys = 63 ^ uint64(bits.LeadingZeros64(uCumKeys/(numBuckets+1)))
	}
	if result.LCumKeys*2+result.LPosition > 56 {
		panic(fmt.Sprintf("ef.lCumKeys (%d) * 2 + ef.lPosition (%d) > 56", result.LCumKeys, result.LPosition))
	}
	result.LowerBitsMaskCumKeys = (uint64(1) << result.LCumKeys) - 1
	result.LowerBitsMaskPosition = (uint64(1) << result.LPosition) - 1

	wordsLowerBits := int(((numBuckets+1)*(result.LCumKeys+result.LPosition)+63)/64 + 1)
	wordsCumKeys := int((numBuckets + 1 + (uCumKeys >> result.LCumKeys) + 63) / 64)
	wordsPosition := int((numBuckets + 1 + (uPosition >> result.LPosition) + 63) / 64)
	totalWords := wordsLowerBits + wordsCumKeys + wordsPosition + jumpSizeWords

	if data == nil {
		data = make([]uint64, totalWords)
	} else {
		data = data[:totalWords]
	}
	result.Data = data
	result.LowerBits = data[:wordsLowerBits]
	result.UpperBitsCumKeys = data[wordsLowerBits : wordsLowerBits+wordsCumKeys]
	result.UpperBitsPosition = data[wordsLowerBits+wordsCumKeys : wordsLowerBits+wordsCumKeys+wordsPosition]
	result.Jump = data[wordsLowerBits+wordsCumKeys+wordsPosition:]
	result.WordsCumKeys = wordsCumKeys
	result.WordsPosition = wordsPosition
	return result
}

// DeriveResult holds all the values computed by DeriveDoubleEFFields
// that the caller must assign back to the DoubleEliasFano struct.
type DeriveResult struct {
	Data                  []uint64
	LowerBits             []uint64
	UpperBitsPosition     []uint64
	UpperBitsCumKeys      []uint64
	Jump                  []uint64
	LowerBitsMaskCumKeys  uint64
	LowerBitsMaskPosition uint64
	LPosition             uint64
	LCumKeys              uint64
	WordsCumKeys          int
	WordsPosition         int
}
