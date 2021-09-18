/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package recsplit

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	log2q      uint64 = 8
	q          uint64 = 1 << log2q
	qMask      uint64 = q - 1
	superQ     uint64 = 1 << 14
	superQMask uint64 = superQ - 1
	qPerSuperQ uint64 = superQ / q       // 64
	superQSize uint64 = 1 + qPerSuperQ/4 // 1 + 64/4 = 17
)

// DoubleEliasFano can be used to encde a monotone sequence
// it is called "double" because the lower bits array contains two sequences interleaved
type DoubleEliasFano struct {
	data                  []uint64
	lowerBits             []uint64
	upperBitsPosition     []uint64
	upperBitsCumKeys      []uint64
	jump                  []uint64
	lowerBitsMaskCumKeys  uint64
	lowerBitsMaskPosition uint64
	numBuckets            uint64
	uCumKeys              uint64
	uPosition             uint64
	lPosition             uint64
	lCumKeys              uint64
	cumKeysMinDelta       uint64
	posMinDelta           uint64
}

// Build construct double Elias Fano index for two given sequences
func (ef *DoubleEliasFano) Build(cumKeys []uint64, position []uint64) {
	//fmt.Printf("cumKeys = %d\nposition = %d\n", cumKeys, position)
	if len(cumKeys) != len(position) {
		panic("len(cumKeys) != len(position)")
	}
	ef.numBuckets = uint64(len(cumKeys) - 1)
	ef.posMinDelta = math.MaxUint64
	ef.cumKeysMinDelta = math.MaxUint64
	for i := uint64(1); i <= ef.numBuckets; i++ {
		if cumKeys[i] < cumKeys[i-1] {
			panic("cumKeys[i] <= cumKeys[i-1]")
		}
		nkeysDelta := cumKeys[i] - cumKeys[i-1]
		if nkeysDelta < ef.cumKeysMinDelta {
			ef.cumKeysMinDelta = nkeysDelta
		}
		if position[i] < position[i-1] {
			panic("position[i] < position[i-1]")
		}
		bucketBits := position[i] - position[i-1]
		if bucketBits < ef.posMinDelta {
			ef.posMinDelta = bucketBits
		}
	}
	//fmt.Printf("cumKeysMinDelta = %d, posMinDelta = %d\n", ef.cumKeysMinDelta, ef.posMinDelta)
	ef.uPosition = position[ef.numBuckets] - ef.numBuckets*ef.posMinDelta + 1
	if ef.uPosition/(ef.numBuckets+1) == 0 {
		ef.lPosition = 0
	} else {
		ef.lPosition = 63 ^ uint64(bits.LeadingZeros64(ef.uPosition/(ef.numBuckets+1)))
	}
	ef.uCumKeys = cumKeys[ef.numBuckets] - ef.numBuckets*ef.cumKeysMinDelta + 1 // Largest possible encoding of the cumKeys
	if ef.uCumKeys/(ef.numBuckets+1) == 0 {
		ef.lCumKeys = 0
	} else {
		ef.lCumKeys = 63 ^ uint64(bits.LeadingZeros64(ef.uCumKeys/(ef.numBuckets+1)))
	}
	//fmt.Printf("uPosition = %d, lPosition = %d, uCumKeys = %d, lCumKeys = %d\n", ef.uPosition, ef.lPosition, ef.uCumKeys, ef.lCumKeys)
	if ef.lCumKeys*2+ef.lPosition > 56 {
		panic(fmt.Sprintf("ef.lCumKeys (%d) * 2 + ef.lPosition (%d) > 56", ef.lCumKeys, ef.lPosition))
	}
	ef.lowerBitsMaskCumKeys = (uint64(1) << ef.lCumKeys) - 1
	ef.lowerBitsMaskPosition = (uint64(1) << ef.lPosition) - 1
	wordsLowerBits := int(((ef.numBuckets+1)*(ef.lCumKeys+ef.lPosition)+63)/64 + 1)
	wordsCumKeys := int((ef.numBuckets + 1 + (ef.uCumKeys >> ef.lCumKeys) + 63) / 64)
	wordsPosition := int((ef.numBuckets + 1 + (ef.uPosition >> ef.lPosition) + 63) / 64)
	jumpWords := ef.jumpSizeWords()
	//fmt.Printf("wordsLowerBits = %d, wordsCumKeys = %d, wordsPosition = %d, jumpWords = %d\n", wordsLowerBits, wordsCumKeys, wordsPosition, jumpWords)
	totalWords := wordsLowerBits + wordsCumKeys + wordsPosition + jumpWords
	ef.data = make([]uint64, totalWords)
	ef.lowerBits = ef.data[:wordsLowerBits]
	ef.upperBitsCumKeys = ef.data[wordsLowerBits : wordsLowerBits+wordsCumKeys]
	ef.upperBitsPosition = ef.data[wordsLowerBits+wordsCumKeys : wordsLowerBits+wordsCumKeys+wordsPosition]
	ef.jump = ef.data[wordsLowerBits+wordsCumKeys+wordsPosition:]

	for i, cumDelta, bitDelta := uint64(0), uint64(0), uint64(0); i <= ef.numBuckets; i, cumDelta, bitDelta = i+1, cumDelta+ef.cumKeysMinDelta, bitDelta+ef.posMinDelta {
		if ef.lCumKeys != 0 {
			//fmt.Printf("i=%d, set_bits cum for %d = %b\n", i, cumKeys[i]-cumDelta, (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition), int(ef.lCumKeys), (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
			//fmt.Printf("loweBits %b\n", ef.lowerBits)
		}
		set(ef.upperBitsCumKeys, ((cumKeys[i]-cumDelta)>>ef.lCumKeys)+i)
		//fmt.Printf("i=%d, set cum for %d = %d\n", i, cumKeys[i]-cumDelta, (cumKeys[i]-cumDelta)>>ef.lCumKeys+i)

		if ef.lPosition != 0 {
			//fmt.Printf("i=%d, set_bits pos for %d = %b\n", i, position[i]-bitDelta, (position[i]-bitDelta)&ef.lowerBitsMaskPosition)
			set_bits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition)+ef.lCumKeys, int(ef.lPosition), (position[i]-bitDelta)&ef.lowerBitsMaskPosition)
			//fmt.Printf("lowerBits %b\n", ef.lowerBits)
		}
		set(ef.upperBitsPosition, ((position[i]-bitDelta)>>ef.lPosition)+i)
		//fmt.Printf("i=%d, set pos for %d = %d\n", i, position[i]-bitDelta, (position[i]-bitDelta)>>ef.lPosition+i)
	}
	//fmt.Printf("loweBits %b\n", ef.lowerBits)
	//fmt.Printf("upperBitsCumKeys %b\n", ef.upperBitsCumKeys)
	//fmt.Printf("upperBitsPosition %b\n", ef.upperBitsPosition)
	// i iterates over the 64-bit words in the wordCumKeys vector
	// c iterates over bits in the wordCumKeys
	// lastSuperQ is the largest multiple of 2^14 (4096) which is no larger than c
	// c/superQ is the index of the current 4096 block of bits
	// superQSize is how many words is required to encode one block of 4096 bits. It is 17 words which is 1088 bits
	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(wordsCumKeys); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBitsCumKeys[i]&(uint64(1)<<b) != 0 {
				if (c & superQMask) == 0 {
					// When c is multiple of 2^14 (4096)
					lastSuperQ = i*64 + b
					ef.jump[(c/superQ)*(superQSize*2)] = lastSuperQ
				}
				if (c & qMask) == 0 {
					// When c is multiple of 2^8 (256)
					var offset = i*64 + b - lastSuperQ // offset can be either 0, 256, 512, 768, ..., up to 4096-256
					// offset needs to be encoded as 16-bit integer, therefore the following check
					if offset >= (1 << 16) {
						panic("")
					}
					// c % superQ is the bit index inside the group of 4096 bits
					idx16 := 2 * ((c % superQ) / q)
					idx64 := (c/superQ)*(superQSize*2) + 2 + (idx16 >> 2)
					shift := 16 * (idx16 % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}

	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(wordsPosition); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBitsPosition[i]&(uint64(1)<<b) != 0 {
				if (c & superQMask) == 0 {
					lastSuperQ = i*64 + b
					ef.jump[(c/superQ)*(superQSize*2)+1] = lastSuperQ
				}
				if (c & qMask) == 0 {
					var offset = i*64 + b - lastSuperQ
					if offset >= (1 << 16) {
						panic("")
					}
					idx16 := 2*((c%superQ)/q) + 1
					idx64 := (c/superQ)*(superQSize*2) + 2 + (idx16 >> 2)
					shift := 16 * (idx16 % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}
	//fmt.Printf("jump: %x\n", ef.jump)
}

// set_bits assumes that bits are set in monotonic order, so that
// we can skip the masking for the second word
func set_bits(bits []uint64, start uint64, width int, value uint64) {
	shift := int(start & 63)
	idx64 := start >> 6
	mask := (uint64(1)<<width - 1) << shift
	//fmt.Printf("mask = %b, idx64 = %d\n", mask, idx64)
	bits[idx64] = (bits[idx64] &^ mask) | (value << shift)
	//fmt.Printf("start = %d, width = %d, shift + width = %d\n", start, width, shift+width)
	if shift+width > 64 {
		// changes two 64-bit words
		bits[idx64+1] = value >> (64 - shift)
	}
}

func set(bits []uint64, pos uint64) {
	bits[pos>>6] |= uint64(1) << (pos & 63)
}

func (ef DoubleEliasFano) jumpSizeWords() int {
	size := ((ef.numBuckets + 1) / superQ) * superQSize * 2 // Whole blocks
	if (ef.numBuckets+1)%superQ != 0 {
		size += (1 + (((ef.numBuckets+1)%superQ+q-1)/q+3)/4) * 2 // Partial block
	}
	return int(size)
}

// Data returns binary representation of double Ellias-Fano index that has been built
func (ef DoubleEliasFano) Data() []uint64 {
	return ef.data
}

func (ef DoubleEliasFano) get2(i uint64) (cumKeys uint64, position uint64,
	windowCumKeys uint64, selectCumKeys int, currWordCumKeys uint64, lower uint64, cumDelta uint64) {
	posLower := i * (ef.lCumKeys + ef.lPosition)
	idx64 := posLower / 64
	shift := posLower % 64
	lower = ef.lowerBits[idx64] >> shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] << (64 - shift)
	}
	//fmt.Printf("i = %d, posLower = %d, lower = %b\n", i, posLower, lower)

	jumpSuperQ := (i / superQ) * superQSize * 2
	jumpInsideSuperQ := (i % superQ) / q
	idx16 := 4*(jumpSuperQ+2) + 2*jumpInsideSuperQ
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask := uint64(0xffff) << shift
	jumpCumKeys := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift
	idx16++
	idx64 = idx16 / 4
	shift = 16 * (idx16 % 4)
	mask = uint64(0xffff) << shift
	jumpPosition := ef.jump[jumpSuperQ+1] + (ef.jump[idx64]&mask)>>shift
	//fmt.Printf("i = %d, jumpCumKeys = %d, jumpPosition = %d\n", i, jumpCumKeys, jumpPosition)

	currWordCumKeys = jumpCumKeys / 64
	currWordPosition := jumpPosition / 64
	windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys] & (uint64(0xffffffffffffffff) << (jumpCumKeys % 64))
	windowPosition := ef.upperBitsPosition[currWordPosition] & (uint64(0xffffffffffffffff) << (jumpPosition % 64))
	deltaCumKeys := int(i & qMask)
	deltaPosition := int(i & qMask)

	for bitCount := bits.OnesCount64(windowCumKeys); bitCount <= deltaCumKeys; bitCount = bits.OnesCount64(windowCumKeys) {
		//fmt.Printf("i = %d, bitCount cum = %d\n", i, bitCount)
		currWordCumKeys++
		windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys]
		deltaCumKeys -= bitCount
	}
	for bitCount := bits.OnesCount64(windowPosition); bitCount <= deltaPosition; bitCount = bits.OnesCount64(windowPosition) {
		//fmt.Printf("i = %d, bitCount pos = %d\n", i, bitCount)
		currWordPosition++
		windowPosition = ef.upperBitsPosition[currWordPosition]
		deltaPosition -= bitCount
	}

	selectCumKeys = select64(windowCumKeys, deltaCumKeys)
	//fmt.Printf("i = %d, select cum in %b for %d = %d\n", i, windowCumKeys, deltaCumKeys, selectCumKeys)
	cumDelta = i * ef.cumKeysMinDelta
	cumKeys = ((currWordCumKeys*64+uint64(selectCumKeys)-i)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta

	lower >>= ef.lCumKeys
	//fmt.Printf("i = %d, lower = %b\n", i, lower)
	selectPosition := select64(windowPosition, deltaPosition)
	//fmt.Printf("i = %d, select pos in %b for %d = %d\n", i, windowPosition, deltaPosition, selectPosition)
	bitDelta := i * ef.posMinDelta
	position = ((currWordPosition*64+uint64(selectPosition)-i)<<ef.lPosition | (lower & ef.lowerBitsMaskPosition)) + bitDelta
	return
}

func (ef DoubleEliasFano) Get2(i uint64) (cumKeys uint64, position uint64) {
	cumKeys, position, _, _, _, _, _ = ef.get2(i)
	return
}

func (ef DoubleEliasFano) Get3(i uint64) (cumKeys uint64, cumKeysNext uint64, position uint64) {
	var windowCumKeys uint64
	var selectCumKeys int
	var currWordCumKeys uint64
	var lower uint64
	var cumDelta uint64
	cumKeys, position, windowCumKeys, selectCumKeys, currWordCumKeys, lower, cumDelta = ef.get2(i)
	windowCumKeys &= (uint64(0xffffffffffffffff) << selectCumKeys) << 1
	for windowCumKeys == 0 {
		currWordCumKeys++
		windowCumKeys = ef.upperBitsCumKeys[currWordCumKeys]
	}

	lower >>= ef.lPosition
	cumKeysNext = ((currWordCumKeys*64+uint64(bits.TrailingZeros64(windowCumKeys))-i-1)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta + ef.cumKeysMinDelta
	return
}
