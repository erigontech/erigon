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

package eliasfano16

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common/bitutil"
)

// EliasFano algo overview https://www.antoniomallia.it/sorted-integers-compression-with-elias-fano-encoding.html
// P. Elias. Efficient storage and retrieval by content and address of static files. J. ACM, 21(2):246–260, 1974.
// Partitioned Elias-Fano Indexes http://groups.di.unipi.it/~ottavian/files/elias_fano_sigir14.pdf

const (
	log2q      uint64 = 8
	q          uint64 = 1 << log2q
	qMask             = q - 1
	superQ     uint64 = 1 << 14
	superQMask        = superQ - 1
	qPerSuperQ        = superQ / q       // 64
	superQSize        = 1 + qPerSuperQ/4 // 1 + 64/4 = 17
)

// EliasFano can be used to encode one monotone sequence
type EliasFano struct {
	data           []uint64
	lowerBits      []uint64
	upperBits      []uint64
	jump           []uint64
	lowerBitsMask  uint64
	count          uint64
	u              uint64
	l              uint64
	maxOffset      uint64
	minDelta       uint64
	i              uint64
	delta          uint64
	wordsUpperBits int
}

func NewEliasFano(count uint64, maxOffset, minDelta uint64) *EliasFano {
	//fmt.Printf("count=%d,maxOffset=%d,minDelta=%d\n", count, maxOffset, minDelta)
	ef := &EliasFano{
		count:     count - 1,
		maxOffset: maxOffset,
		minDelta:  minDelta,
	}
	ef.u = maxOffset - ef.count*ef.minDelta + 1
	ef.wordsUpperBits = ef.deriveFields()
	return ef
}

func (ef *EliasFano) AddOffset(offset uint64) {
	//fmt.Printf("0x%x,\n", offset)
	if ef.l != 0 {
		setBits(ef.lowerBits, ef.i*ef.l, int(ef.l), (offset-ef.delta)&ef.lowerBitsMask)
	}
	//pos := ((offset - ef.delta) >> ef.l) + ef.i
	set(ef.upperBits, ((offset-ef.delta)>>ef.l)+ef.i)
	//fmt.Printf("add:%x, pos=%x, set=%x, res=%x\n", offset, pos, pos/64, uint64(1)<<(pos%64))
	ef.i++
	ef.delta += ef.minDelta
}

func (ef *EliasFano) jumpSizeWords() int {
	size := ((ef.count + 1) / superQ) * superQSize // Whole blocks
	if (ef.count+1)%superQ != 0 {
		size += 1 + (((ef.count+1)%superQ+q-1)/q+3)/4 // Partial block
	}
	return int(size)
}

func (ef *EliasFano) deriveFields() int {
	if ef.u/(ef.count+1) == 0 {
		ef.l = 0
	} else {
		ef.l = 63 ^ uint64(bits.LeadingZeros64(ef.u/(ef.count+1))) // pos of first non-zero bit
		//fmt.Printf("lllllllll: %d, %d\n", 63^uint64(bits.LeadingZeros64(24/7)), msb(ef.u/(ef.count+1)))
	}
	ef.lowerBitsMask = (uint64(1) << ef.l) - 1
	wordsLowerBits := int(((ef.count+1)*ef.l+63)/64 + 1)
	wordsUpperBits := int((ef.count + 1 + (ef.u >> ef.l) + 63) / 64)
	jumpWords := ef.jumpSizeWords()
	totalWords := wordsLowerBits + wordsUpperBits + jumpWords
	if ef.data == nil {
		ef.data = make([]uint64, totalWords)
	} else {
		ef.data = ef.data[:totalWords]
	}

	ef.lowerBits = ef.data[:wordsLowerBits]
	ef.upperBits = ef.data[wordsLowerBits : wordsLowerBits+wordsUpperBits]
	ef.jump = ef.data[wordsLowerBits+wordsUpperBits:]
	return wordsUpperBits
}

// Build construct Elias Fano index for a given sequences
func (ef *EliasFano) Build() {
	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(ef.wordsUpperBits); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBits[i]&(uint64(1)<<b) != 0 {
				if (c & superQMask) == 0 {
					// When c is multiple of 2^14 (4096)
					lastSuperQ = i*64 + b
					ef.jump[(c/superQ)*superQSize] = lastSuperQ
				}
				if (c & qMask) == 0 {
					// When c is multiple of 2^8 (256)
					var offset = i*64 + b - lastSuperQ // offset can be either 0, 256, 512, 768, ..., up to 4096-256
					// offset needs to be encoded as 16-bit integer, therefore the following check
					if offset >= (1 << 16) {
						fmt.Printf("ef.l=%x,ef.u=%x\n", ef.l, ef.u)
						fmt.Printf("offset=%x,lastSuperQ=%x,i=%x,b=%x,c=%x\n", offset, lastSuperQ, i, b, c)
						fmt.Printf("ef.minDelta=%x\n", ef.minDelta)
						//fmt.Printf("ef.upperBits=%x\n", ef.upperBits)
						//fmt.Printf("ef.lowerBits=%x\n", ef.lowerBits)
						//fmt.Printf("ef.wordsUpperBits=%b\n", ef.wordsUpperBits)
						panic("")
					}
					// c % superQ is the bit index inside the group of 4096 bits
					jumpSuperQ := (c / superQ) * superQSize
					jumpInsideSuperQ := (c % superQ) / q
					idx64 := jumpSuperQ + 1 + (jumpInsideSuperQ >> 2)
					shift := 16 * (jumpInsideSuperQ % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}
}

func (ef *EliasFano) get(i uint64) (val, window uint64, sel int, currWord, lower, delta uint64) {
	lower = i * ef.l
	idx64 := lower / 64
	shift := lower % 64
	lower = ef.lowerBits[idx64] >> shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] << (64 - shift)
	}

	jumpSuperQ := (i / superQ) * superQSize
	jumpInsideSuperQ := (i % superQ) / q
	idx64 = jumpSuperQ + 1 + (jumpInsideSuperQ >> 2)
	shift = 16 * (jumpInsideSuperQ % 4)
	mask := uint64(0xffff) << shift
	jump := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift

	currWord = jump / 64
	window = ef.upperBits[currWord] & (uint64(0xffffffffffffffff) << (jump % 64))
	d := int(i & qMask)

	for bitCount := bits.OnesCount64(window); bitCount <= d; bitCount = bits.OnesCount64(window) {
		currWord++
		window = ef.upperBits[currWord]
		d -= bitCount
	}

	sel = bitutil.Select64(window, d)
	delta = i * ef.minDelta
	val = ((currWord*64+uint64(sel)-i)<<ef.l | (lower & ef.lowerBitsMask)) + delta

	return
}

func (ef *EliasFano) Get(i uint64) uint64 {
	val, _, _, _, _, _ := ef.get(i)
	return val
}

func (ef *EliasFano) Get2(i uint64) (val, valNext uint64) {
	var window uint64
	var sel int
	var currWord uint64
	var lower uint64
	var delta uint64
	val, window, sel, currWord, lower, delta = ef.get(i)
	window &= (uint64(0xffffffffffffffff) << sel) << 1
	for window == 0 {
		currWord++
		window = ef.upperBits[currWord]
	}

	lower >>= ef.l
	valNext = ((currWord*64+uint64(bits.TrailingZeros64(window))-i-1)<<ef.l | (lower & ef.lowerBitsMask)) + delta + ef.minDelta
	return
}

// Write outputs the state of golomb rice encoding into a writer, which can be recovered later by Read
func (ef *EliasFano) Write(w io.Writer) error {
	var numBuf [8]byte
	binary.BigEndian.PutUint64(numBuf[:], ef.count)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.u)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.minDelta)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	p := (*[maxDataSize]byte)(unsafe.Pointer(&ef.data[0]))
	b := (*p)[:]
	if _, e := w.Write(b[:len(ef.data)*8]); e != nil {
		return e
	}
	return nil
}

// Read inputs the state of golomb rice encoding from a reader s
func ReadEliasFano(r []byte) (*EliasFano, int) {
	ef := &EliasFano{}
	ef.count = binary.BigEndian.Uint64(r[:8])
	ef.u = binary.BigEndian.Uint64(r[8:16])
	ef.minDelta = binary.BigEndian.Uint64(r[16:24])
	p := (*[maxDataSize / 8]uint64)(unsafe.Pointer(&r[24]))
	ef.data = p[:]
	ef.deriveFields()
	return ef, 24 + 8*len(ef.data)
}

const maxDataSize = 0xFFFFFFFFFFFF

// DoubleEliasFano can be used to encode two monotone sequences
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

func (ef *DoubleEliasFano) deriveFields() (int, int) {
	if ef.uPosition/(ef.numBuckets+1) == 0 {
		ef.lPosition = 0
	} else {
		ef.lPosition = 63 ^ uint64(bits.LeadingZeros64(ef.uPosition/(ef.numBuckets+1)))
	}
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
	totalWords := wordsLowerBits + wordsCumKeys + wordsPosition + jumpWords
	if ef.data == nil {
		ef.data = make([]uint64, totalWords)
	} else {
		ef.data = ef.data[:totalWords]
	}
	ef.lowerBits = ef.data[:wordsLowerBits]
	ef.upperBitsCumKeys = ef.data[wordsLowerBits : wordsLowerBits+wordsCumKeys]
	ef.upperBitsPosition = ef.data[wordsLowerBits+wordsCumKeys : wordsLowerBits+wordsCumKeys+wordsPosition]
	ef.jump = ef.data[wordsLowerBits+wordsCumKeys+wordsPosition:]
	return wordsCumKeys, wordsPosition
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
	ef.uCumKeys = cumKeys[ef.numBuckets] - ef.numBuckets*ef.cumKeysMinDelta + 1 // Largest possible encoding of the cumKeys
	wordsCumKeys, wordsPosition := ef.deriveFields()

	for i, cumDelta, bitDelta := uint64(0), uint64(0), uint64(0); i <= ef.numBuckets; i, cumDelta, bitDelta = i+1, cumDelta+ef.cumKeysMinDelta, bitDelta+ef.posMinDelta {
		if ef.lCumKeys != 0 {
			//fmt.Printf("i=%d, set_bits cum for %d = %b\n", i, cumKeys[i]-cumDelta, (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
			setBits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition), int(ef.lCumKeys), (cumKeys[i]-cumDelta)&ef.lowerBitsMaskCumKeys)
			//fmt.Printf("loweBits %b\n", ef.lowerBits)
		}
		set(ef.upperBitsCumKeys, ((cumKeys[i]-cumDelta)>>ef.lCumKeys)+i)
		//fmt.Printf("i=%d, set cum for %d = %d\n", i, cumKeys[i]-cumDelta, (cumKeys[i]-cumDelta)>>ef.lCumKeys+i)

		if ef.lPosition != 0 {
			//fmt.Printf("i=%d, set_bits pos for %d = %b\n", i, position[i]-bitDelta, (position[i]-bitDelta)&ef.lowerBitsMaskPosition)
			setBits(ef.lowerBits, i*(ef.lCumKeys+ef.lPosition)+ef.lCumKeys, int(ef.lPosition), (position[i]-bitDelta)&ef.lowerBitsMaskPosition)
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
					jumpSuperQ := (c / superQ) * (superQSize * 2)
					jumpInsideSuperQ := 2 * (c % superQ) / q
					idx64 := jumpSuperQ + 2 + (jumpInsideSuperQ >> 2)
					shift := 16 * (jumpInsideSuperQ % 4)
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
					jumpSuperQ := (c / superQ) * (superQSize * 2)
					jumpInsideSuperQ := 2*((c%superQ)/q) + 1
					idx64 := jumpSuperQ + 2 + (jumpInsideSuperQ >> 2)
					shift := 16 * (jumpInsideSuperQ % 4)
					mask := uint64(0xffff) << shift
					ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
				}
				c++
			}
		}
	}
	//fmt.Printf("jump: %x\n", ef.jump)
}

// setBits assumes that bits are set in monotonic order, so that
// we can skip the masking for the second word
func setBits(bits []uint64, start uint64, width int, value uint64) {
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
	//bits[pos>>6] |= uint64(1) << (pos & 63)
	bits[pos/64] |= uint64(1) << (pos % 64)
}

func (ef *DoubleEliasFano) jumpSizeWords() int {
	size := ((ef.numBuckets + 1) / superQ) * superQSize * 2 // Whole blocks
	if (ef.numBuckets+1)%superQ != 0 {
		size += (1 + (((ef.numBuckets+1)%superQ+q-1)/q+3)/4) * 2 // Partial block
	}
	return int(size)
}

// Data returns binary representation of double Ellias-Fano index that has been built
func (ef *DoubleEliasFano) Data() []uint64 {
	return ef.data
}

func (ef *DoubleEliasFano) get2(i uint64) (cumKeys, position uint64,
	windowCumKeys uint64, selectCumKeys int, currWordCumKeys, lower, cumDelta uint64) {
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

	selectCumKeys = bitutil.Select64(windowCumKeys, deltaCumKeys)
	//fmt.Printf("i = %d, select cum in %b for %d = %d\n", i, windowCumKeys, deltaCumKeys, selectCumKeys)
	cumDelta = i * ef.cumKeysMinDelta
	cumKeys = ((currWordCumKeys*64+uint64(selectCumKeys)-i)<<ef.lCumKeys | (lower & ef.lowerBitsMaskCumKeys)) + cumDelta

	lower >>= ef.lCumKeys
	//fmt.Printf("i = %d, lower = %b\n", i, lower)
	selectPosition := bitutil.Select64(windowPosition, deltaPosition)
	//fmt.Printf("i = %d, select pos in %b for %d = %d\n", i, windowPosition, deltaPosition, selectPosition)
	bitDelta := i * ef.posMinDelta
	position = ((currWordPosition*64+uint64(selectPosition)-i)<<ef.lPosition | (lower & ef.lowerBitsMaskPosition)) + bitDelta
	return
}

func (ef *DoubleEliasFano) Get2(i uint64) (cumKeys, position uint64) {
	cumKeys, position, _, _, _, _, _ = ef.get2(i)
	return
}

func (ef *DoubleEliasFano) Get3(i uint64) (cumKeys, cumKeysNext, position uint64) {
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

// Write outputs the state of golomb rice encoding into a writer, which can be recovered later by Read
func (ef *DoubleEliasFano) Write(w io.Writer) error {
	var numBuf [8]byte
	binary.BigEndian.PutUint64(numBuf[:], ef.numBuckets)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.uCumKeys)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.uPosition)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.cumKeysMinDelta)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	binary.BigEndian.PutUint64(numBuf[:], ef.posMinDelta)
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	p := (*[maxDataSize]byte)(unsafe.Pointer(&ef.data[0]))
	b := (*p)[:]
	if _, e := w.Write(b[:len(ef.data)*8]); e != nil {
		return e
	}
	return nil
}

// Read inputs the state of golomb rice encoding from a reader s
func (ef *DoubleEliasFano) Read(r []byte) int {
	ef.numBuckets = binary.BigEndian.Uint64(r[:8])
	ef.uCumKeys = binary.BigEndian.Uint64(r[8:16])
	ef.uPosition = binary.BigEndian.Uint64(r[16:24])
	ef.cumKeysMinDelta = binary.BigEndian.Uint64(r[24:32])
	ef.posMinDelta = binary.BigEndian.Uint64(r[32:40])
	p := (*[maxDataSize / 8]uint64)(unsafe.Pointer(&r[40]))
	ef.data = p[:]
	ef.deriveFields()
	return 40 + 8*len(ef.data)
}
