// Copyright 2022 The Erigon Authors
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

package eliasfano32

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common/bitutil"
	"github.com/erigontech/erigon/db/kv/stream"
)

// EliasFano algo overview https://www.antoniomallia.it/sorted-integers-compression-with-elias-fano-encoding.html
// P. Elias. Efficient storage and retrieval by content and address of static files. J. ACM, 21(2):246–260, 1974.
// Partitioned Elias-Fano Indexes http://groups.di.unipi.it/~ottavian/files/elias_fano_sigir14.pdf
// Quasi-Succinct Indices, Sebastiano Vigna https://arxiv.org/pdf/1206.4300

const (
	log2q      uint64 = 8
	q          uint64 = 1 << log2q
	qMask      uint64 = q - 1
	superQ     uint64 = 1 << 14
	superQMask uint64 = superQ - 1
	qPerSuperQ uint64 = superQ / q       // 64
	superQSize uint64 = 1 + qPerSuperQ/2 // 1 + 64/2 = 33
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
	i              uint64
	wordsUpperBits int
}

func NewEliasFano(count uint64, maxOffset uint64) *EliasFano {
	if count == 0 {
		panic(fmt.Sprintf("too small count: %d", count))
	}
	//fmt.Printf("count=%d,maxOffset=%d,minDelta=%d\n", count, maxOffset, minDelta)
	ef := &EliasFano{
		count:     count - 1,
		maxOffset: maxOffset,
	}
	ef.u = maxOffset + 1
	ef.wordsUpperBits = ef.deriveFields()
	return ef
}

func (ef *EliasFano) Size() datasize.ByteSize { return datasize.ByteSize(len(ef.data) * 8) }

func (ef *EliasFano) AddOffset(offset uint64) {
	//fmt.Printf("0x%x,\n", offset)
	if ef.l != 0 {
		setBits(ef.lowerBits, ef.i*ef.l, int(ef.l), offset&ef.lowerBitsMask)
	}
	//pos := ((offset - ef.delta) >> ef.l) + ef.i
	set(ef.upperBits, (offset>>ef.l)+ef.i)
	//fmt.Printf("add:%x, pos=%x, set=%x, res=%x\n", offset, pos, pos/64, uint64(1)<<(pos%64))
	ef.i++
}

func (ef *EliasFano) jumpSizeWords() int {
	size := ((ef.count + 1) / superQ) * superQSize // Whole blocks
	if (ef.count+1)%superQ != 0 {
		size += 1 + (((ef.count+1)%superQ+q-1)/q+3)/2 // Partial block
	}
	return int(size)
}

func (ef *EliasFano) deriveFields() int {
	if ef.u/(ef.count+1) == 0 {
		ef.l = 0
	} else {
		ef.l = 63 ^ uint64(bits.LeadingZeros64(ef.u/(ef.count+1))) // pos of first non-zero bit
	}
	ef.lowerBitsMask = (uint64(1) << ef.l) - 1
	wordsLowerBits := int(((ef.count+1)*ef.l+63)/64 + 1)
	wordsUpperBits := int((ef.count + 1 + (ef.u >> ef.l) + 63) / 64)
	jumpWords := ef.jumpSizeWords()
	totalWords := wordsLowerBits + wordsUpperBits + jumpWords
	//fmt.Printf("EF: %d, %d,%d,%d\n", totalWords, wordsLowerBits, wordsUpperBits, jumpWords)
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
			if ef.upperBits[i]&(uint64(1)<<b) == 0 {
				continue
			}
			if (c & superQMask) == 0 {
				// When c is multiple of 2^14 (4096)
				lastSuperQ = i*64 + b
				ef.jump[(c/superQ)*superQSize] = lastSuperQ
			}
			if (c & qMask) != 0 {
				c++
				continue
			}
			// When c is multiple of 2^8 (256)
			var offset = i*64 + b - lastSuperQ // offset can be either 0, 256, 512, 768, ..., up to 4096-256
			// offset needs to be encoded as 16-bit integer, therefore the following check
			if offset >= (1 << 32) {
				fmt.Printf("ef.l=%x,ef.u=%x\n", ef.l, ef.u)
				fmt.Printf("offset=%x,lastSuperQ=%x,i=%x,b=%x,c=%x\n", offset, lastSuperQ, i, b, c)
				panic("")
			}
			// c % superQ is the bit index inside the group of 4096 bits
			jumpSuperQ := (c / superQ) * superQSize
			jumpInsideSuperQ := (c % superQ) / q
			idx64, shift := jumpSuperQ+1+(jumpInsideSuperQ>>1), 32*(jumpInsideSuperQ%2)
			mask := uint64(0xffffffff) << shift
			ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
			c++
		}
	}
}

func (ef *EliasFano) get(i uint64) (val uint64, window uint64, sel int, currWord uint64, lower uint64) {
	lower = i * ef.l
	idx64, shift := lower/64, lower%64
	lower = ef.lowerBits[idx64] >> shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] << (64 - shift)
	}

	jumpSuperQ := (i / superQ) * superQSize
	jumpInsideSuperQ := (i % superQ) / q
	idx64, shift = jumpSuperQ+1+(jumpInsideSuperQ>>1), 32*(jumpInsideSuperQ%2)
	mask := uint64(0xffffffff) << shift
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
	val = (currWord*64+uint64(sel)-i)<<ef.l | (lower & ef.lowerBitsMask)

	return
}

func (ef *EliasFano) Get(i uint64) uint64 {
	val, _, _, _, _ := ef.get(i)
	return val
}

func (ef *EliasFano) Get2(i uint64) (val uint64, valNext uint64) {
	var window uint64
	var sel int
	var currWord uint64
	var lower uint64
	val, window, sel, currWord, lower = ef.get(i)
	window &= (uint64(0xffffffffffffffff) << sel) << 1
	for window == 0 {
		currWord++
		window = ef.upperBits[currWord]
	}

	lower >>= ef.l
	valNext = (currWord*64+uint64(bits.TrailingZeros64(window))-i-1)<<ef.l | (lower & ef.lowerBitsMask)
	return
}

func (ef *EliasFano) upper(i uint64) uint64 {
	jumpSuperQ := (i / superQ) * superQSize
	jumpInsideSuperQ := (i % superQ) / q
	idx64, shift := jumpSuperQ+1+(jumpInsideSuperQ>>1), 32*(jumpInsideSuperQ%2)
	mask := uint64(0xffffffff) << shift
	jump := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift
	currWord := jump / 64
	window := ef.upperBits[currWord] & (uint64(0xffffffffffffffff) << (jump % 64))
	d := int(i & qMask)

	for bitCount := bits.OnesCount64(window); bitCount <= d; bitCount = bits.OnesCount64(window) {
		currWord++
		window = ef.upperBits[currWord]
		d -= bitCount
	}

	sel := bitutil.Select64(window, d)
	return currWord*64 + uint64(sel) - i
}

func Seek(data []byte, n uint64) (uint64, bool) {
	ef, _ := ReadEliasFano(data) //for better perf: app-code can use ef.Reset(data).Seek(n)
	return ef.Seek(n)
}

func (ef *EliasFano) search(v uint64, reverse bool) (nextV uint64, nextI uint64, ok bool) {
	if v == 0 {
		if reverse {
			return 0, 0, ef.Min() == 0
		}
		return ef.Min(), 0, true
	}
	if v == ef.Max() {
		return ef.Max(), ef.count, true
	}
	if v > ef.Max() {
		if reverse {
			return ef.Max(), ef.count, true
		}
		return 0, 0, false
	}

	hi := v >> ef.l
	i := sort.Search(int(ef.count+1), func(i int) bool {
		if reverse {
			return ef.upper(ef.count-uint64(i)) <= hi
		}
		return ef.upper(uint64(i)) >= hi
	})
	if reverse {
		for j := uint64(i); j <= ef.count; j++ {
			idx := ef.count - j
			val, _, _, _, _ := ef.get(idx)
			if val <= v {
				return val, idx, true
			}
		}
	} else {
		for j := uint64(i); j <= ef.count; j++ {
			val, _, _, _, _ := ef.get(j)
			if val >= v {
				return val, j, true
			}
		}
	}
	return 0, 0, false
}

// Seek returns the value in the sequence, equal or greater than given value
func (ef *EliasFano) Seek(v uint64) (uint64, bool) {
	n, _, ok := ef.search(v, false /* reverse */)
	return n, ok
}

func (ef *EliasFano) Max() uint64 {
	return ef.maxOffset
}

func (ef *EliasFano) Min() uint64 {
	return ef.Get(0)
}

func (ef *EliasFano) Count() uint64 {
	return ef.count + 1
}

func (ef *EliasFano) Iterator() *EliasFanoIter {
	it := &EliasFanoIter{
		ef:            ef,
		lowerBits:     ef.lowerBits,
		upperBits:     ef.upperBits,
		count:         ef.count,
		lowerBitsMask: ef.lowerBitsMask,
		l:             ef.l,
		upperStep:     uint64(1) << ef.l,
	}

	it.init()
	return it
}

func (ef *EliasFano) ReverseIterator() *EliasFanoIter {
	it := &EliasFanoIter{
		ef:            ef,
		lowerBits:     ef.lowerBits,
		upperBits:     ef.upperBits,
		count:         ef.count,
		lowerBitsMask: ef.lowerBitsMask,
		l:             ef.l,
		upperStep:     uint64(1) << ef.l,
		reverse:       true,
	}

	it.init()
	return it
}

type EliasFanoIter struct {
	ef        *EliasFano
	lowerBits []uint64
	upperBits []uint64

	//constants
	count         uint64
	lowerBitsMask uint64
	l             uint64
	upperStep     uint64
	reverse       bool

	//fields of current value
	upper    uint64
	upperIdx uint64

	//fields of next value
	lowerIdx  uint64
	upperMask uint64

	itemsIterated uint64
}

func (efi *EliasFanoIter) Close() {}
func (efi *EliasFanoIter) HasNext() bool {
	return efi.itemsIterated <= efi.count
}

func (efi *EliasFanoIter) Reset() {
	efi.upper = 0
	efi.upperIdx = 0
	efi.lowerIdx = 0
	efi.upperMask = 0
	efi.itemsIterated = 0
	efi.init()
}

func (efi *EliasFanoIter) init() {
	if efi.itemsIterated != 0 {
		return
	}

	if efi.reverse {
		higherBitsMaxValue := efi.ef.u >> efi.l
		lastUpperBitIdx := (efi.ef.count + 1) + higherBitsMaxValue
		efi.upperMask = 1 << (lastUpperBitIdx % 64) >> 1 // last bit is always 0 delimiter, so we move >> 1
		efi.upperIdx = lastUpperBitIdx / 64
		efi.upper = higherBitsMaxValue << efi.l
		efi.lowerIdx = efi.count * efi.l
	} else {
		efi.upperMask = 1
	}
}

func (efi *EliasFanoIter) Seek(n uint64) {
	//fmt.Printf("b seek2: efi.upperMask(%d)=%d, upperIdx=%d, lowerIdx=%d, itemsIterated=%d\n", n, bits.TrailingZeros64(efi.upperMask), efi.upperIdx, efi.lowerIdx, efi.itemsIterated)
	//fmt.Printf("b seek2: efi.upper=%d\n", efi.upper)
	efi.Reset()
	nn, nextI, ok := efi.ef.search(n, efi.reverse)
	_ = nn
	if !ok {
		efi.itemsIterated = efi.count + 1
		return
	}
	if nextI == 0 && !efi.reverse {
		return
	}
	if nextI == efi.count && efi.reverse {
		return
	}

	if efi.reverse {
		efi.itemsIterated = efi.count - nextI

		// fields of current value
		v, _, sel, currWords, lower := efi.ef.get(nextI + 1)
		efi.upper = v &^ (lower & efi.ef.lowerBitsMask)
		efi.upperIdx = currWords

		// fields of next value
		efi.lowerIdx -= efi.itemsIterated * efi.l
		if sel > 0 {
			efi.upperMask = 1 << (sel - 1)
		} else {
			efi.upperMask = 0
		}
	} else {
		efi.itemsIterated = nextI

		// fields of current value
		v, _, sel, currWords, lower := efi.ef.get(nextI - 1)
		efi.upper = v &^ (lower & efi.ef.lowerBitsMask)
		efi.upperIdx = currWords

		// fields of next value
		efi.lowerIdx = nextI * efi.l
		efi.upperMask = 1 << (sel + 1)
	}

	//fmt.Printf("seek2: efi.upperMask(%d)=%d, upperIdx=%d, lowerIdx=%d, itemsIterated=%d\n", n, bits.TrailingZeros64(efi.upperMask), efi.upperIdx, efi.lowerIdx, efi.itemsIterated)
	//fmt.Printf("seek2: efi.upper=%d\n", efi.upper)
}

func (efi *EliasFanoIter) moveNext() {
	if efi.reverse {
		efi.decrement()
	} else {
		efi.increment()
	}

	efi.itemsIterated++
}

func (efi *EliasFanoIter) increment() {
	if efi.upperMask == 0 {
		efi.upperIdx++
		efi.upperMask = 1
	}
	for efi.upperBits[efi.upperIdx]&efi.upperMask == 0 {
		efi.upper += efi.upperStep
		efi.upperMask <<= 1
		if efi.upperMask == 0 {
			efi.upperIdx++
			efi.upperMask = 1
		}
	}
	efi.upperMask <<= 1
	efi.lowerIdx += efi.l
}

func (efi *EliasFanoIter) decrement() {
	if efi.upperMask == 0 {
		if efi.upperIdx == 0 {
			panic("decrement: unexpected efi.upperIdx underflow")
		}
		efi.upperIdx--
		efi.upperMask = 1 << 63
	}

	for efi.upperBits[efi.upperIdx]&efi.upperMask == 0 {
		if efi.upper < efi.upperStep {
			panic("decrement: unexpected efi.upper underflow")
		}
		efi.upper -= efi.upperStep
		efi.upperMask >>= 1
		if efi.upperMask == 0 {
			if efi.upperIdx == 0 {
				panic("decrement: unexpected efi.upperIdx underflow")
			}
			efi.upperIdx--
			efi.upperMask = 1 << 63
		}
	}

	// note: there can be an underflow here after the last Next()
	// but that is ok since we are protected from ErrEliasFanoIterExhausted
	efi.lowerIdx -= efi.l
	efi.upperMask >>= 1
}

func (efi *EliasFanoIter) Next() (uint64, error) {
	if !efi.HasNext() {
		return 0, stream.ErrIteratorExhausted
	}
	idx64, shift := efi.lowerIdx/64, efi.lowerIdx%64
	lower := efi.lowerBits[idx64] >> shift
	if shift > 0 {
		lower |= efi.lowerBits[idx64+1] << (64 - shift)
	}
	efi.moveNext()
	return efi.upper | (lower & efi.lowerBitsMask), nil
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
	b := unsafe.Slice((*byte)(unsafe.Pointer(&ef.data[0])), len(ef.data)*uint64Size)
	if _, e := w.Write(b); e != nil {
		return e
	}
	return nil
}

// Write outputs the state of golomb rice encoding into a writer, which can be recovered later by Read
func (ef *EliasFano) AppendBytes(buf []byte) []byte {
	var numBuf [8]byte
	binary.BigEndian.PutUint64(numBuf[:], ef.count)
	buf = append(buf, numBuf[:]...)
	binary.BigEndian.PutUint64(numBuf[:], ef.u)
	buf = append(buf, numBuf[:]...)
	b := unsafe.Slice((*byte)(unsafe.Pointer(&ef.data[0])), len(ef.data)*uint64Size)
	buf = append(buf, b...)
	return buf
}

// Read inputs the state of golomb rice encoding from a reader s
func ReadEliasFano(r []byte) (*EliasFano, int) {
	ef := &EliasFano{
		count: binary.BigEndian.Uint64(r[:8]),
		u:     binary.BigEndian.Uint64(r[8:16]),
		data:  unsafe.Slice((*uint64)(unsafe.Pointer(&r[16])), (len(r)-16)/uint64Size),
	}
	ef.maxOffset = ef.u - 1
	ef.deriveFields()
	return ef, 16 + 8*len(ef.data)
}

// Reset - like ReadEliasFano, but for existing object
func (ef *EliasFano) Reset(r []byte) *EliasFano {
	ef.count = binary.BigEndian.Uint64(r[:8])
	ef.u = binary.BigEndian.Uint64(r[8:16])
	ef.data = unsafe.Slice((*uint64)(unsafe.Pointer(&r[16])), (len(r)-16)/uint64Size)
	ef.maxOffset = ef.u - 1
	ef.deriveFields()
	return ef
}

func Max(r []byte) uint64   { return binary.BigEndian.Uint64(r[8:16]) - 1 }
func Count(r []byte) uint64 { return binary.BigEndian.Uint64(r[:8]) + 1 }

const uint64Size = 8

func Min(r []byte) uint64 {
	count := binary.BigEndian.Uint64(r[:8])
	u := binary.BigEndian.Uint64(r[8:16])
	p := unsafe.Slice((*uint64)(unsafe.Pointer(&r[16])), (len(r)-16)/uint64Size)
	var l uint64
	if u/(count+1) == 0 {
		l = 0
	} else {
		l = 63 ^ uint64(bits.LeadingZeros64(u/(count+1))) // pos of first non-zero bit
	}
	wordsLowerBits := int(((count+1)*l+63)/64 + 1)
	wordsUpperBits := int((count + 1 + (u >> l) + 63) / 64)
	lowerBits := p[:wordsLowerBits]
	upperBits := p[wordsLowerBits : wordsLowerBits+wordsUpperBits]
	jump := p[wordsLowerBits+wordsUpperBits:]
	lower := lowerBits[0]

	mask := uint64(0xffffffff)
	j := jump[0] + jump[1]&mask
	currWord := j / 64
	window := upperBits[currWord] & (uint64(0xffffffffffffffff) << (j % 64))

	if bitCount := bits.OnesCount64(window); bitCount <= 0 {
		currWord++
		window = upperBits[currWord]
	}
	sel := bitutil.Select64(window, 0)
	lowerBitsMask := (uint64(1) << l) - 1
	val := (currWord*64+uint64(sel))<<l | (lower & lowerBitsMask)
	return val
}

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
			if ef.upperBitsCumKeys[i]&(uint64(1)<<b) == 0 {
				continue
			}
			if (c & superQMask) == 0 {
				// When c is multiple of 2^14 (4096)
				lastSuperQ = i*64 + b
				ef.jump[(c/superQ)*(superQSize*2)] = lastSuperQ
			}
			if (c & qMask) == 0 {
				// When c is multiple of 2^8 (256)
				var offset = i*64 + b - lastSuperQ // offset can be either 0, 256, 512, 768, ..., up to 4096-256
				// offset needs to be encoded as 16-bit integer, therefore the following check
				if offset >= (1 << 32) {
					panic("")
				}
				// c % superQ is the bit index inside the group of 4096 bits
				jumpSuperQ := (c / superQ) * (superQSize * 2)
				jumpInsideSuperQ := 2 * (c % superQ) / q
				idx64 := jumpSuperQ + 2 + (jumpInsideSuperQ >> 1)
				shift := 32 * (jumpInsideSuperQ % 2)
				mask := uint64(0xffffffff) << shift
				ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
			}
			c++
		}
	}

	for i, c, lastSuperQ := uint64(0), uint64(0), uint64(0); i < uint64(wordsPosition); i++ {
		for b := uint64(0); b < 64; b++ {
			if ef.upperBitsPosition[i]&(uint64(1)<<b) == 0 {
				continue
			}

			if (c & superQMask) == 0 {
				lastSuperQ = i*64 + b
				ef.jump[(c/superQ)*(superQSize*2)+1] = lastSuperQ
			}
			if (c & qMask) == 0 {
				var offset = i*64 + b - lastSuperQ
				if offset >= (1 << 32) {
					panic("")
				}
				jumpSuperQ := (c / superQ) * (superQSize * 2)
				jumpInsideSuperQ := 2*((c%superQ)/q) + 1
				idx64 := jumpSuperQ + 2 + (jumpInsideSuperQ >> 1)
				shift := 32 * (jumpInsideSuperQ % 2)
				mask := uint64(0xffffffff) << shift
				ef.jump[idx64] = (ef.jump[idx64] &^ mask) | (offset << shift)
			}
			c++
		}
	}
	//fmt.Printf("jump: %x\n", ef.jump)
}

// setBits assumes that bits are set in monotonic order, so that
// we can skip the masking for the second word
func setBits(bits []uint64, start uint64, width int, value uint64) {
	idx64, shift := start>>6, int(start&63)
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
		size += (1 + (((ef.numBuckets+1)%superQ+q-1)/q+3)/2) * 2 // Partial block
	}
	return int(size)
}

// Data returns binary representation of double Ellias-Fano index that has been built
func (ef *DoubleEliasFano) Data() []uint64 {
	return ef.data
}

func (ef *DoubleEliasFano) get2(i uint64) (cumKeys uint64, position uint64,
	windowCumKeys uint64, selectCumKeys int, currWordCumKeys uint64, lower uint64, cumDelta uint64) {
	posLower := i * (ef.lCumKeys + ef.lPosition)
	idx64, shift := posLower/64, posLower%64
	lower = ef.lowerBits[idx64] >> shift
	if shift > 0 {
		lower |= ef.lowerBits[idx64+1] << (64 - shift)
	}
	//fmt.Printf("i = %d, posLower = %d, lower = %b\n", i, posLower, lower)

	jumpSuperQ := (i / superQ) * superQSize * 2
	jumpInsideSuperQ := (i % superQ) / q
	idx16 := 2*(jumpSuperQ+2) + 2*jumpInsideSuperQ
	idx64, shift = idx16/2, 32*(idx16%2)
	mask := uint64(0xffffffff) << shift
	jumpCumKeys := ef.jump[jumpSuperQ] + (ef.jump[idx64]&mask)>>shift
	idx16++
	idx64, shift = idx16/2, 32*(idx16%2)
	mask = uint64(0xffffffff) << shift
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
	b := unsafe.Slice((*byte)(unsafe.Pointer(&ef.data[0])), len(ef.data)*uint64Size)
	if _, e := w.Write(b); e != nil {
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
	ef.data = unsafe.Slice((*uint64)(unsafe.Pointer(&r[40])), (len(r)-40)/uint64Size)
	ef.deriveFields()
	return 40 + 8*len(ef.data)
}
