// Copyright 2021 The Erigon Authors
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

package recsplit

import (
	"encoding/binary"
	"io"
	"math/bits"
	"unsafe"

	"github.com/erigontech/erigon-lib/common/bitutil"
)

// Optimal Golomb-Rice parameters for leaves
var bijMemo = []uint32{0, 0, 0, 1, 3, 4, 5, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 21, 22, 23, 25, 26, 28, 29, 30}

// GolombRice can build up the golomb-rice encoding of the sequeuce of numbers, as well as read the numbers back from it.
type GolombRice struct {
	data     []uint64 // Present in the builder and in the reader
	bitCount int      // Specific to the builder - number of bits added to the encoding so far
}

// appendUnaryAll adds the unary encoding of specified sequence of numbers to the end of the
// current encoding
func (g *GolombRice) appendUnaryAll(unary []uint64) {
	bitInc := 0
	for _, u := range unary {
		// Each number u uses u+1 bits for its unary representation
		bitInc += int(u) + 1
	}
	targetSize := (g.bitCount + bitInc + 63) / 64
	for len(g.data) < targetSize {
		g.data = append(g.data, 0)
	}

	for _, u := range unary {
		g.bitCount += int(u)
		appendPtr := g.bitCount / 64
		g.data[appendPtr] |= uint64(1) << (g.bitCount & 63)
		g.bitCount++
	}
}

// appendFixed encodes the next value using specified Golomb parameter. Since we are using Golomb-Rice encoding,
// all Golomb parameters are powers of two. Therefore we input log2 of the Golomb parameter rather than the Golomb parameter itself,
// for convenience
func (g *GolombRice) appendFixed(v uint64, log2golomb int) {
	if log2golomb == 0 {
		return
	}
	lowerBits := v & ((uint64(1) << log2golomb) - 1) // Extract the part of the number that will be encoded using truncated binary encoding
	usedBits := g.bitCount & 63                      // How many bits of the last element of b.data is used by previous value
	targetSize := (g.bitCount + log2golomb + 63) / 64
	//fmt.Printf("g.bitCount = %d, log2golomb = %d, targetSize = %d\n", g.bitCount, log2golomb, targetSize)
	for len(g.data) < targetSize {
		g.data = append(g.data, 0)
	}
	appendPtr := g.bitCount / 64 // The index in b.data corresponding to the last element used by previous value, or if previous values fits perfectly, the index of the next free element
	curWord := g.data[appendPtr]
	curWord |= lowerBits << usedBits // curWord now contains the new value potentially combined with the part of the previous value
	if usedBits+log2golomb > 64 {
		// New value overflows to the next element
		g.data[appendPtr] = curWord
		appendPtr++
		curWord = lowerBits >> (64 - usedBits) // curWord now contains the part of the new value that overflows
	}
	g.data[appendPtr] = curWord
	g.bitCount += log2golomb
}

// Bits returns current number of bits in the compact encoding of the hash function representation
func (g *GolombRice) Bits() int {
	return g.bitCount
}

func (g *GolombRiceReader) ReadReset(bitPos, unaryOffset int) {
	g.currFixedOffset = bitPos
	unaryPos := bitPos + unaryOffset
	g.currPtrUnary = unaryPos / 64
	g.currWindowUnary = g.data[g.currPtrUnary] >> (unaryPos & 63)
	g.currPtrUnary++
	g.validLowerBitsUnary = 64 - (unaryPos & 63)
}

func (g *GolombRiceReader) SkipSubtree(nodes, fixedLen int) {
	if nodes <= 0 {
		panic("nodes <= 0")
	}
	missing := nodes
	var cnt int
	for cnt = bits.OnesCount64(g.currWindowUnary); cnt < missing; cnt = bits.OnesCount64(g.currWindowUnary) {
		g.currWindowUnary = g.data[g.currPtrUnary]
		g.currPtrUnary++
		missing -= cnt
		g.validLowerBitsUnary = 64
	}
	cnt = bitutil.Select64(g.currWindowUnary, missing-1)
	g.currWindowUnary >>= cnt
	g.currWindowUnary >>= 1
	g.validLowerBitsUnary -= cnt + 1

	g.currFixedOffset += fixedLen
}

func (g *GolombRiceReader) ReadNext(log2golomb int) (result uint64) {
	if g.currWindowUnary == 0 {
		result += uint64(g.validLowerBitsUnary)
		g.currWindowUnary = g.data[g.currPtrUnary]
		g.currPtrUnary++
		g.validLowerBitsUnary = 64
		for g.currWindowUnary == 0 {
			result += 64
			g.currWindowUnary = g.data[g.currPtrUnary]
			g.currPtrUnary++
		}
	}

	pos := bits.TrailingZeros64(g.currWindowUnary)

	g.currWindowUnary >>= pos
	g.currWindowUnary >>= 1
	g.validLowerBitsUnary -= pos + 1

	result += uint64(pos)
	result <<= log2golomb

	idx64 := g.currFixedOffset >> 6
	shift := g.currFixedOffset & 63
	fixed := g.data[idx64] >> shift
	if shift+log2golomb > 64 {
		fixed |= g.data[idx64+1] << (64 - shift)
	}
	result |= fixed & ((uint64(1) << log2golomb) - 1)
	g.currFixedOffset += log2golomb
	return result
}

// Data returns the binary representation of the Golomb-Rice code that is built
func (g *GolombRice) Data() []uint64 {
	return g.data
}

const maxDataSize = 0xFFFFFFFFFFFF

// Write outputs the state of golomb rice encoding into a writer, which can be recovered later by Read
func (g *GolombRice) Write(w io.Writer) error {
	var numBuf [8]byte
	binary.BigEndian.PutUint64(numBuf[:], uint64(len(g.data)))
	if _, e := w.Write(numBuf[:]); e != nil {
		return e
	}
	p := (*[maxDataSize]byte)(unsafe.Pointer(&g.data[0]))
	b := (*p)[:]
	if _, e := w.Write(b[:len(g.data)*8]); e != nil {
		return e
	}
	return nil
}

type GolombRiceReader struct {
	data                []uint64 // Present in the builder and in the reader
	currFixedOffset     int      // Specific to the reader
	currWindowUnary     uint64
	currPtrUnary        int
	validLowerBitsUnary int
}
