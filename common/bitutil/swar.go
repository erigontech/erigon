// Copyright 2025 The Erigon Authors
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

package bitutil

// SWAR (SIMD-within-a-register) helpers read a uint64 as eight byte lanes and
// test all of them with a few ALU ops.
//
// Every Has* below answers only zero/nonzero. The subtraction borrows across
// lane boundaries, so a hit can light up a neighbouring lane too and the set
// bits do not locate it. The one exception is the lowest set bit, which no
// borrow can reach from below, so bits.TrailingZeros64(z)>>3 does give the
// first matching lane.
const (
	SWARLow  uint64 = 0x0101010101010101 // low bit of every lane
	SWARHigh uint64 = 0x8080808080808080 // high bit of every lane
)

// HasZero reports, nonzero, whether any lane of x is zero.
func HasZero(x uint64) uint64 { return (x - SWARLow) &^ x & SWARHigh }

// HasByte reports, nonzero, whether any lane of x equals b.
func HasByte(x uint64, b byte) uint64 { return HasZero(x ^ SWARLow*uint64(b)) }

// HasLess reports, nonzero, whether any lane of x is below n. n must be at most
// 128: the &^ x term drops every lane with its high bit set, so above that
// bound lanes in [128, n) go unreported.
func HasLess(x uint64, n byte) uint64 { return (x - SWARLow*uint64(n)) &^ x & SWARHigh }
