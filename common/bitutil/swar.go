// Copyright 2026 The Erigon Authors
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
// Every Has* below is meaningful only as zero or nonzero. The subtraction
// borrows across lane boundaries, so a hit can light up a neighbouring lane too
// and the set bits do not locate it. The one exception is the lowest set bit,
// which no borrow can reach from below, so bits.TrailingZeros64(z)>>3 does give
// the first matching lane.
const (
	swarLow  uint64 = 0x0101010101010101 // low bit of every lane
	swarHigh uint64 = 0x8080808080808080 // high bit of every lane
)

// Broadcast returns b in every lane. In a loop over one fixed byte, hoist it and
// call HasZero(x ^ pat): Go does not move the multiply out of the loop for you.
func Broadcast(b byte) uint64 { return swarLow * uint64(b) }

// HasZero returns a nonzero value if any lane of x is zero, and 0 otherwise.
func HasZero(x uint64) uint64 { return (x - swarLow) &^ x & swarHigh }

// HasByte returns a nonzero value if any lane of x equals b, and 0 otherwise.
func HasByte(x uint64, b byte) uint64 { return HasZero(x ^ Broadcast(b)) }

// HasLess returns a nonzero value if any lane of x is below n, and 0 otherwise.
// n must be at most 128. Above that only lanes in [n-128, 128) are reported: a
// lane below n-128 wraps with its high bit clear, and the &^ x term drops every
// lane at or above 128. So for n == 200 neither 0 nor 199 is reported.
func HasLess(x uint64, n byte) uint64 { return (x - Broadcast(n)) &^ x & swarHigh }
