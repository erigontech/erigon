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

package u256

import (
	"math/bits"

	"github.com/holiman/uint256"
)

// Common big integers often used
var (
	N0    = U64(0)
	N1    = U64(1)
	N2    = U64(2)
	N4    = U64(4)
	N8    = U64(8)
	N27   = U64(27)
	N28   = U64(28)
	N32   = U64(32)
	N35   = U64(35)
	N100  = U64(100)
	Num0  = U64(0)
	Num1  = U64(1)
	Num2  = U64(2)
	Num4  = U64(4)
	Num8  = U64(8)
	Num27 = U64(27)
	Num32 = U64(32)
	Num35 = U64(35)
)

func U64(x uint64) (z uint256.Int) {
	z[3], z[2], z[1], z[0] = 0, 0, 0, x
	return z
}

// Add sets z to the sum x+y
// This is a garbage free version of  uint256.Add
func Add(x, y uint256.Int) (z uint256.Int) {
	var carry uint64
	z[0], carry = bits.Add64(x[0], y[0], 0)
	z[1], carry = bits.Add64(x[1], y[1], carry)
	z[2], carry = bits.Add64(x[2], y[2], carry)
	z[3], _ = bits.Add64(x[3], y[3], carry)
	return z
}

// AddOverflow sets z to the sum x+y, and returns z and whether overflow occurred
func AddOverflow(x, y uint256.Int) (z uint256.Int, _ bool) {
	var carry uint64
	z[0], carry = bits.Add64(x[0], y[0], 0)
	z[1], carry = bits.Add64(x[1], y[1], carry)
	z[2], carry = bits.Add64(x[2], y[2], carry)
	z[3], carry = bits.Add64(x[3], y[3], carry)
	return z, carry != 0
}

// Sub sets z to the difference x-y
// This is a garbage free version of  uint256.Sub
func Sub(x, y uint256.Int) (z uint256.Int) {
	var carry uint64
	z[0], carry = bits.Sub64(x[0], y[0], 0)
	z[1], carry = bits.Sub64(x[1], y[1], carry)
	z[2], carry = bits.Sub64(x[2], y[2], carry)
	z[3], _ = bits.Sub64(x[3], y[3], carry)
	return z
}

// Mul sets z to the product x*y
// This is a garbage free version of  uint256.Mul
func Mul(x, y uint256.Int) (z uint256.Int) {
	var (
		carry0, carry1, carry2 uint64
		res1, res2             uint64
		x0, x1, x2, x3         = x[0], x[1], x[2], x[3]
		y0, y1, y2, y3         = y[0], y[1], y[2], y[3]
	)

	carry0, z[0] = bits.Mul64(x0, y0)
	carry0, res1 = umulHop(carry0, x1, y0)
	carry0, res2 = umulHop(carry0, x2, y0)

	carry1, z[1] = umulHop(res1, x0, y1)
	carry1, res2 = umulStep(res2, x1, y1, carry1)

	carry2, z[2] = umulHop(res2, x0, y2)

	z[3] = x3*y0 + x2*y1 + x0*y3 + x1*y2 + carry0 + carry1 + carry2
	return z
}

// MulOverflow sets z to the product x*y, and returns z and  whether overflow occurred
func MulOverflow(x, y uint256.Int) (z uint256.Int, _ bool) {
	var p = umul(x, y)
	copy(z[:], p[:4])
	return z, (p[4] | p[5] | p[6] | p[7]) != 0
}

// umul computes full 256 x 256 -> 512 multiplication.
func umul(x, y uint256.Int) (res [8]uint64) {
	var (
		carry, carry4, carry5, carry6 uint64
		res1, res2, res3, res4, res5  uint64
	)

	carry, res[0] = bits.Mul64(x[0], y[0])
	carry, res1 = umulHop(carry, x[1], y[0])
	carry, res2 = umulHop(carry, x[2], y[0])
	carry4, res3 = umulHop(carry, x[3], y[0])

	carry, res[1] = umulHop(res1, x[0], y[1])
	carry, res2 = umulStep(res2, x[1], y[1], carry)
	carry, res3 = umulStep(res3, x[2], y[1], carry)
	carry5, res4 = umulStep(carry4, x[3], y[1], carry)

	carry, res[2] = umulHop(res2, x[0], y[2])
	carry, res3 = umulStep(res3, x[1], y[2], carry)
	carry, res4 = umulStep(res4, x[2], y[2], carry)
	carry6, res5 = umulStep(carry5, x[3], y[2], carry)

	carry, res[3] = umulHop(res3, x[0], y[3])
	carry, res[4] = umulStep(res4, x[1], y[3], carry)
	carry, res[5] = umulStep(res5, x[2], y[3], carry)
	res[7], res[6] = umulStep(carry6, x[3], y[3], carry)

	return res
}

// umulStep computes (hi * 2^64 + lo) = z + (x * y) + carry.
func umulStep(z, x, y, carry uint64) (hi, lo uint64) {
	hi, lo = bits.Mul64(x, y)
	lo, carry = bits.Add64(lo, carry, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	lo, carry = bits.Add64(lo, z, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	return hi, lo
}

// umulHop computes (hi * 2^64 + lo) = z + (x * y)
func umulHop(z, x, y uint64) (hi, lo uint64) {
	hi, lo = bits.Mul64(x, y)
	lo, carry := bits.Add64(lo, z, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	return hi, lo
}

// U256Min returns the smaller of x or y.
func Min(x, y uint256.Int) uint256.Int {
	if x.Cmp(&y) > 0 {
		return y
	}
	return x
}
