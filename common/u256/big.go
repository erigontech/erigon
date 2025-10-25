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
	N0    = uint256.NewInt(0)
	N1    = uint256.NewInt(1)
	N2    = uint256.NewInt(2)
	N4    = uint256.NewInt(4)
	N8    = uint256.NewInt(8)
	N27   = uint256.NewInt(27)
	N28   = uint256.NewInt(28)
	N32   = uint256.NewInt(32)
	N35   = uint256.NewInt(35)
	N100  = uint256.NewInt(100)
	Num0  = uint256.NewInt(0)
	Num1  = uint256.NewInt(1)
	Num2  = uint256.NewInt(2)
	Num4  = uint256.NewInt(4)
	Num8  = uint256.NewInt(8)
	Num27 = uint256.NewInt(27)
	Num32 = uint256.NewInt(32)
	Num35 = uint256.NewInt(35)
)

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
