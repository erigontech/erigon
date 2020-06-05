// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package u256

import (
	"github.com/holiman/uint256"
)

// Common big integers often used
var (
	Num0  = uint256.NewInt().SetUint64(0)
	Num1  = uint256.NewInt().SetUint64(1)
	Num2  = uint256.NewInt().SetUint64(2)
	Num8  = uint256.NewInt().SetUint64(8)
	Num32 = uint256.NewInt().SetUint64(32)
	Num35 = uint256.NewInt().SetUint64(35)
)

func SetBytes(z *uint256.Int, buf []byte, length int) *uint256.Int {
	if len(buf) == length {
		return z.SetBytes(buf)
	} else {
		return SetBytesRightPadded(z, buf, length)
	}
}

func SetBytesRightPadded(z *uint256.Int, buf []byte, length int) *uint256.Int {
	var d uint64
	k := 0
	s := uint64(0)
	i := length
	st := len(buf)
	z[0], z[1], z[2], z[3] = 0, 0, 0, 0
	for ; i > 0; i-- {
		if i-1 < st {
			d |= uint64(buf[i-1]) << s
		}
		if s += 8; s == 64 {
			z[k] = d
			k++
			s, d = 0, 0
			if k >= len(z) {
				break
			}
		}
	}
	if k < len(z) {
		z[k] = d
	}
	return z
}
