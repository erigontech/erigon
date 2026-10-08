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

//go:build go1.27 && goexperiment.simd && arm64

package hexutil

import (
	"encoding/hex"
	"simd/archsimd"
)

// NEON is in the arm64 baseline, so the build tag is the only guard these need.

var hexDigits16 = [16]uint8{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}

// encodeHex is hex.Encode with whole 16-byte blocks done by NEON table lookups.
func encodeHex(dst, src []byte) {
	digits := archsimd.LoadUint8x16Array(&hexDigits16)
	lowNib := archsimd.BroadcastUint8x16(0x0f)
	for len(src) >= 16 && len(dst) >= 32 {
		v := archsimd.LoadUint8x16Array((*[16]uint8)(src))
		hi, lo := digits.LookupOrZero(v.ShiftAllRight(4)), digits.LookupOrZero(v.And(lowNib))
		hi.InterleaveLo(lo).StoreArray((*[16]uint8)(dst))
		hi.InterleaveHi(lo).StoreArray((*[16]uint8)(dst[16:]))
		src, dst = src[16:], dst[32:]
	}
	hex.Encode(dst, src)
}

// decodeHex is hex.Decode with whole 32-character blocks done by NEON, by algorithm 3 of
// http://0x80.pl/notesen/2022-01-17-validating-hex-parse.html as in hex_amd64.go.
func decodeHex(dst, src []byte) (int, error) {
	n := 0
	c6 := archsimd.BroadcastUint8x16(0xc6)
	six := archsimd.BroadcastUint8x16(6)
	f0 := archsimd.BroadcastUint8x16(0xf0)
	upper := archsimd.BroadcastUint8x16(0xdf)
	bigA := archsimd.BroadcastUint8x16('A')
	ten := archsimd.BroadcastUint8x16(10)
	for len(src) >= 32 && len(dst) >= 16 {
		c1 := archsimd.LoadUint8x16Array((*[16]uint8)(src))
		c2 := archsimd.LoadUint8x16Array((*[16]uint8)(src[16:]))
		n1 := c1.Add(c6).SubSaturated(six).Sub(f0).Min(c1.And(upper).Sub(bigA).AddSaturated(ten))
		n2 := c2.Add(c6).SubSaturated(six).Sub(f0).Min(c2.And(upper).Sub(bigA).AddSaturated(ten))
		if n1.Max(n2).ReduceMax() > 15 {
			break
		}
		n1.ConcatEven(n2).ShiftAllLeft(4).Or(n1.ConcatOdd(n2)).StoreArray((*[16]uint8)(dst))
		src, dst, n = src[32:], dst[16:], n+16
	}
	m, err := hex.Decode(dst, src)
	return n + m, err
}
