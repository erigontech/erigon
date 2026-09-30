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

//go:build go1.27 && goexperiment.simd

package hexutil

import (
	"encoding/binary"
	"encoding/hex"
	"simd"
)

// encodeHexPortable is hex.Encode with the portable simd package. It has no widening or interleave,
// so each 64-bit lane spreads its low and high 4 bytes into two vectors of digits, and the two are
// interleaved 8 bytes at a time on the way out.
func encodeHexPortable(dst, src []byte) {
	var z simd.Uint8s
	n := z.Len()
	lo32 := simd.BroadcastUint64s(0xffffffff)
	m16 := simd.BroadcastUint64s(0x0000ffff0000ffff)
	m8 := simd.BroadcastUint64s(0x00ff00ff00ff00ff)
	m4 := simd.BroadcastUint64s(0x000f000f000f000f)
	nine := simd.BroadcastInt8s(9)
	c0, c39 := simd.BroadcastUint8s('0'), simd.BroadcastUint8s('a'-'0'-10)
	spread := func(v simd.Uint64s) simd.Uint8s {
		v = v.Or(v.ShiftAllLeft(16)).And(m16)
		v = v.Or(v.ShiftAllLeft(8)).And(m8)
		b := v.ShiftAllRight(4).And(m4).Or(v.And(m4).ShiftAllLeft(8)).ReshapeToUint8s()
		return b.Add(c0).Add(c39.IfElse(b.BitsToInt8().Greater(nine), z))
	}
	var lo, hi [64]byte
	i := 0
	for ; i+n <= len(src); i += n {
		x := simd.LoadUint8s(src[i:]).ReshapeToUint64s()
		spread(x.And(lo32)).Store(lo[:])
		spread(x.ShiftAllRight(32)).Store(hi[:])
		out := dst[2*i : 2*i+2*n]
		for k := 0; k < n; k += 8 {
			binary.LittleEndian.PutUint64(out[2*k:], binary.LittleEndian.Uint64(lo[k:]))
			binary.LittleEndian.PutUint64(out[2*k+8:], binary.LittleEndian.Uint64(hi[k:]))
		}
	}
	hex.Encode(dst[2*i:], src[i:])
}
