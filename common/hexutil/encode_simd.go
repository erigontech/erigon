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

//go:build go1.27 && goexperiment.simd && amd64

package hexutil

import (
	"encoding/hex"
	"simd/archsimd"
)

var (
	hexDigits32 = [32]uint8{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
	hasAVX2 = archsimd.X86.AVX2()
)

// encodeHex is hex.Encode with whole 16-byte blocks done by AVX2. Each byte is widened to a
// uint16 holding its high nibble in the low byte and its low nibble in the high byte, so one
// in-lane byte shuffle turns the nibbles into digits in output order.
func encodeHex(dst, src []byte) {
	i := 0
	if hasAVX2 {
		digits := archsimd.LoadUint8x32Array(&hexDigits32)
		low := archsimd.BroadcastUint16x16(0x0f)
		for ; i+16 <= len(src); i += 16 {
			w := archsimd.LoadUint8x16Array((*[16]uint8)(src[i:])).ExtendToUint16()
			w = w.ShiftAllRight(4).Or(w.And(low).ShiftAllLeft(8))
			digits.PermuteOrZeroGrouped(w.AsUint8x32().AsInt8x32()).StoreArray((*[32]uint8)(dst[2*i:]))
		}
	}
	hex.Encode(dst[2*i:], src[i:])
}
