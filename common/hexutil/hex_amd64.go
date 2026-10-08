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

// encodeHex is hex.Encode with whole 16-byte blocks done by AVX2, two blocks per iteration.
func encodeHex(dst, src []byte) {
	if hasAVX2 {
		digits := archsimd.LoadUint8x32Array(&hexDigits32)
		lowNib := archsimd.BroadcastUint16x16(0x0f)
		for len(src) >= 32 && len(dst) >= 64 {
			encodeBlock(digits, lowNib, (*[16]uint8)(src), (*[32]uint8)(dst))
			encodeBlock(digits, lowNib, (*[16]uint8)(src[16:]), (*[32]uint8)(dst[32:]))
			src, dst = src[32:], dst[64:]
		}
		if len(src) >= 16 && len(dst) >= 32 {
			encodeBlock(digits, lowNib, (*[16]uint8)(src), (*[32]uint8)(dst))
			src, dst = src[16:], dst[32:]
		}
		// The Go code after this uses SSE, which pays a false dependency on Intel while the upper
		// halves of the Y registers are dirty.
		archsimd.ClearAVXUpperBits()
	}
	hex.Encode(dst, src)
}

// encodeBlock widens each byte to a uint16 holding its high nibble in the low byte and its low
// nibble in the high byte, so one in-lane byte shuffle turns the nibbles into digits in output order.
func encodeBlock(digits archsimd.Uint8x32, lowNib archsimd.Uint16x16, src *[16]uint8, dst *[32]uint8) {
	w := archsimd.LoadUint8x16Array(src).ExtendToUint16()
	w = w.ShiftAllRight(4).Or(w.And(lowNib).ShiftAllLeft(8))
	digits.PermuteOrZeroGrouped(w.AsUint8x32().AsInt8x32()).StoreArray(dst)
}

// pairWeights makes VPMADDUBSW compute 16*first + second for each pair of nibbles.
var pairWeights = [32]int8{16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1,
	16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1}

// packBytes moves the low byte of each uint16 of lane 0 to bytes 0-7 and of lane 1 to bytes 8-15.
var packBytes = [32]int8{0, 2, 4, 6, 8, 10, 12, 14, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, 0, 2, 4, 6, 8, 10, 12, 14}

// decodeHex is hex.Decode with whole 32-character blocks done by AVX2, by algorithm 3 of
// http://0x80.pl/notesen/2022-01-17-validating-hex-parse.html: a digit maps to 0-9 and a letter of
// either case to 10-15, anything else to more than 15 on both paths, so the smaller of the two is
// the nibble. A block holding a non-hex character is left to hex.Decode, which reports it.
func decodeHex(dst, src []byte) (int, error) {
	n := 0
	if hasAVX2 {
		c6 := archsimd.BroadcastUint8x32(0xc6)
		six := archsimd.BroadcastUint8x32(6)
		f0 := archsimd.BroadcastUint8x32(0xf0)
		upper := archsimd.BroadcastUint8x32(0xdf)
		bigA := archsimd.BroadcastUint8x32('A')
		ten := archsimd.BroadcastUint8x32(10)
		fifteen := archsimd.BroadcastUint8x32(15)
		weights := archsimd.LoadInt8x32Array(&pairWeights)
		pack := archsimd.LoadInt8x32Array(&packBytes)
		for len(src) >= 32 && len(dst) >= 16 {
			c := archsimd.LoadUint8x32Array((*[32]uint8)(src))
			nib := c.Add(c6).SubSaturated(six).Sub(f0).Min(c.And(upper).Sub(bigA).AddSaturated(ten))
			if nib.Max(fifteen).Equal(fifteen).ToBits() != 0xffffffff {
				break
			}
			b := nib.DotProductPairsSaturated(weights).AsUint8x32().PermuteOrZeroGrouped(pack)
			b.GetLo().Or(b.GetHi()).StoreArray((*[16]uint8)(dst))
			src, dst, n = src[32:], dst[16:], n+16
		}
		archsimd.ClearAVXUpperBits()
	}
	m, err := hex.Decode(dst, src)
	return n + m, err
}
