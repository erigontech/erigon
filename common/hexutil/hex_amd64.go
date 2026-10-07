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
	if hasAVX2 {
		digits := archsimd.LoadUint8x32Array(&hexDigits32)
		low := archsimd.BroadcastUint16x16(0x0f)
		for len(src) >= 16 && len(dst) >= 32 {
			w := archsimd.LoadUint8x16Array((*[16]uint8)(src)).ExtendToUint16()
			w = w.ShiftAllRight(4).Or(w.And(low).ShiftAllLeft(8))
			digits.PermuteOrZeroGrouped(w.AsUint8x32().AsInt8x32()).StoreArray((*[32]uint8)(dst))
			src, dst = src[16:], dst[32:]
		}
	}
	hex.Encode(dst, src)
}

// evenBytes gathers the even byte of each uint16 of a lane into its low half, which is how the
// decoded bytes are packed without VPMOVWB, an AVX-512 instruction.
var evenBytes = [32]int8{0, 2, 4, 6, 8, 10, 12, 14, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 2, 4, 6, 8, 10, 12, 14, -1, -1, -1, -1, -1, -1, -1, -1}

// decodeHex is hex.Decode with whole 32-character blocks done by AVX2. A pair of characters is one
// uint16, so both nibbles are computed in place: (c & 0x0f) + 9*(c >> 6) is the value of every hex
// digit, upper or lower case. A block holding anything else is left to hex.Decode, which reports
// it: the nibbles are mapped back to digits and compared, and 0x10-0x19 would map to '0'-'9' once
// the case bit is set, so those are excluded by the bit the digits and the letters share.
func decodeHex(dst, src []byte) (int, error) {
	n := 0
	if hasAVX2 {
		digits := archsimd.LoadUint8x32Array(&hexDigits32)
		gather := archsimd.LoadInt8x32Array(&evenBytes)
		lowNib := archsimd.BroadcastUint16x16(0x000f)
		loByte := archsimd.BroadcastUint16x16(0x00ff)
		nine := archsimd.BroadcastUint16x16(9)
		lower := archsimd.BroadcastUint8x32(0x20)
		letterOrDigit := archsimd.BroadcastUint8x32(0x60)
		zero := archsimd.BroadcastUint8x32(0)
		for len(src) >= 32 && len(dst) >= 16 {
			chars := archsimd.LoadUint8x32Array((*[32]uint8)(src))
			pairs := chars.AsUint16x16()
			hi, lo := pairs.And(loByte), pairs.ShiftAllRight(8)
			hiNib := hi.And(lowNib).Add(nine.Mul(hi.ShiftAllRight(6)))
			loNib := lo.And(lowNib).Add(nine.Mul(lo.ShiftAllRight(6)))
			// Both nibbles back to digits at once: the low byte of each uint16 holds the first
			// character's digit and the high byte the second's, which is the input order.
			back := digits.PermuteOrZeroGrouped(hiNib.Or(loNib.ShiftAllLeft(8)).AsUint8x32().AsInt8x32())
			roundTrips := back.Equal(chars.Or(lower)).ToBits()
			notControl := chars.And(letterOrDigit).Equal(zero).ToBits()
			if roundTrips != 0xffffffff || notControl != 0 {
				break
			}
			packed := hiNib.ShiftAllLeft(4).Or(loNib).AsUint8x32().PermuteOrZeroGrouped(gather)
			packed.GetLo().StorePart(dst[:8])
			packed.GetHi().StorePart(dst[8:16])
			src, dst, n = src[32:], dst[16:], n+16
		}
	}
	m, err := hex.Decode(dst, src)
	return n + m, err
}
