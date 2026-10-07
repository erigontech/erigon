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

// encodeHex is hex.Encode with whole 8-byte blocks done by NEON. NEON has no byte shuffle on a
// 128-bit vector, so a nibble becomes its digit by arithmetic: (nib+6)>>4 is 1 exactly when the
// nibble is 10 or more, which is the step from '9'+1 to 'a'.
func encodeHex(dst, src []byte) {
	lowNib := archsimd.BroadcastUint16x8(0x000f)
	six := archsimd.BroadcastUint16x8(0x0606)
	one := archsimd.BroadcastUint16x8(0x0101)
	zeroDigit := archsimd.BroadcastUint16x8(0x3030)
	letterStep := archsimd.BroadcastUint16x8(39)
	for len(src) >= 8 && len(dst) >= 16 {
		// Each byte to a uint16 holding its high nibble low and its low nibble high, which is the
		// order the two digits are written in.
		v, _ := archsimd.LoadUint8x16Part(src[:8])
		w := v.ExtendLo8ToUint16()
		nibs := w.ShiftAllRight(4).Or(w.And(lowNib).ShiftAllLeft(8))
		step := nibs.Add(six).ShiftAllRight(4).And(one)
		nibs.Add(zeroDigit).Add(letterStep.Mul(step)).ReshapeToUint8s().StorePart(dst[:16])
		src, dst = src[8:], dst[16:]
	}
	hex.Encode(dst, src)
}

// decodeHex is hex.Decode with whole 16-character blocks done by NEON. A pair of characters is one
// uint16, so both nibbles are computed in place: (c & 0x0f) + 9*(c >> 6) is the value of every hex
// digit, upper or lower case. A block holding anything else is left to hex.Decode, which reports
// it: a character is a digit when c-'0' saturates to zero against 9 and a letter when the same
// holds for (c|0x20)-'a' against 5, so the smaller of the two is zero for every hex digit only.
func decodeHex(dst, src []byte) (int, error) {
	n := 0
	lowNib := archsimd.BroadcastUint16x8(0x000f)
	loByte := archsimd.BroadcastUint16x8(0x00ff)
	nine := archsimd.BroadcastUint16x8(9)
	nine8 := archsimd.BroadcastUint8x16(9)
	five8 := archsimd.BroadcastUint8x16(5)
	zeroDigit8 := archsimd.BroadcastUint8x16('0')
	aDigit8 := archsimd.BroadcastUint8x16('a')
	lower8 := archsimd.BroadcastUint8x16(0x20)
	for len(src) >= 16 && len(dst) >= 8 {
		chars := archsimd.LoadUint8x16Array((*[16]uint8)(src))
		notDigit := chars.Sub(zeroDigit8).SubSaturated(nine8)
		notLetter := chars.Or(lower8).Sub(aDigit8).SubSaturated(five8)
		if notDigit.Min(notLetter).ReduceMax() != 0 {
			break
		}
		pairs := chars.ReshapeToUint16s()
		hi, lo := pairs.And(loByte), pairs.ShiftAllRight(8)
		hiNib := hi.And(lowNib).Add(nine.Mul(hi.ShiftAllRight(6)))
		loNib := lo.And(lowNib).Add(nine.Mul(lo.ShiftAllRight(6)))
		// ConcatEven keeps the even byte of each uint16, which is the decoded byte.
		out := hiNib.ShiftAllLeft(4).Or(loNib).ReshapeToUint8s()
		out.ConcatEven(out).StorePart(dst[:8])
		src, dst, n = src[16:], dst[8:], n+8
	}
	m, err := hex.Decode(dst, src)
	return n + m, err
}
