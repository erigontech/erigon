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

// decodeHex is hex.Decode with whole 32-character blocks done by AVX2. A pair of characters is
// one uint16, so the two nibbles are computed in place: (c & 0x0f) + 9*(c >> 6) is the value of
// every hex digit, upper or lower case. A block with a character that is not a hex digit is left
// to hex.Decode, which reports it: the nibbles are mapped back to digits and compared, so a
// block passes only when every character round-trips.
func decodeHex(dst, src []byte) (int, error) {
	if hasAVX2 {
		digits := archsimd.LoadUint8x32Array(&hexDigits32)
		lowNib := archsimd.BroadcastUint16x16(0x000f)
		loByte := archsimd.BroadcastUint16x16(0x00ff)
		nine := archsimd.BroadcastUint16x16(9)
		lower := archsimd.BroadcastUint8x32(0x20)
		n := 0
		for len(src) >= 32 && len(dst) >= 16 {
			chars := archsimd.LoadUint8x32Array((*[32]uint8)(src))
			pairs := chars.AsUint16x16()
			hi, lo := pairs.And(loByte), pairs.ShiftAllRight(8)
			hiNib := hi.And(lowNib).Add(nine.Mul(hi.ShiftAllRight(6)))
			loNib := lo.And(lowNib).Add(nine.Mul(lo.ShiftAllRight(6)))
			// Both nibbles back to digits at once: the low byte of each uint16 holds the first
			// character's digit and the high byte the second's, which is the input order.
			back := digits.PermuteOrZeroGrouped(hiNib.Or(loNib.ShiftAllLeft(8)).AsUint8x32().AsInt8x32())
			if back.Equal(chars.Or(lower)).ToBits() != 0xffffffff {
				break
			}
			hiNib.ShiftAllLeft(4).Or(loNib).TruncToUint8().StoreArray((*[16]uint8)(dst))
			src, dst, n = src[32:], dst[16:], n+16
		}
		if n > 0 {
			m, err := hex.Decode(dst, src)
			return n + m, err
		}
	}
	return hex.Decode(dst, src)
}
