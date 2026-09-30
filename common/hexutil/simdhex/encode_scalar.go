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

package simdhex

import "encoding/binary"

var pairs = func() (t [256]uint16) {
	const digits = "0123456789abcdef"
	for i := range t {
		t[i] = uint16(digits[i>>4]) | uint16(digits[i&0x0f])<<8
	}
	return t
}()

// encodeTable writes both digits of a byte with one 16-bit store from a 256-entry table.
func encodeTable(dst, src []byte) int {
	dst = dst[:2*len(src)]
	for i, b := range src {
		binary.LittleEndian.PutUint16(dst[2*i:], pairs[b])
	}
	return 2 * len(src)
}

// encodeSWAR turns 4 source bytes into 8 digits inside one uint64: each byte is spread to a
// 16-bit slot, its nibbles swapped into output order, and 'a'-'0'-10 added where a nibble is > 9.
func encodeSWAR(dst, src []byte) int {
	dst = dst[:2*len(src)]
	i := 0
	for ; i+8 <= len(src); i += 8 {
		x := binary.LittleEndian.Uint64(src[i:])
		binary.LittleEndian.PutUint64(dst[2*i:], swar4(uint32(x)))
		binary.LittleEndian.PutUint64(dst[2*i+8:], swar4(uint32(x>>32)))
	}
	for ; i < len(src); i++ {
		binary.LittleEndian.PutUint16(dst[2*i:], pairs[src[i]])
	}
	return 2 * len(src)
}

func swar4(x uint32) uint64 {
	v := uint64(x)
	v = (v | v<<16) & 0x0000ffff0000ffff
	v = (v | v<<8) & 0x00ff00ff00ff00ff
	n := (v>>4)&0x000f000f000f000f | (v&0x000f000f000f000f)<<8
	letters := ((n + 0x0606060606060606) >> 4) & 0x0101010101010101
	return n + 0x3030303030303030 + letters*('a'-'0'-10)
}
