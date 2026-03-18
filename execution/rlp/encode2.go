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

package rlp

import (
	"encoding/binary"
	"math/bits"

	"github.com/erigontech/erigon/common"
)

// General design:
//      - rlp package doesn't manage memory - and Caller must ensure buffers are big enough.
//      - no io.Writer, because it's incompatible with binary.BigEndian functions and Writer can't be used as temporary buffer
//
// Composition:
//     - each Encode method does write to given buffer and return written len
//     - each Parse accept position in payload and return new position
//
// General rules:
//      - functions to calculate prefix len are fast (and pure). it's ok to call them multiple times during encoding of large object for readability.
//      - rlp has 2 data types: List and String (bytes array), and low-level funcs are operate with this types.
//      - functions to Parse (Decode) data - using data type as name (without any prefix): rlp.String(), rlp.List, rlp.U64(), rlp.U256()
//

func ListPrefixLen(dataLen int) int {
	if dataLen >= 56 {
		return 1 + common.BitLenToByteLen(bits.Len64(uint64(dataLen)))
	}
	return 1
}
func EncodeListPrefix(dataLen int, to []byte) int {
	if dataLen >= 56 {
		_ = to[9]
		beLen := common.BitLenToByteLen(bits.Len64(uint64(dataLen)))
		binary.BigEndian.PutUint64(to[1:], uint64(dataLen))
		to[8-beLen] = 247 + byte(beLen)
		copy(to, to[8-beLen:9])
		return 1 + beLen
	}
	to[0] = 192 + byte(dataLen)
	return 1
}

func U64Len(i uint64) int {
	if i < 128 {
		return 1
	}
	return 1 + common.BitLenToByteLen(bits.Len64(i))
}

func EncodeU32(i uint32, to []byte) int {
	return EncodeU64(uint64(i), to)
}

func EncodeU64(i uint64, to []byte) int {
	if i == 0 {
		to[0] = 128
		return 1
	}
	if i < 128 {
		to[0] = byte(i) // fits single byte
		return 1
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	size := common.BitLenToByteLen(bits.Len64(i))
	to[0] = 128 + byte(size)
	copy(to[1:], buf[8-size:])
	return 1 + size
}

func StringLen(s []byte) int {
	sLen := len(s)
	switch {
	case sLen >= 56:
		beLen := common.BitLenToByteLen(bits.Len(uint(sLen)))
		return 1 + beLen + sLen
	case sLen == 0:
		return 1
	case sLen == 1:
		if s[0] < 128 {
			return 1
		}
		return 1 + sLen
	default: // 1<s<56
		return 1 + sLen
	}
}
func EncodeString2(src []byte, dst []byte) int {
	switch {
	case len(src) >= 56:
		beLen := common.BitLenToByteLen(bits.Len(uint(len(src))))
		binary.BigEndian.PutUint64(dst[1:], uint64(len(src)))
		_ = dst[beLen+len(src)]

		dst[8-beLen] = byte(beLen) + 183
		copy(dst, dst[8-beLen:9])
		copy(dst[1+beLen:], src)
		return 1 + beLen + len(src)
	case len(src) == 0:
		dst[0] = 128
		return 1
	case len(src) == 1:
		if src[0] < 128 {
			dst[0] = src[0]
			return 1
		}
		dst[0] = 129
		dst[1] = src[0]
		return 2
	default: // 1<src<56
		_ = dst[len(src)]
		dst[0] = byte(len(src)) + 128
		copy(dst[1:], src)
		return 1 + len(src)
	}
}

