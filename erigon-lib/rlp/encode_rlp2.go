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

	"github.com/erigontech/erigon-lib/common"
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
//      - but for convenience and performance - provided higher-level functions (for example for EncodeHash - for []byte of len 32)
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

func U32Len(i uint32) int {
	if i < 128 {
		return 1
	}
	return 1 + common.BitLenToByteLen(bits.Len32(i))
}

func U64Len(i uint64) int {
	if i < 128 {
		return 1
	}
	return 1 + common.BitLenToByteLen(bits.Len64(i))
}

func EncodeU32(i uint32, to []byte) int {
	if i == 0 {
		to[0] = 128
		return 1
	}
	if i < 128 {
		to[0] = byte(i) // fits single byte
		return 1
	}

	b := to[1:]
	var l int

	// writes i to b in big endian byte order, using the least number of bytes needed to represent i.
	switch {
	case i < (1 << 8):
		b[0] = byte(i)
		l = 1
	case i < (1 << 16):
		b[0] = byte(i >> 8)
		b[1] = byte(i)
		l = 2
	case i < (1 << 24):
		b[0] = byte(i >> 16)
		b[1] = byte(i >> 8)
		b[2] = byte(i)
		l = 3
	default:
		b[0] = byte(i >> 24)
		b[1] = byte(i >> 16)
		b[2] = byte(i >> 8)
		b[3] = byte(i)
		l = 4
	}

	to[0] = 128 + byte(l)
	return 1 + l
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

	b := to[1:]
	var l int

	// writes i to b in big endian byte order, using the least number of bytes needed to represent i.
	switch {
	case i < (1 << 8):
		b[0] = byte(i)
		l = 1
	case i < (1 << 16):
		b[0] = byte(i >> 8)
		b[1] = byte(i)
		l = 2
	case i < (1 << 24):
		b[0] = byte(i >> 16)
		b[1] = byte(i >> 8)
		b[2] = byte(i)
		l = 3
	case i < (1 << 32):
		b[0] = byte(i >> 24)
		b[1] = byte(i >> 16)
		b[2] = byte(i >> 8)
		b[3] = byte(i)
		l = 4
	case i < (1 << 40):
		b[0] = byte(i >> 32)
		b[1] = byte(i >> 24)
		b[2] = byte(i >> 16)
		b[3] = byte(i >> 8)
		b[4] = byte(i)
		l = 5
	case i < (1 << 48):
		b[0] = byte(i >> 40)
		b[1] = byte(i >> 32)
		b[2] = byte(i >> 24)
		b[3] = byte(i >> 16)
		b[4] = byte(i >> 8)
		b[5] = byte(i)
		l = 6
	case i < (1 << 56):
		b[0] = byte(i >> 48)
		b[1] = byte(i >> 40)
		b[2] = byte(i >> 32)
		b[3] = byte(i >> 24)
		b[4] = byte(i >> 16)
		b[5] = byte(i >> 8)
		b[6] = byte(i)
		l = 7
	default:
		b[0] = byte(i >> 56)
		b[1] = byte(i >> 48)
		b[2] = byte(i >> 40)
		b[3] = byte(i >> 32)
		b[4] = byte(i >> 24)
		b[5] = byte(i >> 16)
		b[6] = byte(i >> 8)
		b[7] = byte(i)
		l = 8
	}

	to[0] = 128 + byte(l)
	return 1 + l
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

// EncodeAddress assumes that `to` buffer is already 21-bytes long
func EncodeAddress(a, to []byte) int {
	_ = to[20] // early bounds check to guarantee safety of writes below
	to[0] = 128 + 20
	copy(to[1:21], a[:20])
	return 21
}

// EncodeHash assumes that `to` buffer is already 33-bytes long
func EncodeHash(h, to []byte) int {
	_ = to[32] // early bounds check to guarantee safety of writes below
	to[0] = 128 + 32
	copy(to[1:33], h[:32])
	return 33
}

func HashesLen(hashes []byte) int {
	hashesLen := len(hashes) / 32 * 33
	return ListPrefixLen(hashesLen) + hashesLen
}

func EncodeHashes(hashes []byte, encodeBuf []byte) int {
	pos := 0
	hashesLen := len(hashes) / 32 * 33
	pos += EncodeListPrefix(hashesLen, encodeBuf)
	for i := 0; i < len(hashes); i += 32 {
		pos += EncodeHash(hashes[i:], encodeBuf[pos:])
	}
	return pos
}

func AnnouncementsLen(types []byte, sizes []uint32, hashes []byte) int {
	if len(types) == 0 {
		return 4
	}
	typesLen := StringLen(types)
	var sizesLen int
	for _, size := range sizes {
		sizesLen += U32Len(size)
	}
	hashesLen := len(hashes) / 32 * 33
	totalLen := typesLen + sizesLen + ListPrefixLen(sizesLen) + hashesLen + ListPrefixLen(hashesLen)
	return ListPrefixLen(totalLen) + totalLen
}

// EIP-5793: eth/68 - Add txn type to txn announcement
func EncodeAnnouncements(types []byte, sizes []uint32, hashes []byte, encodeBuf []byte) int {
	if len(types) == 0 {
		encodeBuf[0] = 0xc3
		encodeBuf[1] = 0x80
		encodeBuf[2] = 0xc0
		encodeBuf[3] = 0xc0
		return 4
	}
	pos := 0
	typesLen := StringLen(types)
	var sizesLen int
	for _, size := range sizes {
		sizesLen += U32Len(size)
	}
	hashesLen := len(hashes) / 32 * 33
	totalLen := typesLen + sizesLen + ListPrefixLen(sizesLen) + hashesLen + ListPrefixLen(hashesLen)
	pos += EncodeListPrefix(totalLen, encodeBuf)
	pos += EncodeString2(types, encodeBuf[pos:])
	pos += EncodeListPrefix(sizesLen, encodeBuf[pos:])
	for _, size := range sizes {
		pos += EncodeU32(size, encodeBuf[pos:])
	}
	pos += EncodeListPrefix(hashesLen, encodeBuf[pos:])
	for i := 0; i < len(hashes); i += 32 {
		pos += EncodeHash(hashes[i:], encodeBuf[pos:])
	}
	return pos
}
