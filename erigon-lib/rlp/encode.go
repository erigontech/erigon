/*
   Copyright 2021 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package rlp

import (
	"encoding/binary"
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/common"
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
func EncodeString(s []byte, to []byte) int {
	switch {
	case len(s) >= 56:
		beLen := common.BitLenToByteLen(bits.Len(uint(len(s))))
		binary.BigEndian.PutUint64(to[1:], uint64(len(s)))
		_ = to[beLen+len(s)]

		to[8-beLen] = byte(beLen) + 183
		copy(to, to[8-beLen:9])
		copy(to[1+beLen:], s)
		return 1 + beLen + len(s)
	case len(s) == 0:
		to[0] = 128
		return 1
	case len(s) == 1:
		if s[0] < 128 {
			to[0] = s[0]
			return 1
		}
		to[0] = 129
		to[1] = s[0]
		return 2
	default: // 1<s<56
		_ = to[len(s)]
		to[0] = byte(len(s)) + 128
		copy(to[1:], s)
		return 1 + len(s)
	}
}

// EncodeHash assumes that `to` buffer is already 32bytes long
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

// EIP-5793: eth/68 - Add tx type to tx announcement
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
	pos += EncodeString(types, encodeBuf[pos:])
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
