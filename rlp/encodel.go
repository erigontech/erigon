/*
   Copyright 2021 Erigon contributors

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
		return 1 + (bits.Len64(uint64(dataLen))+7)/8
	}
	return 1
}
func EncodeListPrefix(dataLen int, to []byte) int {
	if dataLen >= 56 {
		_ = to[9]
		beLen := (bits.Len64(uint64(dataLen)) + 7) / 8
		binary.BigEndian.PutUint64(to[1:], uint64(dataLen))
		to[8-beLen] = 247 + byte(beLen)
		copy(to, to[8-beLen:9])
		return 1 + beLen
	}
	to[0] = 192 + byte(dataLen)
	return 1
}
func U64Len(i uint64) int {
	if i > 128 {
		return 1 + (bits.Len64(i)+7)/8
	}
	return 1
}

func EncodeU64(i uint64, to []byte) int {
	if i > 128 {
		beLen := (bits.Len64(i) + 7) / 8
		to[0] = 128 + byte(beLen)
		binary.BigEndian.PutUint64(to[1:], i)
		copy(to[1:], to[1+8-beLen:1+8])
		return 1 + beLen
	}
	if i == 0 {
		to[0] = 128
		return 1
	}
	to[0] = byte(i)
	return 1
}

func EncodeString(s []byte, to []byte) {
	switch {
	case len(s) > 56:
		beLen := (bits.Len(uint(len(s))) + 7) / 8
		binary.BigEndian.PutUint64(to[1:], uint64(len(s)))
		_ = to[beLen+len(s)]

		to[8-beLen] = byte(beLen) + 183
		copy(to, to[8-beLen:9])
		copy(to[1+beLen:], s)
	case len(s) == 0:
		to[0] = 128
	case len(s) == 1:
		_ = to[1]
		if s[0] >= 128 {
			to[0] = 129
		}
		copy(to[1:], s)
	default: // 1<s<56
		_ = to[len(s)]
		to[0] = byte(len(s)) + 128
		copy(to[1:], s)
	}
}

// EncodeHash assumes that `to` buffer is already 32bytes long
func EncodeHash(h, to []byte) int {
	_ = to[32] // early bounds check to guarantee safety of writes below
	to[0] = 128 + 32
	copy(to[1:33], h[:32])
	return 33
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
