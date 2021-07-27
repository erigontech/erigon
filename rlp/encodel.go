package rlp

import (
	"encoding/binary"
	"math/bits"
)

// General design:
//      - no io.Writer, because it's incompatible with binary.BigEndian functions and Writer can't be used as temporary buffer
//      - rlp has 2 data types: List and String (bytes array), and low-level funcs are operate with this types.
//      - but for convenience and performance - provided higher-level functions (for example for EncodeHash - for []byte of len 32)
//
// General rules:
//      - functions to calculate prefix len are fast (and pure). it's ok to call them multiple times during encoding of large object for readability.
//      - functions to Parse (Decode) data - using data type as name (without any prefix): rlp.String(), rlp.List, rlp.U64(), rlp.U256()
//

func ListPrefixLen(dataLen int) int {
	if dataLen >= 56 {
		return 1 + (bits.Len64(uint64(dataLen))+7)/8
	}
	return 1
}
func EncodeListPrefix(dataLen int, to []byte) {
	if dataLen >= 56 {
		_ = to[9]
		beLen := (bits.Len64(uint64(dataLen)) + 7) / 8
		binary.BigEndian.PutUint64(to[1:], uint64(dataLen))
		to[8-beLen] = 247 + byte(beLen)
		copy(to, to[8-beLen:9])
		return
	}
	to[0] = 192 + byte(dataLen)
}
func U64Len(i uint64) int {
	if i > 128 {
		return 1 + (bits.Len64(i)+7)/8
	}
	return 1
}

func EncodeU64(i uint64, to []byte) {
	if i > 128 {
		l := (bits.Len64(i) + 7) / 8
		to[0] = 128 + byte(l)
		binary.BigEndian.PutUint64(to[1:], i)
		copy(to[1:], to[1+8-l:1+8])
		return
	}
	if i == 0 {
		to[0] = 128
		return
	}
	to[0] = byte(i)
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
func EncodeHash(h, to []byte) {
	_ = to[32] // early bounds check to guarantee safety of writes below
	to[0] = 128 + 32
	copy(to[1:33], h)
}
