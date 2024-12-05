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

package common

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func ByteCount(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	bGb, exp := MBToGB(b)
	return fmt.Sprintf("%.1f%cB", bGb, "KMGTPE"[exp])
}

func MBToGB(b uint64) (float64, int) {
	const unit = 1024
	if b < unit {
		return float64(b), 0
	}

	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return float64(b) / float64(div), exp
}

var Copy = bytes.Clone

func Append(data ...[]byte) []byte {
	s := new(bytes.Buffer)
	for _, d := range data {
		s.Write(d)
	}
	return s.Bytes()
}

func EnsureEnoughSize(in []byte, size int) []byte {
	if cap(in) < size {
		newBuf := make([]byte, size)
		copy(newBuf, in)
		return newBuf
	}
	return in[:size] // Reuse the space if it has enough capacity
}

func BitLenToByteLen(bitLen int) (byteLen int) {
	return (bitLen + 7) / 8
}

func Shorten(k []byte, l int) []byte {
	if len(k) > l {
		return k[:l]
	}
	return k
}

func BytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

// isHexCharacter returns bool of c being a valid hexadecimal.
func isHexCharacter(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}

// isHex validates whether each byte is valid hexadecimal string.
func isHex(str string) bool {
	if len(str)%2 != 0 {
		return false
	}
	for _, c := range []byte(str) {
		if !isHexCharacter(c) {
			return false
		}
	}
	return true
}

// Bytes2Hex returns the hexadecimal encoding of d.
func Bytes2Hex(d []byte) string {
	return hex.EncodeToString(d)
}

// RightPadBytes zero-pads slice to the right up to length l.
func RightPadBytes(slice []byte, l int) []byte {
	if l <= len(slice) {
		return slice
	}

	padded := make([]byte, l)
	copy(padded, slice)

	return padded
}

// LeftPadBytes zero-pads slice to the left up to length l.
func LeftPadBytes(slice []byte, l int) []byte {
	if l <= len(slice) {
		return slice
	}

	padded := make([]byte, l)
	copy(padded[l-len(slice):], slice)

	return padded
}

// TrimLeftZeroes returns a subslice of s without leading zeroes
func TrimLeftZeroes(s []byte) []byte {
	idx := 0
	for ; idx < len(s); idx++ {
		if s[idx] != 0 {
			break
		}
	}
	return s[idx:]
}

// TrimRightZeroes returns a subslice of s without trailing zeroes
func TrimRightZeroes(s []byte) []byte {
	idx := len(s)
	for ; idx > 0; idx-- {
		if s[idx-1] != 0 {
			break
		}
	}
	return s[:idx]
}

func KeyCmp(key1, key2 []byte) (int, bool) {
	switch {
	//both keys are empty
	case len(key1) == 0 && len(key2) == 0:
		return 0, true
	//	key1 is empty
	case len(key1) == 0 && len(key2) != 0:
		return 1, false
	//	key2 is empty
	case len(key1) != 0 && len(key2) == 0:
		return -1, false
	default:
		return bytes.Compare(key1, key2), false
	}
}
