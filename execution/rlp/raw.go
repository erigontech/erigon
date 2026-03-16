// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"io"
	"math/bits"
	"reflect"

	"github.com/erigontech/erigon/common"
)

// RawValue represents an encoded RLP value and can be used to delay
// RLP decoding or to precompute an encoding. Note that the decoder does
// not verify whether the content of RawValues is valid RLP.
type RawValue []byte

var rawValueType = reflect.TypeFor[RawValue]()

// StringSize returns the encoded size of a string.
func StringSize(s string) uint64 {
	switch n := len(s); n {
	case 0:
		return 1
	case 1:
		if s[0] <= 0x7f {
			return 1
		} else {
			return 2
		}
	default:
		return uint64(headsize(uint64(n)) + n)
	}
}

// BytesSize returns the encoded size of a byte slice.
func BytesSize(b []byte) uint64 {
	return uint64(StringLen(b))
}

// ListSize returns the encoded size of an RLP list with the given
// content size.
func ListSize(contentSize uint64) uint64 {
	return uint64(headsize(contentSize)) + contentSize
}

// IntSize returns the encoded size of the integer x. Note: The return type of this
// function is 'int' for backwards-compatibility reasons. The result is always positive.
func IntSize(x uint64) int {
	return U64Len(x)
}

// Split returns the content of first RLP value and any
// bytes after the value as subslices of b.
func Split(b []byte) (k Kind, content, rest []byte, err error) {
	k, ts, cs, err := readKind(b)
	if err != nil {
		return 0, nil, b, err
	}
	return k, b[ts : ts+cs], b[ts+cs:], nil
}

// SplitString splits b into the content of an RLP string
// and any remaining bytes after the string.
func SplitString(b []byte) (content, rest []byte, err error) {
	k, content, rest, err := Split(b)
	if err != nil {
		return nil, b, err
	}
	if k == List {
		return nil, b, ErrExpectedString
	}
	return content, rest, nil
}

// SplitUint64 decodes an integer at the beginning of b.
// It also returns the remaining data after the integer in 'rest'.
func SplitUint64(b []byte) (x uint64, rest []byte, err error) {
	content, rest, err := SplitString(b)
	if err != nil {
		return 0, b, err
	}
	switch n := len(content); n {
	case 0:
		return 0, rest, nil
	case 1:
		if content[0] == 0 {
			return 0, b, ErrCanonInt
		}
		return uint64(content[0]), rest, nil
	default:
		if n > 8 {
			return 0, b, errUintOverflow
		}

		x, err = readSize(content, byte(n))
		if err != nil {
			return 0, b, ErrCanonInt
		}
		return x, rest, nil
	}
}

// SplitList splits b into the content of a list and any remaining
// bytes after the list.
func SplitList(b []byte) (content, rest []byte, err error) {
	k, content, rest, err := Split(b)
	if err != nil {
		return nil, b, err
	}
	if k != List {
		return nil, b, ErrExpectedList
	}
	return content, rest, nil
}

// CountValues returns the number of encoded values in b.
// If an error is encountered while parsing, the count
// includes the item that caused the error.
func CountValues(b []byte) (int, error) {
	i := 0
	for ; len(b) > 0; i++ {
		_, tagsize, size, err := readKind(b)
		if err != nil {
			return i + 1, err
		}
		b = b[tagsize+size:]
	}
	return i, nil
}

func readKind(buf []byte) (k Kind, tagsize, contentsize uint64, err error) {
	if len(buf) == 0 {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	b := buf[0]
	switch {
	case b < 0x80:
		k = Byte
		tagsize = 0
		contentsize = 1
	case b < 0xB8:
		k = String
		tagsize = 1
		contentsize = uint64(b - 0x80)
		// Reject strings that should've been single bytes.
		if contentsize == 1 && len(buf) > 1 && buf[1] < 128 {
			return 0, 0, 0, ErrCanonSize
		}
	case b < 0xC0:
		k = String
		tagsize = uint64(b-0xB7) + 1
		contentsize, err = readSize(buf[1:], b-0xB7)
	case b < 0xF8:
		k = List
		tagsize = 1
		contentsize = uint64(b - 0xC0)
	default:
		k = List
		tagsize = uint64(b-0xF7) + 1
		contentsize, err = readSize(buf[1:], b-0xF7)
	}
	if err != nil {
		return 0, 0, 0, err
	}
	// Reject values larger than the input slice.
	if contentsize > uint64(len(buf))-tagsize {
		return 0, 0, 0, ErrValueTooLarge
	}
	return k, tagsize, contentsize, err
}

func readSize(b []byte, slen byte) (uint64, error) {
	if int(slen) > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	var buf [8]byte
	copy(buf[8-slen:], b[:slen])
	s := binary.BigEndian.Uint64(buf[:])
	// Reject sizes < 56 (shouldn't have separate size) and sizes with
	// leading zero bytes.
	if s < 56 || b[0] == 0 {
		return 0, ErrCanonSize
	}
	return s, nil
}

// AppendUint64 appends the RLP encoding of i to b, and returns the resulting slice.
func AppendUint64(b []byte, i uint64) []byte {
	if i == 0 {
		return append(b, 0x80)
	} else if i < 128 {
		return append(b, byte(i))
	}
	var buf [9]byte
	binary.BigEndian.PutUint64(buf[1:], i)
	size := common.BitLenToByteLen(bits.Len64(i))
	buf[8-size] = 0x80 + byte(size)
	return append(b, buf[8-size:]...)
}
