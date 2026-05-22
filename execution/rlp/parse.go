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
	"errors"
	"fmt"
)

var (
	errBase  = errors.New("rlp")
	ErrParse = fmt.Errorf("%w parse", errBase)
)

// beInt parses Big Endian representation of an integer from given payload at given position
func beInt(payload []byte, pos, length int) (int, error) {
	var r int
	if pos+length > len(payload) {
		return 0, fmt.Errorf("%w: unexpected end of payload", ErrParse)
	}
	if length > 0 && payload[pos] == 0 {
		return 0, fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, payload[pos:pos+length])
	}
	for _, b := range payload[pos : pos+length] {
		r = (r << 8) | int(b)
	}
	return r, nil
}

// Prefix parses RLP Prefix from given payload at given position. It returns the offset and length of the RLP element
// as well as the indication of whether it is a list of string
func Prefix(payload []byte, pos int) (dataPos int, dataLen int, isList bool, err error) {
	if pos < 0 {
		return 0, 0, false, fmt.Errorf("%w: negative position not allowed", ErrParse)
	}
	if pos >= len(payload) {
		return 0, 0, false, fmt.Errorf("%w: unexpected end of payload", ErrParse)
	}
	switch first := payload[pos]; {
	case first < SingleByteThreshold:
		dataPos = pos
		dataLen = 1
		isList = false
	case first < LongStringCode+1:
		// String of 0-55 bytes: single byte with value EmptyStringCode + length,
		// followed by the string.
		dataPos = pos + 1
		dataLen = int(first) - EmptyStringCode
		isList = false
		if dataLen == 1 && dataPos < len(payload) && payload[dataPos] < SingleByteThreshold {
			err = fmt.Errorf("%w: non-canonical size information", ErrParse)
		}
	case first < EmptyListCode:
		// String longer than 55 bytes: single byte with value LongStringCode + length-of-length,
		// followed by the length, followed by the string.
		beLen := int(first) - LongStringCode
		dataPos = pos + 1 + beLen
		dataLen, err = beInt(payload, pos+1, beLen)
		isList = false
		if dataLen < 56 {
			err = fmt.Errorf("%w: non-canonical size information", ErrParse)
		}
	case first < LongListCode+1:
		// List of total payload 0-55 bytes: single byte with value EmptyListCode + length,
		// followed by the concatenation of the RLP encodings of the items.
		dataPos = pos + 1
		dataLen = int(first) - EmptyListCode
		isList = true
	default:
		// List of total payload longer than 55 bytes: single byte with value LongListCode +
		// length-of-length, followed by the length, followed by the items.
		beLen := int(first) - LongListCode
		dataPos = pos + 1 + beLen
		dataLen, err = beInt(payload, pos+1, beLen)
		isList = true
		if dataLen < 56 {
			err = fmt.Errorf("%w: non-canonical size information", ErrParse)
		}
	}
	if err == nil {
		if dataPos+dataLen > len(payload) {
			err = fmt.Errorf("%w: unexpected end of payload", ErrParse)
		} else if dataPos+dataLen < 0 {
			err = fmt.Errorf("%w: found too big len", ErrParse)
		}
	}
	return
}

func ParseList(payload []byte, pos int) (dataPos, dataLen int, err error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if !isList {
		return 0, 0, fmt.Errorf("%w: must be a list", ErrParse)
	}
	return
}

func ParseString(payload []byte, pos int) (dataPos, dataLen int, err error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("%w: must be a string, instead of a list", ErrParse)
	}
	return
}
func StringOfLen(payload []byte, pos, expectedLen int) (dataPos int, err error) {
	dataPos, dataLen, err := ParseString(payload, pos)
	if err != nil {
		return 0, err
	}
	if dataLen != expectedLen {
		return 0, fmt.Errorf("%w: expected string of len %d, got %d", ErrParse, expectedLen, dataLen)
	}
	return
}

// U64 parses uint64 number from given payload at given position
func ParseU64(payload []byte, pos int) (int, uint64, error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("%w: uint64 must be a string, not isList", ErrParse)
	}
	if dataLen > 8 {
		return 0, 0, fmt.Errorf("%w: uint64 must not be more than 8 bytes long, got %d", ErrParse, dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, payload[dataPos:dataPos+dataLen])
	}
	var r uint64
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint64(b)
	}
	return dataPos + dataLen, r, nil
}

// U32 parses uint64 number from given payload at given position
func ParseU32(payload []byte, pos int) (int, uint32, error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("%w: uint32 must be a string, not isList", ErrParse)
	}
	if dataLen > 4 {
		return 0, 0, fmt.Errorf("%w: uint32 must not be more than 4 bytes long, got %d", ErrParse, dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, payload[dataPos:dataPos+dataLen])
	}
	var r uint32
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint32(b)
	}
	return dataPos + dataLen, r, nil
}
