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
	"fmt"

	"github.com/holiman/uint256"
)

// BeInt parses Big Endian representation of an integer from given payload at given position
func BeInt(payload []byte, pos, length int) (int, error) {
	var r int
	if pos+length >= len(payload) {
		return 0, fmt.Errorf("unexpected end of payload")
	}
	if length > 0 && payload[pos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[pos:pos+length])
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
		return 0, 0, false, fmt.Errorf("negative position not allowed")
	}
	if pos >= len(payload) {
		return 0, 0, false, fmt.Errorf("unexpected end of payload")
	}
	switch first := payload[pos]; {
	case first < 128:
		dataPos = pos
		dataLen = 1
		isList = false
	case first < 184:
		// string of len < 56, and it is non-legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 128
		isList = false
	case first < 192:
		// string of len >= 56, and it is non-legacy transaction
		beLen := int(first) - 183
		dataPos = pos + 1 + beLen
		dataLen, err = BeInt(payload, pos+1, beLen)
		isList = false
	case first < 248:
		// isList of len < 56, and it is a legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 192
		isList = true
	default:
		// isList of len >= 56, and it is a legacy transaction
		beLen := int(first) - 247
		dataPos = pos + 1 + beLen
		dataLen, err = BeInt(payload, pos+1, beLen)
		isList = true
	}
	if err == nil {
		if dataPos+dataLen > len(payload) {
			err = fmt.Errorf("unexpected end of payload")
		} else if dataPos+dataLen < 0 {
			err = fmt.Errorf("found too big len")
		}
	}
	return
}

func List(payload []byte, pos int) (dataPos int, dataLen int, err error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if !isList {
		return 0, 0, fmt.Errorf("must be a list")
	}
	return
}

func String(payload []byte, pos int) (dataPos int, dataLen int, err error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("must be a string, instead of a list")
	}
	return
}
func StringOfLen(payload []byte, pos, expectedLen int) (dataPos int, err error) {
	dataPos, dataLen, err := String(payload, pos)
	if err != nil {
		return 0, err
	}
	if dataLen != expectedLen {
		return 0, fmt.Errorf("expected string of len %d, got %d", expectedLen, dataLen)
	}
	return
}

// U64 parses uint64 number from given payload at given position
func U64(payload []byte, pos int) (int, uint64, error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("uint64 must be a string, not isList")
	}
	if dataLen > 8 {
		return 0, 0, fmt.Errorf("uint64 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	var r uint64
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint64(b)
	}
	return dataPos + dataLen, r, nil
}

// U32 parses uint64 number from given payload at given position
func U32(payload []byte, pos int) (int, uint32, error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if isList {
		return 0, 0, fmt.Errorf("uint32 must be a string, not isList")
	}
	if dataLen > 4 {
		return 0, 0, fmt.Errorf("uint32 must not be more than 4 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	var r uint32
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint32(b)
	}
	return dataPos + dataLen, r, nil
}

// U256 parses uint256 number from given payload at given position
func U256(payload []byte, pos int, x *uint256.Int) (int, error) {
	dataPos, dataLen, err := String(payload, pos)
	if err != nil {
		return 0, err
	}
	if dataLen > 32 {
		return 0, fmt.Errorf("uint256 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	x.SetBytes(payload[dataPos : dataPos+dataLen])
	return dataPos + dataLen, nil
}
func U256Len(z *uint256.Int) int {
	if z == nil {
		return 1
	}
	nBits := z.BitLen()
	if nBits == 0 {
		return 1
	}
	if nBits <= 7 {
		return 1
	}
	return 1 + (nBits+7)/8
}

func ParseHash(payload []byte, pos int, hashbuf []byte) (int, error) {
	pos, err := StringOfLen(payload, pos, 32)
	if err != nil {
		return 0, fmt.Errorf("%s: hash len: %w", ParseHashErrorPrefix, err)
	}
	copy(hashbuf, payload[pos:pos+32])
	return pos + 32, nil
}

const ParseHashErrorPrefix = "parse hash payload"
