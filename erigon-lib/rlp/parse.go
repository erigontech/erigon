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
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/common"
)

var (
	ErrBase   = fmt.Errorf("rlp")
	ErrParse  = fmt.Errorf("%w parse", ErrBase)
	ErrDecode = fmt.Errorf("%w decode", ErrBase)
)

func IsRLPError(err error) bool { return errors.Is(err, ErrBase) }

// BeInt parses Big Endian representation of an integer from given payload at given position
func BeInt(payload []byte, pos, length int) (int, error) {
	var r int
	if pos+length >= len(payload) {
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
	case first < 128:
		dataPos = pos
		dataLen = 1
		isList = false
	case first < 184:
		// Otherwise, if a string is 0-55 bytes long,
		// the RLP encoding consists of a single byte with value 0x80 plus the
		// length of the string followed by the string. The range of the first
		// byte is thus [0x80, 0xB7].
		dataPos = pos + 1
		dataLen = int(first) - 128
		isList = false
		if dataLen == 1 && dataPos < len(payload) && payload[dataPos] < 128 {
			err = fmt.Errorf("%w: non-canonical size information", ErrParse)
		}
	case first < 192:
		// If a string is more than 55 bytes long, the
		// RLP encoding consists of a single byte with value 0xB7 plus the length
		// of the length of the string in binary form, followed by the length of
		// the string, followed by the string. For example, a length-1024 string
		// would be encoded as 0xB90400 followed by the string. The range of
		// the first byte is thus [0xB8, 0xBF].
		beLen := int(first) - 183
		dataPos = pos + 1 + beLen
		dataLen, err = BeInt(payload, pos+1, beLen)
		isList = false
		if dataLen < 56 {
			err = fmt.Errorf("%w: non-canonical size information", ErrParse)
		}
	case first < 248:
		// isList of len < 56
		// If the total payload of a list
		// (i.e. the combined length of all its items) is 0-55 bytes long, the
		// RLP encoding consists of a single byte with value 0xC0 plus the length
		// of the list followed by the concatenation of the RLP encodings of the
		// items. The range of the first byte is thus [0xC0, 0xF7].
		dataPos = pos + 1
		dataLen = int(first) - 192
		isList = true
	default:
		// If the total payload of a list is more than 55 bytes long,
		// the RLP encoding consists of a single byte with value 0xF7
		// plus the length of the length of the payload in binary
		// form, followed by the length of the payload, followed by
		// the concatenation of the RLP encodings of the items. The
		// range of the first byte is thus [0xF8, 0xFF].
		beLen := int(first) - 247
		dataPos = pos + 1 + beLen
		dataLen, err = BeInt(payload, pos+1, beLen)
		isList = true
		if dataLen < 56 {
			err = fmt.Errorf("%w: : non-canonical size information", ErrParse)
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

func List(payload []byte, pos int) (dataPos, dataLen int, err error) {
	dataPos, dataLen, isList, err := Prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if !isList {
		return 0, 0, fmt.Errorf("%w: must be a list", ErrParse)
	}
	return
}

func String(payload []byte, pos int) (dataPos, dataLen int, err error) {
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
	dataPos, dataLen, err := String(payload, pos)
	if err != nil {
		return 0, err
	}
	if dataLen != expectedLen {
		return 0, fmt.Errorf("%w: expected string of len %d, got %d", ErrParse, expectedLen, dataLen)
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
func U32(payload []byte, pos int) (int, uint32, error) {
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

// U256 parses uint256 number from given payload at given position
func U256(payload []byte, pos int, x *uint256.Int) (int, error) {
	dataPos, dataLen, err := String(payload, pos)
	if err != nil {
		return 0, err
	}
	if dataLen > 32 {
		return 0, fmt.Errorf("%w: uint256 must not be more than 32 bytes long, got %d", ErrParse, dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, payload[dataPos:dataPos+dataLen])
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
	return 1 + common.BitLenToByteLen(nBits)
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

const ParseAnnouncementsErrorPrefix = "parse announcement payload"

func ParseAnnouncements(payload []byte, pos int) ([]byte, []uint32, []byte, int, error) {
	pos, totalLen, err := List(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+totalLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("%s: totalLen %d is beyond the end of payload", ParseAnnouncementsErrorPrefix, totalLen)
	}
	pos, typesLen, err := String(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+typesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("%s: typesLen %d is beyond the end of payload", ParseAnnouncementsErrorPrefix, typesLen)
	}
	types := payload[pos : pos+typesLen]
	pos += typesLen
	pos, sizesLen, err := List(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+sizesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("%s: sizesLen %d is beyond the end of payload", ParseAnnouncementsErrorPrefix, sizesLen)
	}
	sizes := make([]uint32, typesLen)
	for i := 0; i < len(sizes); i++ {
		if pos, sizes[i], err = U32(payload, pos); err != nil {
			return nil, nil, nil, pos, err
		}
	}
	pos, hashesLen, err := List(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+hashesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("%s: hashesLen %d is beyond the end of payload", ParseAnnouncementsErrorPrefix, hashesLen)
	}
	hashes := make([]byte, 32*(hashesLen/33))
	for i := 0; i < len(hashes); i += 32 {
		if pos, err = ParseHash(payload, pos, hashes[i:]); err != nil {
			return nil, nil, nil, pos, err
		}
	}
	return types, sizes, hashes, pos, nil
}
