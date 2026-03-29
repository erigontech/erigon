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

package txpool

// Ethereum-specific RLP encoding/decoding helpers for hashes and ETH/68 announcements.

import (
	"fmt"

	"github.com/erigontech/erigon/execution/rlp"
)

const parseHashErrorPrefix = "parse hash payload"

// encodeHash encodes a single 32-byte hash as an RLP string into to.
// Assumes that to is at least 33 bytes long.
func encodeHash(h, to []byte) int {
	_ = to[32] // early bounds check to guarantee safety of writes below
	to[0] = rlp.EmptyStringCode + 32
	copy(to[1:33], h[:32])
	return 33
}

// encodeHashList encodes a slice of concatenated 32-byte hashes as an RLP list.
func encodeHashList(hashes []byte, encodeBuf []byte) int {
	pos := 0
	hashesLen := len(hashes) / 32 * 33
	pos += rlp.EncodeListPrefixToBuf(hashesLen, encodeBuf)
	for i := 0; i < len(hashes); i += 32 {
		pos += encodeHash(hashes[i:], encodeBuf[pos:])
	}
	return pos
}

func parseRLPHash(payload []byte, pos int, hashbuf []byte) (int, error) {
	pos, err := rlp.StringOfLen(payload, pos, 32)
	if err != nil {
		return 0, fmt.Errorf("%s: hash len: %w", parseHashErrorPrefix, err)
	}
	copy(hashbuf, payload[pos:pos+32])
	return pos + 32, nil
}

// announcementsLen returns the RLP-encoded length of an ETH/68 announcement.
func announcementsLen(types []byte, sizes []uint32, hashes []byte) int {
	if len(types) == 0 {
		return 4
	}
	typesLen := rlp.StringLen(types)
	var sizesLen int
	for _, size := range sizes {
		sizesLen += rlp.U64Len(uint64(size))
	}
	hashesLen := len(hashes) / 32 * 33
	totalLen := typesLen + sizesLen + rlp.ListPrefixLen(sizesLen) + hashesLen + rlp.ListPrefixLen(hashesLen)
	return rlp.ListPrefixLen(totalLen) + totalLen
}

// encodeAnnouncements encodes an ETH/68 (EIP-5793) transaction announcement.
func encodeAnnouncements(types []byte, sizes []uint32, hashes []byte, encodeBuf []byte) int {
	if len(types) == 0 {
		encodeBuf[0] = rlp.EmptyListCode + 3 // list of 3 bytes
		encodeBuf[1] = rlp.EmptyStringCode   // empty types string
		encodeBuf[2] = rlp.EmptyListCode     // empty sizes list
		encodeBuf[3] = rlp.EmptyListCode     // empty hashes list
		return 4
	}
	pos := 0
	typesLen := rlp.StringLen(types)
	var sizesLen int
	for _, size := range sizes {
		sizesLen += rlp.U64Len(uint64(size))
	}
	hashesLen := len(hashes) / 32 * 33
	totalLen := typesLen + sizesLen + rlp.ListPrefixLen(sizesLen) + hashesLen + rlp.ListPrefixLen(hashesLen)
	pos += rlp.EncodeListPrefixToBuf(totalLen, encodeBuf)
	pos += rlp.EncodeStringToBuf(types, encodeBuf[pos:])
	pos += rlp.EncodeListPrefixToBuf(sizesLen, encodeBuf[pos:])
	for _, size := range sizes {
		pos += rlp.EncodeU32ToBuf(size, encodeBuf[pos:])
	}
	pos += rlp.EncodeListPrefixToBuf(hashesLen, encodeBuf[pos:])
	for i := 0; i < len(hashes); i += 32 {
		pos += encodeHash(hashes[i:], encodeBuf[pos:])
	}
	return pos
}

// parseAnnouncements decodes an ETH/68 (EIP-5793) transaction announcement.
func parseAnnouncements(payload []byte, pos int) ([]byte, []uint32, []byte, int, error) {
	pos, totalLen, err := rlp.ParseList(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+totalLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("parse announcement payload: totalLen %d is beyond the end of payload", totalLen)
	}
	pos, typesLen, err := rlp.ParseString(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+typesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("parse announcement payload: typesLen %d is beyond the end of payload", typesLen)
	}
	types := payload[pos : pos+typesLen]
	pos += typesLen
	pos, sizesLen, err := rlp.ParseList(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+sizesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("parse announcement payload: sizesLen %d is beyond the end of payload", sizesLen)
	}
	sizes := make([]uint32, typesLen)
	for i := 0; i < len(sizes); i++ {
		if pos, sizes[i], err = rlp.ParseU32(payload, pos); err != nil {
			return nil, nil, nil, pos, err
		}
	}
	pos, hashesLen, err := rlp.ParseList(payload, pos)
	if err != nil {
		return nil, nil, nil, pos, err
	}
	if pos+hashesLen > len(payload) {
		return nil, nil, nil, pos, fmt.Errorf("parse announcement payload: hashesLen %d is beyond the end of payload", hashesLen)
	}
	hashes := make([]byte, 32*(hashesLen/33))
	for i := 0; i < len(hashes); i += 32 {
		if pos, err = parseRLPHash(payload, pos, hashes[i:]); err != nil {
			return nil, nil, nil, pos, err
		}
	}
	return types, sizes, hashes, pos, nil
}
