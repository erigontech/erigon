// Copyright 2026 The Erigon Authors
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

package v4

import (
	"fmt"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

const (
	planeAccount = tagAccountNode
	planeStorage = tagStorageNode

	// widest RLP a branch can produce: list prefix + 16 hash refs + the empty value slot
	refScratch = 3 + 16*33 + 1

	// widest account body: list prefix + nonce + balance + storage root + code hash
	accountRLPScratch = 3 + 9 + 33 + 33 + 33

	// widest leaf RLP before hashing: list prefix + compact suffix + account body
	leafRefScratch = 3 + 34 + accountRLPScratch
)

func leafRef(suffix []byte, payload []byte, dst []byte) []byte {
	contentLen := rlp.StringLen(suffix) + rlp.StringLen(payload)
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	pos += rlp.EncodeStringToBuf(payload, dst[pos:])
	if pos-start < 32 {
		return dst[:pos]
	}
	hash := keccak.Sum256(dst[start:pos])
	return append(dst[:start], hash[:]...)
}

func storageLeafRef(suffix []byte, payload []byte, dst []byte) []byte {
	if len(payload) > length.Hash {
		panic(fmt.Sprintf("commitment v4: storage leaf payload has length %d", len(payload)))
	}
	innerLen := rlp.StringLen(payload)
	outerLen := innerLen
	if innerLen > 1 || len(payload) == 0 {
		outerLen++
	}
	contentLen := rlp.StringLen(suffix) + outerLen
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	if outerLen > innerLen {
		dst[pos] = byte(0x80 + innerLen)
		pos++
	}
	pos += rlp.EncodeStringToBuf(payload, dst[pos:])
	if pos-start < 32 {
		return dst[start:pos]
	}
	hash := keccak.Sum256(dst[start:pos])
	return append(dst[:start], hash[:]...)
}

func extensionRef(ext []byte, childHash []byte) [32]byte {
	compact := nibbles.HexToCompact(ext)
	contentLen := rlp.StringLen(compact) + rlp.StringLen(childHash)
	var scratch [refScratch]byte
	encoded := scratch[:rlp.ListLen(contentLen)]
	pos := rlp.EncodeListPrefixToBuf(contentLen, encoded)
	pos += rlp.EncodeStringToBuf(compact, encoded[pos:])
	pos += rlp.EncodeStringToBuf(childHash, encoded[pos:])
	return keccak.Sum256(encoded[:pos])
}

func branchRef(refs *[16][]byte) [32]byte {
	contentLen := 1
	for _, ref := range refs {
		switch {
		case len(ref) == 0:
			contentLen++
		case len(ref) < 32:
			contentLen += len(ref)
		case len(ref) == 32:
			contentLen += rlp.StringLen(ref)
		default:
			panic(fmt.Sprintf("commitment v4: invalid branch reference length %d", len(ref)))
		}
	}

	var scratch [refScratch]byte
	encoded := scratch[:rlp.ListLen(contentLen)]
	pos := rlp.EncodeListPrefixToBuf(contentLen, encoded)
	for _, ref := range refs {
		switch {
		case len(ref) == 0:
			encoded[pos] = 0x80
			pos++
		case len(ref) < 32:
			pos += copy(encoded[pos:], ref)
		default:
			pos += rlp.EncodeStringToBuf(ref, encoded[pos:])
		}
	}
	encoded[pos] = 0x80
	pos++
	if pos < 32 {
		panic("commitment v4: inlinable branch child")
	}
	return keccak.Sum256(encoded[:pos])
}
