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
	"bytes"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

const (
	planeAccount = tagAccountNode
	planeStorage = tagStorageNode

	// widest RLP a branch can produce: list prefix + 16 hash refs + the empty value slot
	refScratch = 3 + 16*33 + 1
)

func leafRef(plane byte, suffix []byte, payload []byte, dst []byte) []byte {
	payloadLen := rlp.StringLen(payload)
	if plane == planeAccount {
		payloadLen = len(payload)
	} else if plane != planeStorage {
		panic(fmt.Sprintf("commitment v4: unknown leaf plane 0x%02x", plane))
	}

	contentLen := rlp.StringLen(suffix) + payloadLen
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	encoded := dst
	pos := start
	prefixLen := rlp.EncodeListPrefixToBuf(contentLen, encoded[pos:])
	pos += prefixLen
	stringLen := rlp.EncodeStringToBuf(suffix, encoded[pos:])
	pos += stringLen
	if plane == planeAccount {
		copy(encoded[pos:], payload)
		pos += len(payload)
	} else {
		pos += rlp.EncodeStringToBuf(payload, encoded[pos:])
	}
	encoded = encoded[:pos]
	if len(encoded)-start < 32 {
		return encoded
	}
	hash := keccak.Sum256(encoded[start:])
	return append(encoded[:start], hash[:]...)
}

func storageLeafRef(suffix []byte, payload []byte, dst []byte) []byte {
	var encoded bytes.Buffer
	var prefix [8]byte
	if err := (rlp.RlpSerializableBytes(payload)).ToDoubleRLP(&encoded, prefix[:]); err != nil {
		panic(err)
	}
	contentLen := rlp.StringLen(suffix) + encoded.Len()
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	pos += copy(dst[pos:], encoded.Bytes())
	encodedBytes := dst[start:pos]
	if len(encodedBytes) < 32 {
		return encodedBytes
	}
	hash := keccak.Sum256(encodedBytes)
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
	hash := keccak.Sum256(encoded[:pos])
	return hash
}

func branchRef(refs *[16][]byte, depth int) [32]byte {
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
		panic(fmt.Sprintf("commitment v4: inlinable branch child at depth %d", depth))
	}
	return keccak.Sum256(encoded[:pos])
}
