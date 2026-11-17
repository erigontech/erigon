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

package v3

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

	refScratch = 3 + 16*33 + 1

	accountRLPScratch = 3 + 9 + 33 + 33 + 33

	leafRefScratch = 3 + 34 + accountRLPScratch
)

func appendLeafRLP(dst, suffix, payload []byte) []byte {
	contentLen := rlp.StringLen(suffix) + rlp.StringLen(payload)
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	pos += rlp.EncodeStringToBuf(payload, dst[pos:])
	return dst[:pos]
}

func hashLongRef(dst []byte) []byte {
	if len(dst) < 32 {
		return dst
	}
	hash := keccak.Sum256(dst)
	return append(dst[:0], hash[:]...)
}

func appendStorageLeafRLP(dst, suffix, payload []byte) []byte {
	if len(payload) > length.Hash {
		panic(fmt.Sprintf("commitment v3: storage leaf payload has length %d", len(payload)))
	}
	var inner [1 + length.Hash]byte
	return appendLeafRLP(dst, suffix, inner[:rlp.EncodeStringToBuf(payload, inner[:])])
}

func appendExtensionRLP(dst, ext, childHash []byte) []byte {
	var compact [34]byte
	return appendLeafRLP(dst, nibbles.HexToCompactInto(compact[:0], ext), childHash)
}

func extensionRef(ext []byte, childHash []byte) [32]byte {
	var scratch [refScratch]byte
	return keccak.Sum256(appendExtensionRLP(scratch[:0], ext, childHash))
}

func branchRef(refs *[16][]byte) ([32]byte, error) {
	var scratch [refScratch]byte
	encoded := appendBranchRLP(scratch[:0], refs)
	if len(encoded) < 32 {
		return [32]byte{}, fmt.Errorf("%w: inlinable branch", errFoldNode)
	}
	return keccak.Sum256(encoded), nil
}

func appendBranchRLP(dst []byte, refs *[16][]byte) []byte {
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
			panic(fmt.Sprintf("commitment v3: invalid branch reference length %d", len(ref)))
		}
	}

	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	encoded := dst[start:]
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
	return dst[:start+pos]
}
