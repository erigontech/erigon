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

package commitment

import (
	"encoding/binary"
	"errors"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Node tags separating the two preimage shapes EIP-8297 defines (eip:191-206).
const (
	pbinLeafTag   = 0x00
	pbinBranchTag = 0x01

	// pbinHashBufLen holds the longest preimage either node shape produces: the
	// branch tag, the two-byte prefix bit count, the longest encodable prefix and
	// both child hashes.
	pbinHashBufLen = 1 + 2 + (pbinMaxPathBits+7)/8 + 2*length.Hash
)

// pbinEmptyTreeHash is the hash of an absent subtree: 32 zero bytes (eip:208).
// It is not empty.RootHash — that constant is the RLP empty-string MPT root and
// substituting it here would silently produce a different tree.
var pbinEmptyTreeHash common.Hash

var errPBinCellHash = errors.New("pbin: cell cannot be hashed")

// pbinHasher is the one place H is applied, so swapping the hash function is a
// change to this type alone. Every preimage fits its single scratch buffer, so
// each node costs one hash call and no allocation. Its zero value is ready.
type pbinHasher struct {
	buf [pbinHashBufLen]byte
}

// pbinAppendBitPrefix is the spec's encode_bit_prefix (eip:196-201): a two-byte
// big-endian bit count, then the bits MSB-first zero-padded to a byte boundary.
// The count is what keeps a 7-bit prefix distinct from an 8-bit one that agrees
// with it on the pad bit.
func pbinAppendBitPrefix(dst []byte, p *pbinBitpath) []byte {
	return p.appendPackedBits(binary.BigEndian.AppendUint16(dst, uint16(p.bitLen)))
}

// leafHash is H(0x00 || key || value) over the complete tree key, so a leaf's
// hash does not depend on where in the tree it sits.
func (h *pbinHasher) leafHash(key, value []byte) common.Hash {
	if len(key) != pbinAccountKeyLength && len(key) != pbinStorageKeyLength {
		panic(fmt.Sprintf("pbin: leaf key of %d bytes is neither zone length", len(key)))
	}
	if len(value) != pbinValueLength {
		panic(fmt.Sprintf("pbin: leaf value of %d bytes, want %d", len(value), pbinValueLength))
	}
	buf := append(h.buf[:0], pbinLeafTag)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return keccak.Sum256(buf)
}

// branchHash is H(0x01 || encode_bit_prefix(prefix) || left || right). An absent
// child passes pbinEmptyTreeHash rather than being omitted.
func (h *pbinHasher) branchHash(prefix *pbinBitpath, left, right *common.Hash) common.Hash {
	buf := pbinAppendBitPrefix(append(h.buf[:0], pbinBranchTag), prefix)
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	return keccak.Sum256(buf)
}

// cellHash is the only way a cell becomes a hash. Keeping it single is what
// stops a second hasher drifting from this one.
//
// path is the descent to the cell; a leaf's complete key is path followed by the
// cell's own prefix, which is also what tells the leaf value apart.
func (h *pbinHasher) cellHash(c *pbinCell, path *pbinBitpath) (common.Hash, error) {
	switch c.kind {
	case pbinNodeEmpty:
		return pbinEmptyTreeHash, nil
	case pbinNodeBranch:
		if c.hashLen != length.Hash {
			return common.Hash{}, fmt.Errorf("%w: branch cell holds %d hash bytes", errPBinCellHash, c.hashLen)
		}
		return c.hash, nil
	case pbinNodeLeaf:
		return h.leafCellHash(c, path)
	default:
		return common.Hash{}, fmt.Errorf("%w: unknown node kind %d", errPBinCellHash, c.kind)
	}
}

func (h *pbinHasher) leafCellHash(c *pbinCell, path *pbinBitpath) (common.Hash, error) {
	full := *path
	if int(full.bitLen)+int(c.prefix.bitLen) > pbinMaxPathBits {
		return common.Hash{}, fmt.Errorf("%w: leaf key of %d+%d bits overflows", errPBinCellHash, full.bitLen, c.prefix.bitLen)
	}
	full.append(&c.prefix)
	if full.bitLen != pbinAccountKeyLength*8 && full.bitLen != pbinStorageKeyLength*8 {
		return common.Hash{}, fmt.Errorf("%w: leaf key of %d bits is neither zone length", errPBinCellHash, full.bitLen)
	}

	buf := full.appendPackedBits(append(h.buf[:0], pbinLeafTag))
	value, err := pbinLeafValue(buf[1:], &c.Update)
	if err != nil {
		return common.Hash{}, err
	}
	return keccak.Sum256(append(buf, value[:]...)), nil
}

// pbinLeafValue picks the encoding the key's own position names: the zone byte
// separates storage from the account header, and within the header the
// sub-index selects between BASIC_DATA, CODE_HASH and a header-resident slot.
func pbinLeafValue(key []byte, u *Update) ([pbinValueLength]byte, error) {
	if key[0] == pbinStorageZone {
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	}
	switch subIndex := key[len(key)-1]; {
	case subIndex == pbinBasicDataLeafKey:
		// code_size stays zero while code chunking is out of scope: the shared
		// Update carries no code size and adding one is an external API change.
		return pbinEncodeBasicData(u.Nonce, &u.Balance, 0)
	case subIndex == pbinCodeHashLeafKey:
		return pbinCodeHashValue(u.CodeHash), nil
	case subIndex >= pbinHeaderStorageOffset && subIndex < pbinCodeOffset:
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	default:
		return [pbinValueLength]byte{}, fmt.Errorf("%w: account-zone sub-index %d names no leaf", errPBinCellHash, subIndex)
	}
}
