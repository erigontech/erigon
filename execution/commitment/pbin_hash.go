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

// pbinHashFn is H. EIP-8297 leaves the hash open and names Keccak-256 among the
// candidates (eip:511-513); the execution-specs reference hashes with BLAKE3, so
// tests substitute it to compare roots against that reference. Key derivation
// hashes too, so a suite is only fully swapped when pbinDigestCache is swapped
// with it.
type pbinHashFn func([]byte) common.Hash

// pbinHasher applies H to node preimages. Every preimage fits its single scratch
// buffer, so each node costs one hash call and no allocation. Its zero value is
// ready and hashes with Keccak-256.
type pbinHasher struct {
	buf [pbinHashBufLen]byte
	sum pbinHashFn
}

func (h *pbinHasher) hash(preimage []byte) common.Hash {
	if h.sum != nil {
		return h.sum(preimage)
	}
	return keccak.Sum256(preimage)
}

// pbinAppendBitPrefix is the spec's encode_bit_prefix (eip:196-201): a two-byte
// big-endian bit count, then the bits MSB-first zero-padded to a byte boundary.
// The count is what keeps a 7-bit prefix distinct from an 8-bit one that agrees
// with it on the pad bit.
func pbinAppendBitPrefix(dst []byte, p *pbinBitpath) []byte {
	return p.appendPackedBits(binary.BigEndian.AppendUint16(dst, uint16(p.bitLen)))
}

// branchHash is H(0x01 || encode_bit_prefix(prefix) || left || right). An absent
// child passes pbinEmptyTreeHash rather than being omitted.
func (h *pbinHasher) branchHash(prefix *pbinBitpath, left, right *common.Hash) common.Hash {
	buf := pbinAppendBitPrefix(append(h.buf[:0], pbinBranchTag), prefix)
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	return h.hash(buf)
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
		if c.childrenSet {
			return h.branchHash(&c.prefix, &c.children[0], &c.children[1]), nil
		}
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
	if full.bitLen%8 != 0 {
		return common.Hash{}, fmt.Errorf("%w: leaf key of %d bits is not whole bytes", errPBinCellHash, full.bitLen)
	}

	buf := full.appendPackedBits(append(h.buf[:0], pbinLeafTag))
	key := buf[1:]
	// The length is fixed per zone, which is what keeps keys prefix-free: a key of
	// another zone's length is not a key at all (eip:284-288).
	if want, known := pbinZoneKeyLength(key[0]); !known || len(key) != want {
		return common.Hash{}, fmt.Errorf("%w: leaf key %x is no key of zone %#x", errPBinCellHash, key, key[0])
	}
	value, err := pbinLeafValue(key, &c.Update)
	if err != nil {
		return common.Hash{}, err
	}
	return h.hash(append(buf, value[:]...)), nil
}

// pbinLeafValue picks the encoding the key's own position names: the zone byte
// separates storage and code from the account header, and within the header the
// sub-index selects between BASIC_DATA, CODE_HASH, a header-resident slot and a
// header-resident code chunk.
func pbinLeafValue(key []byte, u *Update) ([pbinValueLength]byte, error) {
	switch key[0] {
	case pbinStorageZone:
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	case pbinCodeZone:
		return pbinCodeChunkValue(u)
	case pbinAccountZone:
	default:
		return [pbinValueLength]byte{}, fmt.Errorf("%w: zone %#x names no leaf", errPBinCellHash, key[0])
	}
	switch subIndex := key[len(key)-1]; {
	case subIndex == pbinBasicDataLeafKey:
		return pbinEncodeBasicData(u.Nonce, &u.Balance, u.CodeSize)
	case subIndex == pbinCodeHashLeafKey:
		return pbinCodeHashValue(u.CodeHash), nil
	case subIndex >= pbinHeaderStorageOffset && subIndex < pbinCodeOffset:
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	case subIndex >= pbinCodeOffset:
		return pbinCodeChunkValue(u)
	default:
		return [pbinValueLength]byte{}, fmt.Errorf("%w: account-zone sub-index %d names no leaf", errPBinCellHash, subIndex)
	}
}
