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
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Node tags separating the two preimage shapes EIP-8297 defines (eip:191-206).
const (
	pbinLeafTag   = 0x00
	pbinBranchTag = 0x01

	// pbinHashBufLen is the longest preimage either shape produces: tag, bit count,
	// packed prefix, both child hashes.
	pbinHashBufLen = 1 + 2 + (pbinMaxPathBits+7)/8 + 2*length.Hash
)

// pbinEmptyTreeHash is the hash of an absent subtree: 32 zero bytes (eip:208).
// Not empty.RootHash — the RLP empty-string MPT root would build a different tree.
var pbinEmptyTreeHash common.Hash

var errPBinCellHash = errors.New("pbin: cell cannot be hashed")

// pbinHashFn is H, which EIP-8297 leaves open (eip:511-513). Tree-key derivation
// hashes with H too, so a suite is only fully swapped when pbinDigestCache is
// swapped with it.
type pbinHashFn func([]byte) common.Hash

// Names for H, as the --experimental.bin-commitment.hash flag spells them.
const (
	PBinHashKeccak = "keccak"
	PBinHashBlake3 = "blake3"
)

// pbinSelectedSum is H for every binary-trie engine this process builds; nil is
// Keccak-256.
var pbinSelectedSum pbinHashFn

// SetPBinHashSuite selects H by name. Call it before the first engine is built:
// roots already computed under the previous suite do not match.
func SetPBinHashSuite(name string) error {
	switch name {
	case "", PBinHashKeccak:
		pbinSelectedSum = nil
	case PBinHashBlake3:
		pbinSelectedSum = func(b []byte) common.Hash { return common.Hash(blake3.Sum256(b)) }
	default:
		return fmt.Errorf("unknown bin commitment hash %q, want %q or %q", name, PBinHashKeccak, PBinHashBlake3)
	}
	return nil
}

func PBinHashSuiteName() string {
	if pbinSelectedSum == nil {
		return PBinHashKeccak
	}
	return PBinHashBlake3
}

// pbinHasher applies H to node preimages. Its zero value is ready and hashes with
// Keccak-256.
type pbinHasher struct {
	buf    [pbinHashBufLen]byte
	sum    pbinHashFn
	tracer witnessTracer // nil on the normal commitment path; see pbin_witness.go
}

func (h *pbinHasher) hash(preimage []byte) common.Hash {
	if h.sum != nil {
		return h.sum(preimage)
	}
	return keccak.Sum256(preimage)
}

// pbinAppendBitPrefix is the spec's encode_bit_prefix (eip:196-201). The leading
// bit count is what keeps a 7-bit prefix distinct from an 8-bit one that agrees
// with it on the pad bit.
func pbinAppendBitPrefix(dst []byte, p *pbinBitpath) []byte {
	return p.appendPackedBits(binary.BigEndian.AppendUint16(dst, uint16(p.bitLen)))
}

// branchHash is H(0x01 || encode_bit_prefix(prefix) || left || right); an absent
// child passes pbinEmptyTreeHash rather than being omitted.
func (h *pbinHasher) branchHash(prefix *pbinBitpath, left, right *common.Hash) common.Hash {
	buf := pbinAppendBitPrefix(append(h.buf[:0], pbinBranchTag), prefix)
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	hash := h.hash(buf)
	h.emitNode(buf, &hash)
	return hash
}

// cellHash hashes the cell reached by path; a leaf's complete key is path
// followed by the cell's own prefix.
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
	// Key length is fixed per zone, which is what keeps the key space prefix-free
	// (eip:284-288).
	if want, known := pbinZoneKeyLength(key[0]); !known || len(key) != want {
		return common.Hash{}, fmt.Errorf("%w: leaf key %x is no key of zone %#x", errPBinCellHash, key, key[0])
	}
	value, err := pbinLeafValue(key, &c.Update)
	if err != nil {
		return common.Hash{}, err
	}
	buf = append(buf, value[:]...)
	hash := h.hash(buf)
	h.emitNode(buf, &hash)
	return hash, nil
}

func pbinLeafValue(key []byte, u *Update) ([pbinValueLength]byte, error) {
	switch key[0] {
	case pbinStorageZone:
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	case pbinCodeZone:
		return pbinRecordLeafValue(u)
	case pbinAccountZone:
	default:
		return [pbinValueLength]byte{}, fmt.Errorf("%w: zone %#x names no leaf", errPBinCellHash, key[0])
	}
	switch subIndex := key[len(key)-1]; {
	case subIndex == pbinBasicDataLeafKey:
		return pbinEncodeBasicData(u.Nonce, &u.Balance, u.CodeSize)
	case subIndex == pbinCodeHashLeafKey:
		return pbinCodeHashValue(u.CodeHash), nil
	case subIndex == pbinDelegationLeafKey:
		// An EIP-7702 indicator is no account field, so the leaf carries its own bytes.
		return pbinRecordLeafValue(u)
	case subIndex >= pbinHeaderStorageOffset && subIndex < pbinHeaderStorageOffset+pbinHeaderStorageSlots:
		return pbinEncodeStorageValue(u.Storage[:u.StorageLen]), nil
	default:
		// Sub-indices the embedding reserves (eip:255-257): not packed from state,
		// so the value must already be 32 whole bytes.
		return pbinRecordLeafValue(u)
	}
}
