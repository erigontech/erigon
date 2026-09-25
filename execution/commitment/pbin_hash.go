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
	"errors"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

const pbinHashBufLen = 1 + 2 + (pbinMaxPathBits+7)/8 + 2*length.Hash

var errPBinCellHash = errors.New("pbin: cell cannot be hashed")

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

// pbinAppendBitPrefix is the spec's encode_bit_prefix (eip:"Node merkelization"). The leading
// bit count is what keeps a 7-bit prefix distinct from an 8-bit one that agrees
// with it on the pad bit.
func pbinAppendBitPrefix(dst []byte, p *pbinBitpath) []byte {
	return eip8297.AppendBitPrefix(dst, p)
}

// branchHash is H(0x01 || encode_bit_prefix(prefix) || left || right); an absent
// child passes pbinEmptyTreeHash rather than being omitted.
func (h *pbinHasher) branchHash(prefix *pbinBitpath, left, right *common.Hash) common.Hash {
	buf := eip8297.BranchPreimage(h.buf[:0], prefix, left, right)
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
		if c.hashLen == length.Hash && h.tracer == nil {
			return c.hash, nil
		}
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
	if int(full.BitLen)+int(c.prefix.BitLen) > pbinMaxPathBits {
		return common.Hash{}, fmt.Errorf("%w: leaf key of %d+%d bits overflows", errPBinCellHash, full.BitLen, c.prefix.BitLen)
	}
	full.Append(&c.prefix)
	if full.BitLen%8 != 0 {
		return common.Hash{}, fmt.Errorf("%w: leaf key of %d bits is not whole bytes", errPBinCellHash, full.BitLen)
	}

	var keyBuf [pbinHashBufLen]byte
	key := full.AppendPackedBits(keyBuf[:0])
	// Key length is fixed per zone, which is what keeps the key space prefix-free
	// (eip:"Tree embedding").
	if want, known := pbinZoneKeyLength(key[0]); !known || len(key) != want {
		return common.Hash{}, fmt.Errorf("%w: leaf key %x is no key of zone %#x", errPBinCellHash, key, key[0])
	}
	value, err := pbinLeafValue(key, &c.Update)
	if err != nil {
		return common.Hash{}, err
	}
	buf := eip8297.LeafPreimage(h.buf[:0], key, value[:])
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
		// Sub-indices the embedding reserves (eip:"Header values"): not packed from state,
		// so the value must already be 32 whole bytes.
		return pbinRecordLeafValue(u)
	}
}
