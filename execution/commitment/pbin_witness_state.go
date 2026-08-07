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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// A decoded witness read as pre-state. pbinWitnessContext already serves it to
// the engine by bit path; this reads it the way a stateless verifier does, by
// address and slot, and drives the engine over it for the post-state root.
//
// Resolution is strict and not optional: a hash the witness carries no preimage
// for is an error on every path, never an empty read. Hex makes that a
// WITNESS_STRICT_VERIFY opt-in because an MPT witness can be legitimately
// incomplete; under bin an unresolved hash is unambiguous, so there is no mode
// where guessing is right.

// PBinAccount is the account state the BASIC_DATA and CODE_HASH leaves hold.
// The binary tree commits no per-account storage root, so there is no field for
// one.
type PBinAccount struct {
	Nonce    uint64
	Balance  uint256.Int
	CodeSize uint64
	CodeHash common.Hash
}

// PBinWitnessState is a decoded binary witness served as pre-state.
type PBinWitnessState struct {
	tree *pbinWitnessTree
	ctx  *pbinWitnessContext
	keys pbinDigestCache
}

func PBinNewWitnessState(nodes [][]byte, root []byte) (*PBinWitnessState, error) {
	tree, err := pbinDecodeWitness(nodes, root)
	if err != nil {
		return nil, err
	}
	return &PBinWitnessState{
		tree: tree,
		ctx:  pbinNewWitnessContext(tree),
		keys: pbinDigestCache{sum: pbinSelectedSum},
	}, nil
}

// SetCode supplies bytecode the witness cannot hold: code a block deploys has no
// pre-state chunk leaves. Everything else is read from the leaves.
func (s *PBinWitnessState) SetCode(addr, code []byte) { s.ctx.setCode(addr, code) }

// Account resolves an address to the state its account leaves hold. ok is
// false when the witness proves the account absent.
func (s *PBinWitnessState) Account(addr []byte) (PBinAccount, bool, error) {
	// An account holds exactly one of the CODE_HASH and DELEGATION leaves, and
	// neither is ever zero, so whichever exists marks the account present —
	// while BASIC_DATA is absent for an account whose nonce, balance and
	// code_size are all zero.
	var acc PBinAccount
	basic, ok, err := s.tree.leaf(s.keys.accountKey(addr, pbinBasicDataLeafKey))
	if err != nil {
		return PBinAccount{}, false, err
	}
	if ok {
		acc.CodeSize = uint64(binary.BigEndian.Uint32(basic[pbinBasicDataCodeSizeOffset:]))
		acc.Nonce = binary.BigEndian.Uint64(basic[pbinBasicDataNonceOffset:])
		acc.Balance.SetBytes(basic[pbinBasicDataBalanceOffset:])
	}

	codeHash, ok, err := s.tree.leaf(s.keys.accountKey(addr, pbinCodeHashLeafKey))
	if err != nil {
		return PBinAccount{}, false, err
	}
	if ok {
		acc.CodeHash = common.BytesToHash(codeHash)
		return acc, true, nil
	}

	indicator, err := s.ctx.delegationCode(addr, acc.CodeSize)
	if err != nil || indicator == nil {
		return PBinAccount{}, false, err
	}
	// A delegated account commits no code hash; EXTCODEHASH defines its hash as
	// the keccak of the indicator bytes.
	acc.CodeHash = common.Hash(keccak.Sum256(indicator))
	return acc, true, nil
}

// Storage resolves one slot. ok is false when the witness proves the slot
// absent, which the tree reads as zero.
func (s *PBinWitnessState) Storage(addr, slot []byte) (common.Hash, bool, error) {
	value, ok, err := s.tree.leaf(s.keys.storageKey(addr, slot))
	if err != nil || !ok {
		return common.Hash{}, false, err
	}
	return common.BytesToHash(value), true, nil
}

// Code returns the account's bytecode: reassembled from the chunk leaves, or
// read from the DELEGATION leaf for a delegated account. ok is false when the
// witness proves the account absent.
func (s *PBinWitnessState) Code(addr []byte) ([]byte, bool, error) {
	code, err := s.ctx.codeFromLeaves(addr)
	if err != nil {
		return nil, false, err
	}
	return code, code != nil, nil
}

// Root applies the block's writes over the witness and returns the post-state
// root. The engine runs against the witness alone, so leaf splitting, branch
// creation, BASIC_DATA packing and code chunking are the ones the chain uses.
func (s *PBinWitnessState) Root(ctx context.Context, plainKeys [][]byte, updates []Update) ([]byte, error) {
	if len(plainKeys) != len(updates) {
		return nil, fmt.Errorf("pbin: %d plain keys for %d updates", len(plainKeys), len(updates))
	}
	trie := NewPBinPatriciaHashed(s.ctx)
	defer trie.Release()

	upd := NewUpdates(ModeUpdate, "", trie.setHashSuite(pbinSelectedSum))
	for i := range plainKeys {
		upd.TouchPlainKeyDirect(string(plainKeys[i]), &updates[i])
	}
	root, err := trie.Process(ctx, upd, "pbin-witness", nil, WarmupConfig{})
	if err != nil {
		return nil, err
	}
	return bytes.Clone(root), nil
}

// codeFromLeaves is the witness's own answer to "what code does this account
// run". The leaves are the single code source under bin: they are committed by
// the root, and the fold re-chunks every account it touches, so the pruned
// witness carries a chunk leaf wherever the post-state pass needs one. The
// reassembly is checked against the CODE_HASH leaf, so it cannot drift from the
// chunker. A nil result means the witness proves the account absent.
func (c *pbinWitnessContext) codeFromLeaves(addr []byte) ([]byte, error) {
	// An account whose nonce, balance and code_size are all zero stores no
	// BASIC_DATA leaf, so its absence is zeros rather than an absent account —
	// the CODE_HASH or DELEGATION leaf is what marks the account present.
	hashValue, hasCodeHash, err := c.tree.leaf(c.keys.accountKey(addr, pbinCodeHashLeafKey))
	if err != nil {
		return nil, err
	}

	var size uint64
	if basic, ok, err := c.tree.leaf(c.keys.accountKey(addr, pbinBasicDataLeafKey)); err != nil {
		return nil, err
	} else if ok {
		size = uint64(binary.BigEndian.Uint32(basic[pbinBasicDataCodeSizeOffset:]))
	}

	if !hasCodeHash {
		return c.delegationCode(addr, size)
	}
	codeHash := common.BytesToHash(hashValue)
	if size == 0 {
		return []byte{}, nil
	}

	code := make([]byte, 0, size)
	for chunk := 0; chunk < pbinCodeChunkCount(size); chunk++ {
		value, ok, err := c.tree.leaf(c.keys.codeChunkKey(codeHash, chunk))
		if err != nil {
			return nil, err
		}
		if !ok {
			// A chunk of 31 zero bytes is stored as no leaf at all, so an absent
			// chunk is the zeros it stands for. code_size delimits the code, not
			// which chunks are present.
			var zero [pbinValueLength]byte
			value = zero[:]
		}
		code = append(code, value[1:]...)
	}
	code = code[:size]
	if got := common.Hash(keccak.Sum256(code)); got != codeHash {
		return nil, fmt.Errorf("%w: code of account %x reassembles to %x, the CODE_HASH leaf says %x",
			errPBinWitnessNode, addr, got, codeHash)
	}
	return code, nil
}

// delegationCode reads a delegated account's code: the leading code_size bytes
// of its DELEGATION leaf. There is nothing to reassemble and no hash to check
// against — the root commits the leaf itself. A nil result means the witness
// proves the account absent.
func (c *pbinWitnessContext) delegationCode(addr []byte, size uint64) ([]byte, error) {
	value, ok, err := c.tree.leaf(c.keys.accountKey(addr, pbinDelegationLeafKey))
	if err != nil || !ok {
		return nil, err
	}
	if size > uint64(len(value)) {
		return nil, fmt.Errorf("%w: delegation leaf of account %x holds %d bytes, code_size says %d",
			errPBinWitnessNode, addr, len(value), size)
	}
	return bytes.Clone(value[:size]), nil
}

func pbinCodeChunkCount(size uint64) int {
	return int((size + pbinChunkDataLen - 1) / pbinChunkDataLen)
}

// leaf resolves the value at a tree key. found is false when the walk reaches a
// node that proves the key absent — a leaf of another key, or a branch prefix
// the key diverges from. A hash the set carries no preimage for is an error, so
// an unresolved subtree is never read as an absent key.
func (w *pbinWitnessTree) leaf(key []byte) ([]byte, bool, error) {
	path, err := pbinWitnessProvedPath(key)
	if err != nil {
		return nil, false, err
	}
	hash, pos := w.root, int16(0)
	if hash == pbinEmptyTreeHash {
		return nil, false, nil
	}
	for {
		node, ok := w.nodes[hash]
		if !ok {
			return nil, false, fmt.Errorf("%w: no preimage for %x, reached at bit %d of key %x",
				errPBinWitnessBlinded, hash, pos, key)
		}
		if node.isLeaf() {
			if !bytes.Equal(node.key, key) {
				return nil, false, nil
			}
			return node.value, true, nil
		}
		end := pos + node.prefix.bitLen
		if end >= path.bitLen || pbinCommonPrefixBitsAt(&path, pos, &node.prefix) != node.prefix.bitLen {
			return nil, false, nil
		}
		hash, pos = node.children[path.bit(end)], end+1
	}
}
