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

// Account resolves an address to the state its two account leaves hold. ok is
// false when the witness proves the account absent.
func (s *PBinWitnessState) Account(addr []byte) (PBinAccount, bool, error) {
	basic, ok, err := s.tree.leaf(s.keys.accountKey(addr, pbinBasicDataLeafKey))
	if err != nil || !ok {
		return PBinAccount{}, false, err
	}
	codeHash, ok, err := s.tree.leaf(s.keys.accountKey(addr, pbinCodeHashLeafKey))
	if err != nil {
		return PBinAccount{}, false, err
	}
	if !ok {
		return PBinAccount{}, false, fmt.Errorf("%w: account %x has a BASIC_DATA leaf but no CODE_HASH leaf",
			errPBinWitnessNode, addr)
	}
	acc := PBinAccount{
		CodeSize: uint64(binary.BigEndian.Uint32(basic[pbinBasicDataCodeSizeOffset:])),
		Nonce:    binary.BigEndian.Uint64(basic[pbinBasicDataNonceOffset:]),
		CodeHash: common.BytesToHash(codeHash),
	}
	acc.Balance.SetBytes(basic[pbinBasicDataBalanceOffset:])
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

// Code returns the account's bytecode, reassembled from the chunk leaves. ok is
// false when the witness proves the account absent.
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
// run". The chunk leaves are the single code source under bin: they are
// committed by the root, and the fold re-chunks every account it touches, so the
// pruned witness carries a chunk leaf wherever the post-state pass needs one.
// The reassembly is checked against the CODE_HASH leaf, so it cannot drift from
// the chunker. A nil result means the witness proves the account absent.
func (c *pbinWitnessContext) codeFromLeaves(addr []byte) ([]byte, error) {
	basic, ok, err := c.tree.leaf(c.keys.accountKey(addr, pbinBasicDataLeafKey))
	if err != nil || !ok {
		return nil, err
	}
	size := uint64(binary.BigEndian.Uint32(basic[pbinBasicDataCodeSizeOffset:]))
	if size == 0 {
		return []byte{}, nil
	}
	hashValue, ok, err := c.tree.leaf(c.keys.accountKey(addr, pbinCodeHashLeafKey))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: account %x runs %d code bytes but has no CODE_HASH leaf",
			errPBinWitnessNode, addr, size)
	}
	codeHash := common.BytesToHash(hashValue)

	code := make([]byte, 0, size)
	for chunk := 0; chunk < pbinCodeChunkCount(size); chunk++ {
		var key []byte
		if chunk < pbinHeaderCodeChunks {
			key = c.keys.codeChunkKey(addr, chunk)
		} else {
			key = c.keys.codeOverflowKey(codeHash, chunk)
		}
		value, ok, err := c.tree.leaf(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: code chunk %d of account %x is absent", errPBinWitnessNode, chunk, addr)
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
