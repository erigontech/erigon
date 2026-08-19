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

// PBinAccount is the account state the header leaves hold. A delegated account
// has no CODE_HASH leaf, and its CodeHash is the keccak of the indicator bytes
// its DELEGATION leaf carries. The binary tree commits no per-account storage
// root, so there is no field for one.
type PBinAccount struct {
	Nonce    uint64
	Balance  uint256.Int
	CodeSize uint64
	CodeHash common.Hash
}

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

func (s *PBinWitnessState) Account(addr []byte) (PBinAccount, bool, error) {
	// An account holds exactly one of the CODE_HASH and DELEGATION leaves, and
	// neither is ever zero, so whichever exists marks the account present —
	// while BASIC_DATA is absent for an account whose nonce, balance and
	// code_size are all zero.
	var acc PBinAccount
	basic, hasBasic, err := s.tree.leaf(s.keys.accountKey(addr, pbinBasicDataLeafKey))
	if err != nil {
		return PBinAccount{}, false, err
	}
	if hasBasic {
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
	if err != nil {
		return PBinAccount{}, false, err
	}
	if indicator == nil {
		if hasBasic {
			return PBinAccount{}, false, fmt.Errorf("%w: account %x has a BASIC_DATA leaf but neither a CODE_HASH nor a DELEGATION leaf",
				errPBinWitnessNode, addr)
		}
		return PBinAccount{}, false, nil
	}
	// A delegated account commits no code hash; EXTCODEHASH defines its hash as
	// the keccak of the indicator bytes.
	acc.CodeHash = common.Hash(keccak.Sum256(indicator))
	return acc, true, nil
}

// An absent slot resolves to the zero hash, matching SLOAD's default value.
func (s *PBinWitnessState) Storage(addr, slot []byte) (common.Hash, bool, error) {
	value, ok, err := s.tree.leaf(s.keys.storageKey(addr, slot))
	if err != nil || !ok {
		return common.Hash{}, false, err
	}
	return common.BytesToHash(value), true, nil
}

// HasStorage reports whether the witness holds a non-zero storage slot of the
// account — EIP-7610's CREATE-collision predicate. The tree commits no
// per-account storage root, so the answer is read from the two key regions an
// account owns. The header slots resolve off the same proof path the account's
// own leaves sit on; the storage zone needs a key of its own walked into it, so
// a witness whose keys never enter the zone reads it as empty.
func (s *PBinWitnessState) HasStorage(addr []byte) bool {
	// Sub-indices 64..127 are the header's storage slots, which is exactly the
	// header stem extended by the two bits pbinHeaderStorageOffset leads with.
	stem := s.keys.accountHeaderStem(addr)
	header := pbinPathFromBits(append(stem, pbinHeaderStorageOffset), int16(8*len(stem)+2))
	if s.tree.hasSubtree(&header) {
		return true
	}
	zone := pbinPathFromBytes(s.keys.accountStoragePrefix(addr))
	return s.tree.hasSubtree(&zone)
}

// Code returns the account's bytecode: reassembled from the chunk leaves, or
// read from the DELEGATION leaf for a delegated account.
func (s *PBinWitnessState) Code(addr []byte) ([]byte, bool, error) {
	code, err := s.ctx.codeFromLeaves(addr)
	if err != nil {
		return nil, false, err
	}
	return code, code != nil, nil
}

// Root applies the block's writes over the witness and returns the post-state
// root.
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
		if !pbinIsEmptyCodeHash(codeHash) {
			return nil, fmt.Errorf("%w: account %x says no code but its CODE_HASH leaf holds %x",
				errPBinWitnessNode, addr, codeHash)
		}
		return []byte{}, nil
	}
	// code_size is four bytes of a leaf the sender chose, and both the buffer and
	// the chunk walk below scale with it, so it is bounded before either happens.
	if size > pbinMaxWitnessCodeSize {
		return nil, fmt.Errorf("%w: account %x claims %d code bytes, more than the %d a witness may carry",
			errPBinWitnessNode, addr, size, uint64(pbinMaxWitnessCodeSize))
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

// delegationCode reads a delegated account's code: the indicator its DELEGATION
// leaf carries. There is nothing to reassemble and no hash to check against —
// the root commits the leaf itself — so the leaf's fixed shape is the only thing
// that can be checked, and code_size has to agree with it.
func (c *pbinWitnessContext) delegationCode(addr []byte, size uint64) ([]byte, error) {
	value, ok, err := c.tree.leaf(c.keys.accountKey(addr, pbinDelegationLeafKey))
	if err != nil || !ok {
		return nil, err
	}
	if size != pbinDelegationCodeLength || len(value) < pbinDelegationCodeLength {
		return nil, fmt.Errorf("%w: delegation leaf of account %x holds %d bytes under code_size %d, want %d bytes of indicator",
			errPBinWitnessNode, addr, len(value), size, pbinDelegationCodeLength)
	}
	return bytes.Clone(value[:pbinDelegationCodeLength]), nil
}

func pbinCodeChunkCount(size uint64) int {
	return int((size + pbinChunkDataLen - 1) / pbinChunkDataLen)
}

// pbinMaxWitnessCodeSize is the largest code a witness may claim for an account.
// It sits well above every deployed contract-size limit; its job is to keep an
// arbitrary code_size from costing a multi-gigabyte buffer before the code it
// describes has been checked against anything.
const pbinMaxWitnessCodeSize = 1 << 20

// hasSubtree reports whether the witness proves a leaf exists under prefix.
// Unlike leaf, an unresolved hash is not an error here: a pruned witness proves
// only the regions its keys walked, so this answers what the node set can see
// and leaves the rest to read as empty.
func (w *pbinWitnessTree) hasSubtree(prefix *pbinBitpath) bool {
	hash, pos := w.root, int16(0)
	for {
		if hash == pbinEmptyTreeHash {
			return false
		}
		if pos >= prefix.bitLen {
			return true
		}
		node, ok := w.nodes[hash]
		if !ok {
			return false
		}
		if node.isLeaf() {
			key := pbinPathFromBytes(node.key)
			return key.hasPrefix(prefix)
		}
		limit := min(prefix.bitLen-pos, node.prefix.bitLen)
		if pbinCommonPrefixBitsAt(prefix, pos, &node.prefix) != limit {
			return false
		}
		if prefix.bitLen-pos <= node.prefix.bitLen {
			return true
		}
		end := pos + node.prefix.bitLen
		hash, pos = node.children[prefix.bit(end)], end+1
	}
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
