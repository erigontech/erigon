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

package eip8297

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon/common"
	keccak "github.com/erigontech/fastkeccak"
)

const maxReferenceKeyLength = 8192

type Node interface{ node() }

type Leaf struct {
	Key   []byte
	Value []byte
}

type Branch struct {
	Prefix []byte
	Left   Node
	Right  Node
}

func (*Leaf) node()   {}
func (*Branch) node() {}

type Tree struct {
	Root Node
}

func (t *Tree) Insert(key, value []byte) {
	if len(key) < 1 || len(key) > maxReferenceKeyLength {
		panic(fmt.Sprintf("eip8297: key length %d out of range", len(key)))
	}
	if len(value) != ValueLength {
		panic(fmt.Sprintf("eip8297: value of %d bytes, want %d", len(value), ValueLength))
	}
	if t.Root == nil {
		t.Root = &Leaf{Key: slices.Clone(key), Value: slices.Clone(value)}
		return
	}
	t.Root = insert(t.Root, bytesToBits(key), key, value, 0)
}

func insert(node Node, bits, key, value []byte, depth int) Node {
	if leaf, ok := node.(*Leaf); ok {
		if bytes.Equal(leaf.Key, key) {
			leaf.Value = slices.Clone(value)
			return leaf
		}
		otherBits := bytesToBits(leaf.Key)
		limit := min(len(bits), len(otherBits))
		run := 0
		for depth+run < limit && bits[depth+run] == otherBits[depth+run] {
			run++
		}
		if depth+run >= limit {
			panic("eip8297: insert violates prefix-freedom")
		}
		newLeaf := &Leaf{Key: slices.Clone(key), Value: slices.Clone(value)}
		branch := &Branch{Prefix: slices.Clone(bits[depth : depth+run])}
		if bits[depth+run] == 0 {
			branch.Left, branch.Right = newLeaf, leaf
		} else {
			branch.Left, branch.Right = leaf, newLeaf
		}
		return branch
	}

	branch := node.(*Branch)
	matched := 0
	for matched < len(branch.Prefix) && depth+matched < len(bits) && bits[depth+matched] == branch.Prefix[matched] {
		matched++
	}
	if depth+matched >= len(bits) {
		panic("eip8297: insert violates prefix-freedom")
	}
	if matched == len(branch.Prefix) {
		split := depth + matched
		if bits[split] == 0 {
			branch.Left = insert(branch.Left, bits, key, value, split+1)
		} else {
			branch.Right = insert(branch.Right, bits, key, value, split+1)
		}
		return branch
	}

	survivor := &Branch{
		Prefix: slices.Clone(branch.Prefix[matched+1:]),
		Left:   branch.Left,
		Right:  branch.Right,
	}
	newLeaf := &Leaf{Key: slices.Clone(key), Value: slices.Clone(value)}
	newBranch := &Branch{Prefix: slices.Clone(branch.Prefix[:matched])}
	if bits[depth+matched] == 0 {
		newBranch.Left, newBranch.Right = newLeaf, survivor
	} else {
		newBranch.Left, newBranch.Right = survivor, newLeaf
	}
	return newBranch
}

func (t *Tree) RootHash() common.Hash { return merkelize(t.Root, nil) }

func StateRoot(entries []Entry) common.Hash {
	return StateRootWithHash(entries, nil)
}

func StateRootWithHash(entries []Entry, sum HashFn) common.Hash {
	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	return merkelize(tree.Root, sum)
}

type Entry struct {
	Key   []byte
	Value []byte
}

type State struct {
	Address []byte
	Nonce   uint64
	Balance uint256.Int
	Code    []byte
	Slots   map[string][]byte
	Deleted bool
}

func EmbedState(batches [][]State) []Entry {
	var order []string
	values := make(map[string][]byte)
	owners := make(map[string]string)
	set := func(key []byte, value [ValueLength]byte, owner []byte) {
		name := string(key)
		if _, seen := values[name]; !seen {
			order = append(order, name)
		}
		if value == ([ValueLength]byte{}) {
			values[name] = nil
		} else {
			values[name] = slices.Clone(value[:])
		}
		if owner != nil {
			owners[name] = string(owner)
		}
	}
	for _, batch := range batches {
		for _, state := range batch {
			address := slices.Clone(state.Address)
			if state.Deleted {
				for key, owner := range owners {
					if owner == string(address) {
						values[key] = nil
					}
				}
			} else if state.Code != nil || state.Nonce != 0 || !state.Balance.IsZero() {
				basic, err := EncodeBasicData(state.Nonce, &state.Balance, uint64(len(state.Code)))
				if err != nil {
					panic(err)
				}
				set(TreeKeyAccount(address, BasicDataLeafKey), basic, address)
				if IsDelegation(state.Code) {
					set(TreeKeyAccount(address, DelegationLeafKey), EncodeDelegation(state.Code), address)
					set(TreeKeyAccount(address, CodeHashLeafKey), [ValueLength]byte{}, address)
				} else {
					set(TreeKeyAccount(address, CodeHashLeafKey), CodeHashValue(keccak.Sum256(state.Code)), address)
					set(TreeKeyAccount(address, DelegationLeafKey), [ValueLength]byte{}, address)
					for index, chunk := range ChunkifyCode(state.Code) {
						set(TreeKeyCodeChunk(keccak.Sum256(state.Code), index), chunk, nil)
					}
				}
			}
			for slot, value := range state.Slots {
				set(TreeKeyStorage(address, []byte(slot)), EncodeStorageValue(value), address)
			}
		}
	}
	entries := make([]Entry, 0, len(order))
	for _, key := range order {
		if values[key] != nil {
			entries = append(entries, Entry{Key: []byte(key), Value: values[key]})
		}
	}
	return entries
}

func bytesToBits(data []byte) []byte {
	bits := make([]byte, 0, len(data)*8)
	for _, value := range data {
		for i := range 8 {
			bits = append(bits, (value>>(7-i))&1)
		}
	}
	return bits
}

func BitsFromBytes(data []byte) []byte {
	return bytesToBits(data)
}

func EncodeBitPrefix(prefix []byte) []byte {
	if len(prefix) >= 1<<16 {
		panic(fmt.Sprintf("eip8297: prefix of %d bits exceeds the encodable count", len(prefix)))
	}
	out := make([]byte, 2+(len(prefix)+7)/8)
	binary.BigEndian.PutUint16(out, uint16(len(prefix)))
	for i, bit := range prefix {
		out[2+i/8] |= bit << (7 - i%8)
	}
	return out
}

func merkelize(node Node, sum HashFn) common.Hash {
	if node == nil {
		return EmptyTreeHash
	}
	var preimage []byte
	switch current := node.(type) {
	case *Leaf:
		preimage = LeafPreimage(nil, current.Key, current.Value)
	case *Branch:
		left := merkelize(current.Left, sum)
		right := merkelize(current.Right, sum)
		prefix := &Bitpath{BitLen: int16(len(current.Prefix))}
		for i, bit := range current.Prefix {
			prefix.SetBitAt(int16(i), uint64(bit))
		}
		preimage = BranchPreimage(nil, prefix, &left, &right)
	}
	if sum != nil {
		return common.Hash(sum(preimage))
	}
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write(preimage)
	return common.BytesToHash(h.Sum(nil))
}

func MerkelizeWith(node Node, sum HashFn) common.Hash {
	return merkelize(node, sum)
}
