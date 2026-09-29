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
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
)

var ErrStreamRootHash = errors.New("eip8297: stream root hash function is nil")

type streamRootBranch struct {
	split  int
	prefix []byte
	left   common.Hash
	right  common.Hash
}

type StreamRootBuilder struct {
	sum      HashFn
	branches []streamRootBranch
	prevKey  []byte
	prevBits []byte
	lastLeaf common.Hash
	hasLeaf  bool
}

func NewStreamRootBuilder(sum HashFn) (*StreamRootBuilder, error) {
	if sum == nil {
		return nil, ErrStreamRootHash
	}
	return &StreamRootBuilder{sum: sum}, nil
}

func (b *StreamRootBuilder) Add(key, value []byte) error {
	if len(key) == 0 || len(key) > maxReferenceKeyLength {
		return fmt.Errorf("eip8297: key length %d out of range", len(key))
	}
	if len(value) != ValueLength {
		return fmt.Errorf("eip8297: value of %d bytes, want %d", len(value), ValueLength)
	}
	if isZeroStreamRootValue(value) {
		return errors.New("eip8297: zero values are not stored")
	}
	if b.hasLeaf && bytes.Compare(key, b.prevKey) <= 0 {
		return errors.New("eip8297: stream keys are not strictly ascending")
	}

	bits := bytesToBits(key)
	leaf := b.hashLeaf(key, value)
	if b.hasLeaf {
		commonBits := streamRootCommonPrefix(b.prevBits, bits)
		if commonBits == len(b.prevBits) || commonBits == len(bits) {
			return errors.New("eip8297: stream keys are not prefix-free")
		}

		subtree := b.lastLeaf
		for len(b.branches) > 0 && b.branches[len(b.branches)-1].split > commonBits {
			branchIndex := len(b.branches) - 1
			branch := b.branches[branchIndex]
			entryDepth := 0
			if branchIndex > 0 {
				entryDepth = b.branches[branchIndex-1].split + 1
			}
			if commonBits >= entryDepth {
				prefixOffset := commonBits - entryDepth
				branch.prefix = append([]byte(nil), branch.prefix[prefixOffset+1:]...)
			}
			b.branches = b.branches[:len(b.branches)-1]
			subtree = b.hashBranch(branch.prefix, branch.left, branch.right)
			if len(b.branches) > 0 {
				b.branches[len(b.branches)-1].right = subtree
			}
		}
		entryDepth := 0
		if len(b.branches) > 0 {
			entryDepth = b.branches[len(b.branches)-1].split + 1
		}
		prefix := append([]byte(nil), bits[entryDepth:commonBits]...)
		branchHash := b.hashBranch(prefix, subtree, leaf)
		if len(b.branches) > 0 {
			b.branches[len(b.branches)-1].right = branchHash
		}
		b.branches = append(b.branches, streamRootBranch{
			split:  commonBits,
			prefix: prefix,
			left:   subtree,
			right:  leaf,
		})
	}

	b.prevKey = append(b.prevKey[:0], key...)
	b.prevBits = append(b.prevBits[:0], bits...)
	b.lastLeaf = leaf
	b.hasLeaf = true
	return nil
}

func (b *StreamRootBuilder) RootHash() common.Hash {
	if !b.hasLeaf {
		return EmptyTreeHash
	}
	root := b.lastLeaf
	for _, branch := range slices.Backward(b.branches) {
		root = b.hashBranch(branch.prefix, branch.left, root)
	}
	return root
}

func (b *StreamRootBuilder) hashLeaf(key, value []byte) common.Hash {
	return common.Hash(b.sum(LeafPreimage(nil, key, value)))
}

func (b *StreamRootBuilder) hashBranch(prefix []byte, left, right common.Hash) common.Hash {
	preimage := make([]byte, 0, 1+2+(len(prefix)+7)/8+2*len(left))
	preimage = append(preimage, BranchTag)
	preimage = append(preimage, EncodeBitPrefix(prefix)...)
	preimage = append(preimage, left[:]...)
	preimage = append(preimage, right[:]...)
	return common.Hash(b.sum(preimage))
}

func isZeroStreamRootValue(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

func streamRootCommonPrefix(a, b []byte) int {
	commonBits := 0
	for commonBits < len(a) && commonBits < len(b) && a[commonBits] == b[commonBits] {
		commonBits++
	}
	return commonBits
}
