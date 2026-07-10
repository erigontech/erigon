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
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/rlp"
)

// foldCtx is a per-goroutine scratch bundle threaded through the whole recursion: one
// HexPatriciaHashed for its keccak state and cell-hash helpers, one reused scratch cell, and
// one reused hash-output buffer. No per-node heap cell is allocated — this reuse is the
// buffer-reuse win the proto validated (the naive per-node-cell fold regresses ~10x on alloc).
type foldCtx struct {
	hph  *HexPatriciaHashed
	cell cell
	buf  []byte
}

// foldChild records one present slot of a branch during a fold: whether it is a storage leaf
// (hashed transitively by computeCellHash from its plain key) or a sub-branch (already folded
// to bh), plus its length contribution to the branch RLP list.
type foldChild struct {
	present bool
	isLeaf  bool
	node    *prefixNode
	bh      common.Hash
	hlen    int16
}

func (fc *foldCtx) setLeaf(node *prefixNode) {
	c := &fc.cell
	c.reset()
	c.storageAddrLen = int16(len(node.plainKey))
	copy(c.storageAddr[:], node.plainKey)
	c.setFromUpdate(node.update)
}

func (fc *foldCtx) setBranch(node *prefixNode, bh common.Hash) {
	c := &fc.cell
	c.reset()
	if n := int16(len(node.ext)); n > 0 {
		c.extLen = n
		copy(c.extension[:], node.ext)
	}
	c.hashLen = length.Hash
	c.hash = bh
}

// foldNode folds the branch node whose branch row sits at branchDepth into its keccak hash,
// recursing touched child branches and hashing storage leaves at their parent-slot depth. It is
// the direct-recursion analog of foldMounted for a fresh (no on-disk siblings) storage subtree:
// a storage leaf is placed raw so computeCellHash re-derives its key tail at the slot depth; a
// sub-branch is folded here and applied as an extension/hash cell so the parent applies extension
// hashing over the branch hash. Only the 17-slot branch keccak is hand-rolled; leaf and extension
// hashing run transitively inside computeCellHash.
func (fc *foldCtx) foldNode(node *prefixNode, branchDepth int16) (common.Hash, error) {
	childDepth := branchDepth + 1
	var rec [16]foldChild
	childIdx := 0
	totalLen := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := node.children[childIdx]
		r := &rec[nib]
		r.present, r.node = true, child
		if child.bitmap == 0 {
			if child.plainKey == nil {
				return common.Hash{}, fmt.Errorf("truthtree fold: storage leaf without plainKey at slot %x", nib)
			}
			r.isLeaf = true
			fc.setLeaf(child)
			r.hlen = fc.hph.computeCellHashLen(&fc.cell, childDepth)
		} else {
			bh, err := fc.foldNode(child, childDepth+int16(len(child.ext)))
			if err != nil {
				return common.Hash{}, err
			}
			r.bh = bh
			r.hlen = length.Hash + 1
		}
		totalLen += int(r.hlen)
		childIdx++
		bm &^= uint16(1) << nib
	}
	totalLen += 17 - bits.OnesCount16(node.bitmap)

	fc.hph.keccak2.Reset()
	var lp [4]byte
	pt := rlp.EncodeListPrefixToBuf(totalLen, lp[:])
	fc.hph.keccak2.Write(lp[:pt])
	b80 := [1]byte{0x80}
	for s := range 17 {
		if s == 16 || !rec[s].present {
			fc.hph.keccak2.Write(b80[:])
			continue
		}
		r := &rec[s]
		if r.isLeaf {
			fc.setLeaf(r.node)
		} else {
			fc.setBranch(r.node, r.bh)
		}
		hb, err := fc.hph.computeCellHash(&fc.cell, childDepth, fc.buf[:0])
		if err != nil {
			return common.Hash{}, err
		}
		fc.buf = hb
		fc.hph.keccak2.Write(hb)
	}
	var h common.Hash
	fc.hph.keccak2.Read(h[:])
	return h, nil
}

// foldFreshStorageRoot folds a fresh account's touched storage subtree (rooted at the depth-64
// account node, whose children are the first storage nibbles) into its storage root, reading no
// on-disk siblings — the fresh-whale case the proto validated. An empty subtree hashes to the
// empty-storage root.
func foldFreshStorageRoot(node *prefixNode) (common.Hash, error) {
	if node == nil || node.bitmap == 0 {
		return empty.RootHash, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())}
	defer fc.hph.Release()
	return fc.foldNode(node, 64)
}
