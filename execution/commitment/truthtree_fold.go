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
	"context"
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

// foldCtx is a per-goroutine scratch bundle threaded through the whole recursion: one
// HexPatriciaHashed for its keccak state and cell-hash helpers, one reused scratch cell, and
// one reused hash-output buffer. No per-node heap cell is allocated — this reuse is the
// buffer-reuse win the proto validated (the naive per-node-cell fold regresses ~10x on alloc).
type foldCtx struct {
	hph      *HexPatriciaHashed
	cell     cell
	buf      []byte
	emit     bool
	deferred []*DeferredBranchUpdate
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
// hashing run transitively inside computeCellHash. The hash is returned by value into fc's reused
// scratch — the non-escaping shape the proto settled on; escape behavior is a manual `-gcflags=-m`
// spot-check, while the alloc-ceiling bench is the CI gate.
func (fc *foldCtx) foldNode(node *prefixNode, prefix []byte, branchDepth int16) (common.Hash, error) {
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
			var childPrefix []byte
			if fc.emit {
				childPrefix = make([]byte, 0, len(prefix)+1+len(child.ext))
				childPrefix = append(childPrefix, prefix...)
				childPrefix = append(childPrefix, byte(nib))
				childPrefix = append(childPrefix, child.ext...)
			}
			bh, err := fc.foldNode(child, childPrefix, childDepth+int16(len(child.ext)))
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
	var cellData [16]cellEncodeData
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
		if fc.emit {
			cellData[s] = cellEncodeDataFromCell(&fc.cell)
		}
	}
	var h common.Hash
	fc.hph.keccak2.Read(h[:])
	if fc.emit {
		if err := fc.emitBranchUpdate(node.bitmap, prefix, &cellData); err != nil {
			return common.Hash{}, err
		}
	}
	return h, nil
}

// emitBranchUpdate encodes this fresh branch (every present child both touched and after, no
// predecessor) with the same BranchEncoder the sequential fold uses and stages a DeferredBranchUpdate
// for prefix, so the emitted records are byte-identical to foldMounted's.
func (fc *foldCtx) emitBranchUpdate(bitmap uint16, prefix []byte, cellData *[16]cellEncodeData) error {
	raw, err := fc.hph.branchEncoder.EncodeBranch(bitmap, bitmap, bitmap, cellData)
	if err != nil {
		return err
	}
	fc.deferred = append(fc.deferred, getDeferredUpdate(nibbles.HexToCompact(prefix), raw, nil))
	return nil
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
	return fc.foldNode(node, nil, 64)
}

// foldFreshStorageRootDeferred folds a fresh account's touched storage subtree like
// foldFreshStorageRoot, but also emits a DeferredBranchUpdate per storage branch prefix, so the
// caller can persist the trie, not just its root. accPrefix is the 64-nibble account path the
// storage-root branch sits at. Fail-closed: any fold error drops every collected update.
func foldFreshStorageRootDeferred(node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	if node == nil || node.bitmap == 0 {
		return empty.RootHash, nil, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), emit: true}
	defer fc.hph.Release()
	h, err := fc.foldNode(node, accPrefix, 64)
	if err != nil {
		for _, upd := range fc.deferred {
			putDeferredUpdate(upd)
		}
		return common.Hash{}, nil, err
	}
	return h, fc.deferred, nil
}

// foldReconciledStorageRoot folds a touched storage subtree against on-disk state, so untouched
// on-disk siblings inside a touched branch survive into the folded root — the reconciliation the
// fresh hand-rolled fold lacks (it reads no disk). It seeds row 0 from the on-disk branch at
// accPrefix, replays the touched keys so unfold reads each deeper on-disk branch and fold keeps its
// untouched children blinded, then collapses row 0 into the account's single storage-root cell at
// the depth-64 seam. A missing branch at accPrefix means the account is storage-less on disk (the
// fresh case) — an empty wall, no siblings. Returns the storage root and the deferred branch
// updates the fold produced.
func foldReconciledStorageRoot(fp *foldPool, ctx context.Context, node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	base, release := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	defer release()

	if err := seedBaseAtPrefix(base, accPrefix); err != nil && !errors.Is(err, errStorageBaseNotBranch) {
		return common.Hash{}, nil, fmt.Errorf("reconciled storage seed: %w", err)
	}

	if err := dfsSubtree(node, append([]byte(nil), accPrefix...), base.followAndUpdate); err != nil {
		return common.Hash{}, nil, err
	}
	for base.activeRows > 1 {
		if err := ctx.Err(); err != nil {
			return common.Hash{}, nil, err
		}
		if err := base.fold(); err != nil {
			return common.Hash{}, nil, fmt.Errorf("reconciled storage fold: %w", err)
		}
	}

	var noChildren [16]cell
	sr, err := aggregateMountedStorageRoot(base, &noChildren, 0)
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("reconciled storage aggregate: %w", err)
	}
	deferred := base.TakeDeferredUpdates()
	if sr.IsEmpty() {
		return empty.RootHash, deferred, nil
	}
	return sr.hash, deferred, nil
}
