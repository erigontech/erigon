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

	"golang.org/x/sync/errgroup"
)

// isSplitPoint reports whether a prefix-trie node is a concurrent-fold split
// point: a branch with at least two children and at least MinSplitKeys touched
// keys in its subtree, hosting no terminator. The plainKey == nil guard is
// correctness, not optimization — a node hosting a terminating key (an account
// at depth 64 above its storage, or a storage-slot leaf) has no terminator slot
// in a branch-indexed split cell, so splitting there would drop that key from
// the branch hash. There is deliberately no depth cap: storage-interior forks
// (depth > 64) are exactly the bottleneck this splits.
func isSplitPoint(node *prefixNode) bool {
	return node != nil && node.plainKey == nil &&
		bits.OnesCount16(node.bitmap) >= 2 && node.subtreeCount >= MinSplitKeys
}

// stripLeadingChildExt removes the single leading extension nibble a folded
// subtree cell carries — the branch column it lands in already implies that
// nibble. Generalizes the row-0 split stitch to any depth.
func stripLeadingChildExt(c *cell) {
	if c.hashedExtLen > 0 {
		c.hashedExtLen--
		copy(c.hashedExtension[:], c.hashedExtension[1:])
	}
	if c.extLen > 0 {
		c.extLen--
		copy(c.extension[:], c.extension[1:])
	}
}

// foldSubtreeAtPrefix folds a group of keys all sharing parentPrefix+[childNib]
// into a single cell ready to drop into the parent branch's grid[1][childNib].
// The worker is hand-mounted at parentPrefix (depth len(parentPrefix)) so the
// fold roots there instead of at the depth-64 storage boundary — the
// arbitrary-depth generalization of the deep-fold mount. The returned cell is
// stripped of its leading extension nibble (the parent column carries it).
func foldSubtreeAtPrefix(w *HexPatriciaHashed, parentPrefix []byte, group []touchedKey) (cell, error) {
	pd := int16(len(parentPrefix))
	col := int(parentPrefix[pd-1])
	copy(w.currentKey[:], parentPrefix)
	w.currentKeyLen = pd - 1
	w.depths[0] = pd
	w.activeRows = 1
	w.grid[0][col].reset()
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	for w.activeRows > 1 {
		if err := w.fold(); err != nil {
			return cell{}, err
		}
	}
	c := w.grid[0][col]
	stripLeadingChildExt(&c)
	return c, nil
}

// foldStorageChild folds the subtree of child (which hangs off the parent branch
// at nibble childNib, carrying child.ext) and returns the cell ready to place in
// the parent branch's grid[1][childNib]. At a deeper qualifying fork it splits
// again: every grandchild subtree folds concurrently in its own worker and the
// branch is aggregated post-order — this is where storage-interior concurrency
// (depth > 64) comes from. Non-split subtrees fold flat in a single worker.
// sem bounds the number of concurrent fold workers across the whole recursion;
// waiting goroutines never hold a slot, so the recursion cannot self-deadlock.
func (sc *StreamingCommitter) foldStorageChild(sem chan struct{}, pu *parallelUpdate, parentPrefix []byte, childNib int, child *prefixNode) (cell, error) {
	childPrefix := make([]byte, 0, len(parentPrefix)+1+len(child.ext))
	childPrefix = append(childPrefix, parentPrefix...)
	childPrefix = append(childPrefix, byte(childNib))
	childPrefix = append(childPrefix, child.ext...)

	if isSplitPoint(child) {
		if len(childPrefix) > 64 {
			sc.storageSplits.Add(1)
		}
		var gchildren [16]cell
		present := child.bitmap
		var g errgroup.Group
		gi := 0
		for bm := child.bitmap; bm != 0; {
			gn := int(bits.TrailingZeros16(bm))
			gc := child.children[gi]
			g.Go(func() error {
				c, err := sc.foldStorageChild(sem, pu, childPrefix, gn, gc)
				if err != nil {
					return err
				}
				gchildren[gn] = c
				return nil
			})
			gi++
			bm &^= uint16(1) << gn
		}
		if err := g.Wait(); err != nil {
			return cell{}, err
		}

		sem <- struct{}{}
		w, release := sc.newStorageWorker()
		branchCell, err := aggregateSubtreeRoot(w, childPrefix, &gchildren, present)
		if err == nil {
			if d := w.TakeDeferredUpdates(); len(d) > 0 {
				pu.appendDeferred(d)
			}
		}
		release()
		<-sem
		if err != nil {
			return cell{}, fmt.Errorf("storage split[%x] aggregate: %w", childNib, err)
		}
		// Lift child.ext: the aggregate is a direct branch cell at childPrefix;
		// placed under the parent at childNib it becomes an extension node whose
		// extension is child.ext (extensionHash is depth-independent).
		if len(child.ext) > 0 {
			copy(branchCell.extension[:], child.ext)
			branchCell.extLen = int16(len(child.ext))
		}
		return branchCell, nil
	}

	group := collectStorageNibbleKeys(child, childPrefix)
	sem <- struct{}{}
	w, release := sc.newStorageWorker()
	c, err := foldSubtreeAtPrefix(w, parentPrefix, group)
	if err == nil {
		if d := w.TakeDeferredUpdates(); len(d) > 0 {
			pu.appendDeferred(d)
		}
	}
	release()
	<-sem
	if err != nil {
		return cell{}, fmt.Errorf("storage leaf[%x] fold: %w", childNib, err)
	}
	return c, nil
}
