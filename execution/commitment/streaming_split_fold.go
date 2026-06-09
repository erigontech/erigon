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

import "math/bits"

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
