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

import "github.com/erigontech/erigon/execution/commitment/nibbles"

// foldChildSubtree folds one subtree's collected keys in a standalone worker
// mounted from the trie root and returns its cell at the deepest shared depth,
// stripped of the leading extension nibble the parent column carries. col is that
// cell's column (the prefix's last nibble). The destination grid cell is reset
// first so no stale pooled-grid field (hashedExtension or account fields a
// single-child foldPropagate leaves untouched) survives into the folded cell.
func foldChildSubtree(w *HexPatriciaHashed, col int, group []touchedKey) (cell, error) {
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

// aggregateSubtreeRoot stitches the present child cells into the branch at depth
// len(prefix)+1 and folds it once to the subtree's root cell at depth len(prefix).
// The destination grid cell is reset first so no stale pooled-grid field survives
// into the folded cell. A child that folded to an empty cell (its whole sub-subtree
// collapsed under deletes) stays in touchMap — so the branch update records the
// deletion against the on-disk pre-image — but is dropped from afterMap. If every
// child collapsed the whole subtree is empty and an empty cell is returned so the
// caller drops this branch bit in turn. The returned cell's hash is the subtree root.
func aggregateSubtreeRoot(w *HexPatriciaHashed, prefix []byte, children *[16]cell, present uint16) (cell, error) {
	d := int16(len(prefix))
	col := int(prefix[d-1])
	after := present
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 && children[x].IsEmpty() {
			after &^= uint16(1) << x
		}
	}
	if after == 0 {
		return cell{}, nil
	}
	copy(w.currentKey[:], prefix)
	w.currentKeyLen = d
	w.depths[0] = d
	w.depths[1] = d + 1
	w.activeRows = 2
	w.grid[0][col].reset()
	w.touchMap[0] = uint16(1) << col
	w.afterMap[0] = uint16(1) << col
	for x := range 16 {
		if after&(uint16(1)<<x) != 0 {
			w.grid[1][x] = children[x]
		} else {
			w.grid[1][x].reset()
		}
	}
	w.touchMap[1] = present
	w.afterMap[1] = after
	if err := w.fold(); err != nil {
		return cell{}, err
	}
	return w.grid[0][col], nil
}

// aggregateStorageRoot stitches the present storage child cells into the storage
// branch and folds it once to the account's storageRoot cell (depth 64) —
// aggregateSubtreeRoot mounted at the 64-nibble account hash. The returned cell's
// hash is the storageRoot setAccountStorageRoot injects into the account leaf.
func aggregateStorageRoot(w *HexPatriciaHashed, accHash []byte, children *[16]cell, present uint16) (cell, error) {
	return aggregateSubtreeRoot(w, accHash[:64], children, present)
}

// foldStorageLeaf folds one first-storage-nibble subtree confined to its own
// depth-65 prefix. The worker is mounted at childPrefix (= accHash + nibble +
// child.ext) and, when an on-disk branch exists there, unfolds it so an incremental
// collapse preserves this subtree's untouched on-disk interior siblings — without
// reading the storage-root branch (which would pull in the sibling first-nibbles
// other workers own) or the account trie above it. The returned cell is the
// childPrefix branch with no extension; the caller lifts child.ext, mirroring the
// recursive split aggregate's convention.
func foldStorageLeaf(w *HexPatriciaHashed, childPrefix []byte, group []touchedKey) (cell, error) {
	pd := int16(len(childPrefix))
	col := int(childPrefix[pd-1])
	copy(w.currentKey[:], childPrefix)
	w.currentKeyLen = pd - 1
	w.depths[0] = pd
	w.activeRows = 1
	w.grid[0][col].reset()
	// Trigger the on-disk unfold at childPrefix when a branch exists there. The
	// seeded hashLen is only a needUnfolding signal; unfoldBranchNode replaces the
	// cell with the real on-disk children.
	branch, err := w.branchFromCacheOrDB(nibbles.HexToCompact(childPrefix))
	if err != nil {
		return cell{}, err
	}
	if len(branch) > 0 {
		w.grid[0][col].hashLen = 32
	}
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
	return w.grid[0][col], nil
}
