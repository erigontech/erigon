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
	if c.hashedExtLen > 0 {
		c.hashedExtLen--
		copy(c.hashedExtension[:], c.hashedExtension[1:])
	}
	if c.extLen > 0 {
		c.extLen--
		copy(c.extension[:], c.extension[1:])
	}
	return c, nil
}

// foldStorageChildCell folds one storage nibble's keys in a standalone worker and
// returns the depth-65 storage-branch child cell — foldChildSubtree mounted at the
// account boundary (depth 64). Production copy of the deep-fold test helper
// foldChildAt; parallel_mount.go is intentionally left untouched.
func foldStorageChildCell(w *HexPatriciaHashed, accNib int, group []touchedKey) (cell, error) {
	return foldChildSubtree(w, accNib, group)
}

// aggregateSubtreeRoot stitches the present child cells into the branch at depth
// len(prefix)+1 and folds it once to the subtree's root cell at depth len(prefix).
// The destination grid cell is reset first so no stale pooled-grid field survives
// into the folded cell. The returned cell's hash is the subtree root.
func aggregateSubtreeRoot(w *HexPatriciaHashed, prefix []byte, children *[16]cell, present uint16) (cell, error) {
	d := int16(len(prefix))
	col := int(prefix[d-1])
	copy(w.currentKey[:], prefix)
	w.currentKeyLen = d
	w.depths[0] = d
	w.depths[1] = d + 1
	w.activeRows = 2
	w.grid[0][col].reset()
	w.touchMap[0] = uint16(1) << col
	w.afterMap[0] = uint16(1) << col
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 {
			w.grid[1][x] = children[x]
		}
	}
	w.touchMap[1] = present
	w.afterMap[1] = present
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
