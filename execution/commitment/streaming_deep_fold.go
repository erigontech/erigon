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

// foldStorageChildCell folds one storage nibble's keys in a standalone worker and
// returns the depth-65 storage-branch child cell. Production copy of the deep-fold
// test helper foldChildAt; parallel_mount.go is intentionally left untouched.
func foldStorageChildCell(w *HexPatriciaHashed, accNib int, group []touchedKey) (cell, error) {
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
	c := w.grid[0][accNib]
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

// aggregateStorageRoot stitches the present storage child cells into the storage
// branch and folds it once to the account's storageRoot cell (depth 64). The
// returned cell's hash is the storageRoot setAccountStorageRoot injects into the
// account leaf. Production copy of concurrentStorageRoot's stitch.
func aggregateStorageRoot(w *HexPatriciaHashed, accHash []byte, accNib int, children *[16]cell, present uint16) (cell, error) {
	copy(w.currentKey[:], accHash[:64])
	w.currentKeyLen = 64
	w.depths[0] = 64
	w.depths[1] = 65
	w.activeRows = 2
	w.touchMap[0] = uint16(1) << accNib
	w.afterMap[0] = uint16(1) << accNib
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
	return w.grid[0][accNib], nil
}
