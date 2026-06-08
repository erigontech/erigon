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
	"sync"

	"github.com/erigontech/erigon/common/empty"
)

// cacheNibble tracks one first-storage-nibble subtree of a cached big-storage
// account: whether its cached child cell is stale and the size gate counters
// (mirrors splitState's keyCount/lastFoldedSize doubling gate, per nibble).
type cacheNibble struct {
	dirty          bool
	keyCount       uint64
	lastFoldedSize uint64
}

// accountStorageCache caches the 16 depth-65 storage-child cells of one
// big-storage account so a re-fold only re-folds the storage nibbles whose
// slots changed and reuses the cached cells for the rest. Streaming-only; it
// reuses the proven storage-fold primitives but never touches parallel_mount.go.
type accountStorageCache struct {
	prefix    []byte // accHash[:64]
	children  [16]cell
	present   uint16
	perNibble [16]cacheNibble
	mu        sync.Mutex
}

// storageWorkerFactory yields a fresh trie worker (clean grid, context bound)
// and its release. The cache calls it once per storage nibble it re-folds and
// once for the final aggregation.
type storageWorkerFactory func() (w *HexPatriciaHashed, release func())

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

// assembleAccountRoot stitches the cached storage child cells into the account
// leaf and folds to the account root. Production copy of the prototype's
// assembleAccountFromChildren; used where a full account-leaf root is needed.
func assembleAccountRoot(w *HexPatriciaHashed, addr, accHash []byte, accNib int, accUpd *Update, children *[16]cell, present uint16) ([]byte, error) {
	copy(w.currentKey[:], accHash[:64])
	w.currentKeyLen = 64
	w.depths[0] = 64
	w.depths[1] = 65
	w.activeRows = 2
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(accUpd)
	w.grid[0][accNib] = ac
	w.touchMap[0] = uint16(1) << accNib
	w.afterMap[0] = uint16(1) << accNib
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 {
			w.grid[1][x] = children[x]
		}
	}
	w.touchMap[1] = present
	w.afterMap[1] = present
	for w.activeRows > 0 {
		if err := w.fold(); err != nil {
			return nil, err
		}
	}
	return w.RootHash()
}

// foldStorageRootCached re-folds only the dirty (or not-yet-cached) storage
// nibbles of accNib's account, reuses the cached clean cells for the rest,
// aggregates them into the storageRoot cell, and clears the per-nibble dirty
// flags. It returns the storageRoot cell and the number of nibbles re-folded
// (so callers/tests can assert only the changed nibbles paid the cost). groups
// holds the current touched keys per first-storage-nibble.
func foldStorageRootCached(newWorker storageWorkerFactory, cache *accountStorageCache, accNib int, groups *[16][]touchedKey) (cell, int, error) {
	folded := 0
	for x := range 16 {
		bit := uint16(1) << x
		n := &cache.perNibble[x]
		if len(groups[x]) == 0 {
			continue
		}
		if cache.present&bit != 0 && !n.dirty {
			continue
		}
		w, release := newWorker()
		c, err := foldStorageChildCell(w, accNib, groups[x])
		if release != nil {
			release()
		}
		if err != nil {
			return cell{}, folded, err
		}
		cache.children[x] = c
		cache.present |= bit
		n.dirty = false
		n.lastFoldedSize = uint64(len(groups[x]))
		folded++
	}

	w, release := newWorker()
	sr, err := aggregateStorageRoot(w, cache.prefix, accNib, &cache.children, cache.present)
	if release != nil {
		release()
	}
	if err != nil {
		return cell{}, folded, err
	}
	return sr, folded, nil
}
