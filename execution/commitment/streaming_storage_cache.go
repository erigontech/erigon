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
	"bytes"
	"math/bits"
	"sort"
	"sync"

	"github.com/erigontech/erigon/common/empty"
)

// storageKeyNibbles is the nibble length of a hashed storage key (32-byte account
// hash + 32-byte slot hash); accountKeyNibbles is a hashed account key's length.
const (
	accountKeyNibbles = 64
	storageKeyNibbles = 128
)

// defaultNestedCap bounds how many big-storage accounts are cached at once. Over
// the cap a newly-qualifying account falls back to the full (cache-free) fold
// rather than evicting an existing cache (no LRU).
const defaultNestedCap = 1024

// cacheNibble tracks one first-storage-nibble subtree of a cached big-storage
// account: whether its cached child cell is stale and the accumulated slot set.
// keys is the side structure that lets a dirty nibble re-fold from its FULL slot
// set across blocks — the deep fan-out rebuilds a nibble purely from the touched
// keys (it does not read the on-disk subtree), so the cache must remember every
// slot ever touched, keyed by hashed key.
type cacheNibble struct {
	dirty bool
	keys  map[string]touchedKey
}

// accountStorageCache caches the 16 depth-65 storage-child cells of one
// big-storage account so a re-fold only re-folds the storage nibbles whose
// slots changed and reuses the cached cells for the rest. Streaming-only; it
// reuses the proven storage-fold primitives but never touches parallel_mount.go.
type accountStorageCache struct {
	prefix      []byte // accHash[:64]
	accPlainKey []byte // un-hashed account key, to re-load the leaf from ctx when storage changes but the account itself is untouched
	children    [16]cell
	present     uint16
	perNibble   [16]cacheNibble
	invalid     bool // structural bypass detected; drop the cache at endBlock
	mu          sync.Mutex
}

// accStorageTouch accumulates one account's storage-touch stats for the current
// block before it is promoted to a cache: the touched-slot count and the set of
// first-storage-nibbles they span, so promotion mirrors the deep walk's
// (count>threshold && nibbleSpan>=2) condition. capped marks an account that
// qualified but was denied a cache by the cap and is folded cache-free.
type accStorageTouch struct {
	slots      uint64
	nibbleMask uint16
	capped     bool
}

// qualifies reports whether the accumulated touches meet the deep walk's
// big-storage condition: more than deepStorageThreshold slots spanning at least
// two first-storage-nibbles.
func (a *accStorageTouch) qualifies() bool {
	return a.slots > deepStorageThreshold && bits.OnesCount16(a.nibbleMask) >= 2
}

// touchNibble routes one storage-slot touch of a cached account to its
// first-storage-nibble, marking it dirty so the next fold re-folds it.
func (c *accountStorageCache) touchNibble(nib byte) {
	c.mu.Lock()
	c.perNibble[nib].dirty = true
	c.mu.Unlock()
}

// retain folds one touched slot into the nibble's accumulated slot set, copying
// the hashed and plain keys (and the update, when carried) so the entry survives
// the per-block prefix-trie reset for a later cross-block re-fold.
func (c *accountStorageCache) retain(nib int, tk touchedKey) {
	n := &c.perNibble[nib]
	if n.keys == nil {
		n.keys = make(map[string]touchedKey)
	}
	e := touchedKey{hk: append([]byte(nil), tk.hk...), pk: append([]byte(nil), tk.pk...)}
	if tk.upd != nil {
		u := *tk.upd
		e.upd = &u
	}
	n.keys[string(e.hk)] = e
}

// sortedKeys returns the nibble's accumulated slot set in ascending hashed-key
// order — the order foldStorageChildCell replays them in.
func (c *accountStorageCache) sortedKeys(nib int) []touchedKey {
	n := &c.perNibble[nib]
	if len(n.keys) == 0 {
		return nil
	}
	out := make([]touchedKey, 0, len(n.keys))
	for _, tk := range n.keys {
		out = append(out, tk)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].hk, out[j].hk) < 0 })
	return out
}

// newPromotedCache builds a cache for a freshly promoted account, marking every
// already-touched first-storage-nibble dirty so the first fold re-folds them.
func newPromotedCache(prefix, accPlainKey []byte, mask uint16) *accountStorageCache {
	c := &accountStorageCache{prefix: prefix, accPlainKey: accPlainKey}
	for x := range 16 {
		if mask&(uint16(1)<<x) != 0 {
			c.perNibble[x].dirty = true
		}
	}
	return c
}

// storageWorkerFactory yields a fresh trie worker (clean grid, context bound),
// its release, and a flushed accessor (nil when the worker is not isolated). The
// cache calls it once per storage nibble it re-folds and once for the final
// aggregation; a non-nil flushed reporting true means the fold self-flushed a
// collapse against an isolating overlay and its cell must not be trusted.
type storageWorkerFactory func() (w *HexPatriciaHashed, release func(), flushed func() bool)

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
// holds the current touched keys per first-storage-nibble. onDeferred, when
// non-nil, receives the deferred branch updates each re-folded nibble and the
// aggregation emit so the Process path can stage them for the caller.
//
// A re-folded nibble that empties (all its slots deleted) is dropped from the
// storage branch (present bit cleared) so the aggregate root reflects the
// collapse. The returned flushed flag is set when a per-nibble fold self-flushed
// a collapse against an isolating overlay: its cell stays self-consistent (the
// root is correct) but the cache should be invalidated so a structurally changed
// account re-promotes fresh rather than reusing cells folded across the collapse.
func foldStorageRootCached(newWorker storageWorkerFactory, cache *accountStorageCache, accNib int, groups *[16][]touchedKey, onDeferred func([]*DeferredBranchUpdate)) (cell, int, bool, error) {
	folded := 0
	flushed := false
	for x := range 16 {
		bit := uint16(1) << x
		n := &cache.perNibble[x]
		if len(groups[x]) == 0 {
			continue
		}
		if cache.present&bit != 0 && !n.dirty {
			continue
		}
		w, release, flushedFn := newWorker()
		c, err := foldStorageChildCell(w, accNib, groups[x])
		if err == nil && onDeferred != nil {
			if d := w.TakeDeferredUpdates(); len(d) > 0 {
				onDeferred(d)
			}
		}
		if flushedFn != nil && flushedFn() {
			flushed = true
		}
		if release != nil {
			release()
		}
		if err != nil {
			return cell{}, folded, flushed, err
		}
		cache.children[x] = c
		if c.IsEmpty() {
			cache.present &^= bit
		} else {
			cache.present |= bit
		}
		n.dirty = false
		folded++
	}

	w, release, _ := newWorker()
	sr, err := aggregateStorageRoot(w, cache.prefix, accNib, &cache.children, cache.present)
	if err == nil && onDeferred != nil {
		if d := w.TakeDeferredUpdates(); len(d) > 0 {
			onDeferred(d)
		}
	}
	if release != nil {
		release()
	}
	if err != nil {
		return cell{}, folded, flushed, err
	}
	return sr, folded, flushed, nil
}
