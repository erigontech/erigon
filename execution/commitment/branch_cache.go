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
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
)

// BranchCache stores commitment-trie branch data with a persistence-friendly
// shape distinct from WarmupCache:
//
//   - Bounded LRU tail with configurable capacity (eviction is well-defined,
//     suitable for long-lived caching across many Process calls without
//     unbounded memory growth).
//   - Single pinned slot for the root branch (always hottest, always present
//     once populated, never subject to LRU eviction). Compact prefix of
//     length 0 (or single-byte "no-key" form) targets this slot.
//   - dirty-flag + PutIfClean invariants (carried from step 4 of the
//     representation-reduction sequence) so cross-block writers can race
//     safely with fold updates.
//   - Lazy-decoded read path (GetDecoded) — same pattern as WarmupCache's
//     GetBranchDecoded; cells are populated from the cached encoded form
//     on first decoded-read and reused thereafter.
//
// Today this is constructed as ephemeral infrastructure (per-Process,
// alongside the trie). Future cross-block persistence work (step 7 of
// the representation-reduction sequence) will lift its lifetime to the
// aggTx level. The wire-up between the trie's read path and BranchCache
// is intentionally NOT done in this commit — see the integration
// discussion captured at the time of step 6.
//
// Distinct from WarmupCache: WarmupCache is a simple unbounded map for
// warmup-fill hot-read patterns within one Process. BranchCache is
// designed for the longer-lived cache lifetime that the cross-block
// persistence work needs.
type BranchCache struct {
	// Pinned tier — single slot for the root branch. Atomic-pointer
	// access so no lock is needed for the hot read path.
	root atomic.Pointer[branchCacheEntry]

	// LRU tail — bounded entries, evicts oldest when full. maphash.LRU
	// wraps hashicorp/golang-lru/v2 which is thread-safe internally.
	tail *maphash.LRU[*branchCacheEntry]

	// Stats — atomic counters surfaced via Stats().
	rootHits, rootMisses atomic.Uint64
	tailHits, tailMisses atomic.Uint64
	bytesServed          atomic.Uint64
}

type branchCacheEntry struct {
	// data is the canonical encoded form (with the leading 2-byte touch-map
	// prefix). Always populated by Put / PutIfClean.
	data []byte

	// Lazy-decoded form. Populated on first GetDecoded for this entry;
	// subsequent reads return the cached cells. decodeOnce ensures decode
	// runs at most once even under concurrent reads.
	decodeOnce   sync.Once
	cells        [16]cell
	cellsBitmap  uint16
	decodedReady bool
	decodeErr    error

	// dirty signals "the canonical store has been written to since this
	// entry was populated; treat as stale until cleared." Same semantics
	// as branchEntry.dirty in WarmupCache (carried from step 4).
	dirty atomic.Bool
}

// DefaultBranchCacheTailCapacity is the LRU tail size used when no
// explicit capacity is given. ~50k entries × ~500 bytes = ~25 MB
// at typical mainnet branch sizes.
const DefaultBranchCacheTailCapacity = 50000

// NewBranchCache constructs a BranchCache with the given LRU tail capacity.
// Capacity <= 0 panics — pass a positive value or DefaultBranchCacheTailCapacity.
func NewBranchCache(tailCapacity int) *BranchCache {
	if tailCapacity <= 0 {
		panic(fmt.Sprintf("BranchCache: tailCapacity must be positive, got %d", tailCapacity))
	}
	tail, err := maphash.NewLRU[*branchCacheEntry](tailCapacity)
	if err != nil {
		panic(fmt.Sprintf("BranchCache: NewLRU: %s", err))
	}
	return &BranchCache{tail: tail}
}

// isRootPrefix reports whether prefix targets the pinned root slot. The
// commitment-trie compact encoding uses a 1-byte even-length flag (0x00)
// to represent the empty nibble path (root branch). Anything longer goes
// to the LRU tail.
func isRootPrefix(prefix []byte) bool {
	return len(prefix) == 1 && prefix[0] == 0x00
}

func (c *BranchCache) lookup(prefix []byte) (*branchCacheEntry, bool) {
	if isRootPrefix(prefix) {
		entry := c.root.Load()
		if entry == nil {
			c.rootMisses.Add(1)
			return nil, false
		}
		c.rootHits.Add(1)
		return entry, true
	}
	entry, ok := c.tail.Get(prefix)
	if !ok {
		c.tailMisses.Add(1)
		return nil, false
	}
	c.tailHits.Add(1)
	return entry, true
}

func (c *BranchCache) store(prefix []byte, entry *branchCacheEntry) {
	if isRootPrefix(prefix) {
		c.root.Store(entry)
		return
	}
	c.tail.Set(prefix, entry)
}

// Get retrieves branch data from the cache. Returns the canonical encoded
// bytes (with the leading 2-byte touch-map prefix).
func (c *BranchCache) Get(prefix []byte) ([]byte, bool) {
	entry, ok := c.lookup(prefix)
	if !ok {
		return nil, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return entry.data, true
}

// GetDecoded retrieves the cached branch in decoded form. Lazy-decodes on
// first access for each entry; subsequent reads return the cached cells
// pointer without redoing the parse work.
//
// Returns the bitmap of present children plus a pointer to the populated
// cells array. Caller derives touchMap/afterMap based on its own context
// (deleted vs present-after) — same convention as WarmupCache.GetBranchDecoded.
//
// The returned *[16]cell aliases storage owned by the cache entry — the
// caller MUST NOT modify the cells in place. Read-only consumption is
// safe across concurrent calls.
func (c *BranchCache) GetDecoded(prefix []byte) (bitmap uint16, cells *[16]cell, ok bool) {
	entry, found := c.lookup(prefix)
	if !found {
		return 0, nil, false
	}
	entry.decodeOnce.Do(func() {
		if len(entry.data) < 2 {
			entry.decodeErr = fmt.Errorf("branch entry too short for touch-map prefix: %d bytes", len(entry.data))
			return
		}
		maps, err := DecodeBranchInto(entry.data[2:], false /* deleted derived per-caller */, &entry.cells)
		if err != nil {
			entry.decodeErr = err
			return
		}
		entry.cellsBitmap = maps.Bitmap
		entry.decodedReady = true
	})
	if !entry.decodedReady {
		return 0, nil, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return entry.cellsBitmap, &entry.cells, true
}

// Put stores branch data in the cache, replacing any existing entry
// (clearing its dirty flag in the process — the new entry is fresh).
// Always copies the input data so the cache owns it independently of
// caller buffer lifetime.
func (c *BranchCache) Put(prefix []byte, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.store(prefix, &branchCacheEntry{data: dataCopy})
}

// PutIfClean stores branch data only if no existing entry is marked dirty.
// Returns true on store, false if a dirty entry was present (indicating a
// canonical-store write is in progress and the caller's data is potentially
// stale).
//
// Same semantics as WarmupCache.PutBranchIfClean — see that doc for the
// race-it-protects-against narrative.
func (c *BranchCache) PutIfClean(prefix []byte, data []byte) bool {
	if existing, ok := c.lookup(prefix); ok && existing.dirty.Load() {
		return false
	}
	c.Put(prefix, data)
	return true
}

// MarkDirty flags the entry at prefix as stale-until-cleared. Subsequent
// PutIfClean calls for this prefix will skip; reads still return the
// entry (the dirty signal is consumed only on the write path today, same
// as WarmupCache.MarkBranchDirty).
//
// No-op if no entry exists at prefix.
func (c *BranchCache) MarkDirty(prefix []byte) {
	if entry, ok := c.lookup(prefix); ok {
		entry.dirty.Store(true)
	}
}

// Invalidate removes the entry at prefix entirely. Use when the caller
// knows the canonical store has changed and the cached entry should not
// be served at all (vs MarkDirty which keeps the entry but blocks
// PutIfClean overwrites).
func (c *BranchCache) Invalidate(prefix []byte) {
	if isRootPrefix(prefix) {
		c.root.Store(nil)
		return
	}
	c.tail.Delete(prefix)
}

// Clear empties the cache and resets stats counters. Use on Reset /
// fork-validation paths to ensure stale entries from one trie root are
// not served against a different root.
func (c *BranchCache) Clear() {
	c.root.Store(nil)
	c.tail.Purge()
	c.rootHits.Store(0)
	c.rootMisses.Store(0)
	c.tailHits.Store(0)
	c.tailMisses.Store(0)
	c.bytesServed.Store(0)
}

// Stats returns a one-line summary of root-tier and tail-tier hit/miss
// counters plus bytes served. Format mirrors WarmupCache.Stats() so
// per-Process log lines can compose them.
func (c *BranchCache) Stats() string {
	rh, rm := c.rootHits.Load(), c.rootMisses.Load()
	th, tm := c.tailHits.Load(), c.tailMisses.Load()
	bb := c.bytesServed.Load()
	pct := func(hit, miss uint64) float64 {
		total := hit + miss
		if total == 0 {
			return 0
		}
		return 100.0 * float64(hit) / float64(total)
	}
	return fmt.Sprintf(
		"branch-cache root hit=%d miss=%d (%.1f%%) | tail hit=%d miss=%d (%.1f%%) | served %.1f MiB | tail entries=%d",
		rh, rm, pct(rh, rm),
		th, tm, pct(th, tm),
		float64(bb)/1024/1024,
		c.tail.Len(),
	)
}
