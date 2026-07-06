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
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// KeyCommitmentState is the commitment-domain key under which the trie
// checkpoint (txNum / blockNum / encoded root state) is stored. It is NOT a
// trie branch: it changes every block, so it must never enter the
// BranchCache — serving a stale checkpoint restores the trie to the wrong
// state and corrupts the computed root. BranchCache.Put/Get reject
// it by construction so no caller can pollute the cache with it.
var KeyCommitmentState = []byte("state")

func isCommitmentStateKey(prefix []byte) bool {
	return bytes.Equal(prefix, KeyCommitmentState)
}

// BranchCache is the aggregator-scope (one per commitment Domain) cache of
// commitment-trie branch data: a single pinned slot for the always-hot root
// branch plus a bounded LRU tail for the rest. It is a passive store — the
// trie walker/encoder drives all reads and writes; the cache never reaches
// into underlying state.
//
// Concurrency: the LRU tail and the atomic-pointer root slot are individually
// thread-safe, but the cache assumes a single writer per prefix (last-Put-wins
// with no cross-writer coordination). The concurrent trie satisfies this by
// partitioning the prefix space by first nibble across mounts and writing the
// root branch only in the sequential post-Wait root fold. Any future design
// that breaks single-writer-per-prefix (e.g. parallel tree-reduce fold, or a
// different prefix partitioning) must add per-prefix coordination at the
// orchestrator layer — do not add internal locking here.
type BranchCache struct {
	// Root tier — single slot for the root branch (always hottest, always
	// present). Atomic-pointer access so no lock is needed for the hot
	// read path.
	root atomic.Pointer[branchCacheEntry]

	// LRU tail — bounded entries with per-shard LRU eviction (no global recency
	// order across shards). See branchCacheTailShards for why it's sharded.
	tail *maphash.ShardedLRU[*branchCacheEntry]

	// Stats — atomic counters surfaced via Stats().
	rootHits, rootMisses atomic.Uint64
	tailHits, tailMisses atomic.Uint64
	bytesServed          atomic.Uint64
	staleEvicted         atomic.Uint64 // entries dropped lazily on read after an unwind

	// coh is the (epoch, floor) unwind-coherence primitive shared with the state
	// and code caches: an entry is valid iff written in the current epoch OR its
	// txN is below the unwind floor. See execution/cache/coherence.
	coh coherence.Gen
}

type branchCacheEntry struct {
	// data is the canonical encoded form (with the leading 2-byte touch-map
	// prefix). Always populated by Put.
	data []byte

	// step is the on-disk file step the cached bytes came from. Returned
	// by Get so callers (e.g. CheckDataAvailable) can validate against
	// the latest visible step. 0 means "step not tracked" — fine for
	// in-memory tests but real callers should always pass the step
	// returned by aggTx.MeteredGetLatest / tx.GetLatest.
	step uint64

	// txN is the txN the cached bytes are valid as of (an upper bound: the
	// value's write txN). With epoch it gates reads after an unwind. 0 means
	// "frozen/untracked" — predates any unwind, always served.
	txN uint64

	// epoch is the unwind generation the entry was written in. Disambiguates a
	// txN reused across forks: an entry from a superseded epoch whose txN is at
	// or above the unwind floor is dropped lazily on its next Get.
	epoch uint32
}

// DefaultBranchCacheTailCapacity is the LRU tail size used when no
// explicit capacity is given. ~50k entries × ~500 bytes = ~25 MB
// at typical mainnet branch sizes.
const DefaultBranchCacheTailCapacity = 50000

// branchCacheTailShards splits the tail into this many independently-locked
// shards so concurrent warmup workers don't serialize on a single LRU mutex.
// Fixed value sized for the default warmup pool (dbg.TipTrieWarmupers ≈ NumCPU*8).
const branchCacheTailShards = 256

// BranchCacheProvider exposes the long-lived BranchCache attached to the
// commitment domain. Implemented by *db/state.AggregatorRoTx via duck typing to
// avoid an execctx→db/state import cycle. Nil means caching is disabled — callers
// MUST treat nil as "behave as if disabled" rather than panic.
type BranchCacheProvider interface {
	BranchCache() *BranchCache
}

// NewBranchCache constructs a BranchCache with the given LRU tail capacity.
// Capacity <= 0 panics — pass a positive value or DefaultBranchCacheTailCapacity.
func NewBranchCache(tailCapacity int) *BranchCache {
	if tailCapacity <= 0 {
		panic(fmt.Sprintf("BranchCache: tailCapacity must be positive, got %d", tailCapacity))
	}
	tail, err := maphash.NewShardedLRU[*branchCacheEntry](tailCapacity, branchCacheTailShards)
	if err != nil {
		panic(fmt.Sprintf("BranchCache: NewShardedLRU: %s", err))
	}
	bc := &BranchCache{
		tail: tail,
	}
	// Before any unwind every entry's txN is at/below the floor, so the epoch
	// check never strands a valid entry.
	bc.coh.Init()
	return bc
}

// isRootPrefix reports whether prefix targets the pinned root slot. The
// commitment-trie compact encoding uses a 1-byte even-length flag (0x00)
// to represent the empty nibble path (root branch). Anything longer goes
// to the LRU tail.
func isRootPrefix(prefix []byte) bool {
	return len(prefix) == 1 && prefix[0] == 0x00
}

// tailHash maps prefix to its LRU-tail key, returning ok=false for a prefix that
// must never be cached (the commitment state checkpoint key).
func tailHash(prefix []byte) (uint64, bool) {
	if isCommitmentStateKey(prefix) {
		return 0, false
	}
	return maphash.Hash(prefix), true
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
	h, ok := tailHash(prefix)
	if !ok {
		return nil, false
	}
	entry, ok := c.tail.GetByHash(h)
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
	h, ok := tailHash(prefix)
	if !ok {
		return
	}
	c.tail.SetByHash(h, entry)
}

// Get retrieves branch data from the cache. Returns the canonical encoded
// bytes (with the leading 2-byte touch-map prefix) plus the on-disk file
// step the bytes came from (0 if not tracked).
//
// The returned slice is cache-owned and shared across callers — it MUST NOT
// be mutated. Callers needing to modify the bytes must copy first (the
// trie-context Branch() boundary already does, via common.Copy).
func (c *BranchCache) Get(prefix []byte) ([]byte, uint64, bool) {
	entry, ok := c.lookup(prefix)
	if !ok {
		return nil, 0, false
	}
	// Lazy unwind invalidation: an entry from a superseded epoch whose txN is at
	// or above the unwind floor reflects dead-fork state — drop it and miss so
	// the read falls through to the reverted domain and repopulates. The floor
	// is the first unwound txN (>= matches GenericCache: an entry stamped exactly
	// at the floor belongs to a rolled-back block).
	if c.coh.IsStale(entry.txN, entry.epoch) {
		c.Invalidate(prefix)
		c.staleEvicted.Add(1)
		return nil, 0, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return entry.data, entry.step, true
}

// Put stores branch data in the cache, replacing any existing entry.
// Always copies the input data so the cache owns it independently of
// caller buffer lifetime. See entry.txN for the txN tagging semantics.
func (c *BranchCache) Put(prefix []byte, data []byte, step, txN uint64) {
	if isCommitmentStateKey(prefix) {
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.store(prefix, &branchCacheEntry{
		data:  dataCopy,
		step:  step,
		txN:   txN,
		epoch: c.coh.Epoch(),
	})
}

// Invalidate removes the entry at prefix entirely from whichever tier holds
// it. Use when the caller knows the canonical store has changed and the cached
// entry should not be served at all.
func (c *BranchCache) Invalidate(prefix []byte) {
	if isRootPrefix(prefix) {
		c.root.Store(nil)
		return
	}
	h, ok := tailHash(prefix)
	if !ok {
		return
	}
	c.tail.DeleteByHash(h)
}

// Unwind invalidates entries that reflect dead-fork state. unwindToTxN is the
// unwind floor — the first rolled-back txNum (SharedDomains passes
// Min(unwindPoint+1)), not the rewind target — because the stale check is
// txN >= floor. O(1) and scan-free: bump the epoch (so entries written in the
// new, live epoch stay valid) and lower the unwind floor to unwindToTxN (so
// old-epoch entries at or above it are dropped lazily on their next Get). The
// floor only ever decreases, so a shallow unwind cannot resurrect entries a
// deeper one invalidated. See coherence.Gen.Unwind.
func (c *BranchCache) Unwind(unwindToTxN uint64) {
	c.coh.Unwind(unwindToTxN)
}

// Clear empties the cache and resets stats counters across ALL tiers
// (root slot, LRU tail). Use on Reset / fork-validation paths to
// ensure stale entries from one trie root are not served against a
// different root.
func (c *BranchCache) Clear() {
	c.root.Store(nil)
	c.tail.Purge()
	c.rootHits.Store(0)
	c.rootMisses.Store(0)
	c.tailHits.Store(0)
	c.tailMisses.Store(0)
	c.bytesServed.Store(0)
	c.staleEvicted.Store(0)
	c.coh.Init()
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
		"branch-cache root hit=%d miss=%d (%.1f%%) | tail hit=%d miss=%d (%.1f%%) entries=%d | served %.1f MiB | staleEvicted=%d",
		rh, rm, pct(rh, rm),
		th, tm, pct(th, tm), c.tail.Len(),
		float64(bb)/1024/1024, c.staleEvicted.Load(),
	)
}
