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

// BranchCache stores commitment-trie branch data:
//
//   - Bounded LRU tail with configurable capacity (eviction is well-defined,
//     suitable for long-lived caching across many Process calls without
//     unbounded memory growth).
//   - Single pinned slot for the root branch (always hottest, always present
//     once populated, never subject to LRU eviction). Compact prefix of
//     length 0 (or single-byte "no-key" form) targets this slot.
//
// Lifetime: aggregator-scope (one instance per Domain). SharedDomains
// pulls the instance via BranchCacheProvider on the AggregatorRoTx;
// commitment-context plumbs it through to the trie via
// InitializeTrieAndUpdates. The previous WarmupCache type (per-Process,
// duplicating account/storage/branch caching above this layer) was
// deleted in the WarmupCache consolidation; BranchCache is now the
// single branch cache.
//
// # Responsibility split (architectural)
//
// The cache is a passive store. Reads and writes are driven by the
// trie walker / encoder; the cache itself never reaches into the
// underlying state.
//
//   - BranchCache: passive store of branch bytes.
//     Doesn't fetch anything.
//   - Branch warmer (warmuper.go): narrow scope — pre-fetches
//     *branches* along touched-key paths via SD.GetLatest. No
//     account/storage prefetch — that conflated branch warm-up with
//     leaf-data fetch. If a fold needs leaf data the trie walker
//     fetches it directly (or it's already in Updates / memoized as
//     stateHash).
//   - Trie walker, block-processing path: receives Updates from the
//     executor, folds them. Memoized stateHashes serve siblings; new
//     values come from Updates. Doesn't reach into leaf data via
//     prefetch.
//   - Trie walker, witness / proof generation path: walks the trie
//     structure and *needs* to fetch state to materialize the proof.
//     This is the walker's responsibility — it drives its own reads
//     against SD. If that path turns out to be cold-bound on real
//     workloads it may indicate a need for separate account / storage
//     caches (the `add_execution_context_with_caches` work has a
//     reference design for these). Treat that as a separate concern
//     from this BranchCache — different scope, different lifetime,
//     different invalidation. Do not regrow the branch warmer's
//     scope to cover it.
//
// The disk_sto / disk_acc counters on the [commitment][cache-fp] log
// line surface any fall-through where the trie compute reaches the
// underlying ctx.Account / ctx.Storage paths. On block-processing
// workloads they should remain zero; non-zero values signal a
// memoization gap or a missing walker-side prefetch.
//
// # Concurrency contract — caller invariants
//
// Internally, the LRU tail is thread-safe (hashicorp/golang-lru/v2) and
// the pinned root slot is an atomic.Pointer. So any combination of
// concurrent Get / Put / Invalidate is mechanically safe — no panics, no
// torn reads. But "mechanically safe" is NOT the same as "logically
// consistent across writers." The cache is designed to be used under one
// caller invariant:
//
//   - Single writer per prefix at any moment. The cache does not coordinate
//     concurrent writes to the same key — last-Put-wins semantics, with no
//     guarantee that the winning value is the one the application wanted.
//
// # Concurrency contract — how the existing concurrent trie satisfies it
//
// The current ConcurrentPatriciaHashed (parallel commitment calculator)
// satisfies that invariant by construction:
//
//   - Mounts partition the prefix space by FIRST NIBBLE. Mount N's
//     encoder only writes branches whose key starts with [0x0N ...].
//     Different mounts therefore never write to the same prefix.
//     (See hex_concurrent_patricia_hashed.go: NewConcurrentPatriciaHashed
//     creates 16 mounts via SpawnSubTrie; each mount has its own HPH,
//     own BranchEncoder, own PatriciaContext / roTx.)
//
//   - Root branch (prefix [0x00]) is written by the single root fold
//     that runs SEQUENTIALLY after errgroup.Wait() in ParallelHashSort.
//     One writer for the pinned root slot.
//
//   - Mount→root grid roll-up is mutex-protected via
//     ConcurrentPatriciaHashed.rootMu — but that updates IN-MEMORY grid
//     cells, not the cache. The cache only sees the eventual root
//     branch when the post-Wait root fold encodes it.
//
// # Concurrency contract — what future parallel fold work must preserve
//
// A future parallel tree-reduce fold would change the picture: the parent
// fold (incl. root) would no longer be a single post-Wait sequential pass.
// Multiple goroutines would compute parent branches in parallel as their
// children complete. This MUST not violate "single writer per prefix" —
// any future Stage F design needs an explicit per-prefix coordination layer
// (atomic counter on parent "children remaining"; only the last-decrementer
// writes the parent). That coordination belongs at the orchestrator layer;
// the cache itself does NOT add per-prefix locking because that would be
// wasted work for the current architecture.
//
// If you are implementing parallel fold (or any other architecture that
// breaks the "single writer per prefix" invariant), do NOT relax the
// invariant by adding internal locking to the cache. Add the
// coordination at the orchestrator layer where the partitioning logic
// lives. The cache stays simple; the orchestrator owns the discipline.
//
// Likewise if you change the prefix partitioning (e.g. by-second-nibble
// mounts, depth-based partitioning, anything other than first-nibble),
// re-validate that distinct workers continue to write disjoint prefix
// spaces. Re-read the partitioning code in
// hex_concurrent_patricia_hashed.go and confirm.
type BranchCache struct {
	// Root tier — single slot for the root branch (always hottest, always
	// present). Atomic-pointer access so no lock is needed for the hot
	// read path.
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
	// prefix). Always populated by Put.
	data []byte

	// step is the on-disk file step the cached bytes came from. Returned
	// by Get so callers (e.g. CheckDataAvailable) can validate against
	// the latest visible step. 0 means "step not tracked" — fine for
	// in-memory tests but real callers should always pass the step
	// returned by aggTx.MeteredGetLatest / tx.GetLatest.
	step uint64

	// txN is the high-water txN this entry is valid through. UnwindTo
	// evicts every entry whose txN > watermark. 0 means "not tracked"
	// (entry survives any watermark).
	txN uint64
}

// DefaultBranchCacheTailCapacity is the LRU tail size used when no
// explicit capacity is given. ~50k entries × ~500 bytes = ~25 MB
// at typical mainnet branch sizes.
const DefaultBranchCacheTailCapacity = 50000

// BranchCacheProvider exposes the long-lived BranchCache attached to the
// commitment domain. Implemented by *db/state.AggregatorRoTx (via duck
// typing) so callers in the SharedDomains construction path can fetch the
// cache without forcing db/state/execctx to import db/state — that import
// would create a cycle since db/state imports execctx (squeeze.go,
// trie_reader_integration_test.go, …).
//
// Returning nil is permitted; callers MUST treat nil as "no shared cache,
// behave as if disabled" rather than panic.
type BranchCacheProvider interface {
	BranchCache() *BranchCache
}

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
	return &BranchCache{
		tail: tail,
	}
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
// bytes (with the leading 2-byte touch-map prefix) plus the on-disk file
// step the bytes came from (0 if not tracked).
func (c *BranchCache) Get(prefix []byte) ([]byte, uint64, bool) {
	if isCommitmentStateKey(prefix) {
		return nil, 0, false
	}
	entry, ok := c.lookup(prefix)
	if !ok {
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
		data: dataCopy,
		step: step,
		txN:  txN,
	})
}

// Invalidate removes the entry at prefix entirely from whichever tier
// holds it. Use when the caller knows the canonical store has changed
// and the cached entry should not be served at all (vs MarkDirty which
// keeps the entry but blocks PutIfClean overwrites).
func (c *BranchCache) Invalidate(prefix []byte) {
	if isRootPrefix(prefix) {
		c.root.Store(nil)
		return
	}
	c.tail.Delete(prefix)
}

// UnwindTo evicts every cache entry whose txN > maxValidTxN across
// the root slot and LRU tail. Returns the number of entries evicted.
// Safe to call alongside concurrent reads; a Put racing with the scan
// may insert an entry the scan already passed, but the next UnwindTo
// will catch it.
func (c *BranchCache) UnwindTo(maxValidTxN uint64) (evicted int) {
	if entry := c.root.Load(); entry != nil && entry.txN > maxValidTxN {
		c.root.Store(nil)
		evicted++
	}
	c.tail.Range(func(hash uint64, entry *branchCacheEntry) bool {
		if entry != nil && entry.txN > maxValidTxN {
			c.tail.DeleteByHash(hash)
			evicted++
		}
		return true
	})
	return evicted
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
		"branch-cache root hit=%d miss=%d (%.1f%%) | tail hit=%d miss=%d (%.1f%%) entries=%d | served %.1f MiB",
		rh, rm, pct(rh, rm),
		th, tm, pct(th, tm), c.tail.Len(),
		float64(bb)/1024/1024,
	)
}
