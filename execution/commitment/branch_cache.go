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
	"time"

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
//
// # Concurrency contract — caller invariants
//
// Internally, the LRU tail is thread-safe (hashicorp/golang-lru/v2) and
// the pinned root slot is an atomic.Pointer. So any combination of
// concurrent Get / GetDecoded / Put / PutIfClean / MarkDirty / Invalidate
// is mechanically safe — no panics, no torn reads. But "mechanically safe"
// is NOT the same as "logically consistent across writers." The cache is
// designed to be used under the following caller invariants:
//
//  1. Single writer per prefix at any moment. The cache does not coordinate
//     concurrent writes to the same key — last-Put-wins semantics, with no
//     guarantee that the winning value is the one the application wanted.
//
//  2. Mark-dirty-then-Put discipline for writers that may race with
//     readers. Caller calls MarkDirty BEFORE producing the new bytes, then
//     Put AFTER the canonical-store write succeeds. This is the
//     deferred-encoding-friendly alternative to inline invalidation
//     (motivated by the prototype investigation that found inline
//     invalidate is incompatible with deferred encoding — see
//     agentspecs/commitment-cache-prototype-dev-context.md).
//
//  3. Decoded cells returned by GetDecoded MUST NOT be mutated. The
//     *[16]cell pointer aliases entry-owned storage and is shared across
//     readers; in-place mutation breaks consistency for all subsequent
//     readers of that prefix.
//
// # Concurrency contract — how the existing concurrent trie satisfies it
//
// The current ConcurrentPatriciaHashed (parallel commitment calculator)
// satisfies all three caller invariants by construction:
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
// Stage F (parallel tree-reduce fold), described in
// agentspecs/trie-data-pipeline-complexity-tax.md, would change condition
// 2 above: the parent fold (incl. root) would no longer be a single
// post-Wait sequential pass. Multiple goroutines would compute parent
// branches in parallel as their children complete. This MUST not violate
// "single writer per prefix" — any future Stage F design needs an
// explicit per-prefix coordination layer (atomic counter on parent
// "children remaining"; only the last-decrementer writes the parent).
// The dirty-flag + PutIfClean primitives in this cache are sufficient
// for that coordination layer; the cache itself does NOT add per-prefix
// locking because that would be wasted work for the current architecture.
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

	// Divergence counter — incremented by RecordDivergence when a caller
	// detects that a cache-served value disagrees with the canonical
	// store. Driven by branchFromCacheOrDB's verify path (gated by
	// BRANCH_CACHE_VERIFY env). Helps localise correctness regressions
	// in cross-block-cache investigations: a non-zero count is a
	// load-bearing signal that the cache lifecycle is broken before any
	// trie root mismatch surfaces downstream.
	verifyDivergences atomic.Uint64

	// writeSeq is incremented on every Put so each entry carries a
	// monotonic ordering tag — divergence-detection uses this with the
	// origin label and timestamp to identify which write produced the
	// stale bytes.
	writeSeq atomic.Uint64
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

	// origin diagnostics — captured at Put time so divergence-detection
	// can identify which write produced the (now-disagreeing) bytes.
	// origin is a short label of the write site (e.g. "CollectUpdate",
	// "L3-fallback-read"); writeSeq is a monotonic counter per
	// BranchCache instance; writeTimeNanos is unix-nanos at write time.
	origin         string
	writeSeq       uint64
	writeTimeNanos int64
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
// caller buffer lifetime. origin is a short label of the write site
// captured for divergence-detection diagnostics.
func (c *BranchCache) Put(prefix []byte, data []byte, origin string) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	c.store(prefix, &branchCacheEntry{
		data:           dataCopy,
		origin:         origin,
		writeSeq:       c.writeSeq.Add(1),
		writeTimeNanos: time.Now().UnixNano(),
	})
}

// PutIfClean stores branch data only if no existing entry is marked dirty.
// Returns true on store, false if a dirty entry was present (indicating a
// canonical-store write is in progress and the caller's data is potentially
// stale).
//
// Same semantics as WarmupCache.PutBranchIfClean — see that doc for the
// race-it-protects-against narrative.
func (c *BranchCache) PutIfClean(prefix []byte, data []byte, origin string) bool {
	if existing, ok := c.lookup(prefix); ok && existing.dirty.Load() {
		return false
	}
	c.Put(prefix, data, origin)
	return true
}

// GetWithOrigin returns the cached bytes plus the diagnostic origin
// metadata captured when the entry was put. ok=false on miss. Used by
// the divergence-detection probe to identify which write produced the
// stale bytes. Does not bump hit/miss counters or affect LRU recency
// (uses a non-counting peek so it can be called alongside Get without
// double-counting).
func (c *BranchCache) GetWithOrigin(prefix []byte) (data []byte, origin string, writeSeq uint64, writeTimeNanos int64, ok bool) {
	var entry *branchCacheEntry
	if isRootPrefix(prefix) {
		entry = c.root.Load()
	} else {
		entry, ok = c.tail.Get(prefix)
		if !ok {
			return nil, "", 0, 0, false
		}
	}
	if entry == nil {
		return nil, "", 0, 0, false
	}
	return entry.data, entry.origin, entry.writeSeq, entry.writeTimeNanos, true
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
	c.verifyDivergences.Store(0)
}

// RecordDivergence increments the divergence counter. Called by
// branchFromCacheOrDB's verify path when a cache-served value disagrees
// with a parallel ctx.Branch read.
func (c *BranchCache) RecordDivergence() {
	c.verifyDivergences.Add(1)
}

// Fingerprint returns a deterministic hash of all current entries (root +
// tail). Two caches with the same set of (key, data) pairs produce the
// same fingerprint regardless of insertion order. Use for cross-run
// divergence localisation: emit per-block in two builds, diff the logs to
// see exactly which block their caches first differ.
//
// Mixes (key-hash, data-hash) pairs because the LRU stores by hash and
// discards the original key bytes on insert. Two entries with the same
// original key produce the same hash, so the fingerprint is still
// equality-equivalent to the (key, data) set modulo hash collision.
//
// Cheap: one FNV-1a fold over data per entry. Not cryptographic.
func (c *BranchCache) Fingerprint() uint64 {
	const fnvOffset uint64 = 14695981039346656037
	const fnvPrime uint64 = 1099511628211
	dataHash := func(data []byte) uint64 {
		h := fnvOffset
		for _, b := range data {
			h ^= uint64(b)
			h *= fnvPrime
		}
		return h
	}
	mix := func(keyHash uint64, data []byte) uint64 {
		// Combine key and data hashes into one entry hash; xor-fold across
		// entries below so the per-cache result is insertion-order
		// independent.
		return keyHash ^ (dataHash(data) * fnvPrime)
	}
	var fp uint64
	if e := c.root.Load(); e != nil && len(e.data) > 0 {
		// Pinned-root key is the constant 1-byte prefix 0x00; use a
		// distinct sentinel hash so the root contribution can't collide
		// with a tail entry hashed to zero.
		fp ^= mix(0xdeadbeefcafe0001, e.data)
	}
	c.tail.Range(func(h uint64, e *branchCacheEntry) bool {
		if len(e.data) > 0 {
			fp ^= mix(h, e.data)
		}
		return true
	})
	return fp
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
		"branch-cache root hit=%d miss=%d (%.1f%%) | tail hit=%d miss=%d (%.1f%%) | served %.1f MiB | tail entries=%d | divergences=%d",
		rh, rm, pct(rh, rm),
		th, tm, pct(th, tm),
		float64(bb)/1024/1024,
		c.tail.Len(),
		c.verifyDivergences.Load(),
	)
}

// VerifyDivergences returns the number of cache-vs-canonical divergences
// recorded since the last Clear. Non-zero indicates a cache lifecycle
// invariant has been violated (a cached entry no longer matches the
// canonical store) — read from outside to assert correctness in tests
// and benches.
func (c *BranchCache) VerifyDivergences() uint64 {
	return c.verifyDivergences.Load()
}
