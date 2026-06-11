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
	"math"
	"os"
	"sync/atomic"

	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

	// accountTrunk — the resident upper-account-trie trunk: account-trie
	// branches at nibble depths 1-4, held in fixed arrays indexed directly by
	// the compact-hex prefix (no hashing, no eviction). The global root
	// (depth 0) is the dedicated root slot above; depth 5+ goes to the LRU
	// tail. Each slot is an independent atomic.Pointer, so reads and writes
	// take no mutex and don't serialize through a shared lock the way the LRU
	// tail (and a storage trunk's deep overflow map) do.
	accountTrunk *trunk

	// Pinned tier — one storageTrunk per hot contract, keyed by the 32-byte
	// account hash. Entries never LRU-evict (sized by the residency policy)
	// but still honor the (txN, epoch) unwind model. Lookup checks this tier
	// between the account trunk and the tail. pinnedEntries counts filled
	// storage slots across all storageTrunks.
	pinned        *maphash.Map[*trunk]
	pinnedEntries atomic.Int64

	// LRU tail — bounded entries, evicts oldest when full. maphash.LRU
	// wraps hashicorp/golang-lru/v2 which is thread-safe internally.
	tail *maphash.LRU[*branchCacheEntry]

	// trunkDisabled (env BRANCH_CACHE_TRUNK_DISABLE) routes depth-1-4 account
	// branches back to the LRU tail instead of the resident account trunk — a
	// runtime A/B switch to isolate whether the resident trunk is the source
	// of a data discrepancy (the LRU self-heals stale entries via eviction;
	// the trunk does not).
	trunkDisabled bool

	// Stats — atomic counters surfaced via Stats().
	rootHits, rootMisses     atomic.Uint64
	trunkHits, trunkMisses   atomic.Uint64
	pinnedHits, pinnedMisses atomic.Uint64
	tailHits, tailMisses     atomic.Uint64
	bytesServed              atomic.Uint64
	staleEvicted             atomic.Uint64 // entries dropped lazily on read after an unwind

	// onMiss fires when lookup misses all tiers. The residency/adaptive layer
	// (added separately) registers here to attribute miss pressure per
	// contract; nil hot path is one atomic load + nil check.
	onMiss atomic.Pointer[MissCallback]

	// preloadClaimed gates the one-shot residency preload trigger.
	preloadClaimed atomic.Bool

	// last-published pinned counter snapshots — PublishMetrics emits the delta
	// since the previous publish so the Prometheus counters track per-Flush
	// activity, not snapshot absolutes.
	lastPublishedPinnedHits   atomic.Uint64
	lastPublishedPinnedMisses atomic.Uint64

	// Cache coherence across unwinds is txN/epoch based — no block awareness,
	// no diffset. An entry is valid iff it was written in the current epoch OR
	// its txN is strictly below unwindFloor (the first unwound txN), so it
	// predates every unwind seen and can't be stale. Unwind bumps the epoch and
	// lowers the floor (O(1), no scan); stale entries are dropped lazily on
	// their next Get. txNums are reused across forks, so the epoch — not the
	// txN — is what tells a superseded entry from a live one. Mirrors
	// execution/cache.GenericCache so every state cache honors one model (#21752).
	epoch       atomic.Uint32
	unwindFloor atomic.Uint64
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

// MissCallback is invoked when lookup misses ALL tiers (root, account trunk,
// pinned storage trunk, LRU tail). Called on the hot read path; the residency
// layer registers it. Implementations must be lock-free / non-blocking.
type MissCallback func(prefix []byte)

// trunk is a resident, lock-free fixed-array tier shared by both tries: the
// accountTrunk holds account-trie branches at nibble depths 1-4 (d4 allocated,
// deep nil); each per-contract storageTrunk holds storage branches at storage
// depths 0-3 with depth 4+ in deep (d4 nil, deep allocated). Slots are
// atomic.Pointer: under the single-writer-per-prefix invariant readers/writers
// take no mutex (just an atomic load/store per slot); only deep (a maphash.Map)
// locks.
type trunk struct {
	d0   atomic.Pointer[branchCacheEntry]
	d1   [16]atomic.Pointer[branchCacheEntry]
	d2   [256]atomic.Pointer[branchCacheEntry]
	d3   [4096]atomic.Pointer[branchCacheEntry]
	d4   *[65536]atomic.Pointer[branchCacheEntry]
	deep *maphash.Map[*branchCacheEntry]
}

// newAccountTrunk builds the global account trunk: dense depth-4 fixed array,
// no deep overflow (account depth 5+ uses the LRU tail).
func newAccountTrunk() *trunk {
	return &trunk{d4: &[65536]atomic.Pointer[branchCacheEntry]{}}
}

// newStorageTrunk builds a per-contract storage trunk: deep overflow for
// storage depth 4+, no depth-4 fixed array.
func newStorageTrunk() *trunk {
	return &trunk{deep: maphash.NewMap[*branchCacheEntry]()}
}

// slot returns the fixed-array slot for a nibble path of length 0-3 (and length
// 4 when the depth-4 array is present, i.e. the account trunk), or nil when the
// path is deeper — the caller then uses deep (storage) or the tail (account).
func (t *trunk) slot(path []byte) *atomic.Pointer[branchCacheEntry] {
	switch len(path) {
	case 0:
		return &t.d0
	case 1:
		return &t.d1[path[0]]
	case 2:
		return &t.d2[uint16(path[0])<<4|uint16(path[1])]
	case 3:
		return &t.d3[uint16(path[0])<<8|uint16(path[1])<<4|uint16(path[2])]
	case 4:
		if t.d4 != nil {
			return &t.d4[uint32(path[0])<<12|uint32(path[1])<<8|uint32(path[2])<<4|uint32(path[3])]
		}
	}
	return nil
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
	bc := &BranchCache{
		tail:          tail,
		accountTrunk:  newAccountTrunk(),
		pinned:        maphash.NewMap[*trunk](),
		trunkDisabled: os.Getenv("BRANCH_CACHE_TRUNK_DISABLE") != "",
	}
	// Before any unwind every entry's txN is at/below the floor, so the epoch
	// check never strands a valid entry.
	bc.unwindFloor.Store(math.MaxUint64)
	return bc
}

// trunkSlot returns the resident account-trunk slot for an account-trie branch
// at nibble depth 1-4, or nil if the prefix is the root (depth 0), a storage
// trunk, or depth >= 5 (served by the LRU tail). The compact-hex prefix maps
// directly to an array index, no hashing. Bit 4 of byte 0 is the odd-length
// flag; the low nibble of byte 0 is the first nibble when odd.
func (c *BranchCache) trunkSlot(prefix []byte) *atomic.Pointer[branchCacheEntry] {
	if c.trunkDisabled {
		return nil
	}
	switch len(prefix) {
	case 1:
		if prefix[0]&0x10 != 0 { // 1 nibble
			return &c.accountTrunk.d1[prefix[0]&0x0f]
		}
	case 2:
		if prefix[0]&0x10 == 0 { // 2 nibbles
			return &c.accountTrunk.d2[prefix[1]]
		}
		return &c.accountTrunk.d3[uint16(prefix[0]&0x0f)<<8|uint16(prefix[1])] // 3 nibbles
	case 3:
		if prefix[0]&0x10 == 0 { // 4 nibbles
			return &c.accountTrunk.d4[uint16(prefix[1])<<8|uint16(prefix[2])]
		}
		// 5 nibbles (odd, 3 bytes) -> LRU tail
	}
	return nil
}

// storageRoute decodes a storage-trunk prefix (compact-hex of 64 account
// nibbles + S storage nibbles) into its contract storageTrunk and the
// storage-nibble path. Returns ok=false for non-storage prefixes (< 64 nibbles)
// so the caller falls through to the LRU tail. When create is true the
// contract's storageTrunk is allocated on demand (PinEntry path). acct is the
// 32-byte packed account hash (the map key).
func (c *BranchCache) storageRoute(prefix []byte, create bool) (st *trunk, acct []byte, stor []byte, ok bool) {
	if len(prefix) < 33 {
		return nil, nil, nil, false
	}
	nib := nibbles.CompactToHex(prefix)
	if len(nib) < 64 {
		return nil, nil, nil, false
	}
	packed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		packed[i] = nib[2*i]<<4 | nib[2*i+1]
	}
	stor = nib[64:]
	st, found := c.pinned.Get(packed)
	if !found {
		if !create {
			return nil, packed, stor, false
		}
		st = newStorageTrunk()
		c.pinned.Set(packed, st)
	}
	return st, packed, stor, true
}

// ContractHashFromPrefix extracts the 32-byte contract (account) hash from a
// storage-trunk prefix (compact-hex of >= 64 account nibbles + storage
// nibbles). ok=false for non-storage prefixes. Used by the residency layer to
// attribute per-contract miss pressure.
func ContractHashFromPrefix(prefix []byte) (hash [32]byte, ok bool) {
	if len(prefix) < 33 {
		return hash, false
	}
	nib := nibbles.CompactToHex(prefix)
	if len(nib) < 64 {
		return hash, false
	}
	for i := 0; i < 32; i++ {
		hash[i] = nib[2*i]<<4 | nib[2*i+1]
	}
	return hash, true
}

// clearTrunk resets every resident account-trunk slot (depths 0-4) to nil in
// place (atomic per-slot stores, not a pointer swap — lock-free readers deref
// c.accountTrunk concurrently).
func (c *BranchCache) clearTrunk() {
	t := c.accountTrunk
	t.d0.Store(nil)
	for i := range t.d1 {
		t.d1[i].Store(nil)
	}
	for i := range t.d2 {
		t.d2[i].Store(nil)
	}
	for i := range t.d3 {
		t.d3[i].Store(nil)
	}
	for i := range t.d4 {
		t.d4[i].Store(nil)
	}
}

func (c *BranchCache) fireOnMiss(prefix []byte) {
	if cb := c.onMiss.Load(); cb != nil {
		(*cb)(prefix)
	}
}

// SetMissCallback installs a hook fired on every all-tier miss. Pass nil to
// clear. Used by the residency/adaptive layer (added separately).
func (c *BranchCache) SetMissCallback(cb MissCallback) {
	if cb == nil {
		c.onMiss.Store(nil)
		return
	}
	c.onMiss.Store(&cb)
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
			c.fireOnMiss(prefix)
			return nil, false
		}
		c.rootHits.Add(1)
		return entry, true
	}
	// Resident account trunk (fixed arrays, depths 1-4). Disjoint from the
	// storage trunks (depth >= 64) and tail, so a miss here is genuine.
	if slot := c.trunkSlot(prefix); slot != nil {
		if entry := slot.Load(); entry != nil {
			c.trunkHits.Add(1)
			return entry, true
		}
		c.trunkMisses.Add(1)
		c.fireOnMiss(prefix)
		return nil, false
	}
	// Pinned tier: per-contract storage trunk (fixed skeleton + deep overflow).
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		var entry *branchCacheEntry
		if slot := st.slot(stor); slot != nil {
			entry = slot.Load()
		} else {
			entry, _ = st.deep.Get(prefix)
		}
		if entry != nil {
			c.pinnedHits.Add(1)
			return entry, true
		}
	}
	c.pinnedMisses.Add(1)
	entry, ok := c.tail.Get(prefix)
	if !ok {
		c.tailMisses.Add(1)
		c.fireOnMiss(prefix)
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
	if slot := c.trunkSlot(prefix); slot != nil {
		slot.Store(entry)
		return
	}
	// Keep a prefix already pinned in a storage trunk in place across the
	// per-block invalidate+Put refresh rather than dropping it to the tail.
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		if slot := st.slot(stor); slot != nil {
			if slot.Load() != nil {
				slot.Store(entry)
				return
			}
		} else if _, exists := st.deep.Get(prefix); exists {
			st.deep.Set(prefix, entry)
			return
		}
	}
	c.tail.Set(prefix, entry)
}

// PinEntry inserts or replaces a pinned cache entry for prefix in its contract's
// storage trunk (allocated on demand). Pinned entries never LRU-evict but still
// honor the (txN, epoch) unwind model. Data is copied; safe to mutate the input
// after the call. Non-storage prefixes (< 64 nibbles) fall through to the tail.
func (c *BranchCache) PinEntry(prefix []byte, data []byte, step, txN uint64) {
	if isCommitmentStateKey(prefix) {
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	entry := &branchCacheEntry{data: dataCopy, step: step, txN: txN, epoch: c.epoch.Load()}
	st, _, stor, ok := c.storageRoute(prefix, true)
	if !ok {
		c.tail.Set(prefix, entry)
		return
	}
	if slot := st.slot(stor); slot != nil {
		if slot.Load() == nil {
			c.pinnedEntries.Add(1)
		}
		slot.Store(entry)
		return
	}
	if _, exists := st.deep.Get(prefix); !exists {
		c.pinnedEntries.Add(1)
	}
	st.deep.Set(prefix, entry)
}

// PinnedCount returns the number of currently pinned storage-trunk entries.
func (c *BranchCache) PinnedCount() int {
	return int(c.pinnedEntries.Load())
}

// PinnedStats returns the pinned-tier hit/miss/entries counters.
func (c *BranchCache) PinnedStats() (hits, misses uint64, entries int) {
	return c.pinnedHits.Load(), c.pinnedMisses.Load(), int(c.pinnedEntries.Load())
}

// TryClaimPreload returns true exactly once per cache lifetime — the residency
// preload trigger uses it so the preload runs once regardless of how many
// SharedDomains instances are constructed.
func (c *BranchCache) TryClaimPreload() bool {
	return c.preloadClaimed.CompareAndSwap(false, true)
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
	// Lazy unwind invalidation: an entry from a superseded epoch whose txN is at
	// or above the unwind floor reflects dead-fork state — drop it and miss so
	// the read falls through to the reverted domain and repopulates. The floor
	// is the first unwound txN (>= matches GenericCache: an entry stamped exactly
	// at the floor belongs to a rolled-back block).
	if entry.epoch != c.epoch.Load() && entry.txN >= c.unwindFloor.Load() {
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
		epoch: c.epoch.Load(),
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
	if slot := c.trunkSlot(prefix); slot != nil {
		slot.Store(nil)
		return
	}
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		if slot := st.slot(stor); slot != nil {
			if slot.Swap(nil) != nil {
				c.pinnedEntries.Add(-1)
			}
		} else if _, exists := st.deep.Get(prefix); exists {
			st.deep.Delete(prefix)
			c.pinnedEntries.Add(-1)
		}
	}
	c.tail.Delete(prefix)
}

// Unwind invalidates entries that reflect dead-fork state. unwindToTxN is the
// txN the chain is rewound to. O(1) and scan-free: bump the epoch (so entries
// written in the new, live epoch stay valid) and lower the unwind floor to
// unwindToTxN (so old-epoch entries at or above it are dropped lazily on their
// next Get). The floor only ever decreases, so a shallow unwind cannot
// resurrect entries a deeper one invalidated. Mirrors GenericCache.Unwind so
// branch and state caches honor one (txN, epoch) model (#21752).
func (c *BranchCache) Unwind(unwindToTxN uint64) {
	c.epoch.Add(1)
	for {
		cur := c.unwindFloor.Load()
		if unwindToTxN >= cur {
			break
		}
		if c.unwindFloor.CompareAndSwap(cur, unwindToTxN) {
			break
		}
	}
}

// Clear empties the cache and resets stats counters across ALL tiers
// (root slot, LRU tail). Use on Reset / fork-validation paths to
// ensure stale entries from one trie root are not served against a
// different root.
func (c *BranchCache) Clear() {
	c.root.Store(nil)
	c.clearTrunk()
	c.pinned = maphash.NewMap[*trunk]()
	c.pinnedEntries.Store(0)
	c.tail.Purge()
	c.rootHits.Store(0)
	c.rootMisses.Store(0)
	c.trunkHits.Store(0)
	c.trunkMisses.Store(0)
	c.pinnedHits.Store(0)
	c.pinnedMisses.Store(0)
	c.tailHits.Store(0)
	c.tailMisses.Store(0)
	c.bytesServed.Store(0)
	c.staleEvicted.Store(0)
	c.epoch.Store(0)
	c.unwindFloor.Store(math.MaxUint64)
}

// Stats returns a one-line summary of the cache tiers' hit/miss counters plus
// bytes served. Format mirrors WarmupCache.Stats() so per-Process log lines can
// compose them.
func (c *BranchCache) Stats() string {
	rh, rm := c.rootHits.Load(), c.rootMisses.Load()
	kh, km := c.trunkHits.Load(), c.trunkMisses.Load()
	ph, pm := c.pinnedHits.Load(), c.pinnedMisses.Load()
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
		"branch-cache root hit=%d miss=%d (%.1f%%) | trunk hit=%d miss=%d (%.1f%%) | pin hit=%d miss=%d (%.1f%%) entries=%d | tail hit=%d miss=%d (%.1f%%) entries=%d | served %.1f MiB | staleEvicted=%d",
		rh, rm, pct(rh, rm),
		kh, km, pct(kh, km),
		ph, pm, pct(ph, pm), int(c.pinnedEntries.Load()),
		th, tm, pct(th, tm), c.tail.Len(),
		float64(bb)/1024/1024, c.staleEvicted.Load(),
	)
}
