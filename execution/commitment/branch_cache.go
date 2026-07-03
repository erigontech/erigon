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
	"os"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/cache/coherence"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// u64ident is the freelru hash callback for uint64 keys already well-distributed
// by maphash — the low 32 bits suffice for shard routing.
func u64ident(k uint64) uint32 { return uint32(k) }

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

// BranchCache stores commitment-trie branch data: a bounded LRU tail plus a
// single never-evicted slot for the root branch (a length-0 / no-key prefix).
// Aggregator-scope (one instance per Domain), pulled via BranchCacheProvider and
// plumbed to the trie through InitializeTrieAndUpdates. It is a passive store —
// the trie walker/encoder drive all reads and writes; the cache never fetches
// state itself.
//
// Concurrency: the LRU tail and the atomic-pointer root slot make any mix of
// concurrent Get/Put/Invalidate mechanically safe, but the cache does not
// coordinate writers — callers must ensure a single writer per prefix
// (last-Put-wins otherwise); add any such coordination at the orchestrator, not
// by locking the cache.
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
	// Allocated on the first pin (via pinnedForWrite): a cache that never pins a
	// contract — the common case for short-lived caches over shallow tries —
	// never pays for the (min 32-bucket) concurrent map.
	pinned        atomic.Pointer[maphash.Map[*trunk]]
	pinnedMu      sync.Mutex
	pinnedEntries atomic.Int64

	// LRU tail — the memory-adaptive spill tier for prefixes past the resident
	// trunk. Allocated on the first tail insert and jump-grown toward tailCap only
	// as demand and the shared memory budget allow (see tailLRU), so a cache over
	// a shallow trie or in a memory-constrained process stays small.
	tail    atomic.Pointer[tailLRU]
	tailCap uint32
	tailMu  sync.Mutex

	// maxDepth is the resident trunk depth for this cache (both the account trunk
	// and every pinned storage trunk), chosen from the active-instance count at
	// construction. closed guards the single paired active-count decrement.
	maxDepth uint8
	closed   atomic.Bool

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

// MissCallback is invoked when lookup misses ALL tiers (root, account trunk,
// pinned storage trunk, LRU tail). Called on the hot read path; the residency
// layer registers it. Implementations must be lock-free / non-blocking.
type MissCallback func(prefix []byte)

// trunk is a resident, lock-free fixed-array tier shared by both tries: the
// accountTrunk holds account-trie branches at nibble depths 1-4 (d4 lazily
// allocated, deep nil); each per-contract storageTrunk holds storage branches at storage
// depths 0-3 with depth 4+ in deep (d4 nil, deep allocated). Slots are
// atomic.Pointer: under the single-writer-per-prefix invariant readers/writers
// take no mutex (just an atomic load/store per slot); only deep (a maphash.Map)
// locks.
// The depth tiers d2/d3/d4 are allocated lazily on the first write at that
// depth. A process-wide production cache fills every tier once; the many
// short-lived caches a test suite spins up over shallow tries never reach the
// deeper tiers, so eager allocation of the dense arrays (d3 32KB, d4 512KB) is
// pure churn there. d0/d1 are tiny and always reached, so they stay inline.
type trunk struct {
	d0   atomic.Pointer[branchCacheEntry]
	d1   [16]atomic.Pointer[branchCacheEntry]
	d2   atomic.Pointer[[256]atomic.Pointer[branchCacheEntry]]
	d3   atomic.Pointer[[4096]atomic.Pointer[branchCacheEntry]]
	d4   atomic.Pointer[[65536]atomic.Pointer[branchCacheEntry]]
	deep *maphash.Map[*branchCacheEntry]

	// maxDepth caps which dense tiers this trunk will allocate: branches deeper
	// than it route to deep (storage) or the LRU tail (account) instead. Set from
	// the active-BranchCache count so a process with a few caches (production)
	// keeps the full depth-4 residency while one with many (the test suite) keeps
	// each trunk shallow. d0/d1 are always resident (tiny, always reached).
	maxDepth uint8
}

func (t *trunk) d2For(forWrite bool) *[256]atomic.Pointer[branchCacheEntry] {
	if p := t.d2.Load(); p != nil {
		return p
	}
	if !forWrite {
		return nil
	}
	p := &[256]atomic.Pointer[branchCacheEntry]{}
	if !t.d2.CompareAndSwap(nil, p) {
		p = t.d2.Load()
	}
	return p
}

func (t *trunk) d3For(forWrite bool) *[4096]atomic.Pointer[branchCacheEntry] {
	if p := t.d3.Load(); p != nil {
		return p
	}
	if !forWrite || t.maxDepth < 3 {
		return nil
	}
	p := &[4096]atomic.Pointer[branchCacheEntry]{}
	if !t.d3.CompareAndSwap(nil, p) {
		p = t.d3.Load()
	}
	return p
}

func (t *trunk) d4For(forWrite bool) *[65536]atomic.Pointer[branchCacheEntry] {
	if p := t.d4.Load(); p != nil {
		return p
	}
	if !forWrite || t.maxDepth < 4 {
		return nil
	}
	p := &[65536]atomic.Pointer[branchCacheEntry]{}
	if !t.d4.CompareAndSwap(nil, p) {
		p = t.d4.Load()
	}
	return p
}

// newAccountTrunk builds the global account trunk: dense fixed arrays up to
// maxDepth, no deep overflow (account depth past the resident tiers uses the
// LRU tail).
func newAccountTrunk(maxDepth uint8) *trunk {
	return &trunk{maxDepth: maxDepth}
}

// newStorageTrunk builds a per-contract storage trunk: deep overflow for
// storage depth past the resident tiers, no depth-4 fixed array.
func newStorageTrunk(maxDepth uint8) *trunk {
	return &trunk{maxDepth: maxDepth, deep: maphash.NewMap[*branchCacheEntry]()}
}

// Adaptive trunk depth: a process with a handful of BranchCaches (production)
// keeps full depth-4 residency; one that spins up many (the test suite) keeps
// each trunk shallow so their fixed-array tiers don't sum past the memory
// envelope. Depth is chosen once per cache from the live instance count.
const (
	trunkDepthFull              = 4
	trunkDepthShallow           = 2
	trunkInstanceDepthThreshold = 10
)

var activeBranchCaches atomic.Int64

func adaptiveTrunkDepth(active int64) uint8 {
	if active <= trunkInstanceDepthThreshold {
		return trunkDepthFull
	}
	return trunkDepthShallow
}

// slot returns the fixed-array slot for a nibble path of length 0-3 (and length
// 4 when the depth-4 array is present, i.e. the account trunk), or nil when the
// path is deeper — the caller then uses deep (storage) or the tail (account).
func (t *trunk) slot(path []byte, forWrite bool) *atomic.Pointer[branchCacheEntry] {
	switch len(path) {
	case 0:
		return &t.d0
	case 1:
		return &t.d1[path[0]]
	case 2:
		if d2 := t.d2For(forWrite); d2 != nil {
			return &d2[uint16(path[0])<<4|uint16(path[1])]
		}
	case 3:
		if d3 := t.d3For(forWrite); d3 != nil {
			return &d3[uint16(path[0])<<8|uint16(path[1])<<4|uint16(path[2])]
		}
	case 4:
		if d4 := t.d4For(forWrite); d4 != nil {
			return &d4[uint32(path[0])<<12|uint32(path[1])<<8|uint32(path[2])<<4|uint32(path[3])]
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

// branchCacheTailShards splits the LRU tail into independently-locked shards so
// concurrent commitment mounts / warmup workers don't serialize on one mutex.
const branchCacheTailShards = 256

// NewBranchCache constructs a BranchCache with the given LRU tail capacity.
// Capacity <= 0 panics — pass a positive value or DefaultBranchCacheTailCapacity.
func NewBranchCache(tailCapacity int) *BranchCache {
	if tailCapacity <= 0 {
		panic(fmt.Sprintf("BranchCache: tailCapacity must be positive, got %d", tailCapacity))
	}
	maxDepth := adaptiveTrunkDepth(activeBranchCaches.Add(1))
	bc := &BranchCache{
		tailCap:       uint32(tailCapacity),
		maxDepth:      maxDepth,
		accountTrunk:  newAccountTrunk(maxDepth),
		trunkDisabled: os.Getenv("BRANCH_CACHE_TRUNK_DISABLE") != "",
	}
	// Before any unwind every entry's txN is at/below the floor, so the epoch
	// check never strands a valid entry.
	bc.coh.Init()
	log.Info("[branch-cache] init", "trunkEnabled", !bc.trunkDisabled, "tailCap", tailCapacity, "trunkDepth", maxDepth)
	return bc
}

// Close drops this cache from the active-instance count so later BranchCaches
// size their trunk depth against real concurrency. Idempotent.
func (c *BranchCache) Close() {
	if c.closed.CompareAndSwap(false, true) {
		activeBranchCaches.Add(-1)
	}
}

// tailForWrite returns the LRU tail, allocating it on first use so a cache whose
// tries never spill past the resident trunk pays nothing for it.
func (c *BranchCache) tailForWrite() *tailLRU {
	if t := c.tail.Load(); t != nil {
		return t
	}
	c.tailMu.Lock()
	defer c.tailMu.Unlock()
	if t := c.tail.Load(); t != nil {
		return t
	}
	t := newTailLRU(c.tailCap)
	c.tail.Store(t)
	return t
}

// tailLen reports the number of resident tail entries, or 0 if the tail has not
// been allocated yet.
func (c *BranchCache) tailLen() int {
	if t := c.tail.Load(); t != nil {
		return t.Len()
	}
	return 0
}

// trunkSlot returns the resident account-trunk slot for an account-trie branch
// at nibble depth 1-4, or nil if the prefix is the root (depth 0), a storage
// trunk, or depth >= 5 (served by the LRU tail). The compact-hex prefix maps
// directly to an array index, no hashing. Bit 4 of byte 0 is the odd-length
// flag; the low nibble of byte 0 is the first nibble when odd.
func (c *BranchCache) trunkSlot(prefix []byte, forWrite bool) *atomic.Pointer[branchCacheEntry] {
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
			if d2 := c.accountTrunk.d2For(forWrite); d2 != nil {
				return &d2[prefix[1]]
			}
			return nil
		}
		if d3 := c.accountTrunk.d3For(forWrite); d3 != nil { // 3 nibbles
			return &d3[uint16(prefix[0]&0x0f)<<8|uint16(prefix[1])]
		}
		return nil
	case 3:
		if prefix[0]&0x10 == 0 { // 4 nibbles
			if d4 := c.accountTrunk.d4For(forWrite); d4 != nil {
				return &d4[uint16(prefix[1])<<8|uint16(prefix[2])]
			}
			return nil
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
	if p := c.pinned.Load(); p != nil {
		if st, found := p.Get(packed); found {
			return st, packed, stor, true
		}
	}
	if !create {
		return nil, packed, stor, false
	}
	st = newStorageTrunk(c.maxDepth)
	c.pinnedForWrite().Set(packed, st)
	return st, packed, stor, true
}

// pinnedForWrite returns the pinned-contract map, allocating it on first pin.
func (c *BranchCache) pinnedForWrite() *maphash.Map[*trunk] {
	if p := c.pinned.Load(); p != nil {
		return p
	}
	c.pinnedMu.Lock()
	defer c.pinnedMu.Unlock()
	if p := c.pinned.Load(); p != nil {
		return p
	}
	p := maphash.NewMap[*trunk]()
	c.pinned.Store(p)
	return p
}

// ContractHashFromPrefix extracts the 32-byte contract (account) hash — keccak
// of the address — from a storage-trunk prefix (compact-hex of >= 64 account
// nibbles + storage nibbles). ok=false for non-storage prefixes. On the
// per-miss hot path, so it decodes the leading 64 nibbles straight out of the
// compact bytes rather than materializing the full hex expansion.
func ContractHashFromPrefix(prefix []byte) (hash [32]byte, ok bool) {
	if len(prefix) < 33 {
		return hash, false
	}
	if prefix[0]&0x10 != 0 { // odd: first nibble is the low nibble of byte 0
		for i := 0; i < 32; i++ {
			hash[i] = prefix[i]&0x0f<<4 | prefix[i+1]>>4
		}
		return hash, true
	}
	// even: the account-hash bytes are stored whole starting at byte 1
	copy(hash[:], prefix[1:33])
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
	if d2 := t.d2.Load(); d2 != nil {
		for i := range d2 {
			d2[i].Store(nil)
		}
	}
	if d3 := t.d3.Load(); d3 != nil {
		for i := range d3 {
			d3[i].Store(nil)
		}
	}
	if d4 := t.d4.Load(); d4 != nil {
		for i := range d4 {
			d4[i].Store(nil)
		}
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
	if slot := c.trunkSlot(prefix, false); slot != nil {
		if entry := slot.Load(); entry != nil {
			c.trunkHits.Add(1)
			return entry, true
		}
		c.trunkMisses.Add(1)
		c.fireOnMiss(prefix)
		return nil, false
	}
	// Pinned tier: per-contract storage trunk (fixed skeleton + deep overflow).
	// Only a lookup that actually routes to a pinned trunk counts toward the
	// pinned hit/miss stats; account-trie and tail-only prefixes are excluded.
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		var entry *branchCacheEntry
		if slot := st.slot(stor, false); slot != nil {
			entry = slot.Load()
		} else {
			entry, _ = st.deep.Get(prefix)
		}
		if entry != nil {
			c.pinnedHits.Add(1)
			return entry, true
		}
		c.pinnedMisses.Add(1)
	}
	tail := c.tail.Load()
	if tail == nil {
		c.tailMisses.Add(1)
		c.fireOnMiss(prefix)
		return nil, false
	}
	entry, ok := tail.Get(maphash.Hash(prefix))
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
	if slot := c.trunkSlot(prefix, true); slot != nil {
		slot.Store(entry)
		return
	}
	// Keep a prefix already pinned in a storage trunk in place across the
	// per-block invalidate+Put refresh rather than dropping it to the tail.
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		if slot := st.slot(stor, false); slot != nil {
			if slot.Load() != nil {
				slot.Store(entry)
				return
			}
		} else if _, exists := st.deep.Get(prefix); exists {
			st.deep.Set(prefix, entry)
			return
		}
	}
	c.tailForWrite().Add(maphash.Hash(prefix), entry)
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
	entry := &branchCacheEntry{data: dataCopy, step: step, txN: txN, epoch: c.coh.Epoch()}
	st, _, stor, ok := c.storageRoute(prefix, true)
	if !ok {
		c.tailForWrite().Add(maphash.Hash(prefix), entry)
		return
	}
	if slot := st.slot(stor, true); slot != nil {
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

// Invalidate removes the entry at prefix entirely from whichever tier
// holds it. Use when the caller knows the canonical store has changed
// and the cached entry should not be served at all (vs MarkDirty which
// keeps the entry but blocks PutIfClean overwrites).
func (c *BranchCache) Invalidate(prefix []byte) {
	if isRootPrefix(prefix) {
		c.root.Store(nil)
		return
	}
	if slot := c.trunkSlot(prefix, false); slot != nil {
		slot.Store(nil)
		return
	}
	if st, _, stor, ok := c.storageRoute(prefix, false); ok {
		if slot := st.slot(stor, false); slot != nil {
			if slot.Swap(nil) != nil {
				c.pinnedEntries.Add(-1)
			}
		} else if _, exists := st.deep.Get(prefix); exists {
			st.deep.Delete(prefix)
			c.pinnedEntries.Add(-1)
		}
	}
	if tail := c.tail.Load(); tail != nil {
		tail.Remove(maphash.Hash(prefix))
	}
}

// Unwind invalidates entries that reflect dead-fork state. unwindToTxN is the
// txN the chain is rewound to. O(1) and scan-free: bump the epoch (so entries
// written in the new, live epoch stay valid) and lower the unwind floor to
// unwindToTxN (so old-epoch entries at or above it are dropped lazily on their
// next Get). The floor only ever decreases, so a shallow unwind cannot
// resurrect entries a deeper one invalidated. Mirrors GenericCache.Unwind so
// branch and state caches honor one (txN, epoch) model.
func (c *BranchCache) Unwind(unwindToTxN uint64) {
	c.coh.Unwind(unwindToTxN)
}

// Clear empties the cache and resets stats counters across ALL tiers
// (root slot, LRU tail). Use on Reset / fork-validation paths to
// ensure stale entries from one trie root are not served against a
// different root.
func (c *BranchCache) Clear() {
	c.root.Store(nil)
	c.clearTrunk()
	c.pinned.Store(nil)
	c.pinnedEntries.Store(0)
	if tail := c.tail.Load(); tail != nil {
		tail.reset()
	}
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
	c.coh.Init()
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
		th, tm, pct(th, tm), c.tailLen(),
		float64(bb)/1024/1024, c.staleEvicted.Load(),
	)
}
