// Copyright 2024 The Erigon Authors
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

package cache

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// putStripeCount sizes the same-key write-serialization stripes; power of two
// so the stripe index is a mask of the key hash.
const putStripeCount = 256

// avgBytesPerEntry is the assumption used to translate a byte budget into
// the entry-count cap the sharded LRU is sized against. 256 B
// approximates account-record + key overhead and storage-slot value+key
// overhead in the same order of magnitude. Actual residency tracked in
// currentSize and reported via PrintStatsAndReset.
const avgBytesPerEntry = 256

// minShardStart keeps a shard's initial table off a handful of slots, which a
// shard count derived from a large GOMAXPROCS would otherwise produce.
const minShardStart = 16

// entry stores the full key alongside the value so callers can detect
// hash collisions (the freelru shard key is the uint64 maphash of the
// byte-string key — Go's randomized stdlib hasher, so collisions are
// rare but not impossible). size carries the byte cost of the entry so
// the OnEvict callback can update currentSize without re-running
// sizeFunc.
type entry[T any] struct {
	key   []byte
	val   T
	size  int
	txNum uint64 // commit/read txNum the cached value reflects (upper bound)
	epoch uint32 // unwind generation the entry was written in
}

// GenericCache is a sharded, LRU-evicting bounded cache for key-value
// data. Eviction mode is fixed at construction (see policy.go).
type GenericCache[T any] struct {
	// data is replaced wholesale only by Clear, with every put stripe held.
	// Growth happens inside it, one shard at a time (see shardedLRU.Add).
	data      atomic.Pointer[shardedLRU[entry[T]]]
	capacityB datasize.ByteSize
	mode      Mode

	// Each shard starts at startCap/shards slots and grows ×genericCacheGrowFactor
	// toward maxCap/shards on demand, so a cache with a small working set stays
	// far below its configured budget. startCap is floored at
	// shardCount×minShardStart and shardCount follows GOMAXPROCS, so the birth
	// footprint is single-digit MB per cache on a large host, not the low KB a
	// single-shard start would cost.
	//
	// reservedBytes is atomic: it is adjusted from the grow path, which runs
	// with a put stripe held, and Clear takes resizeMu before every stripe -- a
	// lock here would invert that.
	startCap      uint32
	maxCap        uint32
	avgEntryBytes int64 // per-domain byte estimate; maps slot count ↔ envelope bytes
	resizeMu      sync.Mutex
	reservedBytes atomic.Int64

	shardCount uint32

	currentSize atomic.Int64

	// enveloped is set only when the cache draws from the shared envelope (via
	// NewGenericCache); closed guards the single paired Release, so neither a test
	// cache built with an explicit fixed size nor a double Close mis-accounts the
	// envelope.
	enveloped bool
	closed    atomic.Bool

	// coh is the shared (epoch, floor) unwind-coherence primitive: an entry is
	// valid iff written in the current epoch OR its txNum is below the unwind
	// floor. See execution/cache/coherence.
	coh coherence.Gen

	// putStripes serialize same-key writers so PutIfAbsent's check+insert is
	// atomic w.r.t. a concurrent Put (freelru offers no conditional insert).
	putStripes [putStripeCount]sync.Mutex

	hits         atomic.Uint64
	misses       atomic.Uint64
	inserts      atomic.Uint64
	evictions    atomic.Uint64 // capacity evictions only, counted from Add's evicted return (see newShards)
	dropped      atomic.Uint64
	staleEvicted atomic.Uint64 // stale entries detected on read after an unwind; dropped unless a racing put revived them

	sizeFunc func(T) int
}

func u64identity(k uint64) uint32 { return uint32(k) }

// initialShardCount starts a lineage at ~64 entries per shard (freelru's own
// small-cache geometry), bounded by ceil.
func initialShardCount(capacity, ceil uint32) uint32 {
	return min(uint32(math.NextPowerOfTwo(uint64(capacity/64))), ceil)
}

// shardCeil caps the shard count at freelru's own choice for a sharded LRU.
func shardCeil() uint32 {
	return uint32(math.NextPowerOfTwo(uint64(runtime.GOMAXPROCS(0) * 16)))
}

const (
	// genericCacheStartCapacity is the slot count a jump-grow cache is born with.
	// A cache whose working set never exceeds it (a test fixture) stays this small
	// regardless of its configured byte budget.
	genericCacheStartCapacity = 1024
	genericCacheGrowFactor    = 4
)

// NewGenericCache creates a jump-grow cache with the specified byte capacity as
// its growth ceiling, using the generic per-entry estimate. mode selects
// ModeEvictLRU (default in this tree) or ModeNoOp (diagnostic baseline).
func NewGenericCache[T any](capacityBytes datasize.ByteSize, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	return NewGenericCacheWithAvg(capacityBytes, avgBytesPerEntry, sizeFunc, mode)
}

// NewGenericCacheWithAvg is NewGenericCache with an explicit per-domain average
// entry size, so the byte-budget ceiling and the envelope accounting reflect the
// domain's real entry cost (accounts ≈ 96 B, storage ≈ 88 B) rather than the
// generic default. It starts small and jump-grows toward the ceiling on demand,
// funding each step from the shared envelope.
func NewGenericCacheWithAvg[T any](capacityBytes datasize.ByteSize, avgBytes uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	// Absolute safety ceiling on the slot array.
	maxCap := min(max(uint32(uint64(capacityBytes)/uint64(avgBytes)), genericCacheStartCapacity), 1<<24)
	// Shard granularity follows the ceiling, not the start size: a shard grows
	// on its own, so its share of maxCap is what bounds one grow's copy. The
	// start size is raised to keep each shard off a one-slot table, which a large
	// GOMAXPROCS would otherwise produce.
	shards := initialShardCount(maxCap, shardCeil())
	start := min(max(uint32(genericCacheStartCapacity), shards*minShardStart), maxCap)
	c := &GenericCache[T]{
		capacityB:     capacityBytes,
		startCap:      start,
		maxCap:        maxCap,
		avgEntryBytes: int64(avgBytes),
		shardCount:    shards,
		enveloped:     true,
		mode:          mode,
		sizeFunc:      sizeFunc,
	}
	c.data.Store(c.newShards(start, maxCap, shards))
	// Take the initial slot array unconditionally so no cache is born unable to
	// hold anything, even when the envelope is already spoken for.
	c.reservedBytes.Store(int64(start) * c.avgEntryBytes)
	cachebudget.Global.Take(c.reservedBytes.Load())
	return c
}

// newGenericCacheEntries builds a cache against an explicit fixed entry-count
// cap (no jump-grow, no envelope). Used by tests that want to exercise eviction
// with small capacities; production constructs via NewGenericCache.
func newGenericCacheEntries[T any](capacityBytes datasize.ByteSize, capacityEntries uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	if capacityEntries == 0 {
		capacityEntries = 1
	}
	c := &GenericCache[T]{
		capacityB:     capacityBytes,
		startCap:      capacityEntries,
		maxCap:        capacityEntries,
		avgEntryBytes: avgBytesPerEntry,
		mode:          mode,
		sizeFunc:      sizeFunc,
	}
	c.shardCount = initialShardCount(capacityEntries, shardCeil())
	c.data.Store(c.newShards(capacityEntries, capacityEntries, c.shardCount))
	return c
}

// fundGrow reserves a shard's grow step from the shared envelope, refusing when
// it is exhausted so the cache stops growing and evicts within its current size.
func (c *GenericCache[T]) fundGrow(slots uint32) bool {
	if !c.enveloped {
		return true
	}
	// Close settles reservedBytes once; a step funded after that would hold
	// envelope bytes nothing releases.
	if c.closed.Load() {
		return false
	}
	delta := int64(slots) * c.avgEntryBytes
	if !cachebudget.Global.Reserve(delta) {
		return false
	}
	c.reservedBytes.Add(delta)
	return true
}

// refundGrow returns a reservation whose grow lost the race to another writer.
func (c *GenericCache[T]) refundGrow(slots uint32) {
	if !c.enveloped {
		return
	}
	delta := int64(slots) * c.avgEntryBytes
	cachebudget.Global.Release(delta)
	c.reservedBytes.Add(-delta)
}

// newShards builds the shard array with this cache's evict callback wired.
// The callback is the sole subtractor of currentSize — every removal (capacity
// eviction, Remove) accounts through it. Eviction victims are picked per shard
// (hash bits 16+), which the put stripes (bits 0-7) don't cover, so any
// subtraction computed outside the callback races a cross-stripe eviction of
// the same entry.
func (c *GenericCache[T]) newShards(startCap, maxCap, shards uint32) *shardedLRU[entry[T]] {
	return newShardedLRU[entry[T]](startCap, maxCap, shards, func(_ uint64, e entry[T]) {
		c.currentSize.Add(-int64(e.size))
	}, c.fundGrow, c.refundGrow)
}

// DomainCache wraps GenericCache[[]byte] to implement the Cache interface.
type DomainCache struct {
	*GenericCache[[]byte]
}

// NewDomainCacheMode creates a new domain cache with the given mode.
func NewDomainCacheMode(capacityBytes datasize.ByteSize, mode Mode) *DomainCache {
	return &DomainCache{
		GenericCache: NewGenericCache(capacityBytes, func(v []byte) int { return len(v) }, mode),
	}
}

// Get retrieves data for the given key, implementing the Cache interface.
func (c *DomainCache) Get(key []byte) ([]byte, bool) {
	entry, ok := c.GenericCache.Get(key)
	if !ok {
		return nil, false
	}
	return entry, true
}

// Put stores data for the given key, implementing the Cache interface.
func (c *DomainCache) Put(key []byte, value []byte, txNum uint64) {
	c.GenericCache.Put(key, value, txNum)
}

// Delete removes the data for the given key, delegating to GenericCache.
func (c *DomainCache) Delete(key []byte) {
	c.GenericCache.Delete(key)
}

// Get retrieves data for the given key.
func (c *GenericCache[T]) Get(key []byte) (T, bool) {
	v, _, ok := c.GetWithTxNum(key)
	return v, ok
}

// GetWithTxNum is Get plus the txNum the cached value reflects, so callers can
// apply a step bound (cStep = txNum/stepSize) against an in-flight unwind's
// maxStep — the same coherence the BranchCache read applies for commitment.
func (c *GenericCache[T]) GetWithTxNum(key []byte) (T, uint64, bool) {
	h := maphash.Hash(key)
	// Snapshot coherence before loading the generation. Clear publishes the
	// replacement generation before lifting the unwind floor, so an entry
	// captured from the retiring generation is always judged by coherence that
	// still carries its unwind. A replacement-generation entry judged by an old
	// snapshot can only cause a safe miss because dropStale rechecks the current
	// generation before removing it.
	coh := c.coh.Snapshot()
	lru := c.data.Load()
	e, ok := lru.Get(h)
	if !ok || !bytes.Equal(e.key, key) {
		c.misses.Add(1)
		var zero T
		return zero, 0, false
	}
	// Lazy unwind invalidation: an entry from a superseded epoch whose txNum is
	// at or above the unwind floor reflects dead-fork state — drop it and miss so
	// the read falls through to the reverted domain and repopulates. The floor is
	// the first unwound txNum (Min(UnwindPoint+1), the first txNum of the first
	// rolled-back block), so an entry stamped exactly at the floor belongs to a
	// dead block — e.g. an EIP-4788 beacon-root write in the block-begin system
	// tx — and must be dropped; >= not > (the surviving block's last txNum is
	// floor-1, so this never drops a live entry).
	if coh.IsStale(e.txNum, e.epoch) {
		c.dropStale(h, key)
		c.staleEvicted.Add(1)
		c.misses.Add(1)
		var zero T
		return zero, 0, false
	}
	c.hits.Add(1)
	return e.val, e.txNum, true
}

// Put stores data for the given key. In ModeEvictLRU the underlying
// sharded LRU evicts cold entries when its entry-count cap is reached.
// In ModeNoOp inserts that would overflow the byte budget are dropped
// (and counted via the dropped metric).
func (c *GenericCache[T]) Put(key []byte, value T, txNum uint64) {
	c.put(key, value, txNum, true)
}

// PutIfAbsent implements Cache.PutIfAbsent (live entry kept, stale one
// replaced).
func (c *GenericCache[T]) PutIfAbsent(key []byte, value T, txNum uint64) {
	c.put(key, value, txNum, false)
}

func (c *GenericCache[T]) put(key []byte, value T, txNum uint64, overwrite bool) {
	c.putStriped(key, value, txNum, overwrite)
}

// putStriped performs the write under the key's stripe, so a conditional put's
// check and insert are atomic against a same-key writer.
func (c *GenericCache[T]) putStriped(key []byte, value T, txNum uint64, overwrite bool) {
	h := maphash.Hash(key)
	valBytes := c.sizeFunc(value)
	newSize := len(key) + valBytes + 24

	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()

	// Sample the epoch under the stripe. Clear holds every stripe across the
	// generation swap and coherence reset, so the stamp cannot belong to a
	// different generation from the one where the entry lands.
	ep := c.coh.Epoch()
	lru := c.data.Load()
	existing, hasExisting := lru.Get(h)

	// Existing key — update by remove-then-add (see newShards for why a size
	// delta would be wrong). Reuse the stored key buffer to avoid an extra
	// allocation; the freshly-decoded value replaces the old one.
	if hasExisting && bytes.Equal(existing.key, key) {
		if !overwrite && !c.coh.IsStale(existing.txNum, existing.epoch) {
			return
		}
		// Reserve the new size before the removal: the byte counter must never
		// transiently under-state usage, or a concurrent ModeNoOp admission on
		// another stripe over-admits past the budget. Over-stating is safe — at
		// worst a new key is dropped, which is within "drop new keys when full".
		c.currentSize.Add(int64(newSize))
		if lru.Replace(h, entry[T]{key: existing.key, val: value, size: newSize, txNum: txNum, epoch: ep}) {
			c.evictions.Add(1)
		}
		return
	}

	if c.mode == ModeNoOp {
		// Refuse once full by either bound — freelru would otherwise evict at the
		// entry-count cap, which ModeNoOp ("drop new keys when full") must not do.
		if c.currentSize.Load()+int64(newSize) > int64(c.capacityB) || lru.Len() >= int(c.maxCap) {
			c.dropped.Add(1)
			return
		}
	}

	// In ModeEvictLRU the byte budget is enforced through the entry-count cap,
	// not a separate currentSize check: capacityEntries is derived from
	// capacityB (capacityB/avgBytesPerEntry, see NewGenericCache /
	// newDomainCacheBytes), so once the slot cap is reached the per-shard LRU
	// evicts the oldest entry inside freelru.Add and currentSize settles at
	// ≈ capacityEntries × avg ≈ capacityB. For the near-fixed-size domains this
	// caches (account ~96 B, storage ~88 B) the variance against avg is small, so
	// currentSize tracks capacityB closely rather than running away — freelru
	// exposes no evict-until-bytes-fit primitive to enforce it more tightly.
	// Eviction is per-shard, not globally-LRU — same trade-off code_cache.go /
	// balcache.go / db/state/cache.go accept.

	// hasExisting here means a 64-bit maphash collision (different key, same
	// hash): the colliding entry has to be displaced through Replace so its
	// size is accounted. The size is reserved first (see the update path above).
	c.currentSize.Add(int64(newSize))
	e := entry[T]{key: bytes.Clone(key), val: value, size: newSize, txNum: txNum, epoch: ep}
	var evicted bool
	if hasExisting {
		evicted = lru.Replace(h, e)
	} else {
		evicted = lru.Add(h, e)
	}
	if evicted {
		c.evictions.Add(1)
	}
	c.inserts.Add(1)
}

// Delete removes the data for the given key. Runs under the key's put stripe
// so the check-then-remove is atomic against same-key puts and excluded from
// the generation swap in Clear, which fences via the stripes.
func (c *GenericCache[T]) Delete(key []byte) {
	h := maphash.Hash(key)
	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()
	lru := c.data.Load()
	if existing, ok := lru.Get(h); ok && bytes.Equal(existing.key, key) {
		lru.Remove(h)
	}
}

// dropStale removes key's entry under its put stripe: the re-check keeps an
// entry a concurrent put revived, and the stripe keeps the removal out of
// generation swaps.
func (c *GenericCache[T]) dropStale(h uint64, key []byte) {
	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()
	lru := c.data.Load()
	if e, ok := lru.Get(h); ok && bytes.Equal(e.key, key) && c.coh.IsStale(e.txNum, e.epoch) {
		lru.Remove(h)
	}
}

// Clear removes all entries and restores the starting capacity. It starts an
// empty coherence generation by advancing the epoch and lifting the unwind
// floor, so subsequent puts are not constrained by an unwind that belongs to
// the retired data. The accounting reset, data swap, and coherence reset run
// with every put stripe held, so a racing writer cannot split those
// publications.
func (c *GenericCache[T]) Clear() {
	// Shrink back to the start size and return the grown budget to the envelope,
	// keeping the cache adaptive across fork-validation/reset (it regrows on
	// demand). A no-op Purge would leave the grown slot array resident.
	c.resizeMu.Lock()
	defer c.resizeMu.Unlock()
	next := c.newShards(c.startCap, c.maxCap, c.shardCount) // allocate before excluding writers
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
	// Settle the reservation only behind the fence: a writer holding a stripe
	// can be mid-grow, and outside it that grow would either attach a
	// reservation to the generation being retired or refund bytes already
	// released here.
	if c.enveloped {
		start := int64(c.startCap) * c.avgEntryBytes
		cachebudget.Global.Release(c.reservedBytes.Swap(start) - start)
	}
	c.currentSize.Store(0)
	c.data.Store(next)
	// Reset coherence only after publishing the empty generation. Paired with
	// GetWithTxNum's snapshot-before-load ordering, this ensures an entry from
	// the retiring generation is judged by pre-Reset coherence that still
	// carries the unwind.
	c.coh.Reset()
	for i := range c.putStripes {
		c.putStripes[i].Unlock()
	}
}

// Close returns this cache's envelope reservation so later caches can grow into
// the freed budget. Idempotent.
func (c *GenericCache[T]) Close() {
	if !c.enveloped || !c.closed.CompareAndSwap(false, true) {
		return
	}
	// Settle behind the same stripe fence Clear uses: a writer holding a stripe
	// can be mid-grow, and outside it that grow would either refund bytes
	// already released here or attach a reservation nothing will release.
	// closed is set first, so a writer arriving after the fence cannot fund one.
	c.resizeMu.Lock()
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
	reserved := c.reservedBytes.Swap(0)
	for i := range c.putStripes {
		c.putStripes[i].Unlock()
	}
	c.resizeMu.Unlock()
	cachebudget.Global.Release(reserved)
}

// Unwind invalidates entries that reflect dead-fork state. unwindToTxNum is the
// first rolled-back txNum (Min(UnwindPoint+1)); every entry at or above it is on
// the dead fork. O(1) and scan-free; stale entries drop lazily on their next
// read. See coherence.Gen.Unwind.
func (c *GenericCache[T]) Unwind(unwindToTxNum uint64) {
	c.coh.Unwind(unwindToTxNum)
}

// Len returns the number of entries in the cache.
func (c *GenericCache[T]) Len() int {
	return c.data.Load().Len()
}

// SizeBytes returns the current size of the cache in bytes.
func (c *GenericCache[T]) SizeBytes() int64 {
	return c.currentSize.Load()
}

// CapacityBytes returns the capacity of the cache in bytes.
func (c *GenericCache[T]) CapacityBytes() datasize.ByteSize {
	return c.capacityB
}

// PrintStatsAndReset prints cache statistics and resets counters.
func (c *GenericCache[T]) PrintStatsAndReset(name string) {
	hits := c.hits.Swap(0)
	misses := c.misses.Swap(0)
	inserts := c.inserts.Swap(0)
	evictions := c.evictions.Swap(0)
	dropped := c.dropped.Swap(0)
	staleEvicted := c.staleEvicted.Swap(0)
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	sizeBytes := c.currentSize.Load()
	usagePct := float64(sizeBytes) / float64(c.capacityB) * 100
	log.Debug(name+" cache stats",
		"mode", c.mode.String(),
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"inserts", inserts, "evictions", evictions, "dropped", dropped,
		"stale_evicted", staleEvicted, "epoch", c.coh.Epoch(),
		"entries", c.data.Load().Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", int64(c.capacityB/datasize.MB), "usage_pct", usagePct,
	)
}
