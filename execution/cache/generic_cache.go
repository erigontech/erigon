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
	"unsafe"

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

// currentSizeEntryOverhead is currentSize's per-entry bookkeeping. It lives
// inside freelru's element, already charged by the slot cost, so only the usage
// report adds it — never a reservation estimate.
const currentSizeEntryOverhead = 24

// freelruElem mirrors freelru's element, which is unexported: the stored value
// with a hashed key, five uint32 list indices and an expiry around it. The
// caches are generic over their value, so the slot cost has to follow that
// value rather than the sizes this package happens to instantiate.
//
//nolint:unused // layout mirror: only unsafe.Sizeof reads it
type freelruElem[T any] struct {
	key                                           uint64
	value                                         T
	nextBucket, prevBucket, bucketPos, next, prev uint32
	expire                                        int64
}

// elemBytesFor is what one table slot costs: the element plus its uint32 bucket
// index in the parallel table.
func elemBytesFor[T any]() int64 {
	var e freelruElem[T]
	return int64(unsafe.Sizeof(e)) + 4
}

// slotChargeBytes is a slot at the 5/4 ratio fitTableSlots pins, plus a byte to
// stay on the covering side of the rounding. It sizes the ceiling only: a
// power-of-two capacity rounds to a 2x table, so every generation below the
// ceiling charges from tableSlots instead.
func slotChargeBytes(elemBytes int64) int64 { return elemBytes*5/4 + 1 }

// tableSlots is the array length freelru allocates for a capacity. It sizes both
// the element array and the bucket array at the result.
func tableSlots(capacity uint32) uint64 {
	if capacity == 0 {
		return 0
	}
	return math.NextPowerOfTwo(uint64(capacity) + uint64(capacity)/4)
}

// freelruShardBytes covers a shard beyond its two arrays: the LRU struct and the
// slice headers. Measured at 239-359 B; charged high to stay covering. The zero
// key and value it also holds scale with the value type, so shardChargeBytes
// adds a slot on top.
const freelruShardBytes = 512

func shardChargeBytes(elemBytes int64) int64 { return freelruShardBytes + elemBytes }

func slotArrayBytes(capacity uint32, elemBytes int64) int64 {
	return int64(tableSlots(capacity)) * elemBytes
}

func shardArrayBytes(totalCap, shards uint32, elemBytes int64) int64 {
	return int64(shards) * (slotArrayBytes(perShard(totalCap, shards), elemBytes) + shardChargeBytes(elemBytes))
}

// maxCacheSlots caps the slot array whatever the byte budget says.
const maxCacheSlots = 16_000_000

// fitTableSlots rounds a capacity down to the largest one freelru does not round
// up. Only 4/5 of a power of two leaves the table ratio at 5/4; anywhere else it
// lands in [5/4, 5/2), where a fixed per-slot charge is wrong in either
// direction, swinging with GOMAXPROCS through the shard count.
func fitTableSlots(perShard uint32) uint32 {
	if perShard < minShardStart {
		return perShard
	}
	// Start at the capacity's own table: it is already the fitted one whenever
	// the capacity sits on the boundary, and stepping straight past it would
	// halve the cache.
	for table := tableSlots(perShard); table >= minShardStart; table /= 2 {
		if fitted := uint32(table / 5 * 4); fitted >= minShardStart && fitted <= perShard {
			return fitted
		}
	}
	return perShard
}

// budgetedSlots splits a byte budget into a slot ceiling and the shard count it
// is divided by, sized so each shard's table sits on the 5/4 boundary.
func budgetedSlots(capacityBytes datasize.ByteSize, payloadBytes uint32, elemBytes int64) (maxCap, shards uint32) {
	perSlot := uint64(payloadBytes) + uint64(slotChargeBytes(elemBytes))
	approx := uint32(min(uint64(capacityBytes)/perSlot, maxCacheSlots))
	shards = max(initialShardCount(approx, shardCeil()), 1)
	perShardCap := fitCeiling(fitTableSlots(approx/shards), capacityBytes, func(c uint32) int64 {
		return generationBytesFor(c*shards, shards, int64(payloadBytes), elemBytes)
	})
	return perShardCap * shards, shards
}

// fitCeiling steps a fitted ceiling down until cost reports it inside the
// budget. A ceiling derived by dividing a budget by the per-slot charge is an
// estimate: it carries neither the per-shard structs nor the gap between a
// fitted table and the 5/4 ratio it is charged at, both of which scale with the
// shard count, so the quotient alone can buy a generation larger than itself.
func fitCeiling(fitted uint32, budget datasize.ByteSize, cost func(uint32) int64) uint32 {
	for fitted > 1 && cost(fitted) > int64(budget) {
		fitted = max(fitTableSlots(fitted-1), 1)
	}
	return fitted
}

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
	payloadBytes  int64 // per-slot payload held outside the freelru element
	elemBytes     int64 // one table slot: the freelru element around entry[T], plus its bucket index
	resizeMu      sync.Mutex
	reservedBytes atomic.Int64

	shardCount uint32

	// currentSize is the logical payload held, a statistic — it is well under
	// capacityB, which reservedBytes (payload plus slot arrays) is measured
	// against instead.
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
// for the bytes an entry points at — what a slice or string header refers to,
// not T's own inline bytes, which the slot cost already charges along with the
// element and its bookkeeping (accounts ≈ 70 B, storage ≈ 64 B). Folding either
// in charges them twice and shrinks the ceiling. It starts small and jump-grows
// toward the ceiling on demand, funding each step from the shared envelope.
func NewGenericCacheWithAvg[T any](capacityBytes datasize.ByteSize, avgBytes uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	if avgBytes == 0 {
		avgBytes = avgBytesPerEntry
	}
	elemBytes := elemBytesFor[entry[T]]()
	// A shard grows on its own, so its share of maxCap bounds one grow's copy.
	budgeted, shards := budgetedSlots(capacityBytes, avgBytes, elemBytes)
	// A budget too small to buy the start capacity keeps it anyway: the byte
	// accounting a cache evicts and drops on has to stay usable at any budget.
	maxCap := max(budgeted, genericCacheStartCapacity)
	// The start size is raised to keep each shard off a one-slot table, which a
	// large GOMAXPROCS would otherwise produce.
	start := min(max(uint32(genericCacheStartCapacity), shards*minShardStart), maxCap)
	c := &GenericCache[T]{
		capacityB:    capacityBytes,
		startCap:     start,
		maxCap:       maxCap,
		payloadBytes: int64(avgBytes),
		elemBytes:    elemBytes,
		shardCount:   shards,
		enveloped:    true,
		mode:         mode,
		sizeFunc:     sizeFunc,
	}
	c.data.Store(c.newShards(start, maxCap, shards))
	// Take the initial slot array unconditionally so no cache is born unable to
	// hold anything, even when the envelope is already spoken for.
	c.reservedBytes.Store(c.generationBytes(start))
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
		capacityB:    capacityBytes,
		startCap:     capacityEntries,
		maxCap:       capacityEntries,
		payloadBytes: avgBytesPerEntry,
		elemBytes:    elemBytesFor[entry[T]](),
		mode:         mode,
		sizeFunc:     sizeFunc,
	}
	c.shardCount = initialShardCount(capacityEntries, shardCeil())
	c.data.Store(c.newShards(capacityEntries, capacityEntries, c.shardCount))
	return c
}

// fundGrow reserves a shard's grow step from the shared envelope, refusing when
// it is exhausted so the cache stops growing and evicts within its current size.
func (c *GenericCache[T]) fundGrow(oldCap, newCap uint32) bool {
	if !c.enveloped {
		return true
	}
	// Close settles reservedBytes once; a step funded after that would hold
	// envelope bytes nothing releases.
	if c.closed.Load() {
		return false
	}
	delta := c.growBytes(oldCap, newCap)
	if !cachebudget.Global.Reserve(delta) {
		return false
	}
	c.reservedBytes.Add(delta)
	return true
}

// refundGrow returns a reservation whose grow lost the race to another writer.
func (c *GenericCache[T]) refundGrow(oldCap, newCap uint32) {
	if !c.enveloped {
		return
	}
	delta := c.growBytes(oldCap, newCap)
	cachebudget.Global.Release(delta)
	c.reservedBytes.Add(-delta)
}

func (c *GenericCache[T]) growBytes(oldCap, newCap uint32) int64 {
	return int64(newCap-oldCap)*c.payloadBytes + slotArrayBytes(newCap, c.elemBytes) - slotArrayBytes(oldCap, c.elemBytes)
}

func (c *GenericCache[T]) generationBytes(totalCap uint32) int64 {
	return generationBytesFor(totalCap, max(c.shardCount, 1), c.payloadBytes, c.elemBytes)
}

// generationBytesFor is what a generation of totalCap slots costs: the payload
// estimate plus the exact shard arrays.
func generationBytesFor(totalCap, shards uint32, payloadBytes, elemBytes int64) int64 {
	return int64(totalCap)*payloadBytes + shardArrayBytes(totalCap, shards, elemBytes)
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
	newSize := len(key) + valBytes + currentSizeEntryOverhead

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

	// ModeEvictLRU has no currentSize check: the budget is enforced by maxCap,
	// which budgetedSlots derives from capacityB counting both the payload and
	// the slot arrays. freelru exposes no evict-until-bytes-fit primitive, and
	// eviction is per-shard rather than globally-LRU — the same trade-off
	// code_cache.go / balcache.go / db/state/cache.go accept.

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
	// Close settles reservedBytes once; a rebuild charged after that would hold
	// envelope bytes nothing releases.
	if c.enveloped && !c.closed.Load() {
		start := c.generationBytes(c.startCap)
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
	log.Debug(name+" cache stats",
		"mode", c.mode.String(),
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"inserts", inserts, "evictions", evictions, "dropped", dropped,
		"stale_evicted", staleEvicted, "epoch", c.coh.Epoch(),
		"entries", c.data.Load().Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", int64(c.capacityB/datasize.MB), "slots_pct", c.slotsPct(),
		"reserved_mb", c.reservedBytes.Load()/int64(datasize.MB),
	)
}

// slotsPct is how full the cache is against the slots it has allocated, not
// against maxCap: a shard refused a grow step evicts at its current size.
// currentSize is not the numerator for a payloadBytes denominator: that
// estimate counts only what an entry points at, so for a value held inline in T
// the two disagree by the whole value. Bytes are reported as size_mb.
func (c *GenericCache[T]) slotsPct() float64 {
	d := c.data.Load()
	// Length first: capacity only rises within a generation, so a capacity read
	// after it can only make the ratio smaller, never report over 100%.
	held := d.Len()
	allocated := d.Cap()
	if allocated == 0 {
		return 0
	}
	return float64(held) / float64(allocated) * 100
}
