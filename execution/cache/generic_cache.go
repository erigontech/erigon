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
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// avgBytesPerEntry is the default estimate of the bytes an entry points at, used
// to translate a byte budget into a slot ceiling. Actual residency is tracked in
// currentSize and reported via PrintStatsAndReset.
const avgBytesPerEntry = 256

// currentSizeEntryOverhead is currentSize's per-entry bookkeeping. The slot
// cost already charges the entry, so only the usage report adds it — never a
// reservation estimate.
const currentSizeEntryOverhead = 24

// maxCacheSlots caps the slot count whatever the byte budget says.
const maxCacheSlots = 16_000_000

// sizeClassSlack covers the allocator rounding an entry up to its size class.
const sizeClassSlack = 16

// entry is immutable once published. key holds the full key, so reads detect
// 64-bit hash collisions. size carries the entry's currentSize charge.
type entry[T any] struct {
	key   string
	val   T
	txNum uint64 // commit/read txNum the cached value reflects (upper bound)
	size  uint32
	epoch uint32 // unwind generation the entry was written in
}

// GenericCache is a sharded, set-associative bounded cache for key-value data.
// Reads take no lock; writers serialize on the key's shard mutex. Eviction mode
// is fixed at construction (see policy.go).
type GenericCache[T any] struct {
	shards     []shard[T]
	shardShift uint8
	capacityB  datasize.ByteSize
	mode       Mode

	// Each shard starts at startBuckets and grows ×genericCacheGrowFactor toward
	// maxBuckets on demand, funding each step from the shared envelope, so a cache
	// with a small working set stays far below its configured budget.
	startBuckets uint32
	maxBuckets   uint32
	startCap     uint32 // slots across all shards at birth
	maxCap       uint32 // slots across all shards at the ceiling
	payloadBytes int64  // per-slot payload held outside the entry
	entryBytes   int64  // one entry allocation without its payload

	// reservedBytes is atomic: grows add to it under one shard mutex while
	// Clear and Close settle it under all of them.
	reservedBytes atomic.Int64

	// currentSize is the logical payload held, a statistic — it is well under
	// capacityB, which reservedBytes (payload plus slots) is measured against.
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

	hits         atomic.Uint64
	misses       atomic.Uint64
	inserts      atomic.Uint64
	evictions    atomic.Uint64 // capacity evictions only
	dropped      atomic.Uint64
	staleEvicted atomic.Uint64 // stale entries detected on read after an unwind; dropped unless a racing put revived them

	sizeFunc func(T) int
}

// initialShardCount gives a cache of this capacity ~64 slots per shard,
// bounded by ceil.
func initialShardCount(capacity, ceil uint32) uint32 {
	return min(uint32(math.NextPowerOfTwo(uint64(capacity/64))), ceil)
}

// shardCeil caps the shard count so writers on different cores rarely share a
// shard mutex.
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
// for the bytes an entry points at — its key and what a slice or string header
// refers to (accounts ≈ 70 B, storage ≈ 64 B), not T's own inline bytes, which
// the entry cost already charges. avgBytes of 0 is meaningful: an inline-only T
// with an empty key points at nothing. The cache starts small and jump-grows
// toward the ceiling on demand, funding each step from the shared envelope.
func NewGenericCacheWithAvg[T any](capacityBytes datasize.ByteSize, avgBytes uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	c := &GenericCache[T]{
		capacityB:    capacityBytes,
		payloadBytes: int64(avgBytes),
		entryBytes:   int64(unsafe.Sizeof(entry[T]{})) + sizeClassSlack,
		enveloped:    true,
		mode:         mode,
		sizeFunc:     sizeFunc,
	}
	approx := min(uint64(capacityBytes)/uint64(c.bucketBytes())*bucketWays, maxCacheSlots)
	shards := max(initialShardCount(max(uint32(approx), genericCacheStartCapacity), shardCeil()), 1)
	start := max((genericCacheStartCapacity+bucketWays*shards-1)/(bucketWays*shards), 1)
	ceiling := min(uint64(capacityBytes)/(uint64(shards)*uint64(c.bucketBytes())), uint64(maxCacheSlots/bucketWays/shards))
	// A budget too small to buy the start capacity keeps it anyway: the byte
	// accounting a cache evicts and drops on has to stay usable at any budget.
	c.init(shards, start, max(uint32(ceiling), start))
	c.reservedBytes.Store(c.generationBytes(start))
	cachebudget.Global.Take(c.reservedBytes.Load())
	return c
}

// newGenericCacheEntries builds a cache against an explicit fixed entry-count
// cap (no jump-grow, no envelope). Used by tests that want to exercise eviction
// with small capacities; production constructs via NewGenericCache.
func newGenericCacheEntries[T any](capacityBytes datasize.ByteSize, capacityEntries uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	c := &GenericCache[T]{
		capacityB:    capacityBytes,
		payloadBytes: avgBytesPerEntry,
		entryBytes:   int64(unsafe.Sizeof(entry[T]{})) + sizeClassSlack,
		mode:         mode,
		sizeFunc:     sizeFunc,
	}
	shards := max(initialShardCount(capacityEntries, shardCeil()), 1)
	buckets := max((capacityEntries+bucketWays*shards-1)/(bucketWays*shards), 1)
	c.init(shards, buckets, buckets)
	return c
}

func (c *GenericCache[T]) init(shards, startBuckets, maxBuckets uint32) {
	c.shards = make([]shard[T], shards)
	c.shardShift = uint8(64 - bits.TrailingZeros32(shards))
	c.startBuckets, c.maxBuckets = startBuckets, maxBuckets
	c.startCap = shards * startBuckets * bucketWays
	c.maxCap = shards * maxBuckets * bucketWays
	for i := range c.shards {
		c.shards[i].tbl.Store(newTable[T](startBuckets))
	}
}

// bucketBytes is what a full bucket costs: its cache line plus every way's
// entry and payload.
func (c *GenericCache[T]) bucketBytes() int64 {
	return int64(unsafe.Sizeof(bucket[T]{})) + bucketWays*(c.entryBytes+c.payloadBytes)
}

// generationBytes is what the cache costs with every shard at this many buckets.
func (c *GenericCache[T]) generationBytes(buckets uint32) int64 {
	return int64(len(c.shards)) * int64(buckets) * c.bucketBytes()
}

func (c *GenericCache[T]) shard(h uint64) *shard[T] { return &c.shards[h>>c.shardShift] }

// fundGrow reserves a shard's grow step from the shared envelope, refusing when
// it is exhausted so the shard stops growing and evicts within its current size.
func (c *GenericCache[T]) fundGrow(oldBuckets, newBuckets uint32) bool {
	if !c.enveloped {
		return true
	}
	// Close settles reservedBytes once; a step funded after that would hold
	// envelope bytes nothing releases.
	if c.closed.Load() {
		return false
	}
	delta := int64(newBuckets-oldBuckets) * c.bucketBytes()
	if !cachebudget.Global.Reserve(delta) {
		return false
	}
	c.reservedBytes.Add(delta)
	return true
}

// growLocked rebuilds s one step larger. Readers keep using the old table, which
// no writer touches once s.mu is held, until the new one is published.
func (c *GenericCache[T]) growLocked(s *shard[T]) bool {
	old := *s.tbl.Load()
	n := uint32(len(old))
	if n >= c.maxBuckets {
		return false
	}
	next := min(n*genericCacheGrowFactor, c.maxBuckets)
	if !c.fundGrow(n, next) {
		return false
	}
	t := newTable[T](next)
	for i := range old {
		for way := range bucketWays {
			e := old[i].slots[way].Load()
			if e == nil {
				continue
			}
			h := maphash.Hash(unsafe.Slice(unsafe.StringData(e.key), len(e.key)))
			b := &(*t)[bucketOf(h, len(*t))]
			// Only a last step that is not a whole multiple can overfill a bucket.
			if w := b.freeWay(); w >= 0 {
				b.store(w, tagOf(h), e)
			} else {
				c.currentSize.Add(-int64(e.size))
				s.n.Add(-1)
				c.evictions.Add(1)
			}
		}
	}
	s.tbl.Store(t)
	return true
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
	// Snapshot coherence before loading the table. Clear publishes the
	// replacement tables before lifting the unwind floor, so an entry captured
	// from a retiring table is always judged by coherence that still carries its
	// unwind. A replacement-table entry judged by an old snapshot can only cause
	// a safe miss because dropStale rechecks the current table before removing it.
	coh := c.coh.Snapshot()
	b := c.shard(h).bucket(h)
	way, e := b.find(h, key)
	if e == nil {
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
	b.touch(way)
	// dbg: hits counter removed to measure its cost
	return e.val, e.txNum, true
}

// Put stores data for the given key. In ModeEvictLRU a full bucket evicts its
// CLOCK victim. In ModeNoOp inserts that would overflow the byte budget are
// dropped (and counted via the dropped metric).
func (c *GenericCache[T]) Put(key []byte, value T, txNum uint64) {
	c.put(key, value, txNum, true)
}

// PutIfAbsent implements Cache.PutIfAbsent (live entry kept, stale one
// replaced).
func (c *GenericCache[T]) PutIfAbsent(key []byte, value T, txNum uint64) {
	c.put(key, value, txNum, false)
}

// put performs the write under the key's shard mutex, so a conditional put's
// check and insert are atomic against a same-key writer.
func (c *GenericCache[T]) put(key []byte, value T, txNum uint64, overwrite bool) {
	h := maphash.Hash(key)
	size := len(key) + c.sizeFunc(value) + currentSizeEntryOverhead

	s := c.shard(h)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Sample the epoch under the shard mutex. Clear holds every shard mutex
	// across the table swap and coherence reset, so the stamp cannot belong to a
	// different generation from the one where the entry lands.
	e := entry[T]{val: value, txNum: txNum, size: uint32(size), epoch: c.coh.Epoch()}
	b := s.bucket(h)
	if way, old := b.find(h, key); old != nil {
		if !overwrite && !c.coh.IsStale(old.txNum, old.epoch) {
			return
		}
		c.currentSize.Add(int64(size) - int64(old.size))
		b.slots[way].Store(newEntry(key, e))
		return
	}

	if c.mode == ModeNoOp && (c.currentSize.Load()+int64(size) > int64(c.capacityB) || c.Len() >= int(c.maxCap)) {
		c.dropped.Add(1)
		return
	}
	// A full bucket grows the shard, so below its ceiling a put never evicts.
	way := b.freeWay()
	for way < 0 && c.growLocked(s) {
		b = s.bucket(h)
		way = b.freeWay()
	}
	if way < 0 && c.mode == ModeNoOp {
		c.dropped.Add(1)
		return
	}
	// Add before subtracting, so a concurrent ModeNoOp admission never reads a
	// transient under-statement of the bytes held.
	c.currentSize.Add(int64(size))
	if way < 0 {
		way = b.victim(h)
		c.currentSize.Add(-int64(b.slots[way].Load().size))
		c.evictions.Add(1)
	} else {
		s.n.Add(1)
	}
	b.store(way, tagOf(h), newEntry(key, e))
	c.inserts.Add(1)
}

// Delete removes the data for the given key. Runs under the key's shard mutex
// so the check-then-remove is atomic against same-key puts and excluded from
// the table swap in Clear.
func (c *GenericCache[T]) Delete(key []byte) {
	h := maphash.Hash(key)
	s := c.shard(h)
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(h)
	if way, e := b.find(h, key); e != nil {
		c.removeLocked(s, b, way, e)
	}
}

// dropStale removes key's entry under its shard mutex: the re-check keeps an
// entry a concurrent put revived, and the mutex keeps the removal out of
// table swaps.
func (c *GenericCache[T]) dropStale(h uint64, key []byte) {
	s := c.shard(h)
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(h)
	if way, e := b.find(h, key); e != nil && c.coh.IsStale(e.txNum, e.epoch) {
		c.removeLocked(s, b, way, e)
	}
}

func (c *GenericCache[T]) removeLocked(s *shard[T], b *bucket[T], way int, e *entry[T]) {
	b.clear(way)
	s.n.Add(-1)
	c.currentSize.Add(-int64(e.size))
}

func (c *GenericCache[T]) lockAll() {
	for i := range c.shards {
		c.shards[i].mu.Lock()
	}
}

func (c *GenericCache[T]) unlockAll() {
	for i := range c.shards {
		c.shards[i].mu.Unlock()
	}
}

// Clear removes all entries and restores the starting capacity. It starts an
// empty coherence generation by advancing the epoch and lifting the unwind
// floor, so subsequent puts are not constrained by an unwind that belongs to
// the retired data. The accounting reset, table swap, and coherence reset run
// with every shard mutex held, so a racing writer cannot split those
// publications.
func (c *GenericCache[T]) Clear() {
	fresh := make([]*[]bucket[T], len(c.shards)) // allocate before excluding writers
	for i := range fresh {
		fresh[i] = newTable[T](c.startBuckets)
	}
	c.lockAll()
	defer c.unlockAll()
	// Close settles reservedBytes once; a rebuild charged after that would hold
	// envelope bytes nothing releases.
	if c.enveloped && !c.closed.Load() {
		start := c.generationBytes(c.startBuckets)
		cachebudget.Global.Release(c.reservedBytes.Swap(start) - start)
	}
	c.currentSize.Store(0)
	for i := range c.shards {
		c.shards[i].tbl.Store(fresh[i])
		c.shards[i].n.Store(0)
	}
	// Reset coherence only after publishing the empty tables. Paired with
	// GetWithTxNum's snapshot-before-load ordering, this ensures an entry from
	// a retiring table is judged by pre-Reset coherence that still carries the
	// unwind.
	c.coh.Reset()
}

// Close returns this cache's envelope reservation so later caches can grow into
// the freed budget. Idempotent.
func (c *GenericCache[T]) Close() {
	if !c.enveloped || !c.closed.CompareAndSwap(false, true) {
		return
	}
	// Settle behind the shard mutexes a grow runs under; closed is set first, so
	// a writer arriving after the fence cannot fund one.
	c.lockAll()
	reserved := c.reservedBytes.Swap(0)
	c.unlockAll()
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
	n := int64(0)
	for i := range c.shards {
		n += c.shards[i].n.Load()
	}
	return int(n)
}

// allocatedSlots is the slots the shards hold right now, which is below maxCap
// for every shard still growing or refused a step.
func (c *GenericCache[T]) allocatedSlots() int {
	n := 0
	for i := range c.shards {
		n += len(*c.shards[i].tbl.Load()) * bucketWays
	}
	return n
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
	log.Debug(
		name+" cache stats",
		"mode", c.mode.String(),
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"inserts", inserts, "evictions", evictions, "dropped", dropped,
		"stale_evicted", staleEvicted, "epoch", c.coh.Epoch(),
		"entries", c.Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", int64(c.capacityB/datasize.MB), "slots_pct", c.slotsPct(),
		"reserved_mb", c.reservedBytes.Load()/int64(datasize.MB),
	)
}

// slotsPct is how full the cache is against the slots it has allocated, not
// against maxCap: a shard refused a grow step evicts at its current size.
func (c *GenericCache[T]) slotsPct() float64 {
	// Length first: allocation only rises between Clears, so a read after it can
	// only make the ratio smaller, never report over 100%.
	held := c.Len()
	allocated := c.allocatedSlots()
	if allocated == 0 {
		return 0
	}
	return float64(held) / float64(allocated) * 100
}
