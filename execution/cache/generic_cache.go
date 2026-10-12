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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
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

// slotChargeBytes is a slot at freelru's best table ratio, 5/4, so it is a lower
// bound: a capacity off that ratio costs up to 5/2. It seeds the ceiling search
// only; every reservation charges the exact table through tableSlots.
func slotChargeBytes(elemBytes int64) int64 { return elemBytes*5/4 + 1 }

// freelruShardBytes covers a shard beyond its two arrays: the LRU struct and the
// slice headers. Measured at 239-359 B; charged high to stay covering. The zero
// key and value it also holds scale with the value type, so shardChargeBytes
// adds a slot on top.
const freelruShardBytes = 512

func shardChargeBytes(elemBytes int64) int64 { return freelruShardBytes + elemBytes }

// maxCacheSlots caps the slot array whatever the byte budget says.
const maxCacheSlots = 16_000_000

// fitCeiling is the largest capacity up to ceiling whose exact cost is inside the
// budget. estimate only seeds the search and may sit on either side of the answer.
// cost must be non-decreasing in capacity, as both callers' are: the payload term
// grows with it and the table it rounds up to never shrinks.
func fitCeiling(estimate, ceiling uint32, budget datasize.ByteSize, cost func(uint32) int64) uint32 {
	if cost(1) > int64(budget) {
		return 1
	}
	ceiling = max(ceiling, 1)
	hi := max(min(estimate, ceiling), 1)
	for hi < ceiling && cost(hi) <= int64(budget) {
		hi = uint32(min(uint64(hi)*2, uint64(ceiling)))
	}
	lo := uint32(1)
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if cost(mid) <= int64(budget) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

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

// GenericCache is a byte-bounded cache for key-value data, backed by the same
// lock-free layer as the content caches: a read takes no lock. Eviction mode is
// fixed at construction (see policy.go).
type GenericCache[T any] struct {
	data      *ByteLRU[entry[T]]
	capacityB datasize.ByteSize
	mode      Mode

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
	dropped      atomic.Uint64
	staleEvicted atomic.Uint64 // stale entries detected on read after an unwind; dropped unless a racing put revived them

	sizeFunc func(T) int
}

func u64identity(k uint64) uint32 { return uint32(k) }

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
// avgBytes of 0 is meaningful, not a request for the default: an inline-only T
// points at nothing outside its slot.
func NewGenericCacheWithAvg[T any](capacityBytes datasize.ByteSize, _ uint32, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	c := &GenericCache[T]{capacityB: capacityBytes, mode: mode, sizeFunc: sizeFunc}
	c.data = newByteLRU(capacityBytes, weighEntry[T], nil)
	return c
}

// weighEntry charges an entry what it holds: its payload, the entry itself and
// otter's node around it.
func weighEntry[T any](_ uint64, e entry[T]) int64 {
	return int64(e.size) + int64(unsafe.Sizeof(e)) + ByteLRUEntryOverheadBytes
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
	e, ok := c.data.Get(h)
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

// Put stores data for the given key. In ModeEvictLRU the byte-bounded layer
// admits and evicts by W-TinyLFU. In ModeNoOp inserts that would overflow the
// byte budget are dropped (and counted via the dropped metric).
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
	newSize := len(key) + c.sizeFunc(value)

	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()

	// Sample the epoch under the stripe, so the stamp cannot belong to a
	// different generation from the one where the entry lands.
	ep := c.coh.Epoch()
	existing, hasExisting := c.data.Get(h)
	sameKey := hasExisting && bytes.Equal(existing.key, key)

	if sameKey && !overwrite && !c.coh.IsStale(existing.txNum, existing.epoch) {
		return
	}
	if c.mode == ModeNoOp && !sameKey {
		// "Drop new keys when full": the lock-free layer would admit and evict.
		if c.data.SizeBytes()+weighEntry(h, entry[T]{size: newSize}) > int64(c.capacityB) {
			c.dropped.Add(1)
			return
		}
	}

	e := entry[T]{key: key, val: value, size: newSize, txNum: txNum, epoch: ep}
	if sameKey {
		e.key = existing.key // the stored buffer still holds this key
	} else {
		e.key = bytes.Clone(key)
	}
	if hasExisting {
		// Add is SetIfAbsent, so the resident entry -- this key's older value or
		// a 64-bit hash collision -- has to go first.
		c.data.Remove(h)
	}
	c.data.Add(h, e)
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
	if existing, ok := c.data.Get(h); ok && bytes.Equal(existing.key, key) {
		c.data.Remove(h)
	}
}

// dropStale removes key's entry under its put stripe: the re-check keeps an
// entry a concurrent put revived, and the stripe keeps the removal out of
// generation swaps.
func (c *GenericCache[T]) dropStale(h uint64, key []byte) {
	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()
	if e, ok := c.data.Get(h); ok && bytes.Equal(e.key, key) && c.coh.IsStale(e.txNum, e.epoch) {
		c.data.Remove(h)
	}
}

// Clear removes all entries and restores the starting capacity. It starts an
// empty coherence generation by advancing the epoch and lifting the unwind
// floor, so subsequent puts are not constrained by an unwind that belongs to
// the retired data. The accounting reset, data swap, and coherence reset run
// with every put stripe held, so a racing writer cannot split those
// publications.
func (c *GenericCache[T]) Clear() {
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
	c.data.Purge()
	// Reset coherence only after emptying, so an entry still in flight from the
	// retired data is judged by pre-Reset coherence that carries the unwind.
	c.coh.Reset()
	for i := range c.putStripes {
		c.putStripes[i].Unlock()
	}
}

// Close returns this cache's envelope reservation so later caches can grow into
// the freed budget. Idempotent.
func (c *GenericCache[T]) Close() {
	c.data.Close()
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
	return c.data.Len()
}

// SizeBytes returns the current size of the cache in bytes.
func (c *GenericCache[T]) SizeBytes() int64 {
	return c.data.SizeBytes()
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
	dropped := c.dropped.Swap(0)
	staleEvicted := c.staleEvicted.Swap(0)
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	sizeBytes := c.data.SizeBytes()
	log.Debug(
		name+" cache stats",
		"mode", c.mode.String(),
		"hits", hits, "misses", misses, "hit_rate", hitRate,
		"inserts", inserts, "dropped", dropped,
		"stale_evicted", staleEvicted, "epoch", c.coh.Epoch(),
		"entries", c.data.Len(), "size_mb", sizeBytes/(1024*1024),
		"capacity_mb", int64(c.capacityB/datasize.MB),
	)
}
