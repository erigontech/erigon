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
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/maypok86/otter/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// entry stores the full key alongside the value so reads can detect hash
// collisions: the layer is keyed by the uint64 maphash of the key, Go's
// randomized stdlib hasher, so collisions are rare but not impossible. size is
// the key and value bytes, so weighing needs no sizeFunc call.
type entry[T any] struct {
	key   string // a string header is 8 bytes smaller than a slice's
	val   T
	txNum uint64 // commit/read txNum the cached value reflects (upper bound)
	size  uint32
	epoch uint32 // unwind generation the entry was written in
}

// GenericCache is a byte-bounded cache for key-value data, backed by the same
// lock-free layer as the content caches: a lookup never blocks on a lock.
// Eviction mode is fixed at construction (see policy.go).
type GenericCache[T any] struct {
	data      *ByteLRU[entry[T]]
	capacityB datasize.ByteSize
	mode      Mode

	// coh is the shared (epoch, floor) unwind-coherence primitive: an entry is
	// valid iff written in the current epoch OR its txNum is below the unwind
	// floor. See execution/cache/coherence.
	coh coherence.Gen

	hits         atomic.Uint64
	misses       atomic.Uint64
	inserts      atomic.Uint64
	dropped      atomic.Uint64
	staleEvicted atomic.Uint64 // stale entries detected on read after an unwind; dropped unless a racing put revived them

	sizeFunc func(T) int

	// putStripes keep puts out of Clear's purge-then-reset window: a put landing
	// in it keeps the retired epoch, and an unwind before the reset would be
	// lifted with the floor, reviving a dead-fork entry.
	putStripes [putStripeCount]sync.Mutex
}

const putStripeCount = 256

// NewGenericCache creates a cache bounded by capacityBytes, drawing its
// reservation from the shared envelope as it fills. mode selects ModeEvictLRU
// (default in this tree) or ModeNoOp (diagnostic baseline).
func NewGenericCache[T any](capacityBytes datasize.ByteSize, sizeFunc func(T) int, mode Mode) *GenericCache[T] {
	return &GenericCache[T]{
		capacityB: capacityBytes,
		mode:      mode,
		sizeFunc:  sizeFunc,
		data:      newByteLRU(capacityBytes, weighEntry[T], nil),
	}
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
	// Snapshot coherence before the load, so an entry is always judged by coherence that still carries its unwind.
	coh := c.coh.Snapshot()
	e, ok := c.data.Get(h)
	if !ok || e.key != string(key) {
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
	h := maphash.Hash(key)
	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()
	newSize := len(key) + c.sizeFunc(value)
	c.data.compute(h, func(existing entry[T], found bool) (entry[T], otter.ComputeOp) {
		sameKey := found && existing.key == string(key)
		if sameKey && !overwrite && !c.coh.IsStale(existing.txNum, existing.epoch) {
			return existing, otter.CancelOp
		}
		e := entry[T]{val: value, size: uint32(min(newSize, math.MaxUint32)), txNum: txNum, epoch: c.coh.Epoch()}
		// "Drop new keys when full": the lock-free layer would admit and evict.
		if c.mode == ModeNoOp {
			grow := weighEntry(h, e)
			if sameKey {
				grow -= weighEntry(h, existing)
			}
			if c.data.SizeBytes()+grow > int64(c.capacityB) {
				c.dropped.Add(1)
				if sameKey {
					return existing, otter.InvalidateOp // the old value is superseded
				}
				return existing, otter.CancelOp
			}
		}
		if sameKey {
			e.key = existing.key // the stored buffer still holds this key
		} else {
			e.key = string(key)
		}
		// A write over a 64-bit hash collision replaces the other key's entry.
		c.inserts.Add(1)
		return e, otter.WriteOp
	})
}

// Delete removes the data for the given key.
func (c *GenericCache[T]) Delete(key []byte) {
	c.data.removeIf(maphash.Hash(key), func(e entry[T]) bool { return e.key == string(key) })
}

// dropStale removes key's entry unless a concurrent put revived it.
func (c *GenericCache[T]) dropStale(h uint64, key []byte) {
	c.data.removeIf(h, func(e entry[T]) bool {
		return e.key == string(key) && c.coh.IsStale(e.txNum, e.epoch)
	})
}

// Clear removes all entries and returns the grown reservation. It starts an
// empty coherence generation by advancing the epoch and lifting the unwind
// floor, so subsequent puts are not constrained by an unwind that belongs to
// the retired data.
func (c *GenericCache[T]) Clear() {
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
	defer func() {
		for i := range c.putStripes {
			c.putStripes[i].Unlock()
		}
	}()
	c.data.Purge()
	// Reset coherence only after emptying, so an entry still in flight from the
	// retired data is judged by pre-Reset coherence that carries the unwind.
	c.coh.Reset()
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
