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

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// putStripeCount sizes the same-key write-serialization stripes; power of two
// so the stripe index is a mask of the key hash.
const putStripeCount = 256

// avgBytesPerEntry is the assumption used to translate a byte budget into
// the entry-count cap that freelru.ShardedLRU is sized against. 256 B
// approximates account-record + key overhead and storage-slot value+key
// overhead in the same order of magnitude. Actual residency tracked in
// currentSize and reported via PrintStatsAndReset.
const avgBytesPerEntry = 256

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
	// data is the sharded LRU, replaced wholesale on a jump-grow with every
	// put stripe held and the new generation fully copied — no write lands in
	// a retired generation and no reader sees a partial one (see maybeGrow).
	data      atomic.Pointer[freelru.ShardedLRU[uint64, entry[T]]]
	capacityB datasize.ByteSize
	mode      Mode

	// Jump-grow: the LRU starts at startCap slots and resizes ×genericCacheGrowFactor
	// toward maxCap as it fills, reserving each step's bytes from the shared
	// envelope; a step the envelope can't fund stops the growth (freelru then
	// evicts within the current size). A cache with a small working set never
	// grows past startCap, so it costs a few KB regardless of its configured
	// budget. resizeMu serialises the resize and guards reservedBytes; curCap is
	// atomic because the put fast-path reads it outside resizeMu.
	startCap      uint32
	maxCap        uint32
	curCap        atomic.Uint32
	avgEntryBytes int64 // per-domain byte estimate; maps slot count ↔ envelope bytes
	resizeMu      sync.Mutex
	reservedBytes int64

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
	evictions    atomic.Uint64
	dropped      atomic.Uint64
	staleEvicted atomic.Uint64 // entries dropped lazily on read after an unwind

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
	start := min(uint32(genericCacheStartCapacity), maxCap)
	c := newGenericCacheEntries[T](capacityBytes, start, sizeFunc, mode)
	c.maxCap = maxCap
	c.avgEntryBytes = int64(avgBytes)
	c.enveloped = true
	// The initial slot array is small; take it unconditionally so no cache is
	// born unable to hold anything.
	c.reservedBytes = int64(start) * c.avgEntryBytes
	cachebudget.Global.Take(c.reservedBytes)
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
	c.curCap.Store(capacityEntries)
	// Before any unwind every entry predates the (nonexistent) floor, so all
	// reads are valid; the floor only drops once an unwind happens.
	c.coh.Init()
	c.data.Store(c.newShards(capacityEntries))
	return c
}

// newShards builds a sharded LRU of the given capacity with this cache's evict
// callback wired, so currentSize follows capacity-driven eviction and Remove.
func (c *GenericCache[T]) newShards(capacity uint32) *freelru.ShardedLRU[uint64, entry[T]] {
	lru, err := freelru.NewSharded[uint64, entry[T]](capacity, u64identity)
	if err != nil {
		panic(err)
	}
	lru.SetOnEvict(func(_ uint64, e entry[T]) {
		c.currentSize.Add(-int64(e.size))
		c.evictions.Add(1)
	})
	return lru
}

// maybeGrow jump-resizes the LRU one step larger when it is full, the ceiling
// hasn't been reached, and the shared envelope can fund the step. Otherwise the
// LRU keeps its size and freelru evicts within it. Must not be called with a
// stripe held (it takes them all).
//
// The copy runs with every put stripe held: writers (and the striped
// stale-drop) are excluded, so no write can land in the generation being
// retired and a conditional put never sees a mid-resize gap it could fill
// with a stale value; readers stay on the retiring generation until the swap
// and never miss. Grows are a handful of steps per cache lifetime, so the
// writer stall is a bounded one-off.
func (c *GenericCache[T]) maybeGrow() {
	c.resizeMu.Lock()
	defer c.resizeMu.Unlock()

	old := c.data.Load()
	curCap := c.curCap.Load()
	if curCap >= c.maxCap || old.Len() < int(curCap) {
		return
	}
	newCap := min(curCap*genericCacheGrowFactor, c.maxCap)
	delta := int64(newCap-curCap) * c.avgEntryBytes
	if !cachebudget.Global.Reserve(delta) {
		return
	}
	next := c.newShards(newCap) // allocate before excluding writers
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
	for _, k := range old.Keys() {
		if v, ok := old.Get(k); ok {
			next.Add(k, v)
		}
	}
	c.data.Store(next)
	c.curCap.Store(newCap)
	for i := range c.putStripes {
		c.putStripes[i].Unlock()
	}
	c.reservedBytes += delta
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
	if c.coh.IsStale(e.txNum, e.epoch) {
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
	if c.putLocked(key, value, txNum, overwrite) {
		// Grow outside the stripe — maybeGrow takes every stripe.
		c.maybeGrow()
	}
}

// putLocked performs the write under the key's stripe and reports whether the
// insert landed in a full LRU with ceiling headroom, i.e. the caller should
// grow. Detection stays on the insert path — Len locks every shard, too costly
// per warm update.
func (c *GenericCache[T]) putLocked(key []byte, value T, txNum uint64, overwrite bool) bool {
	h := maphash.Hash(key)
	valBytes := c.sizeFunc(value)
	newSize := len(key) + valBytes + 24
	ep := c.coh.Epoch()

	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()

	lru := c.data.Load()
	existing, hasExisting := lru.Get(h)

	// Existing key — update in place. Reuse the stored key buffer to
	// avoid an extra allocation; the freshly-decoded value replaces the
	// old one.
	if hasExisting && bytes.Equal(existing.key, key) {
		if !overwrite && !c.coh.IsStale(existing.txNum, existing.epoch) {
			return false
		}
		lru.Add(h, entry[T]{key: existing.key, val: value, size: newSize, txNum: txNum, epoch: ep})
		c.currentSize.Add(int64(newSize - existing.size))
		return false
	}

	if c.mode == ModeNoOp {
		// Refuse once full by either bound — freelru would otherwise evict at the
		// entry-count cap, which ModeNoOp ("drop new keys when full") must not do.
		if c.currentSize.Load()+int64(newSize) > int64(c.capacityB) || lru.Len() >= int(c.maxCap) {
			c.dropped.Add(1)
			return false
		}
	}

	curCap := c.curCap.Load()
	needGrow := c.mode != ModeNoOp && curCap < c.maxCap && lru.Len() >= int(curCap)

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
	// hash): freelru.Add replaces the colliding entry in place WITHOUT firing
	// OnEvict, so subtract the displaced size now — otherwise currentSize drifts
	// up by it permanently.
	if hasExisting {
		c.currentSize.Add(-int64(existing.size))
	}
	keyCopy := common.Copy(key)
	lru.Add(h, entry[T]{key: keyCopy, val: value, size: newSize, txNum: txNum, epoch: ep})
	c.currentSize.Add(int64(newSize))
	c.inserts.Add(1)
	return needGrow
}

// Delete removes the data for the given key. Runs under the key's put stripe:
// an unstriped Remove racing put's read-modify-write would double-subtract the
// displaced entry's size (once via OnEvict, once via put's update delta).
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
// entry a concurrent put revived, and striping the Remove stops it
// double-subtracting the displaced size against put's update delta.
func (c *GenericCache[T]) dropStale(h uint64, key []byte) {
	mu := &c.putStripes[h&(putStripeCount-1)]
	mu.Lock()
	defer mu.Unlock()
	lru := c.data.Load()
	if e, ok := lru.Get(h); ok && bytes.Equal(e.key, key) && c.coh.IsStale(e.txNum, e.epoch) {
		lru.Remove(h)
	}
}

// Clear removes all entries from the cache. It also resets the (epoch,
// unwindFloor) coherence pair: with no entries left, no stale (txNum, epoch)
// can survive, so a fresh floor keeps subsequent Puts at the live epoch
// serviceable. Mirrors CodeCache.Clear (which already did this — the two had
// drifted).
func (c *GenericCache[T]) Clear() {
	c.currentSize.Store(0)
	c.coh.Init()
	// Shrink back to the start size and return the grown budget to the envelope,
	// keeping the cache adaptive across fork-validation/reset (it regrows on
	// demand). A no-op Purge would leave the grown slot array resident.
	c.resizeMu.Lock()
	defer c.resizeMu.Unlock()
	if c.enveloped {
		cachebudget.Global.Release(c.reservedBytes - int64(c.startCap)*c.avgEntryBytes)
		c.reservedBytes = int64(c.startCap) * c.avgEntryBytes
	}
	c.curCap.Store(c.startCap)
	c.data.Store(c.newShards(c.startCap))
}

// Close returns this cache's envelope reservation so later caches can grow into
// the freed budget. Idempotent.
func (c *GenericCache[T]) Close() {
	if c.enveloped && c.closed.CompareAndSwap(false, true) {
		c.resizeMu.Lock()
		reserved := c.reservedBytes
		c.reservedBytes = 0
		c.resizeMu.Unlock()
		cachebudget.Global.Release(reserved)
	}
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
