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

// truncating to 32 bits is safe: maphash already spreads the bits, the low 32 suffice for shard routing.
func u64ident(k uint64) uint32 { return uint32(k) }

// Not a trie branch; must never enter BranchCache.
var KeyCommitmentState = []byte("state")

func isCommitmentStateKey(prefix []byte) bool {
	return bytes.Equal(prefix, KeyCommitmentState)
}

// Callers must ensure a single writer per prefix; the cache itself does not coordinate writers.
type BranchCache struct {
	root atomic.Pointer[branchCacheEntry]

	accountTrunk *trunk

	pinned        atomic.Pointer[maphash.Map[*trunk]]
	pinnedMu      sync.Mutex
	pinnedEntries atomic.Int64

	tail    atomic.Pointer[tailLRU]
	tailCap uint32
	tailMu  sync.Mutex

	maxDepth uint8
	closed   atomic.Bool

	// env BRANCH_CACHE_TRUNK_DISABLE: A/B switch — the LRU tail self-heals stale entries via eviction, the trunk does not.
	trunkDisabled bool

	rootHits, rootMisses     atomic.Uint64
	trunkHits, trunkMisses   atomic.Uint64
	pinnedHits, pinnedMisses atomic.Uint64
	tailHits, tailMisses     atomic.Uint64
	bytesServed              atomic.Uint64
	staleEvicted             atomic.Uint64

	onMiss atomic.Pointer[MissCallback]

	lastPublishedPinnedHits   atomic.Uint64
	lastPublishedPinnedMisses atomic.Uint64

	coh coherence.Gen
}

type branchCacheEntry struct {
	data []byte

	step uint64
	txN  uint64

	// Disambiguates a txN reused across forks.
	epoch uint32
}

// MissCallback is invoked on the hot read path when lookup misses ALL tiers; implementations must be lock-free.
type MissCallback func(prefix []byte)

// d2/d3/d4 allocate lazily: eager allocation (d3 32KB, d4 512KB) is pure churn for the many
// shallow test-suite caches.
type trunk struct {
	d0   atomic.Pointer[branchCacheEntry]
	d1   [16]atomic.Pointer[branchCacheEntry]
	d2   atomic.Pointer[[256]atomic.Pointer[branchCacheEntry]]
	d3   atomic.Pointer[[4096]atomic.Pointer[branchCacheEntry]]
	d4   atomic.Pointer[[65536]atomic.Pointer[branchCacheEntry]]
	deep *maphash.Map[*branchCacheEntry]

	// Caps which dense tiers allocate; deeper branches route to deep (storage) or the LRU tail
	// (account). Set from the active-BranchCache count.
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

func newAccountTrunk(maxDepth uint8) *trunk {
	return &trunk{maxDepth: maxDepth}
}

func newStorageTrunk(maxDepth uint8) *trunk {
	return &trunk{maxDepth: maxDepth, deep: maphash.NewMap[*branchCacheEntry]()}
}

// A process with a handful of BranchCaches (production) keeps full depth-4 residency; one
// spinning up many (the test suite) keeps trunks shallow so fixed arrays don't blow the memory budget.
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

// Returns nil when path is deeper than the resident tiers; caller then uses deep (storage) or the tail (account).
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

// ~50k entries × ~500 bytes = ~25 MB at typical mainnet branch sizes.
const DefaultBranchCacheTailCapacity = 50000

// Duck-typed (not a direct import) to avoid a db/state import cycle: db/state imports execctx.
// Returning nil is permitted; callers MUST treat it as "no shared cache" rather than panic.
type BranchCacheProvider interface {
	BranchCache() *BranchCache
}

// Returning nil means adaptive pinning is disabled.
type AdaptivePinControllerProvider interface {
	AdaptivePinController() *AdaptivePinController
}

const branchCacheTailShards = 256

// Capacity <= 0 panics.
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
	bc.coh.Init()
	log.Debug("[branch-cache] init", "trunkEnabled", !bc.trunkDisabled, "tailCap", tailCapacity, "trunkDepth", maxDepth)
	return bc
}

// Idempotent.
func (c *BranchCache) Close() {
	if c.closed.CompareAndSwap(false, true) {
		if t := c.tail.Load(); t != nil {
			t.Close()
		}
		activeBranchCaches.Add(-1)
	}
}

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

func (c *BranchCache) tailLen() int {
	if t := c.tail.Load(); t != nil {
		return t.Len()
	}
	return 0
}

// nil for root, storage, or depth >= 5 prefixes. Bit 4 of byte 0 is the odd-length flag.
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

// ok=false for non-storage prefixes (< 64 nibbles); caller falls through to the LRU tail.
// create allocates the contract's storageTrunk on demand (PinEntry path).
func (c *BranchCache) storageRoute(prefix []byte, create bool) (st *trunk, acct []byte, stor []byte, ok bool) {
	if len(prefix) < 33 {
		return nil, nil, nil, false
	}
	if !create && c.pinned.Load() == nil {
		return nil, nil, nil, false
	}
	nib := nibbles.CompactToHex(prefix)
	if len(nib) < 64 {
		return nil, nil, nil, false
	}
	packed := make([]byte, 32)
	for i := range 32 {
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

// ok=false for non-storage prefixes. Hot path: decodes the leading 64 nibbles from the compact
// bytes directly rather than materializing the full hex expansion.
func ContractHashFromPrefix(prefix []byte) (hash [32]byte, ok bool) {
	if len(prefix) < 33 {
		return hash, false
	}
	if prefix[0]&0x10 != 0 {
		for i := range 32 {
			hash[i] = prefix[i]&0x0f<<4 | prefix[i+1]>>4
		}
		return hash, true
	}
	copy(hash[:], prefix[1:33])
	return hash, true
}

// Per-slot stores, not a pointer swap: lock-free readers deref c.accountTrunk concurrently.
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

// Pass nil to clear.
func (c *BranchCache) SetMissCallback(cb MissCallback) {
	if cb == nil {
		c.onMiss.Store(nil)
		return
	}
	c.onMiss.Store(&cb)
}

// the compact encoding represents the root branch (empty nibble path) as the single byte 0x00.
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
	if slot := c.trunkSlot(prefix, false); slot != nil {
		if entry := slot.Load(); entry != nil {
			c.trunkHits.Add(1)
			return entry, true
		}
		c.trunkMisses.Add(1)
		c.fireOnMiss(prefix)
		return nil, false
	}
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

// Pinned entries never LRU-evict but still honor the (txN, epoch) unwind model. Data is copied.
// Non-storage prefixes (< 64 nibbles) fall through to the tail.
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

func (c *BranchCache) PinnedCount() int {
	return int(c.pinnedEntries.Load())
}

func (c *BranchCache) Get(prefix []byte) ([]byte, uint64, bool) {
	if isCommitmentStateKey(prefix) {
		return nil, 0, false
	}
	entry, ok := c.lookup(prefix)
	if !ok {
		return nil, 0, false
	}
	if c.coh.IsStale(entry.txN, entry.epoch) {
		c.Invalidate(prefix)
		c.staleEvicted.Add(1)
		return nil, 0, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return entry.data, entry.step, true
}

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

// O(1), scan-free: bumps the epoch and lowers the unwind floor to unwindToTxN; old-epoch entries
// at/above it drop lazily on next Get. Floor only decreases, so a shallow unwind cannot resurrect
// entries a deeper one invalidated.
func (c *BranchCache) Unwind(unwindToTxN uint64) {
	c.coh.Unwind(unwindToTxN)
}

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
	// must reset alongside the counters above, else PublishMetrics computes a wrapped delta against the pre-Clear value.
	c.lastPublishedPinnedHits.Store(0)
	c.lastPublishedPinnedMisses.Store(0)
	c.tailHits.Store(0)
	c.tailMisses.Store(0)
	c.bytesServed.Store(0)
	c.staleEvicted.Store(0)
	c.coh.Init()
}

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
