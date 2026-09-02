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
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/execution/cache/coherence"
)

// k is already a maphash.Hash output, spread uniformly across all 64 bits, so
// the low 32 bits suffice for shard routing.
func u64ident(k uint64) uint32 { return uint32(k) }

// KeyCommitmentState must never enter the BranchCache: it changes every block.
var KeyCommitmentState = []byte{0x00}

// LegacyKeyCommitmentState is the pre-v3 commitment state key.
var LegacyKeyCommitmentState = []byte("state")

// IsCommitmentStateKey reports whether prefix identifies a commitment state record.
func IsCommitmentStateKey(prefix []byte) bool {
	return bytes.Equal(prefix, KeyCommitmentState) || bytes.Equal(prefix, LegacyKeyCommitmentState)
}

// IsCommitmentStateKeyForFormat reports whether prefix is the state key for the
// selected bundled-row or edge-record format.
func IsCommitmentStateKeyForFormat(prefix []byte, edgeRecords bool) bool {
	if edgeRecords {
		return bytes.Equal(prefix, KeyCommitmentState)
	}
	return bytes.Equal(prefix, LegacyKeyCommitmentState)
}

// BranchCache: writer stripes only make stamped publications atomic with
// Clear; callers must still ensure one logical mutation per prefix.
type BranchCache struct {
	root atomic.Pointer[branchCacheEntry]

	// accountTrunk: nibble depths 1-4; depth 5+ spills to the LRU tail.
	accountTrunk *trunk

	pinned        atomic.Pointer[maphash.Map[*trunk]]
	pinnedMu      sync.Mutex
	pinnedEntries atomic.Int64

	tail    atomic.Pointer[tailLRU]
	tailCap uint32
	tailMu  sync.Mutex

	maxDepth uint8
	closed   atomic.Bool

	// trunkDisabled (env BRANCH_CACHE_TRUNK_DISABLE): A/B switch since the LRU
	// self-heals stale entries via eviction and the trunk does not.
	trunkDisabled           bool
	edgeRecordsInCommitment atomic.Bool

	rootHits, rootMisses     atomic.Uint64
	trunkHits, trunkMisses   atomic.Uint64
	pinnedHits, pinnedMisses atomic.Uint64
	tailHits, tailMisses     atomic.Uint64
	bytesServed              atomic.Uint64
	staleEvicted             atomic.Uint64

	onMiss atomic.Pointer[MissCallback]

	lastPublishedPinnedHits   atomic.Uint64
	lastPublishedPinnedMisses atomic.Uint64

	putStripes [256]sync.Mutex

	coh coherence.Gen
}

type branchCacheEntry struct {
	data  []byte
	step  uint64 // on-disk file step; 0 = untracked
	txN   uint64 // write txN, upper bound of validity; 0 = frozen/untracked
	epoch uint32
	// node marks data as a whole v3 node's encoded edge records rather than one value. The two
	// share slots -- the v3 root's node key is byte-identical to KeyCommitmentState, and an edge
	// record for P->n routes to the same trunk slot as node P||n -- so every reader must check it.
	node bool
}

// MissCallback runs on the hot read path when lookup misses every tier that
// applies to a prefix; implementations must be lock-free.
type MissCallback func(prefix []byte)

// trunk slots are atomic.Pointer: under the single-writer-per-prefix
// invariant, readers/writers take no mutex; only deep (a maphash.Map) locks.
type trunk struct {
	d0   atomic.Pointer[branchCacheEntry]
	d1   [16]atomic.Pointer[branchCacheEntry]
	d2   atomic.Pointer[[256]atomic.Pointer[branchCacheEntry]]
	d3   atomic.Pointer[[4096]atomic.Pointer[branchCacheEntry]]
	d4   atomic.Pointer[[65536]atomic.Pointer[branchCacheEntry]]
	deep *maphash.Map[*branchCacheEntry]

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

func (t *trunk) slot(path *[4]byte, n int, forWrite bool) *atomic.Pointer[branchCacheEntry] {
	switch n {
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

const DefaultBranchCacheTailCapacity = 50000 // ~50k * ~500B = ~25MB at mainnet branch sizes

// BranchCacheProvider: returning nil means no shared cache; callers must
// treat that as disabled, not panic.
type BranchCacheProvider interface {
	BranchCache() *BranchCache
}

type AdaptivePinControllerProvider interface {
	AdaptivePinController() *AdaptivePinController
}

const branchCacheTailShards = 256

func (c *BranchCache) putStripe(prefix []byte) *sync.Mutex {
	if len(prefix) == 0 {
		return &c.putStripes[0]
	}
	stripe := prefix[len(prefix)-1]
	if len(prefix) > 1 {
		stripe ^= prefix[0]
	}
	return &c.putStripes[stripe]
}

func (c *BranchCache) lockAllPutStripes() {
	for i := range c.putStripes {
		c.putStripes[i].Lock()
	}
}

func (c *BranchCache) unlockAllPutStripes() {
	for i := len(c.putStripes) - 1; i >= 0; i-- {
		c.putStripes[i].Unlock()
	}
}

func NewBranchCache(tailCapacity int, edgeRecords ...bool) *BranchCache {
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
	if len(edgeRecords) == 0 || edgeRecords[0] {
		bc.edgeRecordsInCommitment.Store(true)
	}
	log.Debug("[branch-cache] init", "trunkEnabled", !bc.trunkDisabled, "tailCap", tailCapacity, "trunkDepth", maxDepth)
	return bc
}

func (c *BranchCache) SetEdgeRecords(edgeRecords bool) {
	c.edgeRecordsInCommitment.Store(edgeRecords)
}

func (c *BranchCache) isCommitmentStateKey(prefix []byte) bool {
	return IsCommitmentStateKeyForFormat(prefix, c.edgeRecordsInCommitment.Load())
}

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

// v3EdgeDepth reports the trie depth of the edge a v3 record key addresses -- the parent path
// length plus its child nibble. ok=false for anything that is not a record key.
func v3EdgeDepth(prefix []byte) (int, bool) {
	if len(prefix) < 2 || prefix[len(prefix)-1]&0xf0 != 0x80 {
		return 0, false
	}
	switch term := prefix[len(prefix)-2]; {
	case term == 0x00:
		return 2*len(prefix) - 3, true
	case term&0xf0 == 0xf0:
		return 2*len(prefix) - 2, true
	default:
		return 0, false
	}
}

// v3EdgeNibble returns nibble i of the edge path a v3 record key addresses.
func v3EdgeNibble(prefix []byte, depth, i int) byte {
	switch {
	case i == depth-1:
		return prefix[len(prefix)-1] & 0x0f
	case depth&1 == 0 && i == depth-2: // odd parent path: its last nibble lives in the term byte
		return prefix[len(prefix)-2] & 0x0f
	case i&1 == 0:
		return prefix[i/2] >> 4
	default:
		return prefix[i/2] & 0x0f
	}
}

// v3TrunkSlot routes by the edge path a record key spells out. Reading it as a compact key instead
// drops the first byte from the index, so two edges differing only in their first nibbles alias.
func (c *BranchCache) v3TrunkSlot(prefix []byte, forWrite bool) *atomic.Pointer[branchCacheEntry] {
	depth, ok := v3EdgeDepth(prefix)
	if !ok || depth > trunkDepthFull {
		return nil
	}
	var nib [4]byte
	for i := range depth {
		nib[i] = v3EdgeNibble(prefix, depth, i)
	}
	return c.accountTrunk.slot(&nib, depth, forWrite)
}

// trunkSlot: bit 4 of byte 0 is the odd-length flag; the low nibble of byte 0
// is the first nibble when odd.
func (c *BranchCache) trunkSlot(prefix []byte, forWrite bool) *atomic.Pointer[branchCacheEntry] {
	if c.trunkDisabled {
		return nil
	}
	if c.edgeRecordsInCommitment.Load() {
		return c.v3TrunkSlot(prefix, forWrite)
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

// storageRoute: ok=false means non-storage, caller falls through to the tail.
func (c *BranchCache) storageRoute(prefix []byte, create bool, nibBuf *[4]byte) (st *trunk, n int, ok bool) {
	edgeRecords := c.edgeRecordsInCommitment.Load()
	if !edgeRecords && (len(prefix) < 33 || prefix[0]&0x20 != 0) {
		return nil, 0, false
	}
	p := c.pinned.Load()
	if !create && p == nil {
		return nil, 0, false
	}
	acctHash, ok := contractHashFromPrefix(prefix, edgeRecords)
	if !ok {
		return nil, 0, false
	}
	packed := acctHash[:]
	if p != nil {
		if st, found := p.Get(packed); found {
			return st, storageNibblesFor(prefix, nibBuf, edgeRecords), true
		}
	}
	if !create {
		return nil, 0, false
	}
	st, _ = c.pinnedForWrite().LoadOrStore(packed, newStorageTrunk(c.maxDepth))
	return st, storageNibblesFor(prefix, nibBuf, edgeRecords), true
}

func storageNibblesFor(prefix []byte, nib *[4]byte, edgeRecords bool) int {
	if edgeRecords {
		return v3StorageNibbles(prefix, nib)
	}
	return storageNibbles(prefix, nib)
}

// v3StorageNibbles is storageNibbles for the edge-record layout: the number of edge nibbles below
// the account boundary, with the first 4 written into nib.
func v3StorageNibbles(prefix []byte, nib *[4]byte) (n int) {
	depth, _ := v3EdgeDepth(prefix)
	n = depth - 64
	if n > 4 {
		return n
	}
	for i := range n {
		nib[i] = v3EdgeNibble(prefix, depth, 64+i)
	}
	return n
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

// ContractHash decodes the contract a storage prefix belongs to, in whichever record format this
// cache holds. ok=false for non-storage prefixes.
func (c *BranchCache) ContractHash(prefix []byte) (hash [32]byte, ok bool) {
	return contractHashFromPrefix(prefix, c.edgeRecordsInCommitment.Load())
}

func contractHashFromPrefix(prefix []byte, edgeRecords bool) (hash [32]byte, ok bool) {
	if !edgeRecords {
		return ContractHashFromPrefix(prefix)
	}
	depth, ok := v3EdgeDepth(prefix)
	if !ok || depth <= 64 {
		return hash, false
	}
	copy(hash[:], prefix[:length.Hash])
	return hash, true
}

// ContractHashFromPrefix: ok=false for non-storage prefixes.
func ContractHashFromPrefix(prefix []byte) (hash [32]byte, ok bool) {
	if len(prefix) < 33 {
		return hash, false
	}
	if prefix[0]&0x10 != 0 { // odd: first nibble is the low nibble of byte 0
		for i := range 32 {
			hash[i] = prefix[i]&0x0f<<4 | prefix[i+1]>>4
		}
		return hash, true
	}
	copy(hash[:], prefix[1:33])
	return hash, true
}

// storageNibbles matches nibbles.CompactToHex(prefix)[64:]; only the first 4
// are written, n is the true count. CompactToHex appends a terminator nibble
// this count excludes; miscounting it shifts n by one and routes to a
// neighbouring depth slot.
func storageNibbles(prefix []byte, nib *[4]byte) (n int) {
	off := 2
	if prefix[0]&0x10 != 0 { // odd: the account hash starts at the low nibble of byte 0
		off = 1
	}
	n = 2*len(prefix) - 64 - off
	if n > 4 {
		return n
	}
	for i := range n {
		j := 64 + i + off
		if b := prefix[j/2]; j&1 == 0 {
			nib[i] = b >> 4
		} else {
			nib[i] = b & 0x0f
		}
	}
	return n
}

// clearTrunk stores nil per-slot rather than swapping the pointer, since
// lock-free readers deref c.accountTrunk concurrently.
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

func (c *BranchCache) SetMissCallback(cb MissCallback) {
	if cb == nil {
		c.onMiss.Store(nil)
		return
	}
	c.onMiss.Store(&cb)
}

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
	var nibBuf [4]byte
	if st, n, ok := c.storageRoute(prefix, false, &nibBuf); ok {
		var entry *branchCacheEntry
		if slot := st.slot(&nibBuf, n, false); slot != nil {
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
	var nibBuf [4]byte
	if st, n, ok := c.storageRoute(prefix, false, &nibBuf); ok {
		if slot := st.slot(&nibBuf, n, false); slot != nil {
			for cur := slot.Load(); cur != nil; cur = slot.Load() {
				if slot.CompareAndSwap(cur, entry) {
					return
				}
			}
			// Get is lock-free; ReplaceIfPresent locks the bucket even on a
			// miss, and a miss is the common case here.
		} else if _, present := st.deep.Get(prefix); present && st.deep.ReplaceIfPresent(prefix, entry) {
			return
		}
	}
	c.tailForWrite().Add(maphash.Hash(prefix), entry)
}

// PinEntry copies data; safe to mutate the input after the call.
func (c *BranchCache) PinEntry(prefix []byte, data []byte, step, txN uint64) {
	if c.isCommitmentStateKey(prefix) {
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	stripe := c.putStripe(prefix)
	stripe.Lock()
	defer stripe.Unlock()

	entry := &branchCacheEntry{data: dataCopy, step: step, txN: txN, epoch: c.coh.Epoch()}
	var nibBuf [4]byte
	st, n, ok := c.storageRoute(prefix, true, &nibBuf)
	if !ok {
		c.tailForWrite().Add(maphash.Hash(prefix), entry)
		return
	}
	if slot := st.slot(&nibBuf, n, true); slot != nil {
		// Swap publishes and reads prior occupancy in one step: eviction
		// (Invalidate from a stale Get) takes no put stripe, so a separate
		// load-then-store here would let it interleave.
		if slot.Swap(entry) == nil {
			c.pinnedEntries.Add(1)
		}
		return
	}
	if _, loaded := st.deep.LoadAndStore(prefix, entry); !loaded {
		c.pinnedEntries.Add(1)
	}
}

func (c *BranchCache) PinnedCount() int {
	return int(c.pinnedEntries.Load())
}

func (c *BranchCache) Get(prefix []byte) ([]byte, uint64, bool) {
	if c.isCommitmentStateKey(prefix) {
		return nil, 0, false
	}
	coh := c.coh.Snapshot()
	entry, ok := c.lookup(prefix)
	if !ok || entry.node {
		return nil, 0, false
	}
	if coh.IsStale(entry.txN, entry.epoch) {
		c.Invalidate(prefix)
		c.staleEvicted.Add(1)
		return nil, 0, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return entry.data, entry.step, true
}

// Put copies the input data.
func (c *BranchCache) Put(prefix []byte, data []byte, step, txN uint64) {
	if c.isCommitmentStateKey(prefix) {
		return
	}
	if c.edgeRecordsInCommitment.Load() {
		// v3 keeps one entry per node, so a single record write merges into its node's entry
		// instead of claiming a slot of its own.
		if nodeKey, nibble, ok := v3NodeKeyOf(prefix); ok {
			var records [16][]byte
			var steps, txNums [16]uint64
			records[nibble] = data
			steps[nibble], txNums[nibble] = step, txN
			c.PutChildren(nodeKey, uint16(1)<<nibble, &records, &steps, &txNums)
		}
		return
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	stripe := c.putStripe(prefix)
	stripe.Lock()
	defer stripe.Unlock()

	c.store(prefix, &branchCacheEntry{
		data:  dataCopy,
		step:  step,
		txN:   txN,
		epoch: c.coh.Epoch(),
	})
}

// A v3 read resolves a whole node: it wants every child record under key(P). One entry per record
// puts each child at its own trunk slot one level below its node, so a node sitting at the deepest
// trunk level has all 16 children spill to the LRU tail while v2 keeps that same node in the trunk
// array -- v3 loses a level of trunk coverage and multiplies entries by the branching factor.
// Keying the entry by the node restores v2's slot mapping and its entry count.

// v3NodeDepth reads the nibble depth out of a node key (pack(P) || term).
func v3NodeDepth(nodeKey []byte) (int, bool) {
	if len(nodeKey) == 0 {
		return 0, false
	}
	switch term := nodeKey[len(nodeKey)-1]; {
	case term == 0x00:
		return 2 * (len(nodeKey) - 1), true
	case term&0xf0 == 0xf0:
		return 2*len(nodeKey) - 1, true
	default:
		return 0, false
	}
}

func v3NodeNibble(nodeKey []byte, depth, i int) byte {
	if depth&1 == 1 && i == depth-1 { // an odd path keeps its last nibble in the term byte
		return nodeKey[len(nodeKey)-1] & 0x0f
	}
	if i&1 == 0 {
		return nodeKey[i/2] >> 4
	}
	return nodeKey[i/2] & 0x0f
}

// v3NodeKeyOf splits an edge record key into the node it belongs to and its child nibble.
func v3NodeKeyOf(recordKey []byte) ([]byte, int, bool) {
	if len(recordKey) < 2 || recordKey[len(recordKey)-1]&0xf0 != 0x80 {
		return nil, 0, false
	}
	return recordKey[:len(recordKey)-1], int(recordKey[len(recordKey)-1] & 0x0f), true
}

func (c *BranchCache) v3NodeTrunkSlot(nodeKey []byte, forWrite bool) *atomic.Pointer[branchCacheEntry] {
	if c.trunkDisabled {
		return nil
	}
	depth, ok := v3NodeDepth(nodeKey)
	if !ok || depth == 0 || depth > trunkDepthFull {
		return nil
	}
	var nib [4]byte
	for i := range depth {
		nib[i] = v3NodeNibble(nodeKey, depth, i)
	}
	return c.accountTrunk.slot(&nib, depth, forWrite)
}

// encodeNodeRecords packs a node's records into one value: present mask, a length per present
// child, then the payloads. Returns nil for a record too long to describe, which is not cached.
func encodeNodeRecords(present uint16, records *[16][]byte) []byte {
	size := 2 + 2*bits.OnesCount16(present)
	for bitset := present; bitset != 0; bitset &= bitset - 1 {
		n := len(records[bits.TrailingZeros16(bitset&-bitset)])
		if n > math.MaxUint16 {
			return nil
		}
		size += n
	}
	blob := make([]byte, size)
	binary.BigEndian.PutUint16(blob, present)
	pos := 2
	for bitset := present; bitset != 0; bitset &= bitset - 1 {
		binary.BigEndian.PutUint16(blob[pos:], uint16(len(records[bits.TrailingZeros16(bitset&-bitset)])))
		pos += 2
	}
	for bitset := present; bitset != 0; bitset &= bitset - 1 {
		pos += copy(blob[pos:], records[bits.TrailingZeros16(bitset&-bitset)])
	}
	return blob
}

// decodeNodeRecords slices the payloads back out. They alias the cache's buffer, so a caller
// keeping them past the read must copy.
func decodeNodeRecords(blob []byte, out *[16][]byte) (uint16, bool) {
	if len(blob) < 2 {
		return 0, false
	}
	present := binary.BigEndian.Uint16(blob)
	header := 2 + 2*bits.OnesCount16(present)
	if len(blob) < header {
		return 0, false
	}
	pos, lenPos := header, 2
	for bitset := present; bitset != 0; bitset &= bitset - 1 {
		nibble := bits.TrailingZeros16(bitset & -bitset)
		size := int(binary.BigEndian.Uint16(blob[lenPos:]))
		lenPos += 2
		if pos+size > len(blob) {
			return 0, false
		}
		out[nibble] = blob[pos : pos+size : pos+size]
		pos += size
	}
	return present, true
}

func (c *BranchCache) storeNode(nodeKey []byte, entry *branchCacheEntry) {
	if isRootPrefix(nodeKey) {
		c.root.Store(entry)
		return
	}
	if slot := c.v3NodeTrunkSlot(nodeKey, true); slot != nil {
		slot.Store(entry)
		return
	}
	c.tailForWrite().Add(maphash.Hash(nodeKey), entry)
}

// peekNode reads without touching the hit counters: it serves the writer's merge, and counting it
// would make the hit rate report the writer's own probes.
func (c *BranchCache) peekNode(nodeKey []byte) *branchCacheEntry {
	if isRootPrefix(nodeKey) {
		return c.root.Load()
	}
	if slot := c.v3NodeTrunkSlot(nodeKey, false); slot != nil {
		return slot.Load()
	}
	if tail := c.tail.Load(); tail != nil {
		if entry, ok := tail.Get(maphash.Hash(nodeKey)); ok {
			return entry
		}
	}
	return nil
}

func (c *BranchCache) lookupNode(nodeKey []byte) *branchCacheEntry {
	if isRootPrefix(nodeKey) {
		entry := c.root.Load()
		if entry == nil {
			c.rootMisses.Add(1)
			return nil
		}
		c.rootHits.Add(1)
		return entry
	}
	if slot := c.v3NodeTrunkSlot(nodeKey, false); slot != nil {
		if entry := slot.Load(); entry != nil {
			c.trunkHits.Add(1)
			return entry
		}
		c.trunkMisses.Add(1)
		return nil
	}
	tail := c.tail.Load()
	if tail == nil {
		c.tailMisses.Add(1)
		return nil
	}
	entry, ok := tail.Get(maphash.Hash(nodeKey))
	if !ok {
		c.tailMisses.Add(1)
		return nil
	}
	c.tailHits.Add(1)
	return entry
}

// GetNode serves a whole node's edge records. The returned slices alias the cache's buffer.
func (c *BranchCache) GetNode(nodeKey []byte, out *[16][]byte) (present uint16, step uint64, ok bool) {
	coh := c.coh.Snapshot()
	entry := c.lookupNode(nodeKey)
	if entry == nil || !entry.node {
		if entry == nil {
			c.fireOnMiss(nodeKey)
		}
		return 0, 0, false
	}
	if coh.IsStale(entry.txN, entry.epoch) {
		c.InvalidateNode(nodeKey)
		c.staleEvicted.Add(1)
		return 0, 0, false
	}
	present, ok = decodeNodeRecords(entry.data, out)
	if !ok {
		return 0, 0, false
	}
	c.bytesServed.Add(uint64(len(entry.data)))
	return present, entry.step, true
}

func (c *BranchCache) InvalidateNode(nodeKey []byte) {
	if isRootPrefix(nodeKey) {
		c.root.Store(nil)
		return
	}
	if slot := c.v3NodeTrunkSlot(nodeKey, false); slot != nil {
		slot.Store(nil)
		return
	}
	if tail := c.tail.Load(); tail != nil {
		tail.Remove(maphash.Hash(nodeKey))
	}
}

// invalidateChild drops one record and keeps the node's siblings: a tombstoned edge should not cost
// the whole node its cache entry.
func (c *BranchCache) invalidateChild(nodeKey []byte, nibble int) {
	stripe := c.putStripe(nodeKey)
	stripe.Lock()
	defer stripe.Unlock()

	entry := c.peekNode(nodeKey)
	if entry == nil || !entry.node {
		return
	}
	var records [16][]byte
	present, ok := decodeNodeRecords(entry.data, &records)
	bit := uint16(1) << nibble
	if !ok || present&bit == 0 {
		return
	}
	present &^= bit
	records[nibble] = nil
	if present == 0 {
		c.InvalidateNode(nodeKey)
		return
	}
	blob := encodeNodeRecords(present, &records)
	if blob == nil {
		c.InvalidateNode(nodeKey)
		return
	}
	c.storeNode(nodeKey, &branchCacheEntry{data: blob, step: entry.step, txN: entry.txN, epoch: entry.epoch, node: true})
}

// PutChildren merges records into their node's single entry. A publish carries only the records
// that changed, so dropping the siblings would send every later read back to the db.
func (c *BranchCache) PutChildren(nodeKey []byte, present uint16, records *[16][]byte, steps, txNums *[16]uint64) {
	if present == 0 {
		return
	}
	stripe := c.putStripe(nodeKey)
	stripe.Lock()
	defer stripe.Unlock()

	merged := *records
	mergedPresent := present
	var step, txN uint64
	for bitset := present; bitset != 0; bitset &= bitset - 1 {
		nibble := bits.TrailingZeros16(bitset & -bitset)
		if len(merged[nibble]) == 0 {
			mergedPresent &^= uint16(1) << nibble
			continue
		}
		step = max(step, steps[nibble])
		// IsStale fires at txN >= floor, so the newest child decides: below the floor, all are.
		txN = max(txN, txNums[nibble])
	}
	if existing := c.peekNode(nodeKey); existing != nil && existing.node {
		var old [16][]byte
		if oldPresent, ok := decodeNodeRecords(existing.data, &old); ok {
			for bitset := oldPresent &^ mergedPresent; bitset != 0; bitset &= bitset - 1 {
				nibble := bits.TrailingZeros16(bitset & -bitset)
				merged[nibble] = old[nibble]
				mergedPresent |= uint16(1) << nibble
			}
			step = max(step, existing.step)
			txN = max(txN, existing.txN)
		}
	}
	if mergedPresent == 0 {
		return
	}
	blob := encodeNodeRecords(mergedPresent, &merged)
	if blob == nil {
		return
	}
	c.storeNode(nodeKey, &branchCacheEntry{data: blob, step: step, txN: txN, epoch: c.coh.Epoch(), node: true})
}

func (c *BranchCache) Invalidate(prefix []byte) {
	if c.edgeRecordsInCommitment.Load() {
		if nodeKey, nibble, ok := v3NodeKeyOf(prefix); ok {
			c.invalidateChild(nodeKey, nibble)
		} else {
			c.InvalidateNode(prefix)
		}
		return
	}
	if isRootPrefix(prefix) {
		c.root.Store(nil)
		return
	}
	if slot := c.trunkSlot(prefix, false); slot != nil {
		slot.Store(nil)
		return
	}
	var nibBuf [4]byte
	if st, n, ok := c.storageRoute(prefix, false, &nibBuf); ok {
		if slot := st.slot(&nibBuf, n, false); slot != nil {
			if slot.Swap(nil) != nil {
				c.pinnedEntries.Add(-1)
			}
		} else if _, loaded := st.deep.LoadAndDelete(prefix); loaded {
			c.pinnedEntries.Add(-1)
		}
	}
	if tail := c.tail.Load(); tail != nil {
		tail.Remove(maphash.Hash(prefix))
	}
}

// Unwind is O(1) and scan-free: it bumps the epoch and lowers the unwind
// floor, so stale entries drop lazily on their next Get.
func (c *BranchCache) Unwind(unwindToTxN uint64) {
	c.coh.Unwind(unwindToTxN)
}

// Clear holds every writer stripe so a publication cannot cross generations.
func (c *BranchCache) Clear() {
	c.lockAllPutStripes()
	defer c.unlockAllPutStripes()

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
	c.lastPublishedPinnedHits.Store(0)
	c.lastPublishedPinnedMisses.Store(0)
	c.tailHits.Store(0)
	c.tailMisses.Store(0)
	c.bytesServed.Store(0)
	c.staleEvicted.Store(0)
	c.coh.Reset()
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
