package state

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/cache"
)

func u32noHash(u uint32) uint32 { return u }            //nolint
func u64noHash(u uint64) uint32 { return uint32(u) }    //nolint
func u128noHash(u u128) uint32  { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32  { return uint32(u.hi) } //nolint
type (
	u128 struct{ hi, lo uint64 }      //nolint
	u192 struct{ hi, lo, ext uint64 } //nolint
)

// nolint
type domainGetFromFileCacheItem struct {
	found bool
	lvl   uint8
	lo    uint64
	v     []byte
}

var domainGetFromFileCacheSize = dbg.EnvDataSize("D_LRU_SIZE", 320*datasize.MB) / datasize.ByteSize(kv.DomainLen-1)

// newDomainVisible gives each visible files set one cache shared by all its txs. It holds only values that point
// into the mmap of a file of the set, which stays open while any tx can reach the set: compressed values are heap copies.
func newDomainVisible(name kv.Domain, compression seg.FileCompression, files visibleFiles) *domainVisible {
	d := &domainVisible{
		name:  name,
		files: files,
	}
	if domainGetFromFileCacheSize > 0 && name != kv.CommitmentDomain && !compression.Has(seg.CompressVals) {
		d.readOnce = &readOnceFilter{}
		d.cache = cache.NewByteLRU(domainGetFromFileCacheSize, func(_ uint64, it domainGetFromFileCacheItem) int64 {
			return cache.ByteLRUEntryOverheadBytes + int64(unsafe.Sizeof(it))
		})
	}
	return d
}

// readOnceFilter keeps a key out of the cache until its second read: an otter insert costs
// far more than a lookup, and most keys are read once.
type readOnceFilter struct {
	bits    [1 << 14]atomic.Uint64
	sampled atomic.Int64
}

func (f *readOnceFilter) readBefore(hi uint64) bool {
	i := hi & (uint64(len(f.bits))*64 - 1)
	bit := uint64(1) << (i % 64)
	if f.bits[i/64].Or(bit)&bit != 0 {
		return true
	}
	// Counts 1/64 of the new bits to keep the counter off the hot path; clears at half full.
	if hi>>58 == 0 && f.sampled.Add(1) > int64(len(f.bits))/2 {
		f.sampled.Store(0)
		for j := range f.bits {
			f.bits[j].Store(0)
		}
	}
	return false
}

var (
	iiGetFromFileCacheLimit   = uint32(dbg.EnvInt("II_LRU", 4096))
	iiGetFromFileCacheTrace   = dbg.EnvBool("II_LRU_TRACE", false)
	iiGetFromFileCacheEnabled = dbg.EnvBool("II_LRU_ENABLED", true)
)

type IISeekInFilesCache struct {
	*freelru.LRU[uint64, iiSeekInFilesCacheItem] // murmur3(key) -> {requestedTxNum, foundTxNum}

	hit, total int
	trace      bool
}
type iiSeekInFilesCacheItem struct {
	requested, found uint64
	lo               uint64 // second half of the key hash: the LRU key alone can collide
	// position of `found` in .ef file fileIdx, which addresses the .vi accessor.
	// A file index is stable because each cache pool serves one visible file list.
	keyOrdinal, rank uint64
	fileIdx          int
}

func NewIISeekInFilesCache() *IISeekInFilesCache {
	if !iiGetFromFileCacheEnabled {
		return nil
	}
	c, err := freelru.New[uint64, iiSeekInFilesCacheItem](iiGetFromFileCacheLimit, u64noHash)
	if err != nil {
		panic(err)
	}
	return &IISeekInFilesCache{LRU: c, trace: iiGetFromFileCacheTrace}
}
func (c *IISeekInFilesCache) SetTrace(v bool) { c.trace = v }
func (c *IISeekInFilesCache) LogStats(fileBaseName string) {
	if c == nil || !c.trace {
		return
	}
	m := c.Metrics()
	log.Warn("[dbg] II_LRU", "a", fileBaseName, "ratio", fmt.Sprintf("%.2f", float64(c.hit)/float64(c.total)), "hit", c.hit, "collisions", m.Collisions, "evictions", m.Evictions, "inserts", m.Inserts, "removals", m.Removals, "limit", iiGetFromFileCacheLimit)
}

func newIIVisible(name string, files visibleFiles) *iiVisible {
	if iiGetFromFileCacheLimit == 0 {
		iiGetFromFileCacheEnabled = false
	}
	ii := &iiVisible{
		name:   name,
		files:  files,
		caches: &sync.Pool{New: func() any { return NewIISeekInFilesCache() }},
	}
	return ii
}

func (v *iiVisible) newSeekInFilesCache() *IISeekInFilesCache {
	return v.caches.Get().(*IISeekInFilesCache)
}

func (v *iiVisible) returnSeekInFilesCache(c *IISeekInFilesCache) {
	if c == nil {
		return
	}
	c.LogStats(v.name)
	v.caches.Put(c)
}
