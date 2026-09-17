package state

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
)

func u32noHash(u uint32) uint32        { return u }            //nolint
func u64noHash(u uint64) uint32        { return uint32(u) }    //nolint
func u128noHash(u u128) uint32         { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32         { return uint32(u.hi) } //nolint
type u128 struct{ hi, lo uint64 }      //nolint
type u192 struct{ hi, lo, ext uint64 } //nolint

// domainGetFromFileCacheItem is a latest-value lookup in the files of one visible set: lvl indexes those files,
// lo is the second half of the key hash (the cache is keyed by the first).
type domainGetFromFileCacheItem struct {
	found bool
	lvl   uint8
	lo    uint64
	v     []byte
}

var (
	domainGetFromFileCacheSize    = dbg.EnvDataSize("D_LRU_SIZE", 64*datasize.MB)
	domainGetFromFileCacheEnabled = dbg.EnvBool("D_LRU_ENABLED", true)
	domainGetFromFileCacheTrace   = dbg.EnvBool("D_LRU_TRACE", false)

	domainGetFromFileCacheHits, domainGetFromFileCacheMisses [kv.DomainLen]atomic.Uint64
)

func init() {
	if domainGetFromFileCacheTrace {
		go logDomainGetFromFileCacheStats(30 * time.Second)
	}
}

func logDomainGetFromFileCacheStats(every time.Duration) {
	for range time.Tick(every) {
		for d := kv.Domain(0); d < kv.DomainLen; d++ {
			hits, misses := domainGetFromFileCacheHits[d].Load(), domainGetFromFileCacheMisses[d].Load()
			if hits+misses > 0 {
				log.Warn("[dbg] D_LRU", "domain", d.String(), "ratio", fmt.Sprintf("%.2f", float64(hits)/float64(hits+misses)), "hits", hits, "misses", misses)
			}
		}
	}
}

func countDomainGetFromFileCache(d kv.Domain, hit bool) {
	if !domainGetFromFileCacheTrace {
		return
	}
	if hit {
		domainGetFromFileCacheHits[d].Add(1)
	} else {
		domainGetFromFileCacheMisses[d].Add(1)
	}
}

// newDomainVisible gives each visible files set one cache shared by all its txs: a value points into a file of the set,
// and those files stay open while any tx can reach the set.
func newDomainVisible(name kv.Domain, files visibleFiles) *domainVisible {
	d := &domainVisible{name: name, files: files}
	if domainGetFromFileCacheEnabled && domainGetFromFileCacheSize > 0 {
		d.cache = cache.NewByteLRU(domainGetFromFileCacheSize, func(_ uint64, it domainGetFromFileCacheItem) int64 {
			return int64(len(it.v)) + cache.ByteLRUEntryOverheadBytes + int64(unsafe.Sizeof(it))
		})
	}
	return d
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
