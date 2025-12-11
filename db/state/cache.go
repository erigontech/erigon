package state

import (
	"fmt"
	"sync"

	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func u32noHash(u uint32) uint32        { return u }            //nolint
func u64noHash(u uint64) uint32        { return uint32(u) }    //nolint
func u128noHash(u u128) uint32         { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32         { return uint32(u.hi) } //nolint
type u128 struct{ hi, lo uint64 }      //nolint
type u192 struct{ hi, lo, ext uint64 } //nolint

type DomainGetFromFileCache struct {
	*freelru.LRU[uint64, domainGetFromFileCacheItem]
	enabled, trace bool
	limit          uint32
}

// nolint
type domainGetFromFileCacheItem struct {
	lvl uint8
	v   []byte
}

var (
	domainGetFromFileCacheLimit   = uint32(dbg.EnvInt("D_LRU", 10_000))
	domainGetFromFileCacheTrace   = dbg.EnvBool("D_LRU_TRACE", false)
	domainGetFromFileCacheEnabled = dbg.EnvBool("D_LRU_ENABLED", true)
)

func NewDomainGetFromFileCache(limit uint32) *DomainGetFromFileCache {
	c, err := freelru.New[uint64, domainGetFromFileCacheItem](limit, u64noHash)
	if err != nil {
		panic(err)
	}
	return &DomainGetFromFileCache{LRU: c, enabled: domainGetFromFileCacheEnabled, trace: domainGetFromFileCacheTrace, limit: limit}
}

func (c *DomainGetFromFileCache) SetTrace(v bool) { c.trace = v }
func (c *DomainGetFromFileCache) LogStats(dt kv.Domain) {
	if c == nil {
		return
	}
	if !c.enabled || !c.trace {
		return
	}
	m := c.Metrics()
	log.Warn("[dbg] DomainGetFromFileCache", "a", dt.String(), "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)), "hit", m.Hits, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", c.limit)
}

func newDomainVisible(name kv.Domain, files []visibleFile) *domainVisible {
	d := &domainVisible{
		name:  name,
		files: files,
	}
	limit := domainGetFromFileCacheLimit
	if name == kv.CodeDomain {
		limit = limit / 10 // CodeDomain has compressed values - means cache will store values (instead of pointers to mmap)
	}
	if limit == 0 {
		domainGetFromFileCacheEnabled = false
	}
	d.caches = &sync.Pool{New: func() any { return NewDomainGetFromFileCache(limit) }}
	return d
}

func (v *domainVisible) newGetFromFileCache() *DomainGetFromFileCache {
	if !domainGetFromFileCacheEnabled {
		return nil
	}
	return v.caches.Get().(*DomainGetFromFileCache)
}
func (v *domainVisible) returnGetFromFileCache(c *DomainGetFromFileCache) {
	if c == nil {
		return
	}
	c.LogStats(v.name)
	v.caches.Put(c)
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

func newIIVisible(name string, files []visibleFile) *iiVisible {
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
