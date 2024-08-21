package state

import (
	"fmt"
	"sync"

	"github.com/elastic/go-freelru"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

func u32noHash(u uint32) uint32        { return u }            //nolint
func u64noHash(u uint64) uint32        { return uint32(u) }    //nolint
func u128noHash(u u128) uint32         { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32         { return uint32(u.hi) } //nolint
type u128 struct{ hi, lo uint64 }      //nolint
type u192 struct{ hi, lo, ext uint64 } //nolint

type DomainGetFromFileCache struct {
	*freelru.LRU[u128, domainGetFromFileCacheItem]
	enabled, trace bool
}

// nolint
type domainGetFromFileCacheItem struct {
	lvl uint8
	v   []byte // pointer to `mmap` - if .kv file is not compressed
}

var (
	domainGetFromFileCacheLimit   = uint32(dbg.EnvInt("D_LRU", 4096))
	domainGetFromFileCacheTrace   = dbg.EnvBool("D_LRU_TRACE", false)
	domainGetFromFileCacheEnabled = dbg.EnvBool("D_LRU_ENABLED", true)
)

func NewDomainGetFromFileCache() *DomainGetFromFileCache {
	c, err := freelru.New[u128, domainGetFromFileCacheItem](domainGetFromFileCacheLimit, u128noHash)
	if err != nil {
		panic(err)
	}
	return &DomainGetFromFileCache{LRU: c, enabled: domainGetFromFileCacheEnabled, trace: domainGetFromFileCacheTrace}
}

func (c *DomainGetFromFileCache) SetTrace(v bool) { c.trace = v }
func (c *DomainGetFromFileCache) LogStats(dt kv.Domain) {
	if c == nil || !c.enabled || !c.trace {
		return
	}
	m := c.Metrics()
	log.Warn("[dbg] DomainGetFromFileCache", "a", dt.String(), "hit", m.Hits, "total", m.Hits+m.Misses, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", domainGetFromFileCacheLimit, "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)))
}

func NewDomainGetFromFileCacheAny() any { return NewDomainGetFromFileCache() }
func newDomainVisible(name kv.Domain, files []visibleFile) *domainVisible {
	d := &domainVisible{
		name:   name,
		files:  files,
		caches: &sync.Pool{New: NewDomainGetFromFileCacheAny},
	}
	// Not on hot-path: better pre-alloc here
	d.preAlloc()
	return d
}
func (v *domainVisible) preAlloc() {
	var preAlloc [10]any
	for i := 0; i < len(preAlloc); i++ {
		preAlloc[i] = v.caches.Get()
	}
	for i := 0; i < len(preAlloc); i++ {
		v.caches.Put(preAlloc[i])
	}
}

func (v *domainVisible) newGetFromFileCache() *DomainGetFromFileCache {
	if !domainGetFromFileCacheEnabled {
		return nil
	}
	if v.name == kv.CommitmentDomain {
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
	iiGetFromFileCacheLimit = uint32(dbg.EnvInt("II_LRU", 4096))
	iiGetFromFileCacheTrace = dbg.EnvBool("II_LRU_TRACE", false)
)

type IISeekInFilesCache struct {
	*freelru.LRU[u128, iiSeekInFilesCacheItem]
	hit, total int
	trace      bool
}
type iiSeekInFilesCacheItem struct {
	requested, found uint64
}

func NewIISeekInFilesCache() *IISeekInFilesCache {
	c, err := freelru.New[u128, iiSeekInFilesCacheItem](iiGetFromFileCacheLimit, u128noHash)
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
	log.Warn("[dbg] IISeekInFilesCache", "a", fileBaseName, "hit", c.hit, "total", c.total, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", iiGetFromFileCacheLimit, "ratio", fmt.Sprintf("%.2f", float64(c.hit)/float64(c.total)))
}

func NewIISeekInFilesCacheAny() any { return NewIISeekInFilesCache() }
func newIIVisible(name string, files []visibleFile) *iiVisible {
	ii := &iiVisible{
		name:   name,
		files:  files,
		caches: &sync.Pool{New: NewIISeekInFilesCacheAny},
	}
	// Not on hot-path: better pre-alloc here
	ii.preAlloc()
	return ii
}
func (v *iiVisible) preAlloc() {
	var preAlloc [10]any
	for i := 0; i < len(preAlloc); i++ {
		preAlloc[i] = v.caches.Get()
	}
	for i := 0; i < len(preAlloc); i++ {
		v.caches.Put(preAlloc[i])
	}
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
