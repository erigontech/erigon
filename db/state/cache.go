package state

import (
	"fmt"
	"sync"

	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func u32noHash(u uint32) uint32 { return u }            //nolint
func u64noHash(u uint64) uint32 { return uint32(u) }    //nolint
func u128noHash(u u128) uint32  { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32  { return uint32(u.hi) } //nolint
type (
	u128 struct{ hi, lo uint64 }      //nolint
	u192 struct{ hi, lo, ext uint64 } //nolint
)

const maxFileCacheKeyLen = length.Addr + length.Hash

// fileCacheKey holds the full key inline: the hash only picks the bucket, so keys with an equal hash stay distinct.
type fileCacheKey struct {
	hi uint64
	b  [maxFileCacheKeyLen]byte
	n  uint8
}

func newFileCacheKey(k []byte, hi uint64) (key fileCacheKey, ok bool) {
	if len(k) > maxFileCacheKeyLen {
		return key, false
	}
	key.hi, key.n = hi, uint8(copy(key.b[:], k))
	return key, true
}

func fileCacheKeyHash(k fileCacheKey) uint32 { return uint32(k.hi) }

type DomainGetFromFileCache struct {
	*freelru.LRU[fileCacheKey, domainGetFromFileCacheItem]
	enabled, trace bool
	limit          uint32
}

// nolint
type domainGetFromFileCacheItem struct {
	lvl uint8
	v   []byte
}

func (c *DomainGetFromFileCache) get(k []byte, hi uint64) (domainGetFromFileCacheItem, bool) {
	key, ok := newFileCacheKey(k, hi)
	if !ok {
		return domainGetFromFileCacheItem{}, false
	}
	return c.Get(key)
}

// add skips keys longer than maxFileCacheKeyLen.
func (c *DomainGetFromFileCache) add(k []byte, hi uint64, item domainGetFromFileCacheItem) {
	if key, ok := newFileCacheKey(k, hi); ok {
		c.Add(key, item)
	}
}

var (
	domainGetFromFileCacheLimit   = uint32(dbg.EnvInt("D_LRU", 10_000))
	domainGetFromFileCacheTrace   = dbg.EnvBool("D_LRU_TRACE", false)
	domainGetFromFileCacheEnabled = dbg.EnvBool("D_LRU_ENABLED", true)
)

func NewDomainGetFromFileCache(limit uint32) *DomainGetFromFileCache {
	c, err := freelru.New[fileCacheKey, domainGetFromFileCacheItem](limit, fileCacheKeyHash)
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
	if m.Hits > 0 {
		log.Warn("[dbg] DomainGetFromFileCache", "a", dt.String(), "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)), "hit", m.Hits, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", c.limit)
	}
}

func newDomainVisible(name kv.Domain, files visibleFiles) *domainVisible {
	d := &domainVisible{
		name:  name,
		files: files,
	}
	limit := domainGetFromFileCacheLimit
	if name == kv.CodeDomain {
		limit /= 10 // CodeDomain has compressed values - means cache will store values (instead of pointers to mmap)
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
	*freelru.LRU[fileCacheKey, iiSeekInFilesCacheItem] // key -> {requestedTxNum, foundTxNum}

	hit, total int
	trace      bool
}
type iiSeekInFilesCacheItem struct {
	requested, found uint64
	// position of `found` in .ef file fileIdx, which addresses the .vi accessor.
	// A file index is stable because each cache pool serves one visible file list.
	keyOrdinal, rank uint64
	fileIdx          int
}

func (c *IISeekInFilesCache) get(k []byte, hi uint64) (iiSeekInFilesCacheItem, bool) {
	key, ok := newFileCacheKey(k, hi)
	if !ok {
		return iiSeekInFilesCacheItem{}, false
	}
	return c.Get(key)
}

// add skips keys longer than maxFileCacheKeyLen.
func (c *IISeekInFilesCache) add(k []byte, hi uint64, item iiSeekInFilesCacheItem) {
	if key, ok := newFileCacheKey(k, hi); ok {
		c.Add(key, item)
	}
}

func NewIISeekInFilesCache() *IISeekInFilesCache {
	if !iiGetFromFileCacheEnabled {
		return nil
	}
	c, err := freelru.New[fileCacheKey, iiSeekInFilesCacheItem](iiGetFromFileCacheLimit, fileCacheKeyHash)
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
