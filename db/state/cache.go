package state

import (
	"flag"
	"fmt"
	"sync"

	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func u32noHash(u uint32) uint32        { return u }            //nolint
func u64noHash(u uint64) uint32        { return uint32(u) }    //nolint
func u128noHash(u u128) uint32         { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32         { return uint32(u.hi) } //nolint
type u128 struct{ hi, lo uint64 }      //nolint
type u192 struct{ hi, lo, ext uint64 } //nolint

type DomainGetFromFileCache struct {
	*freelru.ShardedLRU[uint64, domainGetFromFileCacheItem]
	enabled, trace bool
	limit          uint32
}

// v is a heap-owned copy and the txnum range is stored directly rather than as
// a file index, so entries outlive the files they were read from.
type domainGetFromFileCacheItem struct {
	startTxNum, endTxNum uint64
	v                    []byte
}

var (
	domainGetFromFileCacheLimit   = uint32(dbg.EnvInt("D_LRU", 2_500_000))
	domainGetFromFileCacheTrace   = dbg.EnvBool("D_LRU_TRACE", false)
	domainGetFromFileCacheEnabled = dbg.EnvBool("D_LRU_ENABLED", true)
)

func NewDomainGetFromFileCache(limit uint32) *DomainGetFromFileCache {
	c, err := freelru.NewSharded[uint64, domainGetFromFileCacheItem](limit, u64noHash)
	if err != nil {
		panic(err)
	}
	return &DomainGetFromFileCache{ShardedLRU: c, enabled: domainGetFromFileCacheEnabled, trace: domainGetFromFileCacheTrace, limit: limit}
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

// newDomainCache caps capacity by the key count of the visible files: freelru
// preallocates its full capacity upfront, so an uncapped cache costs hundreds
// of MB per aggregator even when the files hold almost no data.
func newDomainCache(name kv.Domain, files visibleFiles) *DomainGetFromFileCache {
	if !domainGetFromFileCacheEnabled {
		return nil
	}
	limit := domainGetFromFileCacheLimit
	if flag.Lookup("test.v") != nil {
		limit = 10_000
	}
	switch name {
	case kv.CommitmentDomain, kv.ReceiptDomain, kv.RCacheDomain:
		return nil
	case kv.StorageDomain:
		limit += limit / 2
	case kv.CodeDomain:
		limit /= 100 // CodeDomain has compressed values - means cache will store values (instead of pointers to mmap)
	}
	var fileKeys uint64
	for _, f := range files {
		fileKeys += uint64(f.src.decompressor.Count() / 2)
	}
	if fileKeys < uint64(limit) {
		limit = uint32(fileKeys)
	}
	if limit == 0 {
		return nil
	}
	return NewDomainGetFromFileCache(limit)
}

// newDomainVisible shares one cache across all RoTxs of a visible-files
// generation. A merge rewrites existing data without changing lookup results,
// so an unchanged EndTxNum lets the previous cache carry over; a changed one
// means the file-visible data itself moved and the cache must be rebuilt.
func newDomainVisible(name kv.Domain, files visibleFiles, prev *domainVisible) *domainVisible {
	d := &domainVisible{
		name:  name,
		files: files,
	}
	if prev != nil && prev.cache != nil && prev.files.EndTxNum() == files.EndTxNum() {
		d.cache = prev.cache
	} else {
		d.cache = newDomainCache(name, files)
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
