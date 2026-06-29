package state

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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

	// debug: file-reads (cache misses) served per file level and per key length,
	// to explain which level/key shape drives reads. Counted only when trace is on.
	reads      [6]atomic.Uint64
	readsByLen [48]atomic.Uint64
}

// AddRead records a file-read served from file index `level` for a key of keyLen.
func (c *DomainGetFromFileCache) AddRead(level, keyLen int) {
	if c == nil || !c.trace {
		return
	}
	if level > 5 {
		level = 5
	}
	c.reads[level].Add(1)
	if keyLen >= 0 && keyLen < len(c.readsByLen) {
		c.readsByLen[keyLen].Add(1)
	}
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
		log.Warn("[dbg] DomainGetFromFileCache", "a", dt.String(), "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)), "hit", m.Hits, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", c.limit,
			"readsPerLevel", c.readsPerLevelString(), "readsByLen", c.readsByLenString())
	}
}

func (c *DomainGetFromFileCache) readsPerLevelString() string {
	var b []string
	for lvl := range c.reads {
		if n := c.reads[lvl].Load(); n > 0 {
			label := fmt.Sprintf("L%d", lvl)
			if lvl == 5 {
				label = "recent+"
			}
			b = append(b, fmt.Sprintf("%s:%d", label, n))
		}
	}
	return "[" + strings.Join(b, " ") + "]"
}

func (c *DomainGetFromFileCache) readsByLenString() string {
	var b []string
	for keyLen := range c.readsByLen {
		if n := c.readsByLen[keyLen].Load(); n > 0 {
			b = append(b, fmt.Sprintf("%d:%d", keyLen, n))
		}
	}
	return "[" + strings.Join(b, " ") + "]"
}

func newDomainVisible(name kv.Domain, files visibleFiles) *domainVisible {
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
	// One cache per visibleFiles snapshot, shared (concurrent-safe) by every
	// DomainRoTx opened against it. The snapshot is immutable, so its mmap-pointer
	// entries never go stale: a file change builds a new domainVisible with a fresh
	// cache and drops this one — no invalidation.
	if domainGetFromFileCacheEnabled {
		d.cache = NewDomainGetFromFileCache(limit)
	}
	return d
}

func (v *domainVisible) newGetFromFileCache() *DomainGetFromFileCache {
	return v.cache
}
func (v *domainVisible) returnGetFromFileCache(c *DomainGetFromFileCache) {
	if c == nil {
		return
	}
	c.LogStats(v.name) // shared cache: stats are cumulative for this snapshot
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
