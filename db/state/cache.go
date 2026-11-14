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

func newDomainVisible(name kv.Domain, files visibleFiles) *domainVisible {
	return &domainVisible{name: name, files: files}
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
