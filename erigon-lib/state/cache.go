package state

import (
	"fmt"

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
	trace bool
}

type domainGetFromFileCacheItem struct {
	lvl uint8
	v   []byte // pointer to `mmap` - if .kv file is not compressed
}

var domainGetFromFileCacheLimit = uint32(dbg.EnvInt("D_LRU", 128))

func NewDomainGetFromFileCache(trace bool) *DomainGetFromFileCache {
	c, err := freelru.New[u128, domainGetFromFileCacheItem](domainGetFromFileCacheLimit, u128noHash)
	if err != nil {
		panic(err)
	}
	return &DomainGetFromFileCache{LRU: c, trace: trace}
}

func (c *DomainGetFromFileCache) LogStats(dt kv.Domain) {
	if c == nil || !c.trace {
		return
	}

	m := c.Metrics()
	log.Warn("[dbg] DomainGetFromFileCache", "a", dt.String(), "hit", m.Hits, "total", m.Hits+m.Misses, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", domainGetFromFileCacheLimit, "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)))
}
