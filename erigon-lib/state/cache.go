package state

import (
	"github.com/elastic/go-freelru"
)

func u32noHash(u uint32) uint32        { return u }            //nolint
func u64noHash(u uint64) uint32        { return uint32(u) }    //nolint
func u128noHash(u u128) uint32         { return uint32(u.hi) } //nolint
func u192noHash(u u192) uint32         { return uint32(u.hi) } //nolint
type u128 struct{ hi, lo uint64 }      //nolint
type u192 struct{ hi, lo, ext uint64 } //nolint

type DomainGetFromFileCache struct {
	*freelru.LRU[u128, domainGetFromFileCacheItem]
}

type domainGetFromFileCacheItem struct {
	lvl uint8
	v   []byte // pointer to `mmap` - if .kv file is not compressed
}

const latestStateCachePerDomain = 128

func NewDomainGetFromFileCache() *DomainGetFromFileCache {
	c, err := freelru.New[u128, domainGetFromFileCacheItem](latestStateCachePerDomain, u128noHash)
	if err != nil {
		panic(err)
	}
	return &DomainGetFromFileCache{c}
}
