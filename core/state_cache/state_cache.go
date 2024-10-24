package state_cache

import (
	"github.com/erigontech/erigon-lib/kv"
	lru "github.com/hashicorp/golang-lru/v2"
)

const cacheSize = 500_000 // this should be equivalent to around 1MB of memory

type StateCache struct {
	caches map[kv.Domain]*lru.Cache[string, []byte]
}

func getCacheCapacity(dmn kv.Domain) int {
	switch dmn {
	case kv.AccountsDomain:
		return 20_000
	case kv.StorageDomain:
		return 100_000
	default:
		return 2_000
	}
}

func NewStateCache() *StateCache {
	return &StateCache{
		caches: make(map[kv.Domain]*lru.Cache[string, []byte]),
	}
}

func (sc *StateCache) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	cache, ok := sc.caches[domain]
	if !ok {
		return nil, false
	}
	return cache.Get(string(key))
}

func (sc *StateCache) Put(domain kv.Domain, key []byte, value []byte) {
	cache, ok := sc.caches[domain]
	if !ok {
		cache, _ = lru.New[string, []byte](getCacheCapacity(domain))
		sc.caches[domain] = cache
	}
	cache.Add(string(key), value)
}

func (sc *StateCache) Delete(domain kv.Domain, key []byte) {
	cache, ok := sc.caches[domain]
	if !ok {
		return
	}
	// do it with no alloc with unsafe
	cache.Remove(string(key))
}

func (sc *StateCache) DeletePrefix(domain kv.Domain, key []byte) {
	cache, ok := sc.caches[domain]
	if !ok {
		return
	}
	// I fucking hate this - but this is not a lot of iterations worst case
	for _, k := range cache.Keys() {
		if len(k) >= len(key) && string(k[:len(key)]) == string(key) {
			cache.Remove(k)
		}
	}
}
