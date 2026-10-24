package caches

import (
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const (
	MaxShuffledIndiciesCacheSize          = 8
	MaxLookaheadShuffledIndiciesCacheSize = 4
)

type shuffledIndiciesCacheKey struct {
	epoch uint64
	root  common.Hash
}

type ShuffledIndiciesCache struct {
	cache *lru.Cache[shuffledIndiciesCacheKey, []uint64]
}

var (
	ShuffledIndiciesCacheGlobal          = NewShuffledIndiciesCache("shuffled_indicies", MaxShuffledIndiciesCacheSize)
	LookaheadShuffledIndiciesCacheGlobal = NewShuffledIndiciesCache("lookahead_shuffled_indicies", MaxLookaheadShuffledIndiciesCacheSize)
)

func NewShuffledIndiciesCache(name string, size int) *ShuffledIndiciesCache {
	cache, err := lru.New[shuffledIndiciesCacheKey, []uint64](name, size)
	if err != nil {
		panic(err)
	}
	return &ShuffledIndiciesCache{
		cache: cache,
	}
}

func (c *ShuffledIndiciesCache) Get(epoch uint64, root common.Hash) ([]uint64, bool) {
	return c.cache.Get(shuffledIndiciesCacheKey{epoch: epoch, root: root})
}

func (c *ShuffledIndiciesCache) Put(epoch uint64, root common.Hash, shuffledSet []uint64) {
	c.cache.Add(shuffledIndiciesCacheKey{epoch: epoch, root: root}, shuffledSet)
}
