package caches

import (
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const (
	MaxActiveValidatorsCacheSize          = 8
	MaxLookaheadActiveValidatorsCacheSize = 4
)

type activeValidatorsCacheKey struct {
	epoch uint64
	root  common.Hash
}

type ActiveValidatorsCache struct {
	cache *lru.Cache[activeValidatorsCacheKey, []uint64]
}

var (
	ActiveValidatorsCacheGlobal          = NewActiveValidatorsCache("active_validators", MaxActiveValidatorsCacheSize)
	LookaheadActiveValidatorsCacheGlobal = NewActiveValidatorsCache("lookahead_active_validators", MaxLookaheadActiveValidatorsCacheSize)
)

func NewActiveValidatorsCache(name string, size int) *ActiveValidatorsCache {
	cache, err := lru.New[activeValidatorsCacheKey, []uint64](name, size)
	if err != nil {
		panic(err)
	}
	return &ActiveValidatorsCache{
		cache: cache,
	}
}

func (c *ActiveValidatorsCache) Get(epoch uint64, root common.Hash) ([]uint64, bool) {
	return c.cache.Get(activeValidatorsCacheKey{epoch: epoch, root: root})
}

func (c *ActiveValidatorsCache) Put(epoch uint64, root common.Hash, activeValidators []uint64) {
	c.cache.Add(activeValidatorsCacheKey{epoch: epoch, root: root}, activeValidators)
}
