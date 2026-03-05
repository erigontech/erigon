package caches

import (
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const MaxActiveValidatorsCacheSize = 8

type activeValidatorsCacheKey struct {
	epoch uint64
	root  common.Hash
}

type activeValidatorsCacheVal struct {
	activeValidators   []uint64
	totalActiveBalance uint64
}

type ActiveValidatorsCache struct {
	cache *lru.Cache[activeValidatorsCacheKey, activeValidatorsCacheVal]
}

var ActiveValidatorsCacheGlobal = NewActiveValidatorsCache(MaxActiveValidatorsCacheSize)

func NewActiveValidatorsCache(size int) *ActiveValidatorsCache {
	cache, err := lru.New[activeValidatorsCacheKey, activeValidatorsCacheVal]("active_validators", size)
	if err != nil {
		panic(err)
	}
	return &ActiveValidatorsCache{
		cache: cache,
	}
}

func (c *ActiveValidatorsCache) Get(epoch uint64, root common.Hash) ([]uint64, uint64, bool) {
	val, ok := c.cache.Get(activeValidatorsCacheKey{epoch: epoch, root: root})
	if !ok {
		return nil, 0, false
	}
	return val.activeValidators, val.totalActiveBalance, true
}

func (c *ActiveValidatorsCache) Put(epoch uint64, root common.Hash, activeValidators []uint64, totalActiveBalance uint64) {
	c.cache.Add(activeValidatorsCacheKey{epoch: epoch, root: root}, activeValidatorsCacheVal{
		activeValidators:   activeValidators,
		totalActiveBalance: totalActiveBalance,
	})
}
