package caches

import (
	"math"
	"sync"

	"github.com/erigontech/erigon/common"
)

var ActiveValidatorsCacheGlobal = NewActiveValidatorsCache(MaxActiveValidatorsCacheSize)

const MaxActiveValidatorsCacheSize = 8

type activeValidatorsCacheVal struct {
	epoch            uint64
	root             common.Hash
	activeValidators []uint64
}

type ActiveValidatorsCache struct {
	list []*activeValidatorsCacheVal

	cap int
	mu  sync.Mutex
}

func NewActiveValidatorsCache(cap int) *ActiveValidatorsCache {
	return &ActiveValidatorsCache{
		list: make([]*activeValidatorsCacheVal, MaxActiveValidatorsCacheSize),
		cap:  cap,
	}
}

func (c *ActiveValidatorsCache) Get(epoch uint64, root common.Hash) ([]uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.list); i++ {
		if c.list[i] != nil && c.list[i].epoch == epoch && c.list[i].root == root {
			return c.list[i].activeValidators, true
		}
	}
	return nil, false
}

func (c *ActiveValidatorsCache) Put(epoch uint64, root common.Hash, activeValidators []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.list) == 0 {
		return
	}
	minEpochIdx := 0
	minEpoch := uint64(math.MaxUint64)

	for i := 0; i < len(c.list); i++ {
		if c.list[i] == nil {
			c.list[i] = &activeValidatorsCacheVal{
				epoch:            epoch,
				root:             root,
				activeValidators: activeValidators,
			}
			return
		}
		if minEpoch > c.list[i].epoch {
			minEpoch = c.list[i].epoch
			minEpochIdx = i
		}
	}
	c.list[minEpochIdx] = &activeValidatorsCacheVal{
		epoch:            epoch,
		root:             root,
		activeValidators: activeValidators,
	}
}
