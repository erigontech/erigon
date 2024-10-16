package caches

import (
	"math"
	"sync"

	"github.com/erigontech/erigon-lib/common"
)

var ShuffledIndiciesCacheGlobal = NewShuffledIndiciesCache(MaxShuffledIndiciesCacheSize)

const MaxShuffledIndiciesCacheSize = 8

type shuffledIndiciesCacheVal struct {
	epoch       uint64
	root        common.Hash
	shuffledSet []uint64
}

type ShuffledIndiciesCache struct {
	list []*shuffledIndiciesCacheVal

	cap int
	mu  sync.Mutex
}

func NewShuffledIndiciesCache(cap int) *ShuffledIndiciesCache {
	return &ShuffledIndiciesCache{
		list: make([]*shuffledIndiciesCacheVal, MaxShuffledIndiciesCacheSize),
		cap:  cap,
	}
}

func (c *ShuffledIndiciesCache) Get(epoch uint64, root common.Hash) ([]uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.list); i++ {
		if c.list[i] != nil && c.list[i].epoch == epoch && c.list[i].root == root {
			return c.list[i].shuffledSet, true
		}
	}
	return nil, false
}

func (c *ShuffledIndiciesCache) Put(epoch uint64, root common.Hash, shuffledSet []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.list) == 0 {
		return
	}
	minEpochIdx := 0
	minEpoch := uint64(math.MaxUint64)

	for i := 0; i < len(c.list); i++ {
		if c.list[i] == nil {
			c.list[i] = &shuffledIndiciesCacheVal{
				epoch:       epoch,
				root:        root,
				shuffledSet: shuffledSet,
			}
			return
		}
		if minEpoch > c.list[i].epoch {
			minEpoch = c.list[i].epoch
			minEpochIdx = i
		}
	}
	c.list[minEpochIdx] = &shuffledIndiciesCacheVal{
		epoch:       epoch,
		root:        root,
		shuffledSet: shuffledSet,
	}

}
