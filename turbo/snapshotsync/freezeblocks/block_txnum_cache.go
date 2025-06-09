package freezeblocks

import (
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/turbo/snapshotsync"
)

// BlockTxNumLookupCache implements a lookup table for partially memoized binary search
type BlockTxNumLookupCache struct {
	mu      sync.RWMutex
	cache   map[snapshotsync.Range][]uint64
	cadence float64
}

func NewBlockTxNumLookupCache(cadence int) *BlockTxNumLookupCache {
	return &BlockTxNumLookupCache{
		cache:   make(map[snapshotsync.Range][]uint64),
		cadence: float64(cadence),
	}
}

// ensureLookup creates the lookup table if it doesn't exist
func (c *BlockTxNumLookupCache) ensureLookup(r snapshotsync.Range) []uint64 {
	// Try read lock first for common case
	c.mu.RLock()
	if lookup, ok := c.cache[r]; ok {
		c.mu.RUnlock()
		return lookup
	}
	c.mu.RUnlock()

	// Need to create - acquire write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if lookup, ok := c.cache[r]; ok {
		return lookup
	}

	rangeLen := float64(r.To() - r.From())
	size := 1 + (1 << bits.Len64(uint64(math.Ceil(rangeLen/c.cadence))-1))

	lookup := make([]uint64, size)
	for i := range lookup {
		lookup[i] = math.MaxUint64 // sentinel for unset values
	}

	c.cache[r] = lookup
	return lookup
}

// GetMaxTxNum retrieves the cached max transaction number for a block
// Returns (maxTxNum, true) if cached, (0, false) if not cached
func (c *BlockTxNumLookupCache) GetMaxTxNum(r snapshotsync.Range, blockNum uint64) (maxTxNum uint64, exists bool) {
	lookup := c.ensureLookup(r)

	// Check if this block number maps to a cache slot
	idx, managed := c.blockToIndex(lookup, r, blockNum)
	if !managed {
		return 0, false
	}

	maxTxNum = atomic.LoadUint64(&lookup[idx])
	return maxTxNum, maxTxNum != math.MaxUint64
}

// SetMaxTxNum caches the max transaction number for a block
// Only sets if the block maps to a cache slot
func (c *BlockTxNumLookupCache) SetMaxTxNum(r snapshotsync.Range, blockNum uint64, maxTxNum uint64) (isSet bool) {
	lookup := c.ensureLookup(r)

	// Check if this block number maps to a cache slot
	idx, managed := c.blockToIndex(lookup, r, blockNum)
	if !managed {
		return false
	}

	atomic.CompareAndSwapUint64(&lookup[idx], math.MaxUint64, maxTxNum)
	return true
}

// GetLastMaxTxNum retrieves the cached max transaction number for the last block in range
func (c *BlockTxNumLookupCache) GetLastMaxTxNum(r snapshotsync.Range) (maxTxNum uint64, exists bool) {
	lookup := c.ensureLookup(r)
	lastIdx := len(lookup) - 1
	maxTxNum = atomic.LoadUint64(&lookup[lastIdx])
	return maxTxNum, maxTxNum != math.MaxUint64
}

// SetLastMaxTxNum caches the max transaction number for the last block in range
func (c *BlockTxNumLookupCache) SetLastMaxTxNum(r snapshotsync.Range, maxTxNum uint64) {
	lookup := c.ensureLookup(r)
	lastIdx := len(lookup) - 1
	atomic.CompareAndSwapUint64(&lookup[lastIdx], math.MaxUint64, maxTxNum)
}

// blockToIndex maps a block number to its lookup table index
// Returns (index, true) if this block should be cached, (0, false) otherwise
func (c *BlockTxNumLookupCache) blockToIndex(lookup []uint64, r snapshotsync.Range, blockNum uint64) (lookupIndex int, cachedByLookup bool) {
	// Check bounds
	if blockNum < r.From() || blockNum >= r.To() {
		return 0, false
	}

	if blockNum == r.To()-1 {
		// we don't store r.To() but r.To()-1
		return len(lookup) - 1, true
	}

	rangeSize := r.To() - r.From()
	intervals := uint64(len(lookup) - 1)
	per_interval_width := float64(rangeSize) / float64(intervals)

	offset := float64(blockNum - r.From())

	if math.Mod(offset, per_interval_width) == 0 {
		// divisible
		return int(offset / per_interval_width), true
	}

	// if offset+1 is change point causes index to increment...use it
	// e.g. 10000-10500; cadence = 64; lookup size = 9
	// then: per_interval_width=62.5
	// for 10062, lookup_index = 0, lookup_index_1 = 1
	lookup_index := int(math.Floor(offset / per_interval_width))
	lookup_index_1 := int(math.Floor((offset + 1) / per_interval_width))
	if lookup_index_1-lookup_index == 1 {
		return lookup_index, true
	}

	return 0, false
}
