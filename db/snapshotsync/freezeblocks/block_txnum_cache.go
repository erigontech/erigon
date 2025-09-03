package freezeblocks

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/db/snapshotsync"
)

const E2StepSize = 1_000

type GetMaxTxNum func(blockNum uint64) (uint64, error)

// BlockTxNumLookupCache implements a partial lookup table
// to find answer "what's the block number for this txNum"
type BlockTxNumLookupCache struct {
	mu      sync.RWMutex
	cache   map[snapshotsync.Range][]atomic.Uint64
	cadence float64
}

func NewBlockTxNumLookupCache(cadence int) *BlockTxNumLookupCache {
	if E2StepSize%cadence != 0 {
		panic(fmt.Sprintf("E2StepSize (%d) must be divisible by cadence (%d)", E2StepSize, cadence))
	}

	return &BlockTxNumLookupCache{
		cache:   make(map[snapshotsync.Range][]atomic.Uint64),
		cadence: float64(cadence),
	}
}

// ensureLookup creates the lookup table if it doesn't exist
func (c *BlockTxNumLookupCache) ensureLookup(r snapshotsync.Range) []atomic.Uint64 {
	// lookup[i] stores maxTxNum for blockNumber = from + i * cadence   for i in [0, size-2]
	// lookup[size-1] stores maxTxNum for blockNumber = to - 1

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
	size := int(rangeLen/c.cadence) + 1

	lookup := make([]atomic.Uint64, size)
	for i := range lookup {
		lookup[i].Store(math.MaxUint64)
	}

	c.cache[r] = lookup
	return lookup
}

func (c *BlockTxNumLookupCache) Find(r snapshotsync.Range, queryTxNum uint64, getter GetMaxTxNum) (blockNum uint64, err error) {
	// assumption...txNum is in the given range

	// i) first find lookup block index at which queryTxNum might reside. This will be a range of blocks. This step uses lookup cache
	// ii) then in that narrow block range, search for block containing queryTxNum. This cannot employ cache.
	lookup := c.ensureLookup(r)
	var txNum uint64
	lookupIndex := sort.Search(len(lookup), func(i int) bool {
		if err != nil {
			return true
		}
		txNum = lookup[i].Load()
		if txNum == math.MaxUint64 {
			// not found
			blkNum := c.index2BlkNum(r.From(), len(lookup), i)
			txNum, err = getter(blkNum)
			if err != nil {
				return true
			}
			lookup[i].Store(txNum)
		}

		return txNum >= queryTxNum

	})

	if err != nil {
		return 0, err
	}

	if lookupIndex == 0 {
		return r.From(), nil
	}

	from, to := c.index2BlkNum(r.From(), len(lookup), lookupIndex-1), c.index2BlkNum(r.From(), len(lookup), lookupIndex)
	rangeFrom := from

	for from < to {
		h := (from + to) >> 1

		if h == rangeFrom {
			txNum = lookup[lookupIndex-1].Load()
		} else {
			txNum, err = getter(h)
			if err != nil {
				return 0, err
			}
		}

		if txNum >= queryTxNum {
			to = h
		} else {
			from = h + 1
		}

	}

	return from, nil
}

func (c *BlockTxNumLookupCache) GetLastMaxTxNum(r snapshotsync.Range, getter GetMaxTxNum) (maxTxNum uint64, err error) {
	lookup := c.ensureLookup(r)
	lastIdx := len(lookup) - 1
	maxTxNum = lookup[lastIdx].Load()
	if maxTxNum != math.MaxUint64 {
		return maxTxNum, nil
	}

	maxTxNum, err = getter(r.To() - 1)
	if err != nil {
		return 0, err
	}
	lookup[lastIdx].Store(maxTxNum)

	return maxTxNum, nil
}

func (c *BlockTxNumLookupCache) index2BlkNum(from uint64, lookupSize, idxlookupIdx int) uint64 {
	if idxlookupIdx >= lookupSize {
		panic(fmt.Sprintf("index2BlkNum: idxlookupIdx (%d) >= lookupSize (%d)", idxlookupIdx, lookupSize))
	}
	if idxlookupIdx == lookupSize-1 {
		return from + uint64(idxlookupIdx)*uint64(c.cadence) - 1
	}
	return from + uint64(idxlookupIdx)*uint64(c.cadence)
}
