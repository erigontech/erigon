package freezeblocks

import (
	"math"
	"math/bits"

	"github.com/erigontech/erigon/turbo/snapshotsync"
)

// idea is to use lookup tables to implement uniform binary search
type BlockTxNumLookupCache struct {
	cache   map[snapshotsync.Range][]uint64
	cadence float64
}

func NewBlockTxNumLookupCache() *BlockTxNumLookupCache {
	return &BlockTxNumLookupCache{
		cache:   make(map[snapshotsync.Range][]uint64),
		cadence: 8.0, // lookup exists every `cadence` blocks
	}
}

func (c *BlockTxNumLookupCache) NewQuery(r snapshotsync.Range) *CacheQuery {
	lookup, ok := c.cache[r]
	if !ok {
		size := c.LookupSize(&r)
		lookup = make([]uint64, size)
		for i := 0; i < len(lookup); i++ {
			lookup[i] = math.MaxUint64
		}
	}

	c.cache[r] = lookup

	return NewCacheQuery(0, len(lookup)-1, bits.Len64(uint64(len(lookup))), lookup)
}

func (c *BlockTxNumLookupCache) LookupSize(r *snapshotsync.Range) uint64 {
	len := float64(r.To()) - float64(r.From())
	return 1 + (1 << (bits.Len64(uint64(math.Ceil(len/c.cadence))) - 1))
}

type CacheQuery struct {
	l, r   int // r is inclusive
	lookup []uint64
	depth  int
}

func NewCacheQuery(l, r, depth int, lookup []uint64) *CacheQuery {
	return &CacheQuery{l, r, lookup, depth}
}

func (q *CacheQuery) GetValue() (val uint64, ok bool, exhaust bool) {
	if q.depth == 0 {
		return 0, false, true
	}

	m := (q.l + q.r) >> 1
	if q.lookup[m] == math.MaxUint64 {
		return 0, false, false
	}

	return q.lookup[m], true, false
}

func (q *CacheQuery) SetValue(val uint64) {
	m := (q.l + q.r) >> 1
	q.lookup[m] = val
}

func (q *CacheQuery) Left() {
	if q.depth == 0 {
		return
	}
	q.depth--
	m := (q.l + q.r) >> 1
	q.r = m
}

func (q *CacheQuery) Right() {
	if q.depth == 0 {
		return
	}
	q.depth--
	m := (q.l + q.r) >> 1
	q.l = m + 1
}

func (q *CacheQuery) First() (uint64, bool) {
	return q.lookup[0], q.lookup[0] != math.MaxUint64
}

func (q *CacheQuery) Last() (uint64, bool) {
	len := len(q.lookup) - 1
	return q.lookup[len], q.lookup[len] != math.MaxUint64
}

func (q *CacheQuery) SetFirst(val uint64) {
	q.lookup[0] = val
}

func (q *CacheQuery) SetLast(val uint64) {
	q.lookup[len(q.lookup)-1] = val
}
