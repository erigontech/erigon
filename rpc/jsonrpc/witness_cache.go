// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"math"
	"sync"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

const (
	witnessCacheMaxBlocks = 96
	bytesPerMB            = 1024 * 1024
)

// witnessCacheMaxBytes converts a MB byte-cap to bytes, clamping to avoid int
// overflow on 32-bit builds or absurd values. 0 stays 0 (byte cap disabled).
func witnessCacheMaxBytes(mb uint) int {
	const maxMB = uint(math.MaxInt / bytesPerMB)
	if mb > maxMB {
		mb = maxMB
	}
	return int(mb) * bytesPerMB
}

// witnessResultCache maps a canonical block hash to its pre-marshaled legacy-mode
// witness. Keying by hash needs no reorg reconcile: number-based lookups resolve to
// the current canonical hash, and an orphaned hash ages out of the LRU.
//
// It wraps a hashicorp LRU (count cap) with a resident-bytes cap and the serving
// mode, so the builder and serve paths read one source of truth for both. The
// embedded pointer promotes Get/Contains/Len; Add is overridden here to keep the
// resident-bytes accounting in sync.
//
// It also carries the push feed: a non-nil cache always has a non-nil feed, and
// store is the only build-path insert, so every built witness is also published.
type witnessResultCache struct {
	*lru.Cache[common.Hash, *ExecutionWitnessResult]

	feed *witnessFeed

	// headCapture builds each head witness from a pinned parent snapshot instead of
	// commitment history; cacheOnly makes a serve miss return out-of-window instead
	// of recomputing. Both are fixed at construction and never mutated.
	headCapture bool
	cacheOnly   bool

	maxBytes int // resident-bytes cap; 0 disables the byte cap (count-only)

	mu            sync.Mutex
	residentBytes int
	entryBytes    map[common.Hash]int
}

// WitnessCacheCapacity is the number of witnesses the cache actually holds for a
// requested block count, after clamping to witnessCacheMaxBlocks.
func WitnessCacheCapacity(blocks uint) uint {
	if blocks > witnessCacheMaxBlocks {
		return witnessCacheMaxBlocks
	}
	return blocks
}

func newWitnessResultCache(blocks uint, maxBytes int, headCapture, cacheOnly bool) *witnessResultCache {
	c := &witnessResultCache{
		feed:        newWitnessFeed(),
		headCapture: headCapture,
		cacheOnly:   cacheOnly,
		maxBytes:    maxBytes,
		entryBytes:  make(map[common.Hash]int),
	}
	cache, err := lru.NewWithEvict[common.Hash, *ExecutionWitnessResult](int(WitnessCacheCapacity(blocks)), c.onEvict)
	if err != nil {
		panic(err)
	}
	c.Cache = cache
	return c
}

// HeadCapture reports whether the cache runs in pinned-parent head-capture mode.
func (c *witnessResultCache) HeadCapture() bool { return c.headCapture }

// CacheOnly reports whether a serve miss must fail out-of-window instead of recomputing.
func (c *witnessResultCache) CacheOnly() bool { return c.cacheOnly }

// ResidentBytes reports the estimated bytes of the witnesses currently cached.
func (c *witnessResultCache) ResidentBytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.residentBytes
}

// store is the build paths' insert: it caches the witness and publishes it. The result is
// shared with readers from here on and must not change.
func (c *witnessResultCache) store(num uint64, hash common.Hash, r *ExecutionWitnessResult) {
	r.headerByNumber = nil // only the build's BLOCKHASH lookups read it
	c.Add(hash, r)
	c.feed.publish(witnessPush{num: num, hash: hash, result: r})
}

func (c *witnessResultCache) subscribe() chan witnessPush     { return c.feed.subscribe() }
func (c *witnessResultCache) unsubscribe(ch chan witnessPush) { c.feed.unsubscribe(ch) }

// Add stores a result keyed by hash and keeps resident-bytes accounting in sync,
// evicting oldest entries when either the count cap or the byte cap would be exceeded.
func (c *witnessResultCache) Add(hash common.Hash, r *ExecutionWitnessResult) bool {
	size := witnessResultSize(r)

	c.mu.Lock()
	prev, existed := c.entryBytes[hash]
	c.mu.Unlock()

	// onEvict may fire here for a count-cap eviction of the oldest entry; it runs
	// outside the LRU's internal lock and takes c.mu itself, so hold neither now.
	evicted := c.Cache.Add(hash, r)

	c.mu.Lock()
	if existed {
		c.residentBytes -= prev
	}
	c.entryBytes[hash] = size
	c.residentBytes += size
	c.mu.Unlock()

	if c.maxBytes > 0 {
		for c.overByteCap() && c.Cache.Len() > 1 {
			c.Cache.RemoveOldest() // fires onEvict, subtracting its bytes
			evicted = true
		}
	}
	return evicted
}

func (c *witnessResultCache) overByteCap() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.residentBytes > c.maxBytes
}

// onEvict keeps resident-bytes accounting correct on every LRU removal (count-cap
// eviction and byte-cap RemoveOldest). The LRU invokes it outside its internal lock,
// so taking c.mu here cannot deadlock against Add.
func (c *witnessResultCache) onEvict(hash common.Hash, _ *ExecutionWitnessResult) {
	c.mu.Lock()
	if sz, ok := c.entryBytes[hash]; ok {
		c.residentBytes -= sz
		delete(c.entryBytes, hash)
	}
	c.mu.Unlock()
}

// witnessResultSize estimates a cached result's resident bytes: each byte string plus its slice header.
func witnessResultSize(r *ExecutionWitnessResult) int {
	if r == nil {
		return 0
	}
	n := 0
	for _, list := range [][]hexutil.Bytes{r.State, r.Codes, r.Keys, r.Headers} {
		for _, b := range list {
			n += len(b) + sliceHeaderBytes
		}
	}
	return n
}

const sliceHeaderBytes = int(unsafe.Sizeof([]byte(nil)))
