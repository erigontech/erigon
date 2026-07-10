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
	"sync"

	"github.com/erigontech/erigon/common"
)

// witnessCacheMaxBlocks is the hard upper bound on the entry-count cap; the
// flag is clamped to it so the cache can never hold more than a rolling window
// near chain tip.
const witnessCacheMaxBlocks = 96

// headerRef identifies a canonical block by number and hash. reconcile uses it
// to evict entries a reorg invalidated.
type headerRef struct {
	num  uint64
	hash common.Hash
}

// witnessCacheEntry is one cached legacy-mode witness together with the
// canonical hash it was built for and its resident byte size (cached so
// eviction need not re-measure the result).
type witnessCacheEntry struct {
	hash   common.Hash
	result *ExecutionWitnessResult
	bytes  uint64
}

// witnessCache is a bounded in-memory ring of recently built legacy-mode
// execution witnesses keyed by block number. At most one entry per number is
// kept — a reorg replaces it and get requires the requested hash to match.
// Entries are evicted lowest-number-first once either the entry-count cap or
// the resident-byte cap binds.
type witnessCache struct {
	mu         sync.RWMutex
	entries    map[uint64]witnessCacheEntry
	maxEntries int
	maxBytes   uint64
	totalBytes uint64
	evictions  uint64
}

// newWitnessCache builds a cache holding at most blocks entries (clamped to
// witnessCacheMaxBlocks) and maxMB MiB of resident witness bytes.
func newWitnessCache(blocks, maxMB uint) *witnessCache {
	if blocks > witnessCacheMaxBlocks {
		blocks = witnessCacheMaxBlocks
	}
	return &witnessCache{
		entries:    make(map[uint64]witnessCacheEntry, blocks),
		maxEntries: int(blocks),
		maxBytes:   uint64(maxMB) * 1024 * 1024,
	}
}

// witnessResultBytes is the resident size of a result: the summed lengths of
// its state nodes, codes, keys, and headers.
func witnessResultBytes(r *ExecutionWitnessResult) uint64 {
	var n uint64
	for _, b := range r.State {
		n += uint64(len(b))
	}
	for _, b := range r.Codes {
		n += uint64(len(b))
	}
	for _, b := range r.Keys {
		n += uint64(len(b))
	}
	for _, b := range r.Headers {
		n += uint64(len(b))
	}
	return n
}

// get returns the cached witness for (num, hash). A stored entry whose hash
// differs from hash is a miss so a reorged block never serves stale bytes.
func (c *witnessCache) get(num uint64, hash common.Hash) (*ExecutionWitnessResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[num]
	if !ok || e.hash != hash {
		return nil, false
	}
	return e.result, true
}

// put stores result under (num, hash), replacing any existing entry at num,
// then evicts lowest-number-first until both caps hold.
func (c *witnessCache) put(num uint64, hash common.Hash, result *ExecutionWitnessResult) {
	if result == nil {
		return
	}
	size := witnessResultBytes(result)
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.entries[num]; ok {
		c.totalBytes -= prev.bytes
	}
	c.entries[num] = witnessCacheEntry{hash: hash, result: result, bytes: size}
	c.totalBytes += size
	c.evictLocked()
	c.updateGaugesLocked()
}

// reconcile applies a canonical-header batch: evict any entry whose number
// matches a header but whose hash differs, then drop every entry above the
// highest header number so a reorg to a shorter chain leaves no orphans.
func (c *witnessCache) reconcile(headers []headerRef) {
	if len(headers) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var highest uint64
	for _, h := range headers {
		if h.num > highest {
			highest = h.num
		}
		if e, ok := c.entries[h.num]; ok && e.hash != h.hash {
			c.dropLocked(h.num, e)
		}
	}
	for num, e := range c.entries {
		if num > highest {
			c.dropLocked(num, e)
		}
	}
	c.updateGaugesLocked()
}

// evictLocked removes lowest-number entries until the count cap holds and the
// byte cap holds or only one entry remains (a single witness larger than the
// whole byte cap is still served rather than caching nothing).
func (c *witnessCache) evictLocked() {
	for len(c.entries) > c.maxEntries || (c.totalBytes > c.maxBytes && len(c.entries) > 1) {
		lowest, entry, ok := c.lowestLocked()
		if !ok {
			return
		}
		c.dropLocked(lowest, entry)
	}
}

func (c *witnessCache) lowestLocked() (uint64, witnessCacheEntry, bool) {
	var (
		lowest    uint64
		entry     witnessCacheEntry
		haveFirst bool
	)
	for num, e := range c.entries {
		if !haveFirst || num < lowest {
			lowest, entry, haveFirst = num, e, true
		}
	}
	return lowest, entry, haveFirst
}

func (c *witnessCache) dropLocked(num uint64, e witnessCacheEntry) {
	c.totalBytes -= e.bytes
	delete(c.entries, num)
	c.evictions++
	witnessCacheEvictCounter.Inc()
}

// updateGaugesLocked publishes the current resident byte and entry counts. Callers
// hold the write lock so the gauges track the cache after every mutation settles.
func (c *witnessCache) updateGaugesLocked() {
	witnessCacheBytesResidentGauge.SetUint64(c.totalBytes)
	witnessCacheEntriesResidentGauge.SetInt(len(c.entries))
}

func (c *witnessCache) entryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *witnessCache) residentBytes() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalBytes
}

func (c *witnessCache) evictionCount() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictions
}
