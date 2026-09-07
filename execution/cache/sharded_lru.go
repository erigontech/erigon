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

package cache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/elastic/go-freelru"

	"github.com/erigontech/erigon/common/math"
)

// shardedLRU is freelru.ShardedLRU with the shards owned separately, so a
// full shard is rebuilt one step larger on its own instead of the whole cache
// being copied into a new generation behind every put stripe. A grow then
// blocks one shard's writers for capacity/shards entries, not everyone for the
// whole cache.
//
// Shards are selected from bits 16+ of the key, and bucket indexing inside a
// shard runs off a mixed hash (see u64mix), so the two stay independent however
// large a shard's table grows.
type shardedLRU[V any] struct {
	shards []*freelru.LRU[uint64, V]
	mus    []sync.Mutex
	curCap []uint32
	mask   uint64
	maxCap uint32 // per shard
	// startCapPerShard is what each shard was born with; a shard above it grew.
	startCapPerShard uint32
	onEvict          func(uint64, V)

	// fundGrow is asked for the envelope bytes a step needs; a refusal leaves
	// the shard at its current size, evicting within it. refundGrow returns a
	// reservation whose grow lost the race, so a loser cannot strand envelope
	// bytes and stop another cache from growing.
	fundGrow   func(slots uint32) bool
	refundGrow func(slots uint32)

	n atomic.Int64
}

func newShardedLRU[V any](startCap, maxCap, shards uint32, onEvict func(uint64, V),
	fundGrow func(uint32) bool, refundGrow func(uint32)) *shardedLRU[V] {
	shards = max(uint32(math.NextPowerOfTwo(uint64(shards))), 1)
	s := &shardedLRU[V]{
		shards:     make([]*freelru.LRU[uint64, V], shards),
		mus:        make([]sync.Mutex, shards),
		curCap:     make([]uint32, shards),
		mask:       uint64(shards - 1),
		maxCap:     max(perShard(maxCap, shards), 1),
		onEvict:    onEvict,
		fundGrow:   fundGrow,
		refundGrow: refundGrow,
	}
	start := min(max(perShard(startCap, shards), 1), s.maxCap)
	s.startCapPerShard = start
	for i := range s.shards {
		s.curCap[i] = start
		s.shards[i] = s.newShard(start)
	}
	return s
}

func perShard(capacity, shards uint32) uint32 { return (capacity + shards - 1) / shards }

func (s *shardedLRU[V]) newShard(capacity uint32) *freelru.LRU[uint64, V] {
	// Size the table to a power of two of capacity+25%, as freelru's own sharded
	// constructor does: it keeps the load factor off 1.0, and it is what selects
	// the mask path for the bucket index. Without it the fallback keys off the
	// high hash bits, which are the shard-selection bits and so are constant
	// inside a shard.
	l, err := freelru.NewWithSize[uint64, V](capacity, uint32(math.NextPowerOfTwo(uint64(capacity)+uint64(capacity)/4)), u64mix)
	if err != nil {
		panic(fmt.Sprintf("shardedLRU: New(%d): %s", capacity, err))
	}
	if s.onEvict != nil {
		l.SetOnEvict(s.onEvict)
	}
	return l
}

func (s *shardedLRU[V]) idx(h uint64) uint64 { return (h >> 16) & s.mask }

// u64mix spreads the whole key into the 32 bits freelru indexes buckets with.
// Truncating instead would let a large per-shard table index buckets with bits
// idx already fixed for every key in the shard, leaving most of its buckets
// unreachable and the rest on long chains.
func u64mix(k uint64) uint32 { return uint32((k * 0x9E3779B97F4A7C15) >> 32) }

func (s *shardedLRU[V]) Len() int { return int(s.n.Load()) }

func (s *shardedLRU[V]) Get(h uint64) (v V, ok bool) {
	i := s.idx(h)
	s.mus[i].Lock()
	v, ok = s.shards[i].Get(h)
	s.mus[i].Unlock()
	return v, ok
}

func (s *shardedLRU[V]) Remove(h uint64) {
	i := s.idx(h)
	s.mus[i].Lock()
	if s.shards[i].Remove(h) {
		s.n.Add(-1)
	}
	s.mus[i].Unlock()
}

// Replace overwrites a key. The removal is what fires onEvict for the old value
// -- freelru.Add swaps a present key in place and skips the callback -- and the
// shard lock spans the pair, so the gap where the key is absent is never
// observed. A caller that looked the key up released the shard in between, so
// this can still insert; the shard stays within curCap either way, and growing
// on the count it added is left to the next Add.
func (s *shardedLRU[V]) Replace(h uint64, v V) (evicted bool) {
	i := s.idx(h)
	s.mus[i].Lock()
	before := s.shards[i].Len()
	s.shards[i].Remove(h)
	evicted = s.shards[i].Add(h, v)
	if delta := s.shards[i].Len() - before; delta != 0 {
		s.n.Add(int64(delta))
	}
	s.mus[i].Unlock()
	return evicted
}

// Add stores a key, growing that one shard first when it is full and below its
// ceiling. Only this shard's readers and writers wait, and only for its own
// entries.
func (s *shardedLRU[V]) Add(h uint64, v V) (evicted bool) {
	i := s.idx(h)
	s.mus[i].Lock()
	if newCap, reserved, ok := s.growStep(i); ok {
		// Allocate without the shard lock: the replacement is the largest thing
		// a grow does, and nothing needs to be excluded while it is built. Several
		// writers can reach here on the same full shard, so the loser hands its
		// reservation back rather than stranding it.
		s.mus[i].Unlock()
		next := s.newShard(newCap)
		s.mus[i].Lock()
		if s.curCap[i] < newCap {
			s.migrateLocked(i, next, newCap)
		} else if s.refundGrow != nil {
			s.refundGrow(reserved)
		}
	}
	before := s.shards[i].Len()
	evicted = s.shards[i].Add(h, v)
	if delta := s.shards[i].Len() - before; delta != 0 {
		s.n.Add(int64(delta))
	}
	s.mus[i].Unlock()
	return evicted
}

// growStep reports the next capacity for shard i, the slots it reserved from the
// envelope, and whether the step is funded. Called with the shard lock held.
func (s *shardedLRU[V]) growStep(i uint64) (newCap, reserved uint32, ok bool) {
	if s.curCap[i] >= s.maxCap || s.shards[i].Len() < int(s.curCap[i]) {
		return 0, 0, false
	}
	newCap = min(s.curCap[i]*genericCacheGrowFactor, s.maxCap)
	reserved = newCap - s.curCap[i]
	if s.fundGrow != nil && !s.fundGrow(reserved) {
		return 0, 0, false
	}
	return newCap, reserved, true
}

// migrateLocked rebuilds shard i one step larger. Only that shard's readers and
// writers wait, and only for its own entries -- Keys() is oldest-first, so
// insertion order alone carries the recency across.
func (s *shardedLRU[V]) migrateLocked(i uint64, next *freelru.LRU[uint64, V], newCap uint32) {
	old := s.shards[i]
	for _, k := range old.Keys() {
		// growStep only reports a strictly larger capacity, so the copy cannot
		// evict. Assert it rather than let a later change silently lose the
		// shard's oldest keys and desync the counters.
		if v, ok := old.Peek(k); ok && next.Add(k, v) {
			panic("shardedLRU: grow target evicted during migration")
		}
	}
	s.shards[i] = next
	s.curCap[i] = newCap
}
