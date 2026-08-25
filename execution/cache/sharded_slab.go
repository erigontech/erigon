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

	"github.com/erigontech/erigon/execution/cache/slablru"
)

// shardedSlabLRU is a sharded LRU built at its final capacity. Elements are
// slab-allocated, so residency follows what is stored rather than what is
// configured and the cache never has to be resized.
//
// Shards are selected on bits 16+ of the key: the low bits index buckets inside
// a shard, so sharding on them would put every key of a shard in one bucket.
type shardedSlabLRU[V any] struct {
	shards []*slablru.LRU[uint64, V]
	mus    []sync.Mutex
	mask   uint64
	n      atomic.Int64
}

func newShardedSlabLRU[V any](capacity, shards uint32, onEvict func(uint64, V)) *shardedSlabLRU[V] {
	shards = max(uint32(nextPow2(uint64(shards))), 1)
	s := &shardedSlabLRU[V]{
		shards: make([]*slablru.LRU[uint64, V], shards),
		mus:    make([]sync.Mutex, shards),
		mask:   uint64(shards - 1),
	}
	perShard := max((capacity+shards-1)/shards, 1)
	for i := range s.shards {
		l, err := slablru.New[uint64, V](perShard, u64identity)
		if err != nil {
			panic(fmt.Sprintf("shardedSlabLRU: New(%d): %s", perShard, err))
		}
		l.SetOnEvict(func(k uint64, v V) {
			s.n.Add(-1)
			if onEvict != nil {
				onEvict(k, v)
			}
		})
		s.shards[i] = l
	}
	return s
}

func nextPow2(n uint64) uint64 {
	p := uint64(1)
	for p < n {
		p <<= 1
	}
	return p
}

func (s *shardedSlabLRU[V]) idx(h uint64) uint64 { return (h >> 16) & s.mask }
func (s *shardedSlabLRU[V]) Len() int            { return int(s.n.Load()) }

func (s *shardedSlabLRU[V]) Get(h uint64) (v V, ok bool) {
	i := s.idx(h)
	s.mus[i].Lock()
	v, ok = s.shards[i].Get(h)
	s.mus[i].Unlock()
	return v, ok
}

func (s *shardedSlabLRU[V]) Add(h uint64, v V) (evicted bool) {
	i := s.idx(h)
	s.mus[i].Lock()
	evicted = s.shards[i].Add(h, v)
	s.mus[i].Unlock()
	s.n.Add(1)
	return evicted
}

func (s *shardedSlabLRU[V]) Remove(h uint64) {
	i := s.idx(h)
	s.mus[i].Lock()
	s.shards[i].Remove(h)
	s.mus[i].Unlock()
}
