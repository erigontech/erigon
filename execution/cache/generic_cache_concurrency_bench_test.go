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
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
)

// Steady-state throughput with many more goroutines than cores. Accesses are a
// hot/cold mix -- a hot set about the size of the ceiling plus a long cold tail
// -- and every miss fills read-through, so eviction runs continuously and the
// measured hit rate lands near what the storage domain shows in production.
// Both the hit and the miss path are therefore exercised in realistic
// proportion; a benchmark that only hits, or only misses, says nothing here.
func BenchmarkGenericCacheParallelMixed(b *testing.B) {
	const (
		hotKeys  = 1 << 20
		coldKeys = 10 << 20
		samples  = 8 << 20
		hotPct   = 79
	)
	rnd := rand.New(rand.NewSource(1))
	keys := make([]uint64, samples)
	for i := range keys {
		if rnd.Intn(100) < hotPct {
			keys[i] = uint64(rnd.Intn(hotKeys)) * 0x9E3779B97F4A7C15
		} else {
			keys[i] = uint64(hotKeys+rnd.Intn(coldKeys)) * 0x9E3779B97F4A7C15
		}
	}

	c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88,
		func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	val := make([]byte, 32)

	k := make([]byte, 8)
	for _, key := range keys {
		binary.BigEndian.PutUint64(k, key)
		if _, ok := c.Get(k); !ok {
			c.Put(k, val, 1)
		}
	}

	hits0, misses0 := c.hits.Load(), c.misses.Load()
	var stream atomic.Uint64
	b.SetParallelism(64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine enters the shared stream at its own offset, so shard
		// contention comes from hashing rather than from a shared cursor.
		i := int(stream.Add(1)) * 7919
		k := make([]byte, 8)
		for pb.Next() {
			binary.BigEndian.PutUint64(k, keys[i&(samples-1)])
			i++
			if _, ok := c.Get(k); !ok {
				c.Put(k, val, 1)
			}
		}
	})
	b.StopTimer()
	hits, misses := c.hits.Load()-hits0, c.misses.Load()-misses0
	b.ReportMetric(100*float64(hits)/float64(hits+misses), "hit%")
}

// Filling a cache from cold to a large working set. On main this pays whole-
// cache migration copies; here each shard migrates on its own. Uses only the
// public put path, so it runs unchanged on both.
func BenchmarkGenericCacheFill(b *testing.B) {
	const keys = 1 << 20
	val := make([]byte, 32)
	for b.Loop() {
		c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88, func(v []byte) int { return len(v) }, ModeEvictLRU)
		k := make([]byte, 8)
		for i := range uint64(keys) {
			binary.BigEndian.PutUint64(k, i*0x9E3779B97F4A7C15)
			c.Put(k, val, 1)
		}
		c.Close()
	}
}

// The longest a single put is held up while filling a cache from cold: on main
// that is a whole-cache migration behind every put stripe, here it is one
// shard's migration behind that shard's mutex.
func BenchmarkGenericCacheWorstPut(b *testing.B) {
	const keys = 1 << 20
	val := make([]byte, 32)
	var worst time.Duration
	for b.Loop() {
		c := NewGenericCacheWithAvg[[]byte](128*datasize.MB, 88, func(v []byte) int { return len(v) }, ModeEvictLRU)
		k := make([]byte, 8)
		for i := range uint64(keys) {
			binary.BigEndian.PutUint64(k, i*0x9E3779B97F4A7C15)
			start := time.Now()
			c.Put(k, val, 1)
			if d := time.Since(start); d > worst {
				worst = d
			}
		}
		c.Close()
	}
	b.ReportMetric(float64(worst.Microseconds()), "worst-put-us")
}
