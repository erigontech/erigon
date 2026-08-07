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
	"fmt"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/cachebudget"
)

func BenchmarkGenericCache_InsertAtCapacityGrowthDenied(b *testing.B) {
	for _, workers := range []int{1, 128} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			benchmarkGenericCacheInsertAtCapacity(b, workers)
		})
	}
}

func benchmarkGenericCacheInsertAtCapacity(b *testing.B, workers int) {
	const capacity = uint32(8_192)
	c := newGenericCacheEntries[[]byte](64*datasize.MB, capacity, func(v []byte) int { return len(v) }, ModeEvictLRU)
	defer c.Close()
	value := []byte{1}
	var key [8]byte
	for i := range capacity {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		c.Put(key[:], value, 1)
	}
	c.maxCap = capacity * genericCacheGrowFactor
	growEntries := int64(c.maxCap - capacity)
	c.avgEntryBytes = cachebudget.Global.Limit()/growEntries + 1
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	chunk := (b.N + workers - 1) / workers
	for worker := range workers {
		start := worker * chunk
		end := min(start+chunk, b.N)
		wg.Go(func() {
			var key [16]byte
			binary.BigEndian.PutUint64(key[:8], uint64(worker+1))
			for i := start; i < end; i++ {
				binary.BigEndian.PutUint64(key[8:], uint64(i))
				c.Put(key[:], value, 1)
			}
		})
	}
	wg.Wait()
	b.ReportMetric(float64(workers), "workers")
}
