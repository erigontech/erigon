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
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/cachebudget"
)

func BenchmarkGrowLRU_InsertAtCapacityGrowthDenied(b *testing.B) {
	for _, workers := range []int{1, 128} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			benchmarkGrowLRUInsertAtCapacity(b, workers)
		})
	}
}

func benchmarkGrowLRUInsertAtCapacity(b *testing.B, workers int) {
	const capacity = uint32(genericCacheStartCapacity)
	const keyMix = uint64(0x9e3779b97f4a7c15)
	g := newGrowLRU[int](64*datasize.MB, avgBytesPerEntry, nil)
	defer g.Close()
	for i := uint64(0); g.entryCount.Load() < int64(capacity); i++ {
		g.Add(i*keyMix, int(i))
		if i > 1<<20 {
			b.Fatal("could not fill growLRU to its starting capacity")
		}
	}
	g.maxCap = capacity * genericCacheGrowFactor
	growEntries := int64(g.maxCap - capacity)
	g.avgBytes = cachebudget.Global.Limit()/growEntries + 1
	if delta := growEntries * g.avgBytes; delta <= cachebudget.Global.Limit() {
		b.Fatalf("growth delta %d does not exceed the cache budget %d", delta, cachebudget.Global.Limit())
	}
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	chunk := (b.N + workers - 1) / workers
	for worker := range workers {
		start := worker * chunk
		end := min(start+chunk, b.N)
		wg.Go(func() {
			for i := start; i < end; i++ {
				g.Add((uint64(i)+(uint64(1)<<32))*keyMix, i)
			}
		})
	}
	wg.Wait()
	if got := g.curCap.Load(); got != capacity {
		b.Fatalf("growLRU grew to %d slots; benchmark requires denied growth at %d", got, capacity)
	}
	b.ReportMetric(float64(workers), "workers")
}
