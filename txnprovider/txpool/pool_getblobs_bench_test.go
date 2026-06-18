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

package txpool

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func BenchmarkGetBlobs(b *testing.B) {
	const writerCriticalSectionWork = 2000

	pool, ctx := newGetBlobsTestPool(b, 1)
	realHashes := addTestBlobTxn(b, pool, ctx, 1, 0)

	query := append([]common.Hash{}, realHashes...)
	for i := 0; i < 4; i++ {
		query = append(query, common.Hash{0xff, byte(i)})
	}
	got := pool.GetBlobs(query)
	require.Len(b, got, len(query))
	require.NotNil(b, got[0].Blob, "real blob hash must resolve")

	b.Run("NoContention", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pool.GetBlobs(query)
		}
	})

	b.Run("WithPoolWriters", func(b *testing.B) {
		stop := make(chan struct{})
		started := make(chan struct{})
		var startedOnce sync.Once
		var wg sync.WaitGroup
		var writerCounter uint64
		writers := max(2, runtime.GOMAXPROCS(0)-1)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					pool.lock.Lock()
					for i := 0; i < writerCriticalSectionWork; i++ {
						writerCounter++
					}
					pool.lock.Unlock()
					startedOnce.Do(func() { close(started) })
				}
			}()
		}

		// Wait until a writer has actually contended on pool.lock before measuring: at -benchtime=1x
		// the measured loop is a single call that can finish before any writer goroutine is scheduled.
		<-started

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = pool.GetBlobs(query)
		}
		b.StopTimer()
		close(stop)
		wg.Wait()
		if writerCounter == 0 {
			b.Fatal("writers did not run")
		}
	})
}
