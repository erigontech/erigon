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
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkPruneFloorCacheHit(b *testing.B) {
	cache := pruneFloorCache[uint64]{ttl: time.Hour}
	read := func() (uint64, error) { return 1, nil }
	if _, err := cache.get(context.Background(), 1, read); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := cache.get(context.Background(), 1, read); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPruneFloorCacheHitParallel(b *testing.B) {
	for _, views := range []uint64{1, 8} {
		b.Run(fmt.Sprintf("views=%d", views), func(b *testing.B) {
			cache := pruneFloorCache[uint64]{ttl: time.Hour}
			ctx := context.Background()
			read := func() (uint64, error) { return 1, nil }
			for view := range views {
				key := pruneFloorCacheKey{head: 1, dbViewID: view}
				if _, err := cache.getForKey(ctx, key, read); err != nil {
					b.Fatal(err)
				}
			}
			var worker atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				key := pruneFloorCacheKey{head: 1, dbViewID: (worker.Add(1) - 1) % views}
				for pb.Next() {
					if _, err := cache.getForKey(ctx, key, read); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
