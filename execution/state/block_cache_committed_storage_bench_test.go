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

package state

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkCommittedStorage prices the committed-storage cache path that every
// block worker hits on SLOAD. Run with -cpu=1,8,16 to see behaviour under the
// contention the parallel executor actually produces.
func BenchmarkCommittedStorage(b *testing.B) {
	const nAddrs, nKeys = 64, 64
	addrs := make([]accounts.Address, nAddrs)
	keys := make([]accounts.StorageKey, nKeys)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.Address{byte(i), byte(i >> 8)})
	}
	for i := range keys {
		keys[i] = accounts.InternKey(common.Hash{byte(i), byte(i >> 8)})
	}
	val := []byte{1, 2, 3, 4}

	// Warm read: all keys pre-filled, workers only read (the SLOAD cache-hit hot path).
	b.Run("read_warm", func(b *testing.B) {
		c := NewBlockStateCache()
		for _, a := range addrs {
			for _, k := range keys {
				c.PutCommittedStorage(a, k, val)
			}
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				c.GetCommittedStorage(addrs[i&(nAddrs-1)], keys[(i*7)&(nKeys-1)])
				i++
			}
		})
	})

	// Read-through fill: first touch misses then fills (the profiled PutCommittedStorage
	// contention), subsequent touches hit.
	b.Run("fill_read", func(b *testing.B) {
		c := NewBlockStateCache()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				a, k := addrs[i&(nAddrs-1)], keys[(i*7)&(nKeys-1)]
				if _, ok := c.GetCommittedStorage(a, k); !ok {
					c.PutCommittedStorage(a, k, val)
				}
				i++
			}
		})
	})
}
