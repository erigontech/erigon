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

// BenchmarkCachedReaderAccountRead prices one apply-loop account read that hits
// the block state cache, on each of its two paths.
func BenchmarkCachedReaderAccountRead(b *testing.B) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	acc := cacheReadTestAccount()

	b.Run("committed", func(b *testing.B) {
		cache := NewBlockStateCache()
		cache.PutCommittedAccount(addr, acc)
		r := NewCurrentCachedReaderV3(nil, cache)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := r.ReadAccountData(addr)
			if err != nil || got == nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("written", func(b *testing.B) {
		cache := NewBlockStateCache()
		cache.WriteAccount(addr, accounts.SerialiseV3(acc), 1)
		r := NewCurrentCachedReaderV3(nil, cache)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := r.ReadAccountData(addr)
			if err != nil || got == nil {
				b.Fatal(err)
			}
		}
	})
}
