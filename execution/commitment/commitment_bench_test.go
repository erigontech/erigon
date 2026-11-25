// Copyright 2024 The Erigon Authors
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

package commitment

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
)

func populateUpdates(b *testing.B, upd *Updates, n int) {
	b.Helper()
	key := make([]byte, 20)
	val := make([]byte, 8)
	for i := range n {
		binary.BigEndian.PutUint64(key[12:], uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i+1))
		upd.TouchPlainKey(string(key), val, upd.TouchStorage)
	}
}

func BenchmarkHashSort_ModeDirect(b *testing.B) {
	for _, n := range []int{50, 5000, 50000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			upd := NewUpdates(ModeDirect, b.TempDir(), keyHasherNoop)

			ctx := context.Background()
			noop := func(hk, pk []byte, update *Update) error { return nil }

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Re-populate for each iteration since ETL is consumed by Load
				b.StopTimer()
				upd.Reset()
				populateUpdates(b, upd, n)
				b.StartTimer()

				if err := upd.HashSort(ctx, nil, noop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkContractHashFromPrefix(b *testing.B) {
	prefix := make([]byte, 34)
	for i := range prefix {
		prefix[i] = byte(i * 37)
	}
	prefix[0] = 0x1a
	for b.Loop() {
		_, _ = ContractHashFromPrefix(prefix)
	}
}

func BenchmarkHashSort_ModeUpdate(b *testing.B) {
	for _, n := range []int{50, 5000, 50000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			upd := NewUpdates(ModeUpdate, b.TempDir(), keyHasherNoop)

			ctx := context.Background()
			noop := func(hk, pk []byte, update *Update) error { return nil }

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				// Re-populate for each iteration since HashSort clears the tree
				b.StopTimer()
				populateUpdates(b, upd, n)
				b.StartTimer()

				if err := upd.HashSort(ctx, nil, noop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
