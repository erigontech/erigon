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

package eip8297

import (
	"fmt"
	"testing"
)

func BenchmarkParallelStreamRootSkewed(b *testing.B) {
	entries := parallelStreamRootSkewedEntries(100_000)
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			var residentTotal uint64
			b.ResetTimer()
			for range b.N {
				parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), workers)
				if err != nil {
					b.Fatal(err)
				}
				for _, entry := range entries {
					if err := parallel.Add(entry.Key, entry.Value); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := parallel.RootHash(); err != nil {
					b.Fatal(err)
				}
				residentTotal += parallel.peakResident.Load()
			}
			b.StopTimer()
			b.ReportMetric(float64(residentTotal)/float64(b.N), "resident-bytes/op")
		})
	}
}
