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

package commitment

import (
	"fmt"
	"testing"
)

// The Benchmark_Commitment_* family cannot measure the trie build: runParallelBench
// calls WrapKeyUpdates, which drives every Insert, outside the timed region. This
// times the build itself across Reset cycles, so the arena's per-batch reuse is
// what the numbers reflect.
func Benchmark_PrefixTrieBuildAcrossResets(b *testing.B) {
	for _, keys := range []int{5_000, 20_000} {
		b.Run(fmt.Sprintf("%dk-keys", keys/1000), func(b *testing.B) {
			const keyLen = 64
			corpus := make([][]byte, keys)
			for i := range corpus {
				k := make([]byte, keyLen)
				v := i
				for j := range keyLen {
					k[j] = byte(v % 16)
					v /= 3
				}
				corpus[i] = k
			}
			tr := newPrefixTrie()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, k := range corpus {
					tr.Insert(k, k, nil)
				}
				tr.Reset()
			}
		})
	}
}
