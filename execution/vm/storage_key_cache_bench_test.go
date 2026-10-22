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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
)

// BenchmarkInternAddressHit measures the path every repeat word takes — the one
// the table exists to keep off unique.Make.
func BenchmarkInternAddressHit(b *testing.B) {
	evm := &EVM{}
	words := make([]uint256.Int, 64)
	for i := range words {
		words[i] = uint256.Int{uint64(i+1) * 0x9e3779b97f4a7c15, uint64(i+1) * 0xc2b2ae3d27d4eb4f, uint64(i+1) & 0xffffffff, 0}
	}
	requireDistinctBuckets(b, words, addrIndex)
	for range addressCacheMinOps + 1 {
		evm.internAddress(&words[0])
	}
	for i := range words {
		evm.internAddress(&words[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		internSink = evm.internAddress(&words[i&63])
	}
}

func BenchmarkInternStorageKeyHit(b *testing.B) {
	evm := &EVM{}
	words := make([]uint256.Int, 64)
	for i := range words {
		w := uint256.Int{uint64(i+1) * 0x9e3779b97f4a7c15, uint64(i+1) * 0xc2b2ae3d27d4eb4f, uint64(i+1) * 0x165667b19e3779f9, 0}
		// slotIndex xors all four limbs, so limb 3 sets the bucket outright.
		w[3] = w[0] ^ w[1] ^ w[2] ^ uint64(i)
		words[i] = w
	}
	requireDistinctBuckets(b, words, slotIndex)
	for range storageKeyCacheMinOps + 1 {
		evm.internStorageKey(&words[0])
	}
	for i := range words {
		evm.internStorageKey(&words[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keySink = evm.internStorageKey(&words[i&63])
	}
}

// requireDistinctBuckets fails the benchmark unless every word owns a bucket.
// A retuned constant or a resized table would otherwise quietly move part of the
// loop onto the miss path, which is the path these benchmarks exist to stay off.
func requireDistinctBuckets(b *testing.B, words []uint256.Int, index func(*uint256.Int) uint64) {
	b.Helper()
	buckets := make(map[uint64]struct{}, len(words))
	for i := range words {
		buckets[index(&words[i])] = struct{}{}
	}
	if len(buckets) != len(words) {
		b.Fatalf("%d words occupy %d buckets, so %d of them take the miss path",
			len(words), len(buckets), len(words)-len(buckets))
	}
}
