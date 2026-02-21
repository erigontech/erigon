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

package eliasfano32

import (
	"math/rand/v2"
	"testing"
)

// buildEF constructs and builds an EliasFano over count values with the given stride.
// stride=1   → dense, l=0  (block numbers: every block)
// stride=123 → sparse, l≈6 (used in BenchmarkEF)
// stride=1e3 → sparse, l≈9 (tx positions)
func buildEF(count, stride uint64) *EliasFano {
	maxOffset := (count - 1) * stride
	ef := NewEliasFano(count, maxOffset)
	for i := uint64(0); i < count; i++ {
		ef.AddOffset(i * stride)
	}
	ef.Build()
	return ef
}

// BenchmarkGet measures raw random-access cost of Get(i) on a large sequence.
// Reports ns/op per single Get call (b.N * count iterations total).
func BenchmarkGet(b *testing.B) {
	const count = 1_000_000

	cases := []struct {
		name   string
		stride uint64
	}{
		{"stride1_l0", 1},       // dense: l=0, no lower bits, all info in upperBits
		{"stride123_l6", 123},   // medium: l≈6
		{"stride1000_l9", 1000}, // sparse: l≈9
	}

	for _, tc := range cases {
		tc := tc
		ef := buildEF(count, tc.stride)

		// precompute random permutation of indices so the access pattern is random
		indices := rand.Perm(count)

		b.Run(tc.name+"/sequential", func(b *testing.B) {
			b.ReportAllocs()
			n := uint64(0)
			for b.Loop() {
				_ = ef.Get(n % count)
				n++
			}
		})

		b.Run(tc.name+"/random", func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_ = ef.Get(uint64(indices[n%count]))
				n++
			}
		})
	}
}

// BenchmarkSeek measures Seek on a single large EF with uniform-random targets.
// This does NOT reflect real-world usage (see BenchmarkSeekPool) because:
//   - real EFs are mostly tiny (83% have 1–3 word upperBits = 8–24 bytes)
//   - real seeks are not uniform: 63% hit the fast-lane (upper(0) >= hi) on mainnet
//
// Use this benchmark only to measure raw binary-search cost on large EFs.
func BenchmarkSeek(b *testing.B) {
	const count = 1_000_000

	cases := []struct {
		name   string
		stride uint64
	}{
		{"stride1_l0", 1},
		{"stride123_l6", 123},
		{"stride1000_l9", 1000},
	}

	for _, tc := range cases {
		tc := tc
		ef := buildEF(count, tc.stride)
		maxOffset := (count - 1) * tc.stride

		targets := make([]uint64, count)
		for i := range targets {
			targets[i] = uint64(rand.Int64N(int64(maxOffset + 1)))
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_, _ = ef.Seek(targets[n%count])
				n++
			}
		})
	}
}

// BenchmarkSeekPool models real mainnet seek patterns:
//   - a pool of many small EFs placed at random offsets in a large global range
//     (matching the real distribution: 83% of EFs have 1–3 word upperBits = 8–24 bytes)
//   - seek targets uniform over the global range, so many seeks land before the first
//     element of the chosen EF, exercising the fast-lane path (upper(0) >= hi)
func BenchmarkSeekPool(b *testing.B) {
	const (
		numEFs    = 100_000
		globalMax = 1 << 25 // 33M — representative global value range
		stride    = 1000
	)

	rng := rand.New(rand.NewPCG(1, 2))

	efs := make([]*EliasFano, numEFs)
	for i := range efs {
		count := uint64(rng.IntN(7)) + 2 // 2–8 elements → 1–3 word upperBits
		start := uint64(rng.Int64N(globalMax - int64(count)*stride + 1))
		ef := NewEliasFano(count, start+(count-1)*stride)
		for j := uint64(0); j < count; j++ {
			ef.AddOffset(start + j*stride)
		}
		ef.Build()
		efs[i] = ef
	}

	targets := make([]uint64, numEFs)
	for i := range targets {
		targets[i] = uint64(rng.Int64N(globalMax + 1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		_, _ = efs[n%numEFs].Seek(targets[n%numEFs])
		n++
	}
}
