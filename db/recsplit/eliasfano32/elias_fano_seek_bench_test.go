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
	"fmt"
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

// BenchmarkGet2 measures Get2(i) which returns both element i and i+1.
func BenchmarkGet2(b *testing.B) {
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
		ef := buildEF(count, tc.stride)
		indices := rand.Perm(count - 1) // -1 because Get2 reads element i+1

		b.Run(tc.name+"/sequential", func(b *testing.B) {
			b.ReportAllocs()
			n := uint64(0)
			for b.Loop() {
				_, _ = ef.Get2(n % (count - 1))
				n++
			}
		})

		b.Run(tc.name+"/random", func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_, _ = ef.Get2(uint64(indices[n%(count-1)]))
				n++
			}
		})
	}
}

// BenchmarkSeek measures Seek on a single large EF with uniform-random targets.
// This does NOT reflect real-world usage because:
//   - real EFs are mostly tiny (83% have 1–3 word upperBits = 8–24 bytes)
//   - real seeks are not uniform: 63% hit the fast-lane (upper(0) >= hi) on mainnet
//
// Use this benchmark only to measure raw search cost on EFs of various sizes.
func BenchmarkSeek(b *testing.B) {
	counts := []uint64{32, 1000, 100_000}
	strides := []struct {
		name   string
		stride uint64
	}{
		{"stride1_l0", 1},
		{"stride123_l6", 123},
		{"stride1000_l9", 1000},
	}

	for _, count := range counts {
		for _, tc := range strides {
			ef := buildEF(count, tc.stride)
			maxOffset := (count - 1) * tc.stride

			const nTargets = 100_000
			targets := make([]uint64, nTargets)
			for i := range targets {
				targets[i] = uint64(rand.Int64N(int64(maxOffset + 1)))
			}

			b.Run(fmt.Sprintf("n%d/%s", count, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				n := 0
				for b.Loop() {
					_, _ = ef.Seek(targets[n%nTargets])
					n++
				}
			})
		}
	}
}

// BenchmarkAddOffset measures the build path (AddOffset + Build) cost.
func BenchmarkAddOffset(b *testing.B) {
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
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			maxOffset := (count - 1) * tc.stride
			for b.Loop() {
				ef := NewEliasFano(count, maxOffset)
				for i := uint64(0); i < count; i++ {
					ef.AddOffset(i * tc.stride)
				}
				ef.Build()
			}
		})
	}
}

// buildDoubleEF constructs a DoubleEliasFano with n+1 buckets using random deltas.
func buildDoubleEF(n int) *DoubleEliasFano {
	cumKeys := make([]uint64, n+1)
	position := make([]uint64, n+1)
	for i := 1; i <= n; i++ {
		cumKeys[i] = cumKeys[i-1] + uint64(rand.IntN(8)+1)
		position[i] = position[i-1] + uint64(rand.IntN(16)+1)
	}
	var ef DoubleEliasFano
	ef.Build(cumKeys, position)
	return &ef
}

// BenchmarkDoubleGet2 measures DoubleEliasFano.Get2 with random access over 1M buckets.
func BenchmarkDoubleGet2(b *testing.B) {
	const n = 1 << 20
	ef := buildDoubleEF(n)
	indices := rand.Perm(n)
	b.ResetTimer()
	b.ReportAllocs()
	var sink uint64
	i := 0
	for b.Loop() {
		c, p := ef.Get2(uint64(indices[i%n]))
		sink += c + p
		i++
	}
	_ = sink
}

// BenchmarkDoubleGet3 measures DoubleEliasFano.Get3 with random access over 1M buckets.
func BenchmarkDoubleGet3(b *testing.B) {
	const n = 1 << 20
	ef := buildDoubleEF(n)
	indices := rand.Perm(n)
	b.ResetTimer()
	b.ReportAllocs()
	var sink uint64
	i := 0
	for b.Loop() {
		c, cn, p := ef.Get3(uint64(indices[i%n]))
		sink += c + cn + p
		i++
	}
	_ = sink
}
