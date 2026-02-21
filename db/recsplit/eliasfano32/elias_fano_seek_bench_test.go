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

// BenchmarkSeek measures Seek (searchForward) on a large sequence.
// Three seek patterns:
//   - exact:    seek to a value that exists in the sequence
//   - between:  seek to a value between two sequence elements (finds next)
//   - random:   seek to a uniformly random value in [0, maxOffset]
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

		// precompute seek targets so loop body is just the Seek call
		targets := make([]uint64, count)
		for i := range targets {
			targets[i] = uint64(rand.Int64N(int64(maxOffset + 1)))
		}
		exactTargets := make([]uint64, count)
		for i := range exactTargets {
			exactTargets[i] = uint64(rand.Int64N(int64(count))) * tc.stride
		}
		betweenTargets := make([]uint64, count)
		for i := range betweenTargets {
			// value that falls between two sequence elements (only meaningful when stride > 1)
			base := uint64(rand.Int64N(int64(count-1))) * tc.stride
			betweenTargets[i] = base + tc.stride/2 + 1
			if tc.stride == 1 {
				betweenTargets[i] = base // stride=1: no gap, use exact
			}
		}

		b.Run(tc.name+"/exact", func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_, _ = ef.Seek(exactTargets[n%count])
				n++
			}
		})

		b.Run(tc.name+"/between", func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_, _ = ef.Seek(betweenTargets[n%count])
				n++
			}
		})

		b.Run(tc.name+"/random", func(b *testing.B) {
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				_, _ = ef.Seek(targets[n%count])
				n++
			}
		})
	}
}

// TestSeekCorrectness verifies Seek returns correct results across stride patterns.
func TestSeekCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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
		t.Run(tc.name, func(t *testing.T) {
			const count = 1_000_000
			ef := buildEF(count, tc.stride)
			maxOffset := (count - 1) * tc.stride

			rng := rand.New(rand.NewPCG(42, 0))
			const seeks = 100_000
			notFound := 0
			for range seeks {
				v := uint64(rng.Int64N(int64(maxOffset + 1)))
				got, ok := ef.Seek(v)
				if !ok {
					notFound++
					continue
				}
				if got < v {
					t.Errorf("Seek(%d) returned %d < v", v, got)
				}
			}
			t.Logf("seeks=%d  notFound=%d", seeks, notFound)
		})
	}
}
