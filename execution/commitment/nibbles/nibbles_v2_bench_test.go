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

package nibbles

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
)

// BenchmarkLocalityV1VsV2 measures DB-key sort-order locality for V1 (HP-prefix
// front) vs V2 (suffix-parity) over 100K seeded random paths with realistic
// commitment-trie length distribution. For each scheme we sort encoded keys
// byte-wise and report the mean common nibble prefix length between consecutive
// neighbors. This is evidence for issue #17838 — not a pass/fail assertion.
func BenchmarkLocalityV1VsV2(b *testing.B) {
	const N = 100_000
	rng := rand.New(rand.NewSource(0xBEEFCAFE))
	paths := make([][]byte, N)
	for i := range paths {
		// realistic-ish: uniform over [16, MaxPathNibbles]; covers shallow internal
		// trie paths and full Keccak-derived account/storage paths
		length := 16 + rng.Intn(MaxPathNibbles-16+1)
		p := make([]byte, length)
		for j := range p {
			p[j] = byte(rng.Intn(16))
		}
		paths[i] = p
	}

	var v1Mean, v2Mean float64
	for b.Loop() {
		v1Keys := make([][]byte, N)
		v2Keys := make([][]byte, N)
		for i, p := range paths {
			v1Keys[i] = v1HexToCompactNoTerm(p)
			v2Keys[i] = EncodeKeyV2(p)
		}
		v1Idx := make([]int, N)
		v2Idx := make([]int, N)
		for i := range v1Idx {
			v1Idx[i] = i
			v2Idx[i] = i
		}
		slices.SortFunc(v1Idx, func(a, b int) int {
			return bytes.Compare(v1Keys[a], v1Keys[b])
		})
		slices.SortFunc(v2Idx, func(a, b int) int {
			return bytes.Compare(v2Keys[a], v2Keys[b])
		})

		var v1Sum, v2Sum int64
		for i := 1; i < N; i++ {
			v1Sum += int64(commonNibblePrefix(paths[v1Idx[i-1]], paths[v1Idx[i]]))
			v2Sum += int64(commonNibblePrefix(paths[v2Idx[i-1]], paths[v2Idx[i]]))
		}
		v1Mean = float64(v1Sum) / float64(N-1)
		v2Mean = float64(v2Sum) / float64(N-1)
	}
	b.ReportMetric(v1Mean, "v1_neighbor_prefix")
	b.ReportMetric(v2Mean, "v2_neighbor_prefix")
}
