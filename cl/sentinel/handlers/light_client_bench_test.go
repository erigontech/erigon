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

package handlers

import (
	"testing"

	"github.com/erigontech/erigon/common"
)

// BenchmarkLightClientPrefixConstruction benchmarks the prefix construction
// for light client responses, comparing the optimized version (stack allocation)
// against the old version (heap allocation with append).
func BenchmarkLightClientPrefixConstruction(b *testing.B) {
	forkDigest := common.Bytes4{0xAA, 0xBB, 0xCC, 0xDD}

	b.Run("Optimized", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var prefix [5]byte
			prefix[0] = SuccessfulResponsePrefix
			copy(prefix[1:], forkDigest[:])
			_ = prefix
		}
	})

	b.Run("Old", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			prefix := append([]byte{SuccessfulResponsePrefix}, forkDigest[:]...)
			_ = prefix
		}
	})
}
