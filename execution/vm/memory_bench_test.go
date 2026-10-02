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
	"fmt"
	"testing"
)

func BenchmarkResize(b *testing.B) {
	memory := NewMemory()
	var i uint64
	for b.Loop() {
		memory.Resize(i)
		i++
	}
}

// BenchmarkResizeCold grows a fresh memory word-by-word, the pattern a call
// frame sees when it gets a CallContext whose buffer the pool has not warmed.
func BenchmarkResizeCold(b *testing.B) {
	for _, target := range []uint64{4 * 1024, 64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("%dKiB", target/1024), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var m Memory
				for size := uint64(32); size <= target; size += 32 {
					m.Resize(size)
				}
			}
		})
	}
}
