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

package simdhex

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func BenchmarkAppendEncode(b *testing.B) {
	for _, n := range []int{20, 32, 64, 256, 1024, 4096} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(i * 7)
		}
		out := make([]byte, 2*n)
		b.Run(fmt.Sprintf("stdlib/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				hex.Encode(out, src)
			}
		})
		b.Run(fmt.Sprintf("table/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				encodeTable(out, src)
			}
		})
		b.Run(fmt.Sprintf("swar/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				encodeSWAR(out, src)
			}
		})
		b.Run(fmt.Sprintf("simd/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				Encode(out, src)
			}
		})
	}
}
