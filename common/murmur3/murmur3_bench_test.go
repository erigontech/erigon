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

package murmur3

import (
	"fmt"
	"math/rand"
	"testing"

	libmurmur3 "github.com/spaolacci/murmur3"
)

func BenchmarkMurmur128(b *testing.B) {
	rnd := rand.New(rand.NewSource(44))
	for _, size := range murmurBenchSizes {
		key := make([]byte, size)
		rnd.Read(key)
		b.Run(fmt.Sprintf("port/len%d", size), func(b *testing.B) {
			var sink uint64
			for b.Loop() {
				h1, _ := Sum128WithSeed(key, 42)
				sink = h1
			}
			_ = sink
		})
		b.Run(fmt.Sprintf("library/len%d", size), func(b *testing.B) {
			var sink uint64
			for b.Loop() {
				h1, _ := libmurmur3.Sum128WithSeed(key, 42)
				sink = h1
			}
			_ = sink
		})
	}
}

// Key lengths match real index keys: 8=txnum, 20=address, 32=hash, 52=addr+slot, 80=commitment path
var murmurBenchSizes = []int{8, 20, 32, 52, 80, 128}
