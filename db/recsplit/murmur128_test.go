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

package recsplit

import (
	"math/rand"
	"testing"

	"github.com/spaolacci/murmur3"
)

// murmur128WithSeed must be bit-identical to the library used to build existing index files
func TestMurmur128Equivalence(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	for length := 0; length <= 130; length++ {
		for trial := 0; trial < 20; trial++ {
			key := make([]byte, length)
			rnd.Read(key)
			seed := rnd.Uint32()
			wantHi, wantLo := murmur3.Sum128WithSeed(key, seed)
			gotHi, gotLo := murmur128WithSeed(key, seed)
			if gotHi != wantHi || gotLo != wantLo {
				t.Fatalf("mismatch len=%d seed=%d key=%x: got (%x,%x) want (%x,%x)",
					length, seed, key, gotHi, gotLo, wantHi, wantLo)
			}
		}
	}
}
