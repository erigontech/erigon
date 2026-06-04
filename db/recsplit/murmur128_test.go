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

// murmur128PairWithSeed(k1, k2) must equal hashing the concatenation k1||k2
func TestMurmur128PairEquivalence(t *testing.T) {
	rnd := rand.New(rand.NewSource(43))
	for len1 := 0; len1 <= 40; len1++ {
		for len2 := 0; len2 <= 40; len2++ {
			key1 := make([]byte, len1)
			key2 := make([]byte, len2)
			rnd.Read(key1)
			rnd.Read(key2)
			seed := rnd.Uint32()
			concat := append(append([]byte{}, key1...), key2...)
			wantHi, wantLo := murmur3.Sum128WithSeed(concat, seed)
			gotHi, gotLo := murmur128PairWithSeed(key1, key2, seed)
			if gotHi != wantHi || gotLo != wantLo {
				t.Fatalf("mismatch len1=%d len2=%d seed=%d: got (%x,%x) want (%x,%x)",
					len1, len2, seed, gotHi, gotLo, wantHi, wantLo)
			}
		}
	}
}

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
