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

// Reference vectors from the source library (spaolacci/murmur3 murmur_test.go),
// kept verbatim so correctness does not rest solely on the differential tests.
var murmurRefVectors = []struct {
	seed   uint32
	h1, h2 uint64
	s      string
}{
	{0x00, 0x0000000000000000, 0x0000000000000000, ""},
	{0x00, 0xcbd8a7b341bd9b02, 0x5b1e906a48ae1d19, "hello"},
	{0x00, 0x342fac623a5ebc8e, 0x4cdcbc079642414d, "hello, world"},
	{0x00, 0xb89e5988b737affc, 0x664fc2950231b2cb, "19 Jan 2038 at 3:14:07 AM"},
	{0x00, 0xcd99481f9ee902c9, 0x695da1a38987b6e7, "The quick brown fox jumps over the lazy dog."},
	{0x01, 0x4610abe56eff5cb5, 0x51622daa78f83583, ""},
	{0x01, 0xa78ddff5adae8d10, 0x128900ef20900135, "hello"},
	{0x01, 0x8b95f808840725c6, 0x1597ed5422bd493b, "hello, world"},
	{0x01, 0x2a929de9c8f97b2f, 0x56a41d99af43a2db, "19 Jan 2038 at 3:14:07 AM"},
	{0x01, 0xfb3325171f9744da, 0xaaf8b92a5f722952, "The quick brown fox jumps over the lazy dog."},
	{0x2a, 0xf02aa77dfa1b8523, 0xd1016610da11cbb9, ""},
	{0x2a, 0xc4b8b3c960af6f08, 0x2334b875b0efbc7a, "hello"},
	{0x2a, 0xb91864d797caa956, 0xd5d139a55afe6150, "hello, world"},
	{0x2a, 0xfd8f19ebdc8c6b6a, 0xd30fdc310fa08ff9, "19 Jan 2038 at 3:14:07 AM"},
	{0x2a, 0x74f33c659cda5af7, 0x4ec7a891caf316f0, "The quick brown fox jumps over the lazy dog."},
}

func TestMurmur128RefVectors(t *testing.T) {
	for _, v := range murmurRefVectors {
		key := []byte(v.s)
		if h1, h2 := murmur128WithSeed(key, v.seed); h1 != v.h1 || h2 != v.h2 {
			t.Errorf("key %q seed %d: got (%x,%x) want (%x,%x)", v.s, v.seed, h1, h2, v.h1, v.h2)
		}
		for split := 0; split <= len(key); split++ {
			if h1, h2 := murmur128PairWithSeed(key[:split], key[split:], v.seed); h1 != v.h1 || h2 != v.h2 {
				t.Errorf("key %q seed %d split %d: got (%x,%x) want (%x,%x)", v.s, v.seed, split, h1, h2, v.h1, v.h2)
			}
		}
	}
}

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
