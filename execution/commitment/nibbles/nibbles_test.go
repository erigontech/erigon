// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHexToCompact(t *testing.T) {
	tests := []struct{ hex, compact []byte }{
		// empty keys, with and without terminator.
		{hex: []byte{}, compact: []byte{0x00}},
		{hex: []byte{Terminator}, compact: []byte{0x20}},
		// odd length, no terminator
		{hex: []byte{1, 2, 3, 4, 5}, compact: []byte{0x11, 0x23, 0x45}},
		// even length, no terminator
		{hex: []byte{0, 1, 2, 3, 4, 5}, compact: []byte{0x00, 0x01, 0x23, 0x45}},
		// odd length, terminator
		{hex: []byte{15, 1, 12, 11, 8, Terminator /*term*/}, compact: []byte{0x3f, 0x1c, 0xb8}},
		// even length, terminator
		{hex: []byte{0, 15, 1, 12, 11, 8, Terminator /*term*/}, compact: []byte{0x20, 0x0f, 0x1c, 0xb8}},
	}
	for _, test := range tests {
		if c := HexToCompact(test.hex); !bytes.Equal(c, test.compact) {
			t.Errorf("HexToCompact(%x) -> %x, want %x", test.hex, c, test.compact)
		}
		if h := CompactToHex(test.compact); !bytes.Equal(h, test.hex) {
			t.Errorf("CompactToHex(%x) -> %x, want %x", test.compact, h, test.hex)
		}
	}
}

func TestKeybytesHex(t *testing.T) {
	tests := []struct{ key, hexIn, hexOut []byte }{
		{key: []byte{}, hexIn: []byte{Terminator}, hexOut: []byte{Terminator}},
		{key: []byte{}, hexIn: []byte{}, hexOut: []byte{Terminator}},
		{
			key:    []byte{0x12, 0x34, 0x56},
			hexIn:  []byte{1, 2, 3, 4, 5, 6, Terminator},
			hexOut: []byte{1, 2, 3, 4, 5, 6, Terminator},
		},
		{
			key:    []byte{0x12, 0x34, 0x5},
			hexIn:  []byte{1, 2, 3, 4, 0, 5, Terminator},
			hexOut: []byte{1, 2, 3, 4, 0, 5, Terminator},
		},
		{
			key:    []byte{0x12, 0x34, 0x56},
			hexIn:  []byte{1, 2, 3, 4, 5, 6},
			hexOut: []byte{1, 2, 3, 4, 5, 6, Terminator},
		},
	}
	for _, test := range tests {
		if h := KeybytesToHex(test.key); !bytes.Equal(h, test.hexOut) {
			t.Errorf("KeybytesToHex(%x) -> %x, want %x", test.key, h, test.hexOut)
		}
		if k := HexToKeybytes(test.hexIn); !bytes.Equal(k, test.key) {
			t.Errorf("HexToKeybytes(%x) -> %x, want %x", test.hexIn, k, test.key)
		}
	}
}

func TestHexCompactRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 1000; i++ {
		// random length 0..64
		l := rng.Intn(65)
		hex := make([]byte, l)
		for j := range hex {
			hex[j] = byte(rng.Intn(16)) // valid nibbles 0..15
		}

		// half with terminator, half without
		if i%2 == 0 {
			hex = append(hex, Terminator)
		}

		compact := HexToCompact(hex)
		got := CompactToHex(compact)

		if !bytes.Equal(got, hex) {
			t.Fatalf("roundtrip failed: input=%x compact=%x got=%x", hex, compact, got)
		}
	}
}

func TestCommonPrefixLen(t *testing.T) {
	tests := []struct {
		name string
		a, b []byte
		want int
	}{
		{name: "both empty", a: []byte{}, b: []byte{}, want: 0},
		{name: "a empty, b non-empty", a: []byte{}, b: []byte{1, 2, 3}, want: 0},
		{name: "a non-empty, b empty", a: []byte{1, 2, 3}, b: []byte{}, want: 0},
		{name: "equal slices", a: []byte{1, 2, 3, 4}, b: []byte{1, 2, 3, 4}, want: 4},
		{name: "no common prefix", a: []byte{1, 2, 3}, b: []byte{4, 5, 6}, want: 0},
		{name: "partial prefix", a: []byte{1, 2, 3, 4}, b: []byte{1, 2, 9, 9}, want: 2},
		{name: "a is prefix of b", a: []byte{1, 2, 3}, b: []byte{1, 2, 3, 4, 5}, want: 3},
		{name: "b is prefix of a", a: []byte{1, 2, 3, 4, 5}, b: []byte{1, 2, 3}, want: 3},
		{name: "single byte equal", a: []byte{7}, b: []byte{7}, want: 1},
		{name: "single byte differing", a: []byte{7}, b: []byte{8}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, CommonPrefixLen(tt.a, tt.b))
		})
	}
}

func TestHasTerm(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want bool
	}{
		{name: "empty", in: []byte{}, want: false},
		{name: "ends with terminator", in: []byte{1, 2, 3, Terminator}, want: true},
		{name: "ends with 0x0f", in: []byte{1, 2, 3, 0x0f}, want: false},
		{name: "ends with 0x00", in: []byte{1, 2, 3, 0x00}, want: false},
		{name: "single terminator", in: []byte{Terminator}, want: true},
		{name: "single non-terminator", in: []byte{0x05}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, HasTerm(tt.in))
		})
	}
}

func FuzzHexCompactRoundtrip(f *testing.F) {
	// Seed cases
	f.Add([]byte{})
	f.Add([]byte{Terminator})
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{15, 1, 12, 11, 8, Terminator})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Filter to valid hex nibbles (0..15) with optional terminator
		hex := make([]byte, 0, len(data))
		for _, b := range data {
			n := b % 17 // 0..16
			if n == Terminator {
				// Only allow terminator at the end
				continue
			}
			hex = append(hex, n)
		}
		// Optionally add terminator based on first byte
		if len(data) > 0 && data[0]%2 == 0 {
			hex = append(hex, Terminator)
		}

		compact := HexToCompact(hex)
		got := CompactToHex(compact)
		if !bytes.Equal(got, hex) {
			t.Fatalf("roundtrip failed: input=%x compact=%x got=%x", hex, compact, got)
		}
	})
}

func BenchmarkHexToCompact(b *testing.B) {
	testBytes := []byte{0, 15, 1, 12, 11, 8, Terminator /*term*/}
	for b.Loop() {
		HexToCompact(testBytes)
	}
}

func BenchmarkCompactToHex(b *testing.B) {
	testBytes := []byte{0, 15, 1, 12, 11, 8, Terminator /*term*/}
	for b.Loop() {
		CompactToHex(testBytes)
	}
}

func BenchmarkKeybytesToHex(b *testing.B) {
	testBytes := []byte{7, 6, 6, 5, 7, 2, 6, 2, Terminator}
	for b.Loop() {
		KeybytesToHex(testBytes)
	}
}

func BenchmarkHexToKeybytes(b *testing.B) {
	testBytes := []byte{7, 6, 6, 5, 7, 2, 6, 2, Terminator}
	for b.Loop() {
		HexToKeybytes(testBytes)
	}
}
