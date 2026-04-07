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
