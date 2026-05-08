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
	"errors"
	"math/rand"
	"testing"
)

type v2Vector struct {
	name    string
	nibbles []byte
	key     []byte
}

func repeatByte(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func v2Vectors() []v2Vector {
	return []v2Vector{
		{
			name:    "empty",
			nibbles: []byte{},
			key:     []byte{0x00},
		},
		{
			name:    "single_nibble_a",
			nibbles: []byte{0xa},
			key:     []byte{0xa0, 0x01},
		},
		{
			name:    "two_nibbles_2f",
			nibbles: []byte{0x2, 0xf},
			key:     []byte{0x2f, 0x00},
		},
		{
			name:    "three_nibbles_2fb",
			nibbles: []byte{0x2, 0xf, 0xb},
			key:     []byte{0x2f, 0xb0, 0x01},
		},
		{
			name:    "four_nibbles_2fb3",
			nibbles: []byte{0x2, 0xf, 0xb, 0x3},
			key:     []byte{0x2f, 0xb3, 0x00},
		},
		{
			name:    "four_nibbles_2fb0_disambiguates_from_3",
			nibbles: []byte{0x2, 0xf, 0xb, 0x0},
			key:     []byte{0x2f, 0xb0, 0x00},
		},
		{
			name:    "four_zeros",
			nibbles: []byte{0x0, 0x0, 0x0, 0x0},
			key:     []byte{0x00, 0x00, 0x00},
		},
		{
			name:    "three_zeros",
			nibbles: []byte{0x0, 0x0, 0x0},
			key:     []byte{0x00, 0x00, 0x01},
		},
		{
			name:    "max_128_a",
			nibbles: repeatByte(0xa, 128),
			key:     append(repeatByte(0xaa, 64), 0x00),
		},
		{
			name:    "127_a",
			nibbles: repeatByte(0xa, 127),
			key:     append(append(repeatByte(0xaa, 63), 0xa0), 0x01),
		},
	}
}

func TestEncodeKeyV2_Vectors(t *testing.T) {
	for _, v := range v2Vectors() {
		t.Run(v.name, func(t *testing.T) {
			got := EncodeKeyV2(v.nibbles)
			if !bytes.Equal(got, v.key) {
				t.Fatalf("EncodeKeyV2(%x) = %x, want %x", v.nibbles, got, v.key)
			}
		})
	}
}

func TestDecodeKeyV2_Vectors(t *testing.T) {
	for _, v := range v2Vectors() {
		t.Run(v.name, func(t *testing.T) {
			got, err := DecodeKeyV2(v.key)
			if err != nil {
				t.Fatalf("DecodeKeyV2(%x) returned error: %v", v.key, err)
			}
			if !bytes.Equal(got, v.nibbles) {
				t.Fatalf("DecodeKeyV2(%x) = %x, want %x", v.key, got, v.nibbles)
			}
		})
	}
}

func TestEncodeKeyV2_RoundTrip(t *testing.T) {
	for _, v := range v2Vectors() {
		t.Run(v.name, func(t *testing.T) {
			encoded := EncodeKeyV2(v.nibbles)
			decoded, err := DecodeKeyV2(encoded)
			if err != nil {
				t.Fatalf("round-trip decode error: %v", err)
			}
			if !bytes.Equal(decoded, v.nibbles) {
				t.Fatalf("round-trip mismatch: got %x, want %x", decoded, v.nibbles)
			}
		})
	}
}

func TestEncodeKeyV2_MaxLen(t *testing.T) {
	got := EncodeKeyV2(make([]byte, 128))
	if len(got) != 65 {
		t.Fatalf("EncodeKeyV2(128 nibbles) length = %d, want 65", len(got))
	}
}

func TestDecodeKeyV2_Errors(t *testing.T) {
	overlong := make([]byte, 67)
	cases := []struct {
		name string
		key  []byte
		want error
	}{
		{
			name: "empty_input",
			key:  []byte{},
			want: ErrV2KeyLength,
		},
		{
			name: "overlong_67_bytes",
			key:  overlong,
			want: ErrV2KeyLength,
		},
		{
			name: "parity_byte_0x02",
			key:  []byte{0x2f, 0x02},
			want: ErrV2KeyParity,
		},
		{
			name: "parity_byte_0xff",
			key:  []byte{0x2f, 0xff},
			want: ErrV2KeyParity,
		},
		{
			name: "shape_parity1_no_packed_byte",
			key:  []byte{0x01},
			want: ErrV2KeyShape,
		},
		{
			name: "non_canonical_pad_last_byte",
			key:  []byte{0x2f, 0xb3, 0x01},
			want: ErrV2NonCanonicalPad,
		},
		{
			name: "non_canonical_pad_mid_key",
			key:  []byte{0x00, 0xa1, 0x01},
			want: ErrV2NonCanonicalPad,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeKeyV2(c.key)
			if err == nil {
				t.Fatalf("DecodeKeyV2(%x) = %x, want error %v", c.key, got, c.want)
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("DecodeKeyV2(%x) error = %v, want %v", c.key, err, c.want)
			}
			if got != nil {
				t.Fatalf("DecodeKeyV2(%x) returned %x with error, want nil result", c.key, got)
			}
		})
	}
}

func commonNibblePrefix(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func commonBytePrefix(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func assertLocality(t *testing.T, a, b []byte) {
	t.Helper()
	k := commonNibblePrefix(a, b)
	want := k / 2
	keyA := EncodeKeyV2(a)
	keyB := EncodeKeyV2(b)
	got := commonBytePrefix(keyA, keyB)
	if got < want {
		t.Fatalf("locality violation: paths %x and %x share k=%d nibbles, "+
			"want >= %d shared encoded bytes, got %d (keyA=%x, keyB=%x)",
			a, b, k, want, got, keyA, keyB)
	}
}

func TestEncodeKeyV2_SubtreeLocality(t *testing.T) {
	// Adversarial deterministic cases — parity-byte ordering edge cases from the
	// brainstorm. Each pair stresses an interaction between the trailing parity
	// byte and a continuation that begins with nibble 0.
	deterministic := []struct {
		name string
		a, b []byte
	}{
		{"identical_empty", []byte{}, []byte{}},
		{"empty_vs_single", []byte{}, []byte{0x1}},
		{"odd_parent_vs_grandchild_starting_with_0", []byte{0x2, 0xf, 0xb}, []byte{0x2, 0xf, 0xb, 0x0, 0x0}},
		{"odd_parent_vs_child_appending_0", []byte{0x2, 0xf, 0xb}, []byte{0x2, 0xf, 0xb, 0x0}},
		{"siblings_diverge_at_boundary_0_vs_1", []byte{0x2, 0xf, 0xb, 0x0}, []byte{0x2, 0xf, 0xb, 0x1}},
		{"even_parent_vs_odd_grandchild_starting_with_0", []byte{0x2, 0xf}, []byte{0x2, 0xf, 0x0}},
		{"identical_paths_three_nibbles", []byte{0xa, 0xb, 0xc}, []byte{0xa, 0xb, 0xc}},
		{"completely_disjoint_first_nibble", []byte{0xa}, []byte{0xb}},
		{"max_length_adjacent_last_nibble_differs", repeatByte(0xa, 128), append(repeatByte(0xa, 127), 0xb)},
		{"odd_parent_zero_grandchild_zero", []byte{0x0, 0x0, 0x0}, []byte{0x0, 0x0, 0x0, 0x0, 0x0}},
	}
	for _, c := range deterministic {
		t.Run("det/"+c.name, func(t *testing.T) {
			assertLocality(t, c.a, c.b)
		})
	}

	rng := rand.New(rand.NewSource(0xC0DEFEED))
	for i := range 10_000 {
		a := randomPath(rng)
		b := randomPath(rng)
		k := commonNibblePrefix(a, b)
		want := k / 2
		keyA := EncodeKeyV2(a)
		keyB := EncodeKeyV2(b)
		got := commonBytePrefix(keyA, keyB)
		if got < want {
			t.Fatalf("iter=%d: locality violation: paths %x and %x share k=%d nibbles, "+
				"want >= %d shared encoded bytes, got %d (keyA=%x, keyB=%x)",
				i, a, b, k, want, got, keyA, keyB)
		}
	}
}

func randomPath(rng *rand.Rand) []byte {
	length := rng.Intn(MaxPathNibbles + 1)
	out := make([]byte, length)
	for i := range out {
		out[i] = byte(rng.Intn(16))
	}
	return out
}

func TestEncodeKeyV2_Panics(t *testing.T) {
	cases := []struct {
		name    string
		nibbles []byte
	}{
		{
			name:    "nibble_0x10",
			nibbles: []byte{0x10},
		},
		{
			name:    "nibble_0xff",
			nibbles: []byte{0x1, 0xff, 0x2},
		},
		{
			name:    "length_129_over_max",
			nibbles: make([]byte, 129),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("EncodeKeyV2(%x) did not panic", c.nibbles)
				}
			}()
			_ = EncodeKeyV2(c.nibbles)
		})
	}
}

func FuzzEncodeDecodeKeyV2(f *testing.F) {
	seedLengths := []uint{0, 1, 2, 3, 4, 8, 9, 64, 127, 128}
	for i, length := range seedLengths {
		f.Add(length, uint64(0xC0DEFEED+i))
	}
	f.Fuzz(func(t *testing.T, length uint, seed uint64) {
		// clamp to valid range; encoder panics above MaxPathNibbles by contract
		n := int(length % (MaxPathNibbles + 1))
		rng := rand.New(rand.NewSource(int64(seed)))
		path := make([]byte, n)
		for i := range path {
			path[i] = byte(rng.Intn(16))
		}
		encoded := EncodeKeyV2(path)
		decoded, err := DecodeKeyV2(encoded)
		if err != nil {
			t.Fatalf("decode error after encode: path=%x encoded=%x err=%v", path, encoded, err)
		}
		if !bytes.Equal(decoded, path) {
			t.Fatalf("round-trip mismatch: path=%x encoded=%x decoded=%x", path, encoded, decoded)
		}
	})
}
