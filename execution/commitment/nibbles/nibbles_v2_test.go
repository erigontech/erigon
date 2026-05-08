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
