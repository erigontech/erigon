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

package rlp

import (
	"testing"
)

func BenchmarkDecode(b *testing.B) {
	enc := encodeTestSlice(90000)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()

	for b.Loop() {
		var s []uint
		if err := DecodeBytes(enc, &s); err != nil {
			b.Fatalf("Decode error: %v", err)
		}
	}
}

func BenchmarkDecodeIntSliceReuse(b *testing.B) {
	enc := encodeTestSlice(100000)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()

	var s []uint
	for b.Loop() {
		if err := DecodeBytes(enc, &s); err != nil {
			b.Fatalf("Decode error: %v", err)
		}
	}
}

func BenchmarkDecodeByteArrays(b *testing.B) {
	type slot struct {
		Addr [20]byte
		Key  [32]byte
	}
	in := make([]slot, 1024)
	for i := range in {
		in[i].Addr[0], in[i].Addr[19] = byte(i>>8)|0x80, byte(i)
		in[i].Key[0], in[i].Key[31] = byte(i>>8)|0x80, byte(i)
	}
	enc, err := EncodeToBytes(in)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()

	var out []slot
	for b.Loop() {
		if err := DecodeBytes(enc, &out); err != nil {
			b.Fatalf("Decode error: %v", err)
		}
	}
}
