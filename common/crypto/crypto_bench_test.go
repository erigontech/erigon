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

package crypto

import (
	"testing"
)

func BenchmarkSha3(b *testing.B) {
	a := []byte("hello world")
	for b.Loop() {
		Keccak256(a)
	}
}

func BenchmarkKeccak256Hash(b *testing.B) {
	b.Run("1", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkHash = Keccak256Hash(benchPayload1)
		}
	})
	b.Run("500", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkHash = Keccak256Hash(benchPayload)
		}
	})
	// A caller-local buffer: it must not escape to the heap.
	b.Run("local32", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var buf [32]byte
			sinkHash = Keccak256Hash(buf[:])
		}
	})
}

func BenchmarkKeccak256(b *testing.B) {
	b.Run("500", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkBytes = Keccak256(benchPayload)
		}
	})
	// The rlpx shape: two 32-byte inputs joined on the stack.
	b.Run("two32", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var x, y [32]byte
			sinkBytes = Keccak256(x[:], y[:])
		}
	})
	// A join too large for the stack buffer.
	b.Run("joined", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkBytes = Keccak256(benchPayload, benchPayload)
		}
	})
	b.Run("local32", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var buf [32]byte
			sinkBytes = Keccak256(buf[:])
		}
	})
}

var benchPayload = make([]byte, 500)

var benchPayload1 = make([]byte, 1)
