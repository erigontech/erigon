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

package crypto

import (
	"testing"

	keccak "github.com/Giulio2002/fastkeccak"
	"golang.org/x/crypto/sha3"
)

// BenchmarkKeccak256_Sha3 benchmarks the standard x/crypto/sha3 LegacyKeccak256 one-shot hash.
func BenchmarkKeccak256_Sha3(b *testing.B) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := sha3.NewLegacyKeccak256()
		h.Write(data)
		h.Sum(nil)
	}
}

// BenchmarkKeccak256_FastKeccak benchmarks the fastkeccak Sum256 one-shot hash.
func BenchmarkKeccak256_FastKeccak(b *testing.B) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keccak.Sum256(data)
	}
}

// BenchmarkKeccakStreaming_Sha3 benchmarks the standard sha3 streaming hasher (Reset+Write+Read).
func BenchmarkKeccakStreaming_Sha3(b *testing.B) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}
	h := sha3.NewLegacyKeccak256().(keccakState)
	var buf [32]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		h.Write(data)
		h.Read(buf[:])
	}
}

// BenchmarkKeccakStreaming_FastKeccak benchmarks the fastkeccak streaming hasher (Reset+Write+Sum256).
func BenchmarkKeccakStreaming_FastKeccak(b *testing.B) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}
	var h keccak.Hasher
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		h.Write(data)
		h.Sum256()
	}
}

// BenchmarkKeyToHexNibbleHash benchmarks the key-to-nibble hashing used in commitment (uses fastkeccak).
func BenchmarkKeyToHexNibbleHash(b *testing.B) {
	key := make([]byte, 20) // account key
	for i := range key {
		key[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		KeyToHexNibbleHash(key)
	}
}
