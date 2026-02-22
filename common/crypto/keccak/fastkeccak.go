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

package keccak

import (
	"hash"

	keccak "github.com/erigontech/fastkeccak"
)

// KeccakState wraps the keccak hasher (backed by fastkeccak). In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type KeccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// fastKeccakState wraps fastkeccak.Hasher to implement the KeccakState interface.
// It provides the same streaming hash interface but uses platform-optimized assembly
// (NEON SHA3 on arm64, unrolled permutation on amd64) for significantly faster hashing.
type fastKeccakState struct {
	h keccak.Hasher
}

func NewFastKeccak() *fastKeccakState {
	return &fastKeccakState{}
}

func (f *fastKeccakState) Write(p []byte) (int, error) {
	f.h.Write(p)
	return len(p), nil
}

func (f *fastKeccakState) Sum(b []byte) []byte {
	hash := f.h.Sum256()
	return append(b, hash[:]...)
}

func (f *fastKeccakState) Reset() {
	f.h.Reset()
}

func (f *fastKeccakState) Size() int      { return 32 }
func (f *fastKeccakState) BlockSize() int { return 136 }

func (f *fastKeccakState) Read(out []byte) (int, error) {
	return f.h.Read(out)
}

// Sum256 computes the Keccak-256 hash of data in a single shot with zero heap allocations
// on platforms with assembly backends (arm64, amd64).
func Sum256(data []byte) [32]byte {
	return keccak.Sum256(data)
}
