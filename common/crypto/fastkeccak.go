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
	keccak "github.com/Giulio2002/fastkeccak"
)

// fastKeccakState wraps fastkeccak.Hasher to implement the keccakState interface.
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

func (f *fastKeccakState) Read(p []byte) (int, error) {
	hash := f.h.Sum256()
	return copy(p, hash[:]), nil
}
