// Copyright 2024 The Erigon Authors
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

package common

import (
	"sync"

	keccak "github.com/erigontech/fastkeccak"
)

type Hasher struct {
	Sha keccak.KeccakState
}

var keccakStatePool = sync.Pool{
	New: func() any {
		return keccak.NewFastKeccak()
	},
}

// NewKeccakState returns a reset KeccakState from a shared pool.
func NewKeccakState() keccak.KeccakState {
	sha := keccakStatePool.Get().(keccak.KeccakState)
	sha.Reset()
	return sha
}

func ReturnKeccakState(sha keccak.KeccakState) { keccakStatePool.Put(sha) }

func NewHasher() *Hasher           { return &Hasher{Sha: NewKeccakState()} }
func ReturnHasherToPool(h *Hasher) { ReturnKeccakState(h.Sha) }

// HashData returns the Keccak-256 hash of data. The error is always nil.
func HashData(data []byte) Hash {
	return keccak.Sum256(data)
}
