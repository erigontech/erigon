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

var hashersPool = sync.Pool{
	New: func() any {
		return &Hasher{Sha: keccak.NewFastKeccak()}
	},
}

func NewHasher() *Hasher {
	h := hashersPool.Get().(*Hasher)
	h.Sha.Reset()
	return h
}
func ReturnHasherToPool(h *Hasher) { hashersPool.Put(h) }

func HashData(data []byte) (Hash, error) {
	h := NewHasher()
	defer ReturnHasherToPool(h)

	_, err := h.Sha.Write(data)
	if err != nil {
		return Hash{}, err
	}

	var buf Hash
	_, err = h.Sha.Read(buf[:])
	if err != nil {
		return Hash{}, err
	}
	return buf, nil
}
