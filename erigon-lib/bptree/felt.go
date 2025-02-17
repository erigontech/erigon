// Copyright 2022 The Erigon Authors
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

package bptree

import (
	"crypto/sha256"
	"encoding/binary"
)

type Felt uint64

func (v *Felt) Binary() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(*v))
	return b
}

func hash2(bytes1, bytes2 []byte) []byte {
	hashBuilder := sha256.New()
	bytes1Written, _ := hashBuilder.Write(bytes1)
	ensure(bytes1Written == len(bytes1), "hash2: invalid number of bytes1 written")
	bytes2Written, _ := hashBuilder.Write(bytes2)
	ensure(bytes2Written == len(bytes2), "hash2: invalid number of bytes2 written")
	return hashBuilder.Sum(nil)
}

func deref(pointers []*Felt) []Felt {
	pointees := make([]Felt, 0)
	for _, ptr := range pointers {
		if ptr != nil {
			pointees = append(pointees, *ptr)
		} else {
			break
		}
	}
	return pointees
}
