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

package state

import (
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// pubkeyIndexBase is immutable after construction and shared by a state and its copies.
type pubkeyIndexBase struct {
	entries map[[48]byte]uint64
	length  int
}

type pubkeyIndex struct {
	base  *pubkeyIndexBase
	delta map[[48]byte]uint64
}

func newPubkeyIndex(validators *solid.ValidatorSet) pubkeyIndex {
	length := validators.Length()
	entries := make(map[[48]byte]uint64, length)
	for index := range length {
		entries[validators.Get(index).PublicKey()] = uint64(index)
	}
	return pubkeyIndex{base: &pubkeyIndexBase{entries: entries, length: length}}
}

func pubkeyIndexFromBase(base *pubkeyIndexBase, validators *solid.ValidatorSet) pubkeyIndex {
	if base == nil {
		return newPubkeyIndex(validators)
	}
	deltaLength := max(validators.Length()-base.length, 0)
	copied := pubkeyIndex{base: base}
	if deltaLength > 0 {
		copied.delta = make(map[[48]byte]uint64, deltaLength)
	}
	for offset := range deltaLength {
		index := base.length + offset
		copied.delta[validators.Get(index).PublicKey()] = uint64(index)
	}
	return copied
}

func (p *pubkeyIndex) add(key [48]byte, index uint64) {
	if p.delta == nil {
		p.delta = make(map[[48]byte]uint64)
	}
	p.delta[key] = index
}

func (p *pubkeyIndex) get(key [48]byte) (uint64, bool) {
	if index, ok := p.delta[key]; ok {
		return index, true
	}
	if p.base == nil {
		return 0, false
	}
	index, ok := p.base.entries[key]
	return index, ok
}
