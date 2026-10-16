// Copyright 2022 The go-ethereum Authors
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

package state

import (
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

// TransientKey is an EIP-1153 slot key. It stays a raw word, not an interned StorageKey:
// transient slots never leave the transaction.
type TransientKey uint256.Int

// transientStorage is a representation of EIP-1153 "Transient Storage".
type transientStorage map[accounts.Address]*transientSlots

// transientScanLimit is a swiss-map group: Go hashes a 32-byte key even in a
// one-group map, and a linear scan of this many keys is cheaper than that hash.
const transientScanLimit = 8

type transientSlots struct {
	slots []transientSlot
	index map[TransientKey]int // nil until len(slots) > transientScanLimit
}

type transientSlot struct {
	key   TransientKey
	value uint256.Int
}

// newTransientStorage creates a new instance of a transientStorage.
func newTransientStorage() transientStorage {
	return make(transientStorage)
}

// Swap sets the transient-storage `value` for `key` at the given `addr` and returns the previous value.
func (t transientStorage) Swap(addr accounts.Address, key TransientKey, value uint256.Int) (prev uint256.Int) {
	s := t[addr]
	if s == nil {
		if value.IsZero() {
			return prev
		}
		s = &transientSlots{}
		t[addr] = s
	}
	if i := s.find(key); i >= 0 {
		prev = s.slots[i].value
		s.slots[i].value = value
		return prev
	}
	if !value.IsZero() {
		s.add(key, value)
	}
	return prev
}

// Get gets the transient storage for `key` at the given `addr`.
func (t transientStorage) Get(addr accounts.Address, key TransientKey) uint256.Int {
	if s := t[addr]; s != nil {
		if i := s.find(key); i >= 0 {
			return s.slots[i].value
		}
	}
	return uint256.Int{}
}

func (s *transientSlots) find(key TransientKey) int {
	if s.index != nil {
		if i, ok := s.index[key]; ok {
			return i
		}
		return -1
	}
	for i := range s.slots {
		if s.slots[i].key == key {
			return i
		}
	}
	return -1
}

func (s *transientSlots) add(key TransientKey, value uint256.Int) {
	s.slots = append(s.slots, transientSlot{key: key, value: value})
	if s.index != nil {
		s.index[key] = len(s.slots) - 1
		return
	}
	if len(s.slots) > transientScanLimit {
		s.index = make(map[TransientKey]int, 2*len(s.slots))
		for i := range s.slots {
			s.index[s.slots[i].key] = i
		}
	}
}
