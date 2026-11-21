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

// transientScanLimit is how many slots are scanned before an index is built: Go
// hashes a 32-byte map key even in a one-group map, and the scan is cheaper.
const transientScanLimit = 32

// transientSlots only appends, so a slot index stays valid for the journal until
// the transaction ends.
type transientSlots struct {
	values []uint256.Int
	keys   []TransientKey       // scanned while small; nil once index is built
	index  map[TransientKey]int // nil until len(values) > transientScanLimit
}

// newTransientStorage creates a new instance of a transientStorage.
func newTransientStorage() transientStorage {
	return make(transientStorage)
}

// Set sets the transient-storage `value` for `key` at the given `addr` and journals the previous value.
func (t transientStorage) Set(j *journal, addr accounts.Address, key *TransientKey, value *uint256.Int) {
	s, i := t.find(addr, key)
	if i < 0 {
		if value.IsZero() {
			return
		}
		s, i = t.add(addr, key)
	}
	if prev := s.values[i]; prev != *value {
		j.transientStorageChange(addr, i, prev)
		s.values[i] = *value
	}
}

// restore writes back a value that Set journalled for the slot index. It skips slots
// that Prepare's clear dropped: a revert crosses it when transactions share one snapshot.
func (t transientStorage) restore(addr accounts.Address, slot int, value uint256.Int) {
	if s := t[addr]; s != nil && slot < len(s.values) {
		s.values[slot] = value
	}
}

// Get gets the transient storage for `key` at the given `addr`.
func (t transientStorage) Get(addr accounts.Address, key TransientKey) uint256.Int {
	if s := t[addr]; s != nil {
		if i := s.find(&key); i >= 0 {
			return s.values[i]
		}
	}
	return uint256.Int{}
}

func (t transientStorage) find(addr accounts.Address, key *TransientKey) (*transientSlots, int) {
	s := t[addr]
	if s == nil {
		return nil, -1
	}
	i := s.find(key)
	return s, i
}

func (t transientStorage) add(addr accounts.Address, key *TransientKey) (*transientSlots, int) {
	s := t[addr]
	if s == nil {
		s = &transientSlots{}
		t[addr] = s
	}
	i := s.add(key)
	return s, i
}

func (s *transientSlots) find(key *TransientKey) int {
	if s.index != nil {
		if i, ok := s.index[*key]; ok {
			return i
		}
		return -1
	}
	for i := range s.keys {
		if s.keys[i] == *key {
			return i
		}
	}
	return -1
}

func (s *transientSlots) add(key *TransientKey) int {
	i := len(s.values)
	s.values = append(s.values, uint256.Int{})
	if s.index != nil {
		s.index[*key] = i
		return i
	}
	s.keys = append(s.keys, *key)
	if len(s.keys) > transientScanLimit {
		s.index = make(map[TransientKey]int, 2*len(s.keys))
		for j := range s.keys {
			s.index[s.keys[j]] = j
		}
		s.keys = nil
	}
	return i
}
