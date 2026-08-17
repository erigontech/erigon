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

// transientStorage implements EIP-1153 transient storage.
type transientStorage map[accounts.Address]Storage

func newTransientStorage() transientStorage {
	return make(transientStorage)
}

func (t transientStorage) Set(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	slots, ok := t[addr]
	if value.IsZero() {
		if !ok {
			return
		}
		slots[key] = value
		return
	}

	if !ok {
		slots = make(Storage)
		t[addr] = slots
	}
	slots[key] = value
}

func (t transientStorage) Get(addr accounts.Address, key accounts.StorageKey) uint256.Int {
	val, ok := t[addr]
	if !ok {
		return uint256.Int{}
	}
	return val[key]
}
