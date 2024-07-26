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
	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
)

// transientStorage is a representation of EIP-1153 "Transient Storage".
type transientStorage map[libcommon.Address]Storage

// newTransientStorage creates a new instance of a transientStorage.
func newTransientStorage() transientStorage {
	return make(transientStorage)
}

// Set sets the transient-storage `value` for `key` at the given `addr`.
func (t transientStorage) Set(addr libcommon.Address, key libcommon.Hash, value uint256.Int) {
	if _, ok := t[addr]; !ok {
		t[addr] = make(Storage)
	}
	t[addr][key] = value
}

// Get gets the transient storage for `key` at the given `addr`.
func (t transientStorage) Get(addr libcommon.Address, key libcommon.Hash) uint256.Int {
	val, ok := t[addr]
	if !ok {
		return *uint256.NewInt(0)
	}
	return val[key]
}
