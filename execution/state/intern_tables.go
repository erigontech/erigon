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

package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// InternStorageKey returns word as a StorageKey.
func (ibs *IntraBlockState) InternStorageKey(word *uint256.Int) accounts.StorageKey {
	return accounts.InternKey(word.Bytes32())
}

// InternAddress returns the low 20 bytes of word as an Address.
func (ibs *IntraBlockState) InternAddress(word *uint256.Int) accounts.Address {
	return accounts.InternAddress(word.Bytes20())
}
