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

package bal

import (
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type indexedChange interface{ GetIndex() uint32 }

func finalChangeUpTo[T indexedChange](changes []T, maxTxIndex uint32) (T, bool) {
	for _, change := range slices.Backward(changes) {
		if change.GetIndex() <= maxTxIndex {
			return change, true
		}
	}
	var zero T
	return zero, false
}

// ToWriteSet returns the latest BAL changes at or before maxTxIndex as state writes.
func ToWriteSet(blockAccessList types.BlockAccessList, maxTxIndex uint32) *state.WriteSet {
	writes := &state.WriteSet{}
	for _, accountChanges := range blockAccessList {
		addr := accountChanges.Address
		if balance, ok := finalChangeUpTo(accountChanges.BalanceChanges, maxTxIndex); ok {
			writes.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: balance.Value,
			})
		}
		if nonce, ok := finalChangeUpTo(accountChanges.NonceChanges, maxTxIndex); ok {
			writes.SetNonce(addr, &state.VersionedWrite[uint64]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: nonce.Value,
			})
		}
		if code, ok := finalChangeUpTo(accountChanges.CodeChanges, maxTxIndex); ok {
			writes.SetCode(addr, &state.VersionedWrite[accounts.Code]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.CodePath}, Val: accounts.NewCode(code.Bytecode),
			})
		}
		for _, slotChanges := range accountChanges.StorageChanges {
			if change, ok := finalChangeUpTo(slotChanges.Changes, maxTxIndex); ok {
				writes.SetStorage(addr, slotChanges.Slot, &state.VersionedWrite[uint256.Int]{
					WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slotChanges.Slot}, Val: change.Value,
				})
			}
		}
	}
	return writes
}
