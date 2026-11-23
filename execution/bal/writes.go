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

// changedIn returns the latest change at or before maxTxIndex when it is after afterTxIndex.
func changedIn[T indexedChange](changes []T, afterTxIndex int64, maxTxIndex uint32) (T, bool) {
	change, ok := finalChangeUpTo(changes, maxTxIndex)
	if !ok || int64(change.GetIndex()) <= afterTxIndex {
		var zero T
		return zero, false
	}
	return change, true
}

// ToWriteSet returns the latest BAL changes at or before maxTxIndex as state writes.
func ToWriteSet(blockAccessList types.BlockAccessList, maxTxIndex uint32) *state.WriteSet {
	return ToWriteSetAfter(blockAccessList, -1, maxTxIndex)
}

// ToWriteSetAfter is ToWriteSet limited to fields whose latest change at or before maxTxIndex is after afterTxIndex.
func ToWriteSetAfter(blockAccessList types.BlockAccessList, afterTxIndex int64, maxTxIndex uint32) *state.WriteSet {
	writes := &state.WriteSet{}
	for i := range blockAccessList {
		accountChanges := &blockAccessList[i]
		addr := accounts.InternAddress(accountChanges.Address)
		if balance, ok := changedIn(accountChanges.BalanceChanges, afterTxIndex, maxTxIndex); ok {
			writes.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: balance.Value,
			})
		}
		if nonce, ok := changedIn(accountChanges.NonceChanges, afterTxIndex, maxTxIndex); ok {
			writes.SetNonce(addr, &state.VersionedWrite[uint64]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: nonce.Value,
			})
		}
		if code, ok := changedIn(accountChanges.CodeChanges, afterTxIndex, maxTxIndex); ok {
			// Emit CodeHashPath alongside CodePath so codeHash is single-sourced from
			// the bytecode on every path; the BAL carries only the bytecode.
			c := accounts.NewCode(code.Bytecode)
			writes.SetCode(addr, &state.VersionedWrite[accounts.Code]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.CodePath}, Val: c,
			})
			writes.SetCodeHash(addr, &state.VersionedWrite[accounts.CodeHash]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.CodeHashPath}, Val: c.Hash,
			})
		}
		for _, slotChanges := range accountChanges.StorageChanges {
			if change, ok := changedIn(slotChanges.Changes, afterTxIndex, maxTxIndex); ok {
				writes.SetStorage(addr, slotChanges.Slot, &state.VersionedWrite[uint256.Int]{
					WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slotChanges.Slot}, Val: change.Value,
				})
			}
		}
	}
	return writes
}
