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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestVersionMapWriteView_ReadsValuesFromMap pins the write-view contract: it enumerates a
// tx's key-set and reads each value back from the version map's cells, so commitment and
// domain-apply need no retained write-set copy.
func TestVersionMapWriteView_ReadsValuesFromMap(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xa1})
	key := accounts.InternKey([32]byte{0x07})
	bal := *uint256.NewInt(1234)
	slotVal := *uint256.NewInt(77)
	const txIdx = 3

	vm := NewVersionMap(nil)

	ws := &WriteSet{}
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: txIdx}, valStatus: ValueChanged},
		Val:         bal,
	})
	ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: txIdx}, valStatus: ValueChanged},
		Val:         slotVal,
	})
	vm.FlushVersionedWrites(ws, true)

	view := NewVersionMapWriteView(ws, vm, txIdx)
	require.False(t, view.IsEmpty())

	gotBal := false
	for a, vw := range view.Balances() {
		if a == addr {
			gotBal = true
			require.True(t, vw.Val.Eq(&bal), "balance value must be read back from the version map cell")
		}
	}
	require.True(t, gotBal, "BalancePath key must enumerate")

	gotSlot := false
	for a, slots := range view.Storages() {
		if a != addr {
			continue
		}
		if vw, ok := slots[key]; ok {
			gotSlot = true
			require.True(t, vw.Val.Eq(&slotVal), "storage value must be read back from the version map cell")
		}
	}
	require.True(t, gotSlot, "StoragePath key must enumerate")

	// An empty key-set yields an empty view.
	require.True(t, NewVersionMapWriteView(&WriteSet{}, vm, 99).IsEmpty())
}

// TestVersionMapWriteView_StorageChangesFiltersNetZero pins that StorageChanges drops a
// net-zero (ValueUnchanged) write while Storages keeps it — the status read from the
// version map cell decides, so the domain/commitment leaf is untouched for net-zero.
func TestVersionMapWriteView_StorageChangesFiltersNetZero(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xb2})
	changed := accounts.InternKey([32]byte{0x01})
	unchanged := accounts.InternKey([32]byte{0x02})
	const txIdx = 4

	vm := NewVersionMap(nil)

	ws := &WriteSet{}
	ws.SetStorage(addr, changed, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: changed, Version: Version{TxIndex: txIdx}, valStatus: ValueChanged},
		Val:         *uint256.NewInt(9),
	})
	ws.SetStorage(addr, unchanged, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: unchanged, Version: Version{TxIndex: txIdx}, valStatus: ValueUnchanged},
		Val:         *uint256.NewInt(0),
	})
	vm.FlushVersionedWrites(ws, true)

	view := NewVersionMapWriteView(ws, vm, txIdx)

	changedCount := 0
	for a, slots := range view.StorageChanges() {
		if a == addr {
			changedCount += len(slots)
		}
	}
	require.Equal(t, 1, changedCount, "ValueUnchanged write must be filtered from StorageChanges")

	allCount := 0
	for a, slots := range view.Storages() {
		if a == addr {
			allCount += len(slots)
		}
	}
	require.Equal(t, 2, allCount, "unfiltered Storages keeps both writes")
}

// TestVersionMap_BlockTxIndex pins the global-txNum ⇄ block-relative-TxIndex mapping
// (txNum = baseTxNum + TxIndex + 1).
func TestVersionMap_BlockTxIndex(t *testing.T) {
	vm := NewVersionMap(nil)
	vm.SetBaseTxNum(100)
	require.Equal(t, -1, vm.blockTxIndex(100)) // block-start system tx
	require.Equal(t, 0, vm.blockTxIndex(101))  // first regular tx
	require.Equal(t, 5, vm.blockTxIndex(106))
}
