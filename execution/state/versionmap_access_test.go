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
	"iter"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestTxAccessList_RecordsKeysAndReadsValuesFromMap pins the access-list contract: a tx's
// write-set is recorded key-only into the version map's per-tx access list, and WritesAt
// enumerates those keys while reading each value back from the map's cells — so the
// retained write-set need not escape execution.
func TestTxAccessList_RecordsKeysAndReadsValuesFromMap(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xa1})
	key := accounts.InternKey([32]byte{0x07})
	bal := *uint256.NewInt(1234)
	slotVal := *uint256.NewInt(77)
	const txIdx = 3

	vm := NewVersionMap(nil)

	// A tx's writes land in the version map's cells…
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

	// …and only its key-set is frozen into the access list.
	vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

	view := vm.WritesAtTxIndex(txIdx, false)
	require.False(t, view.IsEmpty())

	gotBal := false
	for a, vw := range view.Balances() {
		if a == addr {
			gotBal = true
			require.True(t, vw.Val.Eq(&bal), "balance value must be read back from the version map cell")
		}
	}
	require.True(t, gotBal, "recorded BalancePath key must enumerate")

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
	require.True(t, gotSlot, "recorded StoragePath key must enumerate")

	// An unrecorded slot yields an empty view.
	require.True(t, vm.WritesAtTxIndex(99, false).IsEmpty())
}

// TestTxAccessList_StorageChangesMatchesWriteSet pins the B→C equivalence: the
// key-only access-list view and a retained-write-set view produce the same
// StorageChanges result (net-zero ValueUnchanged writes filtered) because the
// status is read from the version map cell, not the key-set.
func TestTxAccessList_StorageChangesMatchesWriteSet(t *testing.T) {
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
	vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

	collect := func(view WriteSetView, seq func(WriteSetView) iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]]) map[accounts.StorageKey]uint256.Int {
		got := map[accounts.StorageKey]uint256.Int{}
		for a, slots := range seq(view) {
			if a != addr {
				continue
			}
			for k, vw := range slots {
				got[k] = vw.Val
			}
		}
		return got
	}
	changedSeq := func(v WriteSetView) iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
		return v.StorageChanges()
	}
	allSeq := func(v WriteSetView) iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
		return v.Storages()
	}

	wsView := NewVersionMapWriteView(ws, vm, txIdx)
	alView := vm.WritesAtTxIndex(txIdx, false)

	require.Equal(t, collect(wsView, changedSeq), collect(alView, changedSeq), "StorageChanges must match across view backings")
	require.Len(t, collect(alView, changedSeq), 1, "ValueUnchanged write must be filtered")
	require.Equal(t, collect(wsView, allSeq), collect(alView, allSeq), "Storages (unfiltered) must match across view backings")
	require.Len(t, collect(alView, allSeq), 2, "unfiltered view keeps both writes")
}

// TestVersionMap_BlockTxIndex pins the global-txNum ⇄ block-relative-TxIndex mapping
// (txNum = baseTxNum + TxIndex + 1) the access list is keyed by.
func TestVersionMap_BlockTxIndex(t *testing.T) {
	vm := NewVersionMap(nil)
	vm.SetBaseTxNum(100)
	require.Equal(t, -1, vm.blockTxIndex(100)) // block-start system tx
	require.Equal(t, 0, vm.blockTxIndex(101))  // first regular tx
	require.Equal(t, 5, vm.blockTxIndex(106))
}
