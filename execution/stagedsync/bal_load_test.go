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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// finalChange must pick the change with the highest tx Index — the
// block-end value — regardless of slice order.
func TestFinalChange(t *testing.T) {
	t.Parallel()

	if _, ok := finalChange([]*types.BalanceChange(nil)); ok {
		t.Fatal("finalChange on an empty list returned ok=true")
	}

	single, ok := finalChange([]*types.BalanceChange{
		{Index: 7, Value: *uint256.NewInt(70)},
	})
	require.True(t, ok)
	assert.Equal(t, uint64(70), single.Value.Uint64())

	ascending, ok := finalChange([]*types.BalanceChange{
		{Index: 0, Value: *uint256.NewInt(10)},
		{Index: 2, Value: *uint256.NewInt(20)},
		{Index: 9, Value: *uint256.NewInt(90)},
	})
	require.True(t, ok)
	assert.Equal(t, uint64(90), ascending.Value.Uint64())

	// Highest index wins even when it is not last in slice order.
	outOfOrder, ok := finalChange([]*types.BalanceChange{
		{Index: 5, Value: *uint256.NewInt(500)},
		{Index: 2, Value: *uint256.NewInt(200)},
	})
	require.True(t, ok)
	assert.Equal(t, uint64(500), outOfOrder.Value.Uint64())
}

// TestLoadFromBAL_MatchesApplyWrites is the Stage-1 differential test:
// loading calcState from a BAL must produce exactly the same accumulated
// state as feeding the equivalent per-tx VersionedWrites stream through
// ApplyWrites. The BAL carries multiple tx-indexed changes per key; the
// incremental side feeds the same values as a multi-write stream in tx
// order — last-write-wins on that side must equal highest-index on the
// BAL side.
func TestLoadFromBAL_MatchesApplyWrites(t *testing.T) {
	t.Parallel()

	addrA := accounts.InternAddress([20]byte{0xaa})
	addrB := accounts.InternAddress([20]byte{0xbb})
	addrC := accounts.InternAddress([20]byte{0xcc})
	addrD := accounts.InternAddress([20]byte{0xdd}) // reads only — must be ignored
	addrE := accounts.InternAddress([20]byte{0xee})

	slotS1 := accounts.InternKey(common.Hash{0x01})
	slotS2 := accounts.InternKey(common.Hash{0x02})
	codeC := []byte{0x60, 0x00, 0x60, 0x00, 0x52}

	bal := types.BlockAccessList{
		{
			Address: addrA,
			BalanceChanges: []*types.BalanceChange{
				{Index: 0, Value: *uint256.NewInt(10)},
				{Index: 3, Value: *uint256.NewInt(99)}, // final
			},
			NonceChanges: []*types.NonceChange{
				{Index: 1, Value: 5},
			},
		},
		{
			Address: addrB,
			StorageChanges: []*types.SlotChanges{
				{Slot: slotS1, Changes: []*types.StorageChange{
					{Index: 0, Value: *uint256.NewInt(1)},
					{Index: 2, Value: *uint256.NewInt(77)}, // final
				}},
				{Slot: slotS2, Changes: []*types.StorageChange{
					{Index: 1, Value: *uint256.NewInt(42)},
				}},
			},
		},
		{
			Address: addrC,
			BalanceChanges: []*types.BalanceChange{
				{Index: 0, Value: *uint256.NewInt(1000)},
			},
			CodeChanges: []*types.CodeChange{
				{Index: 0, Bytecode: codeC},
			},
		},
		{
			Address:      addrD,
			StorageReads: []accounts.StorageKey{slotS1}, // pure read — ignored
		},
		{
			Address: addrE,
			BalanceChanges: []*types.BalanceChange{
				{Index: 5, Value: *uint256.NewInt(500)}, // out-of-order; final
				{Index: 2, Value: *uint256.NewInt(200)},
			},
		},
	}

	csBAL := newTestCalcState()
	csBAL.LoadFromBAL(bal)

	// Incremental equivalent: the same values fed as a multi-write stream
	// in ascending tx order — ApplyWrites' last-write-wins must land on
	// the same end state as the BAL's highest-index value.
	csInc := newTestCalcState()
	csInc.ApplyWrites(state.VersionedWrites{
		{Address: addrA, Path: state.BalancePath, Val: *uint256.NewInt(10)},
		{Address: addrA, Path: state.BalancePath, Val: *uint256.NewInt(99)},
		{Address: addrA, Path: state.NoncePath, Val: uint64(5)},
		{Address: addrB, Path: state.StoragePath, Key: slotS1, Val: *uint256.NewInt(1)},
		{Address: addrB, Path: state.StoragePath, Key: slotS1, Val: *uint256.NewInt(77)},
		{Address: addrB, Path: state.StoragePath, Key: slotS2, Val: *uint256.NewInt(42)},
		{Address: addrC, Path: state.BalancePath, Val: *uint256.NewInt(1000)},
		{Address: addrC, Path: state.CodePath, Val: codeC},
		{Address: addrE, Path: state.BalancePath, Val: *uint256.NewInt(200)},
		{Address: addrE, Path: state.BalancePath, Val: *uint256.NewInt(500)},
	})

	assert.Equal(t, csInc.accounts, csBAL.accounts, "accounts: BAL load diverged from the write stream")
	assert.Equal(t, csInc.storageState, csBAL.storageState, "storageState: BAL load diverged from the write stream")
	assert.Equal(t, csInc.storageDirty, csBAL.storageDirty, "storageDirty: BAL load diverged from the write stream")

	if _, ok := csBAL.accounts[addrD]; ok {
		t.Error("a read-only BAL account leaked into calcState — LoadFromBAL must ignore StorageReads")
	}
}
