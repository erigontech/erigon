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
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// balFoldUpdates runs the BAL-driven commitment fold the calculator performs: seed the
// version map from the BAL, stamp the EIP-161 removals into it as data, then emit each
// touched key via GetAccount. base supplies untouched fields; a high fold txNum reads the
// block-end floor. Returns the emitted leaf updates keyed by plain key.
func balFoldUpdates(t *testing.T, blockBAL types.BlockAccessList, eip161Enabled, isAura bool, base state.AccountBaseReader) map[string]commitment.Update {
	t.Helper()
	const foldTxNum = 1 << 20 // above every BAL change index → GetAccount reads the block-end floor
	vm := state.NewVersionMap(blockBAL)
	vm.SetBaseTxNum(0)
	vm.SeedEmptyRemovals(blockBAL, eip161Enabled, isAura, base)
	return emitTouchedUpdates(t, vm, foldTxNum, base, bal.ToWriteSet(blockBAL, math.MaxUint32))
}

func acctPlainKey(addr accounts.Address) string { v := addr.Value(); return string(v[:]) }

// TestBALFold_EmptyAccountBecomesDelete pins the EIP-161 reconstruction on the BAL path:
// the BAL carries no deletion marker, so a touched account whose block-end state is empty
// must fold to a leaf-removing DeleteUpdate (not a zero-valued leaf), while a funded one
// stays a regular update and a read-only account is never emitted.
func TestBALFold_EmptyAccountBecomesDelete(t *testing.T) {
	t.Parallel()

	emptied := accounts.InternAddress([20]byte{0xab})
	live := accounts.InternAddress([20]byte{0xcd})
	readOnly := accounts.InternAddress([20]byte{0xdd})
	blockBAL := types.BlockAccessList{
		{Address: emptied.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}},
		{Address: live.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(100)}}},
		{Address: readOnly.Value(), StorageReads: []common.Hash{{0x01}}},
	}

	got := balFoldUpdates(t, blockBAL, true, false, nil)

	require.Equal(t, commitment.DeleteUpdate, got[acctPlainKey(emptied)].Flags, "emptied account folds as DeleteUpdate")
	require.Equal(t, commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate, got[acctPlainKey(live)].Flags, "funded account folds as a regular update")
	require.Equal(t, *uint256.NewInt(100), got[acctPlainKey(live)].Balance)
	_, readOnlyEmitted := got[acctPlainKey(readOnly)]
	require.False(t, readOnlyEmitted, "a read-only BAL account must not be emitted")
}

// TestBALFold_PreSpuriousDragonKeepsEmpty pins the carve-out: with EIP-161 disabled, a
// touched empty account is created and persists, so it folds to a regular (zero) update.
func TestBALFold_PreSpuriousDragonKeepsEmpty(t *testing.T) {
	t.Parallel()

	empty := accounts.InternAddress([20]byte{0xab})
	blockBAL := types.BlockAccessList{
		{Address: empty.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}}},
	}

	got := balFoldUpdates(t, blockBAL, false, false, nil)
	require.Equal(t, commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate, got[acctPlainKey(empty)].Flags,
		"pre-SpuriousDragon an empty touched account persists as a regular update")
}

// TestBALFold_SelfDestructKeepsCodeNotDeleted pins the post-EIP-6780 corner: a drained
// (balance 0) account that still has code is NOT empty, so it must stay a regular update —
// folding it to a delete would drop a live leaf.
func TestBALFold_SelfDestructKeepsCodeNotDeleted(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress([20]byte{0x5d})
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	blockBAL := types.BlockAccessList{
		{
			Address:        contract.Value(),
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: uint256.Int{}}},
			CodeChanges:    []*types.CodeChange{{Index: 1, Bytecode: code}},
		},
	}

	got := balFoldUpdates(t, blockBAL, true, false, nil)
	require.NotEqual(t, commitment.DeleteUpdate, got[acctPlainKey(contract)].Flags,
		"a zero-balance account that still has code folds as a regular update, not a delete")
	require.Equal(t, accounts.NewCode(code).Hash.Value(), got[acctPlainKey(contract)].CodeHash)
}

// TestBALFold_AbsentAccountHasNoLeaf pins that an account the BAL folded away (e.g. a
// same-tx create+destroy) gets no entry at all — the fold must not fabricate a leaf.
func TestBALFold_AbsentAccountHasNoLeaf(t *testing.T) {
	t.Parallel()

	transient := accounts.InternAddress([20]byte{0x7e})
	live := accounts.InternAddress([20]byte{0xcd})
	blockBAL := types.BlockAccessList{
		{Address: live.Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(100)}}},
	}

	got := balFoldUpdates(t, blockBAL, true, false, nil)
	_, emitted := got[acctPlainKey(transient)]
	assert.False(t, emitted, "no leaf may be emitted for an account the BAL folded away")
}
