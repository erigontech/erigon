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
	"bytes"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// finalChange returns the last (block-end) change; BAL change lists reach it
// already validated strictly-increasing by tx Index.
func TestFinalChange(t *testing.T) {
	t.Parallel()

	if _, ok := finalChangeUpTo([]*types.BalanceChange(nil), math.MaxUint32); ok {
		t.Fatal("finalChangeUpTo on an empty list returned ok=true")
	}

	single, ok := finalChangeUpTo([]*types.BalanceChange{
		{Index: 7, Value: *uint256.NewInt(70)},
	}, math.MaxUint32)
	require.True(t, ok)
	assert.Equal(t, uint64(70), single.Value.Uint64())

	ascending, ok := finalChangeUpTo([]*types.BalanceChange{
		{Index: 0, Value: *uint256.NewInt(10)},
		{Index: 2, Value: *uint256.NewInt(20)},
		{Index: 9, Value: *uint256.NewInt(90)},
	}, math.MaxUint32)
	require.True(t, ok)
	assert.Equal(t, uint64(90), ascending.Value.Uint64())
}

// finalChangeUpTo returns the field's value as of a mid-block tx index — the
// primitive that lets the fold checkpoint at a step boundary from the per-tx BAL.
func TestFinalChangeUpTo(t *testing.T) {
	t.Parallel()

	changes := []*types.BalanceChange{
		{Index: 0, Value: *uint256.NewInt(10)},
		{Index: 2, Value: *uint256.NewInt(20)},
		{Index: 9, Value: *uint256.NewInt(90)},
	}

	if _, ok := finalChangeUpTo([]*types.BalanceChange{{Index: 5, Value: *uint256.NewInt(1)}}, 4); ok {
		t.Fatal("ceiling below the only change must return ok=false")
	}

	// Exactly at, and between, change indices → latest change ≤ ceiling.
	at2, ok := finalChangeUpTo(changes, 2)
	require.True(t, ok)
	assert.Equal(t, uint64(20), at2.Value.Uint64(), "ceiling==Index returns that change")

	between, ok := finalChangeUpTo(changes, 5)
	require.True(t, ok)
	assert.Equal(t, uint64(20), between.Value.Uint64(), "ceiling between changes returns the earlier one")

	// At/above the last index → whole-block value (== finalChange).
	whole, ok := finalChangeUpTo(changes, 9)
	require.True(t, ok)
	assert.Equal(t, uint64(90), whole.Value.Uint64())
	all, ok := finalChangeUpTo(changes, ^uint32(0))
	require.True(t, ok)
	assert.Equal(t, uint64(90), all.Value.Uint64(), "MaxUint32 ceiling == finalChange")

	if _, ok := finalChangeUpTo([]*types.BalanceChange(nil), 100); ok {
		t.Fatal("finalChangeUpTo on an empty list returned ok=true")
	}
}

// TestLoadFromBAL_MatchesApplyWrites is a differential test:
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
				{Index: 2, Value: *uint256.NewInt(200)},
				{Index: 5, Value: *uint256.NewInt(500)}, // final
			},
		},
	}

	csBAL := newTestCalcState()
	csBAL.LoadFromBAL(bal, true, false)

	// Incremental equivalent: the same values fed as a multi-write stream
	// in ascending tx order — ApplyWrites' last-write-wins must land on
	// the same end state as the BAL's highest-index value.
	csInc := newTestCalcState()
	// The typed WriteSet is a per-(addr,path) map, so a repeated Set is
	// last-write-wins — the same end state the BAL's highest-index value yields.
	bal2 := func(a accounts.Address, p state.AccountPath) state.WriteHeader {
		return state.WriteHeader{Address: a, Path: p}
	}
	incWrites := &state.WriteSet{}
	incWrites.SetBalance(addrA, &state.VersionedWrite[uint256.Int]{WriteHeader: bal2(addrA, state.BalancePath), Val: *uint256.NewInt(10)})
	incWrites.SetBalance(addrA, &state.VersionedWrite[uint256.Int]{WriteHeader: bal2(addrA, state.BalancePath), Val: *uint256.NewInt(99)})
	incWrites.SetNonce(addrA, &state.VersionedWrite[uint64]{WriteHeader: bal2(addrA, state.NoncePath), Val: uint64(5)})
	incWrites.SetStorage(addrB, slotS1, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addrB, Path: state.StoragePath, Key: slotS1}, Val: *uint256.NewInt(1)})
	incWrites.SetStorage(addrB, slotS1, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addrB, Path: state.StoragePath, Key: slotS1}, Val: *uint256.NewInt(77)})
	incWrites.SetStorage(addrB, slotS2, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addrB, Path: state.StoragePath, Key: slotS2}, Val: *uint256.NewInt(42)})
	incWrites.SetBalance(addrC, &state.VersionedWrite[uint256.Int]{WriteHeader: bal2(addrC, state.BalancePath), Val: *uint256.NewInt(1000)})
	incWrites.SetCode(addrC, &state.VersionedWrite[accounts.Code]{WriteHeader: bal2(addrC, state.CodePath), Val: accounts.NewCode(codeC)})
	incWrites.SetBalance(addrE, &state.VersionedWrite[uint256.Int]{WriteHeader: bal2(addrE, state.BalancePath), Val: *uint256.NewInt(200)})
	incWrites.SetBalance(addrE, &state.VersionedWrite[uint256.Int]{WriteHeader: bal2(addrE, state.BalancePath), Val: *uint256.NewInt(500)})
	csInc.ApplyWrites(incWrites, false)

	assert.Equal(t, csInc.accounts, csBAL.accounts, "accounts: BAL load diverged from the write stream")
	assert.Equal(t, csInc.storageState, csBAL.storageState, "storageState: BAL load diverged from the write stream")
	assert.Equal(t, csInc.storageDirty, csBAL.storageDirty, "storageDirty: BAL load diverged from the write stream")

	if _, ok := csBAL.accounts[addrD]; ok {
		t.Error("a read-only BAL account leaked into calcState — LoadFromBAL must ignore StorageReads")
	}
}

// TestLoadFromBAL_EmptyAccountBecomesDelete pins the EIP-161 reconstruction: the
// BAL carries no deletion marker, so a touched account whose block-end state is
// empty (balance 0, nonce 0, empty code) must be marked Deleted by LoadFromBAL —
// otherwise FlushToUpdates writes a zero-valued leaf where serial removes the
// leaf, giving a divergent trie root.
func TestLoadFromBAL_EmptyAccountBecomesDelete(t *testing.T) {
	t.Parallel()

	emptied := accounts.InternAddress([20]byte{0xab})
	live := accounts.InternAddress([20]byte{0xcd})
	bal := types.BlockAccessList{
		{Address: emptied, BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(0)}}},
		{Address: live, BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(100)}}},
	}

	cs := newTestCalcState()
	cs.LoadFromBAL(bal, true, false) // spuriousDragon on, non-AuRa

	require.True(t, cs.accounts[emptied].Deleted,
		"an EIP-161 empty account in the BAL must be reconstructed as a delete, not a zero-valued update")
	require.False(t, cs.accounts[live].Deleted,
		"a non-empty account must remain a regular update")

	// Full chain: the reconstruction must actually emit a DeleteUpdate (leaf
	// removal) through FlushToUpdates, not just set the flag — that is what
	// makes the BAL-driven trie root match serial's DomainDel.
	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	emptiedKey := emptied.Value()
	assert.Equal(t, commitment.DeleteUpdate, lookupKeyUpdate(t, updates, string(emptiedKey[:])).Flags,
		"emptied account must flush as DeleteUpdate")

	// The carve-out: with SpuriousDragon inactive, empties are NOT removed
	// (pre-fork a touched empty account is created and persists).
	csPre := newTestCalcState()
	csPre.LoadFromBAL(types.BlockAccessList{
		{Address: emptied, BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(0)}}},
	}, false, false)
	require.False(t, csPre.accounts[emptied].Deleted,
		"pre-SpuriousDragon, an empty touched account is not removed")
}

// TestLoadFromBAL_SelfDestructKeepsCodeNotDeleted pins the post-EIP-6780 corner:
// SELFDESTRUCT of a pre-existing contract drains its balance (→0) but keeps its
// code and nonce, so its block-end state is NOT empty. The EIP-161 gate keys on
// codeHash==empty, so a zero-balance account that still has code must remain a
// regular update — reconstructing it as a delete would drop a live leaf and
// diverge the root. (Here the code rides in via a CodeChange; in production the
// codeHash comes from the pre-block state the fold lazy-loads.)
func TestLoadFromBAL_SelfDestructKeepsCodeNotDeleted(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress([20]byte{0x5d})
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	bal := types.BlockAccessList{
		{
			Address:        contract,
			BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(0)}},
			CodeChanges:    []*types.CodeChange{{Index: 0, Bytecode: code}},
		},
	}

	cs := newTestCalcState()
	cs.LoadFromBAL(bal, true, false) // SpuriousDragon on

	require.False(t, cs.accounts[contract].Deleted,
		"a zero-balance account that still has code must not be reconstructed as a delete")

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	key := contract.Value()
	assert.NotEqual(t, commitment.DeleteUpdate, lookupKeyUpdate(t, updates, string(key[:])).Flags,
		"a contract keeping its code must flush as a regular update, not DeleteUpdate")
}

// TestLoadFromBAL_CreatedThenDestroyedHasNoLeaf pins the EIP-6780 same-tx
// create+destroy case: AsBlockAccessList folds it to a net-zero, so the account
// gets no BAL entry at all — LoadFromBAL must then leave no leaf for it (a
// fabricated zero leaf would diverge from serial, which never creates one).
func TestLoadFromBAL_CreatedThenDestroyedHasNoLeaf(t *testing.T) {
	t.Parallel()

	transient := accounts.InternAddress([20]byte{0x7e})
	live := accounts.InternAddress([20]byte{0xcd})
	// The transient (created-then-destroyed) account is absent from the BAL.
	bal := types.BlockAccessList{
		{Address: live, BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(100)}}},
	}

	cs := newTestCalcState()
	cs.LoadFromBAL(bal, true, false)

	_, ok := cs.accounts[transient]
	require.False(t, ok, "a created-then-destroyed account absent from the BAL must not enter calcState")

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	key := transient.Value()
	found := false
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, _ *commitment.Update) error {
		if bytes.Equal(k, key[:]) {
			found = true
		}
		return nil
	}))
	assert.False(t, found, "no leaf may be emitted for an account the BAL folded away")
}
