// Copyright 2025 The Erigon Authors
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

// Package-level baseline tests for the 2-cache IBS refactor (issue #19623).
// Phase 1: tests only, no production code changes.
// These tests document current invariants and will catch regressions across
// subsequent phases.
package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// writeIndex builds a lookup map from a VersionedWrites slice.
// Key is (Address, Path, Key); value is the Val field.
func writeIndex(writes VersionedWrites) map[AccountKey]any {
	idx := make(map[AccountKey]any, len(writes))
	for _, w := range writes {
		idx[AccountKey{Path: w.Path, Key: w.Key}] = w.Val
		// Keyed per-address via a composite; store per-address sub-map below.
		_ = w.Address
	}
	return idx
}

// addrWriteIndex is like writeIndex but scoped to a single address.
func addrWriteIndex(writes VersionedWrites, addr accounts.Address) map[AccountKey]any {
	idx := make(map[AccountKey]any)
	for _, w := range writes {
		if w.Address == addr {
			idx[AccountKey{Path: w.Path, Key: w.Key}] = w.Val
		}
	}
	return idx
}

// TestVersionedWritesMatchStateObjects verifies that after a sequence of EVM
// state operations every field in every stateObject is reflected in
// VersionedWrites with the same value.
//
// This is the fundamental invariant that allows Phase 2 to derive StateUpdates
// directly from VersionedWrites without reconstructing an IBS.
func TestVersionedWritesMatchStateObjects(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 0)

	addr1 := accounts.InternAddress(common.HexToAddress("0x1111"))
	addr2 := accounts.InternAddress(common.HexToAddress("0x2222"))
	key1 := accounts.InternKey(common.HexToHash("0x0001"))
	key2 := accounts.InternKey(common.HexToHash("0x0002"))
	code1 := []byte{0xde, 0xad, 0xbe, 0xef}

	// addr1: balance + nonce + code + two storage slots
	err := ibs.SetBalance(addr1, *uint256.NewInt(100), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr1, 7)
	require.NoError(t, err)
	err = ibs.SetCode(addr1, code1)
	require.NoError(t, err)
	err = ibs.SetState(addr1, key1, *uint256.NewInt(42))
	require.NoError(t, err)
	err = ibs.SetState(addr1, key2, *uint256.NewInt(99))
	require.NoError(t, err)

	// addr2: CreateAccount + balance only
	ibs.CreateAccount(addr2, true)
	err = ibs.SetBalance(addr2, *uint256.NewInt(200), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)

	// Capture VersionedWrites BEFORE FinalizeTx (journal.dirties still intact).
	writes := ibs.VersionedWrites(true)

	// — addr1 checks —
	idx1 := addrWriteIndex(writes, addr1)

	wbal1, ok := idx1[AccountKey{Path: BalancePath, Key: accounts.NilKey}]
	require.True(t, ok, "addr1: BalancePath write missing from VersionedWrites")
	bal1, err := ibs.GetBalance(addr1)
	require.NoError(t, err)
	require.Equal(t, bal1, wbal1.(uint256.Int), "addr1: balance mismatch between stateObject and VersionedWrites")

	wnonce1, ok := idx1[AccountKey{Path: NoncePath, Key: accounts.NilKey}]
	require.True(t, ok, "addr1: NoncePath write missing from VersionedWrites")
	nonce1, err := ibs.GetNonce(addr1)
	require.NoError(t, err)
	require.Equal(t, nonce1, wnonce1.(uint64), "addr1: nonce mismatch between stateObject and VersionedWrites")

	wcode1, ok := idx1[AccountKey{Path: CodePath, Key: accounts.NilKey}]
	require.True(t, ok, "addr1: CodePath write missing from VersionedWrites")
	gotCode1, err := ibs.GetCode(addr1)
	require.NoError(t, err)
	require.Equal(t, gotCode1, wcode1.([]byte), "addr1: code mismatch between stateObject and VersionedWrites")

	wstor1, ok := idx1[AccountKey{Path: StoragePath, Key: key1}]
	require.True(t, ok, "addr1: StoragePath[key1] write missing from VersionedWrites")
	stor1, err := ibs.GetState(addr1, key1)
	require.NoError(t, err)
	require.Equal(t, stor1, wstor1.(uint256.Int), "addr1: storage[key1] mismatch between stateObject and VersionedWrites")

	wstor2, ok := idx1[AccountKey{Path: StoragePath, Key: key2}]
	require.True(t, ok, "addr1: StoragePath[key2] write missing from VersionedWrites")
	stor2, err := ibs.GetState(addr1, key2)
	require.NoError(t, err)
	require.Equal(t, stor2, wstor2.(uint256.Int), "addr1: storage[key2] mismatch between stateObject and VersionedWrites")

	// — addr2 checks —
	idx2 := addrWriteIndex(writes, addr2)

	wbal2, ok := idx2[AccountKey{Path: BalancePath, Key: accounts.NilKey}]
	require.True(t, ok, "addr2: BalancePath write missing from VersionedWrites")
	bal2, err := ibs.GetBalance(addr2)
	require.NoError(t, err)
	require.Equal(t, bal2, wbal2.(uint256.Int), "addr2: balance mismatch between stateObject and VersionedWrites")
}

// TestSnapshotRandomWithVersionMap extends the snapshot-revert correctness
// check to the versionMap path. It verifies that after reverting to a
// snapshot, both stateObject accessors and VersionedWrites agree on the
// pre-snapshot values.
//
// In Phase 5 (remove stateObject), the stateObject side of this comparison
// disappears; the VersionedWrites side must still pass.
func TestSnapshotRandomWithVersionMap(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress(common.HexToAddress("0xAAAA"))
	key := accounts.InternKey(common.HexToHash("0x0001"))

	ibs := NewWithVersionMap(reader, mvhm)
	ibs.SetTxContext(1, 0)

	// Pre-snapshot state
	err := ibs.SetBalance(addr, *uint256.NewInt(50), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr, 3)
	require.NoError(t, err)
	err = ibs.SetState(addr, key, *uint256.NewInt(11))
	require.NoError(t, err)

	snap := ibs.PushSnapshot()

	// Post-snapshot modifications
	err = ibs.SetBalance(addr, *uint256.NewInt(999), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr, 42)
	require.NoError(t, err)
	err = ibs.SetState(addr, key, *uint256.NewInt(77))
	require.NoError(t, err)

	// Revert to snapshot
	ibs.RevertToSnapshot(snap, nil)
	ibs.PopSnapshot(snap)

	// stateObject accessors must reflect pre-snapshot values
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(50), &bal, "balance should be reverted to pre-snapshot value")

	nonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(3), nonce, "nonce should be reverted to pre-snapshot value")

	stor, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(11), &stor, "storage should be reverted to pre-snapshot value")

	// VersionedWrites must reflect the same reverted values.
	writes := ibs.VersionedWrites(true)
	idx := addrWriteIndex(writes, addr)

	wbal, ok := idx[AccountKey{Path: BalancePath, Key: accounts.NilKey}]
	require.True(t, ok, "BalancePath write must still exist after revert")
	require.Equal(t, *uint256.NewInt(50), wbal.(uint256.Int), "VersionedWrites balance should reflect reverted value")

	wnonce, ok := idx[AccountKey{Path: NoncePath, Key: accounts.NilKey}]
	require.True(t, ok, "NoncePath write must still exist after revert")
	require.Equal(t, uint64(3), wnonce.(uint64), "VersionedWrites nonce should reflect reverted value")

	wstor, ok := idx[AccountKey{Path: StoragePath, Key: key}]
	require.True(t, ok, "StoragePath write must still exist after revert")
	require.Equal(t, *uint256.NewInt(11), wstor.(uint256.Int), "VersionedWrites storage should reflect reverted value")
}

// TestCommittedStateWithVersionMap verifies that GetCommittedState returns the
// pre-transaction value (the EIP-1283 "original value") when reading through
// a versionMap.
//
// Concretely: tx0 writes val1 to (addr, key) and flushes to the versionMap;
// tx1 then writes val2 to the same slot. A call to GetCommittedState from tx1
// must return val1 (the value as seen at the start of tx1), not val2.
//
// This invariant is required for SSTORE gas calculation correctness and must
// hold after all 2-cache refactor phases.
func TestCommittedStateWithVersionMap(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress(common.HexToAddress("0xBBBB"))
	key := accounts.InternKey(common.HexToHash("0x0001"))

	val1 := *uint256.NewInt(111)
	val2 := *uint256.NewInt(222)

	// — tx0 (txIndex 0) — writes val1, flushes to versionMap —
	ibs0 := NewWithVersionMap(reader, mvhm)
	ibs0.SetTxContext(1, 0)

	err := ibs0.SetState(addr, key, val1)
	require.NoError(t, err)

	// Capture and flush before FinalizeTx (journal.dirties still populated).
	writes0 := ibs0.VersionedWrites(true)
	mvhm.FlushVersionedWrites(writes0, true, "")

	// — tx1 (txIndex 1) — reads committed state before modifying —
	ibs1 := NewWithVersionMap(reader, mvhm)
	ibs1.SetTxContext(1, 1)

	// Before tx1 writes anything, committed state must be val1.
	committed, err := ibs1.GetCommittedState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val1, committed, "GetCommittedState must return pre-tx value (val1) before any tx1 write")

	// tx1 now writes val2 to the same slot.
	err = ibs1.SetState(addr, key, val2)
	require.NoError(t, err)

	// Even after writing val2, committed state must still be val1.
	committed2, err := ibs1.GetCommittedState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val1, committed2, "GetCommittedState must continue to return val1 after tx1 writes val2")

	// Current (non-committed) state must be val2.
	current, err := ibs1.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val2, current, "GetState must return the current tx1 value (val2)")
}

// TestCrossBlockStateReadConsistency verifies that a new IBS created for
// block N+1 correctly reads state written by block N through SharedDomains.
//
// This is the baseline cross-block read correctness test. It must pass
// throughout all 2-cache refactor phases.
func TestCrossBlockStateReadConsistency(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)

	addr := accounts.InternAddress(common.HexToAddress("0xDDDD"))
	key := accounts.InternKey(common.HexToHash("0x0001"))
	wantBalance := uint256.NewInt(777)
	wantNonce := uint64(13)
	wantStorage := *uint256.NewInt(555)

	// — Block N: write state then commit to domains via Writer —
	{
		ibsN := New(NewReaderV3(domains.AsGetter(tx)))
		ibsN.SetTxContext(1, 0)

		err := ibsN.SetBalance(addr, *wantBalance, tracing.BalanceChangeUnspecified)
		require.NoError(t, err)
		err = ibsN.SetNonce(addr, wantNonce)
		require.NoError(t, err)
		err = ibsN.SetState(addr, key, wantStorage)
		require.NoError(t, err)

		w := NewWriter(domains.AsPutDel(tx), nil, 2)
		err = ibsN.FinalizeTx(&chain.Rules{}, w)
		require.NoError(t, err)
	}

	// — Block N+1: fresh IBS reads state that block N wrote to domains —
	ibsN1 := New(NewReaderV3(domains.AsGetter(tx)))

	gotBal, err := ibsN1.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, wantBalance, &gotBal, "block N+1 must read block N's committed balance")

	gotNonce, err := ibsN1.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, wantNonce, gotNonce, "block N+1 must read block N's committed nonce")

	gotStorage, err := ibsN1.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, wantStorage, gotStorage, "block N+1 must read block N's committed storage")
}

// TestDomainApplyFromVersionedWrites verifies that applying VersionedWrites
// through the existing round-trip path (ApplyVersionedWrites → IBS →
// FinalizeTx → Writer) produces the correct domain state.
//
// This is the baseline that Phase 2's StateUpdatesFromVersionedWrites must
// reproduce without the IBS round-trip.
func TestDomainApplyFromVersionedWrites(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress(common.HexToAddress("0xEEEE"))
	key := accounts.InternKey(common.HexToHash("0x0001"))
	wantBalance := *uint256.NewInt(321)
	wantNonce := uint64(9)
	wantStorage := *uint256.NewInt(456)

	// — Step 1: produce VersionedWrites via a tx —
	ibsTx := NewWithVersionMap(reader, mvhm)
	ibsTx.SetTxContext(1, 0)

	err := ibsTx.SetBalance(addr, wantBalance, tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibsTx.SetNonce(addr, wantNonce)
	require.NoError(t, err)
	err = ibsTx.SetState(addr, key, wantStorage)
	require.NoError(t, err)

	writes := ibsTx.VersionedWrites(true)
	require.NotEmpty(t, writes, "VersionedWrites must not be empty")

	// — Step 2: apply VersionedWrites through existing round-trip path —
	ibsApply := New(reader)
	err = ibsApply.ApplyVersionedWrites(writes)
	require.NoError(t, err)

	w := NewWriter(domains.AsPutDel(tx), nil, 3)
	err = ibsApply.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	// — Step 3: read back from domains, assert correct state —
	ibsRead := New(NewReaderV3(domains.AsGetter(tx)))

	gotBal, err := ibsRead.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, wantBalance, gotBal, "domain must contain balance from VersionedWrites")

	gotNonce, err := ibsRead.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, wantNonce, gotNonce, "domain must contain nonce from VersionedWrites")

	gotStorage, err := ibsRead.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, wantStorage, gotStorage, "domain must contain storage from VersionedWrites")
}
