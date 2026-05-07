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
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// newTestCalcState constructs a calcState with no domain reader (lazy-load
// disabled). Tests pre-populate cs.accounts directly to exercise
// FlushToUpdates' branching without needing a real SharedDomains.
func newTestCalcState() *calcState {
	return &calcState{
		accounts:     make(map[accounts.Address]*calcAccountState),
		storageState: make(map[accounts.Address]map[accounts.StorageKey]uint256.Int),
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]bool),
	}
}

func newTestUpdates() *commitment.Updates {
	return commitment.NewUpdates(commitment.ModeUpdate, "", commitment.KeyToHexNibbleHash)
}

// TestFlushToUpdates_DeletedWithIncarnation_EmitsZeroAccountUpdate is
// DEFENSIVE coverage for the first branch of FlushToUpdates' switch:
// when acc.Deleted is true AND Incarnation>0 AND all-zero fields, emit
// a zero-account UPDATE rather than a DeleteUpdate.
//
// Note: under the *current* normalizeWriteSet + ApplyWrites semantics
// this state is unreachable from a real LightCollector writeset —
// see TestSDOfPreExistingContract_FullPipeline below, which drives
// the full pipeline end-to-end and shows that completion entries for
// Balance/Nonce/CodeHash always arrive after SelfDestructPath and
// reset acc.Deleted=false. The actual mechanism that fixes the hive
// chain.rlp block 2 (4055cae5) regression is the default branch
// emitting the pre-block field values via the completion-loop path,
// not this zero-account branch.
//
// The test populates cs.accounts directly to cover the FlushToUpdates
// branch in isolation against future ApplyWrites/normalizeWriteSet
// changes (e.g. a refactor that drops the completion loop or stops
// resetting Deleted on subsequent field writes).
func TestFlushToUpdates_DeletedWithIncarnation_EmitsZeroAccountUpdate(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0x40, 0x55, 0xca, 0xe5})

	cs.accounts[addr] = &calcAccountState{
		Balance:     uint256.Int{},
		Nonce:       0,
		CodeHash:    empty.CodeHash,
		Incarnation: 1,
		Deleted:     true,
		dirty:       true,
	}

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)

	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))

	assert.Equal(t,
		commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate,
		got.Flags,
		"SD'd contract with incarnation>0 must emit BalanceUpdate|NonceUpdate|CodeUpdate, not DeleteUpdate")
	assert.True(t, got.Balance.IsZero(), "balance must be zero")
	assert.Equal(t, uint64(0), got.Nonce, "nonce must be zero")
	assert.Equal(t, empty.CodeHash, got.CodeHash, "codeHash must be empty.CodeHash")
}

// TestFlushToUpdates_DeletedWithoutIncarnation_EmitsDelete verifies that a
// touched-empty account (e.g. system address 0xff..fe after a Cancun
// EIP-4788 system call) is emitted as DeleteUpdate, matching serial's
// EIP-161 emptyRemoval path.
//
// Without this differentiation, parallel exec misses the leaf removal at
// post-Cancun blocks where the system address is touched-and-empty,
// producing a different trie root. This is the bug the import hit at
// block 42 (Cancun start).
func TestFlushToUpdates_DeletedWithoutIncarnation_EmitsDelete(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
	})

	cs.accounts[addr] = &calcAccountState{
		Balance:     uint256.Int{},
		Nonce:       0,
		CodeHash:    empty.CodeHash,
		Incarnation: 0,
		Deleted:     true,
		dirty:       true,
	}

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)

	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))

	assert.Equal(t, commitment.DeleteUpdate, got.Flags,
		"touched-empty account with incarnation==0 must emit DeleteUpdate (EIP-161 emptyRemoval)")
}

// TestFlushToUpdates_DeletedWithRetainedBalance_EmitsRegularUpdate is
// DEFENSIVE coverage for the third branch of FlushToUpdates' switch:
// when acc.Deleted is true but balance/nonce/codeHash retain non-zero
// values, the trie leaf must survive with the actual values rather
// than being zeroed by the SD-with-incarnation branch.
//
// Note: under the *current* ApplyWrites semantics this state is
// unreachable from a real LightCollector writeset, because
// `BalancePath` always clears `Deleted` (see
// TestApplyWrites_BalancePathClearsDeleted) and LightCollector emits
// `SelfDestructPath` before the `BalancePath` reset. The test
// populates cs.accounts directly to cover the FlushToUpdates branch
// in isolation against future ApplyWrites changes (e.g. write-order
// races, refactors that drop the Deleted-clearing in BalancePath, or
// new code paths that produce Deleted+RetainedBalance writesets).
//
// The actual `extcodehash_subcall_create2_oog[fork_Amsterdam-...]`
// regression that prompted this defensive case is fixed upstream by
// the removal of the redundant `IncarnationPath > 0` clause in
// `normalizeWriteSet` (the OOG path leaves Nonce=0 → empty-account
// → DeleteUpdate, not Deleted+RetainedBalance). End-to-end coverage
// of that path lives in the eest_devnet suite, not in this unit test.
func TestFlushToUpdates_DeletedWithRetainedBalance_EmitsRegularUpdate(t *testing.T) {
	cs := newTestCalcState()
	// 0x2adc25... is the CREATE2 deterministic address from the failing
	// fixture; any address works for the test.
	addr := accounts.InternAddress([20]byte{0x2a, 0xdc, 0x25, 0x66})
	retainedBalance := *uint256.NewInt(0x0421fe)

	cs.accounts[addr] = &calcAccountState{
		Balance:     retainedBalance,
		Nonce:       0,
		CodeHash:    empty.CodeHash,
		Incarnation: 1, // bumped during CREATE2 frame, retained through revert
		Deleted:     true,
		dirty:       true,
	}

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)

	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))

	assert.Equal(t,
		commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate,
		got.Flags,
		"Deleted account with retained non-zero balance must emit BalanceUpdate (regular UPDATE), NOT DeleteUpdate or zero-account UPDATE")
	assert.Equal(t, retainedBalance, got.Balance,
		"retained balance must survive into the trie leaf — zero-account UPDATE branch over-aggressively zeroed it")
	assert.Equal(t, uint64(0), got.Nonce, "nonce stays at 0")
	assert.Equal(t, empty.CodeHash, got.CodeHash, "codeHash stays empty (CREATE2 didn't deploy)")
}

// TestFlushToUpdates_LiveAccount_EmitsFullUpdate verifies the regular path:
// a non-deleted dirty account emits its actual balance/nonce/codeHash.
func TestFlushToUpdates_LiveAccount_EmitsFullUpdate(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0xab})
	bal := *uint256.NewInt(12345)
	codeHashArr := [32]byte{0xde, 0xad, 0xbe, 0xef}

	cs.accounts[addr] = &calcAccountState{
		Balance:  bal,
		Nonce:    7,
		CodeHash: codeHashArr,
		Deleted:  false,
		dirty:    true,
	}

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)

	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))

	assert.Equal(t,
		commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate,
		got.Flags)
	assert.Equal(t, bal, got.Balance)
	assert.Equal(t, uint64(7), got.Nonce)
	assert.Equal(t, common.Hash(codeHashArr), got.CodeHash)
}

// TestApplyWrites_IncarnationPath verifies that an IncarnationPath write
// from the apply pipeline is captured into the calcAccountState. This is
// the channel through which the executor signals "this account had a
// non-zero pre-block incarnation" alongside SelfDestructPath=true.
func TestApplyWrites_IncarnationPath(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0xc1})

	writes := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.IncarnationPath, Val: uint64(1)},
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, Val: true},
	}
	cs.ApplyWrites(writes)

	acc, ok := cs.accounts[addr]
	require.True(t, ok, "ensureAccount should have created an entry")
	assert.Equal(t, uint64(1), acc.Incarnation, "IncarnationPath write must populate Incarnation")
	assert.True(t, acc.Deleted, "SelfDestructPath=true must set Deleted")

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))
	assert.Equal(t,
		commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate,
		got.Flags,
		"Deleted+Incarnation>0 routes through the zero-account UPDATE branch")
}

// TestApplyWrites_BalancePathClearsDeleted verifies that a non-empty
// account write after a SelfDestructPath resets the Deleted flag — the
// same way TouchAccount drops the DeleteUpdate flag in serial when a
// non-empty value arrives after a delete.
func TestApplyWrites_BalancePathClearsDeleted(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0xd1})

	writes := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, Val: true},
		&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(42)},
	}
	cs.ApplyWrites(writes)

	acc, ok := cs.accounts[addr]
	require.True(t, ok)
	assert.False(t, acc.Deleted, "subsequent BalancePath write must clear Deleted")
	assert.Equal(t, uint64(42), acc.Balance.Uint64())
}

// preBlockReader is a minimal StateReader stub for the integration test
// below — returns the configured pre-block account for a single address.
type preBlockReader struct {
	addr accounts.Address
	acc  *accounts.Account
}

func (r *preBlockReader) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	if a == r.addr {
		return r.acc, nil
	}
	return nil, nil
}
func (r *preBlockReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return nil, nil
}
func (r *preBlockReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *preBlockReader) HasStorage(accounts.Address) (bool, error)               { return false, nil }
func (r *preBlockReader) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (r *preBlockReader) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (r *preBlockReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (r *preBlockReader) SetTrace(bool, string)                                   {}
func (r *preBlockReader) Trace() bool                                             { return false }
func (r *preBlockReader) TracePrefix() string                                     { return "" }

// TestSDOfPreExistingContract_FullPipeline drives the production pipeline
// end-to-end for the SD-of-pre-existing-contract case that motivated this
// PR (hive chain.rlp block 2 `0x4055cae5`):
//
//	LightCollector.DeleteAccount(addr, original)
//	  → normalizeWriteSet(writes, vm, txIndex, incarnation, stateReader)
//	  → calcState.ApplyWrites(normalized)
//	  → calcState.FlushToUpdates(updates)
//
// This addresses the reviewer concern that the FlushToUpdates unit tests
// populate cs.accounts directly, bypassing the production flow where
// normalizeWriteSet's completion loop appends BalancePath/NoncePath/
// CodeHashPath entries (read from stateReader) AFTER SelfDestructPath. In
// ApplyWrites those completion writes call `acc.Deleted = false`, which
// would normally route the SD into the regular UPDATE branch.
//
// What this test verifies:
//  1. The end-state in cs.accounts has acc.Deleted=false (because
//     completion entries for Balance/Nonce/CodeHash arrived after
//     SelfDestructPath).
//  2. The completion fills in PRE-block values (X/Y/Z), not zeros.
//  3. FlushToUpdates therefore emits a regular UPDATE with the pre-block
//     X/Y/Z values, NOT a zero-account UPDATE.
//
// This is the reviewer's read of the pipeline. If this matches what
// serial does for SD-of-pre-existing-contract via DomainPut/DomainDel,
// the trie root matches without the zero-account UPDATE branch ever
// firing — meaning that branch is defensive and the unit test
// `TestFlushToUpdates_DeletedWithIncarnation_EmitsZeroAccountUpdate`
// covers a state production never reaches.
//
// The test currently asserts what the production flow produces. If a
// future change to normalizeWriteSet (e.g. dropping the completion loop)
// makes the zero-account branch reachable, that test catches the new
// flow; if not, the defensive coverage still guards FlushToUpdates'
// switch in isolation.
func TestSDOfPreExistingContract_FullPipeline(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x40, 0x55, 0xca, 0xe5})

	preBlockBalance := *uint256.NewInt(1_000_000)
	preBlockNonce := uint64(7)
	preBlockCodeHash := accounts.InternCodeHash(common.Hash{0xab, 0xcd, 0xef})
	preBlockIncarnation := uint64(3)

	original := &accounts.Account{
		Balance:     preBlockBalance,
		Nonce:       preBlockNonce,
		CodeHash:    preBlockCodeHash,
		Incarnation: preBlockIncarnation,
	}

	// Step 1: simulate LightCollector.DeleteAccount producing the raw
	// writeset for an SD'd pre-existing contract (matches the production
	// emit sequence in execution/state/rw_v3.go's LightCollector).
	rawWrites := state.VersionedWrites{
		&state.VersionedWrite{
			Address: addr,
			Path:    state.SelfDestructPath,
			Val:     true,
			Version: state.Version{TxIndex: 0, Incarnation: 0},
		},
		&state.VersionedWrite{
			Address: addr,
			Path:    state.IncarnationPath,
			Val:     original.Incarnation,
			Version: state.Version{TxIndex: 0, Incarnation: 0},
		},
	}

	// Step 2: drive through normalizeWriteSet with a stateReader that
	// returns the pre-block account.
	vm := state.NewVersionMap(nil)
	stateReader := &preBlockReader{addr: addr, acc: original}
	normalized := normalizeWriteSet(rawWrites, vm, 0, 0, stateReader)

	// Sanity: normalizeWriteSet should append completion entries for the
	// missing Balance/Nonce/CodeHash with pre-block values from the
	// stateReader.
	pathSeen := map[state.AccountPath]any{}
	for _, w := range normalized {
		if w.Path == state.BalancePath || w.Path == state.NoncePath ||
			w.Path == state.CodeHashPath || w.Path == state.IncarnationPath ||
			w.Path == state.SelfDestructPath {
			pathSeen[w.Path] = w.Val
		}
	}
	require.Contains(t, pathSeen, state.SelfDestructPath, "SelfDestructPath must survive normalizeWriteSet")
	require.Contains(t, pathSeen, state.IncarnationPath, "IncarnationPath must survive normalizeWriteSet")
	require.Contains(t, pathSeen, state.BalancePath, "completion loop must add BalancePath")
	require.Contains(t, pathSeen, state.NoncePath, "completion loop must add NoncePath")
	require.Contains(t, pathSeen, state.CodeHashPath, "completion loop must add CodeHashPath")

	// The completion loop reads via vm.Read first (which has nothing for
	// Balance/Nonce/Code on a fresh DeleteAccount) then falls back to
	// stateReader. Confirm the values match the pre-block account.
	assert.Equal(t, preBlockBalance, pathSeen[state.BalancePath].(uint256.Int))
	assert.Equal(t, preBlockNonce, pathSeen[state.NoncePath].(uint64))
	assert.Equal(t, preBlockCodeHash, pathSeen[state.CodeHashPath].(accounts.CodeHash))

	// Step 3: drive normalized writeset through calcState.ApplyWrites.
	cs := newTestCalcState()
	cs.ApplyWrites(normalized)

	acc, ok := cs.accounts[addr]
	require.True(t, ok, "calcState must have an entry for the SD'd address")

	// Step 4: assert what calcState ends up with after the production
	// pipeline. The completion BalancePath/NoncePath/CodeHashPath writes
	// arrive after SelfDestructPath and reset acc.Deleted to false — see
	// ApplyWrites' BalancePath case `acc.Deleted = false`.
	assert.False(t, acc.Deleted,
		"completion BalancePath/NoncePath/CodeHashPath writes after SelfDestructPath reset Deleted=false; the Deleted+Incarnation>0 branch in FlushToUpdates is therefore unreachable from real LightCollector writesets through the production pipeline")
	assert.Equal(t, preBlockBalance, acc.Balance,
		"acc.Balance reflects the pre-block value populated via stateReader")
	assert.Equal(t, preBlockNonce, acc.Nonce,
		"acc.Nonce reflects the pre-block value populated via stateReader")
	assert.Equal(t, [32]byte(preBlockCodeHash.Value()), acc.CodeHash,
		"acc.CodeHash reflects the pre-block value populated via stateReader")
	assert.Equal(t, preBlockIncarnation, acc.Incarnation,
		"acc.Incarnation reflects the LightCollector-emitted IncarnationPath")

	// Step 5: drive FlushToUpdates and inspect the emitted commitment.Update.
	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))

	// The default branch fires (acc.Deleted=false), emitting the
	// pre-block balance/nonce/codeHash. This is what serial's DomainPut
	// would write after the same SD: the leaf survives, encoded with the
	// pre-block field values; commitment.Update has no Incarnation field,
	// so the trie's leaf-hash inputs match between serial and parallel.
	assert.Equal(t,
		commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate,
		got.Flags,
		"production pipeline ends in the default FlushToUpdates branch (regular UPDATE)")
	assert.Equal(t, preBlockBalance, got.Balance,
		"trie sees pre-block balance — leaf survives without incarnation in commitment.Update")
	assert.Equal(t, preBlockNonce, got.Nonce)
	assert.Equal(t, common.Hash(preBlockCodeHash.Value()), got.CodeHash)
}

func lookupKeyUpdate(t *testing.T, updates *commitment.Updates, plainKey string) *commitment.Update {
	t.Helper()
	var found *commitment.Update
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		if string(k) == plainKey {
			cp := *u
			found = &cp
		}
		return nil
	}))
	require.NotNil(t, found, "no Update emitted for plainKey %x", plainKey)
	return found
}
