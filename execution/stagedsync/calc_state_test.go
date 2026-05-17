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
// DEFENSIVE-ONLY coverage for the first branch of FlushToUpdates'
// switch: acc.Deleted=true AND Incarnation>0 AND all-zero fields →
// zero-account UPDATE (not DeleteUpdate).
//
// Reachability: this branch is currently UNREACHABLE from real
// production writesets. The realistic-pipeline test
// TestSDOfPreExistingContract_FullPipeline below drives the full
// IBS.Selfdestruct → blockIO.WriteSet → normalizeWriteSet →
// ApplyWrites → FlushToUpdates flow and shows that the BalancePath=0
// write IBS emits via versionWritten arrives in ApplyWrites after
// SelfDestructPath and resets acc.Deleted=false — meaning the
// SD-of-pre-existing-contract case lands in the default branch with
// {Balance=0, Nonce=preBlock, CodeHash=preBlock}, not in the
// zero-account branch.
//
// This test populates cs.accounts directly to keep the branch
// covered against future ApplyWrites/normalizeWriteSet refactors
// (e.g. dropping IBS' BalancePath=0 emit, or changing the order of
// writes such that BalancePath no longer clears Deleted) — at which
// point this branch could become reachable and this test would
// become a load-bearing assertion rather than defensive coverage.
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
// captured before SelfDestructPath does NOT survive the SD: the SD case
// zeros all account fields (Balance/Nonce/CodeHash/Incarnation) so
// FlushToUpdates routes into the EIP-161 DeleteUpdate branch, matching
// serial's DomainDel behavior of removing the leaf for a pure SD.
//
// The previous expectation (Incarnation preserved → zero-account UPDATE
// flags) was based on a misreading of serial — empirically serial emits
// DeleteUpdate for a pure SD-of-pre-existing-contract, not a zero-account
// leaf with retained incarnation. TestRecreateAndRewind (block 3 SD)
// fails under the old expectation.
func TestApplyWrites_IncarnationPath(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0xc1})

	writes := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.IncarnationPath, ValU64: uint64(1)},
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, ValBool: true},
	}
	cs.ApplyWrites(writes)

	acc, ok := cs.accounts[addr]
	require.True(t, ok, "ensureAccount should have created an entry")
	assert.True(t, acc.Deleted, "SelfDestructPath=true must set Deleted")
	assert.Equal(t, uint64(0), acc.Incarnation, "SelfDestructPath must zero Incarnation (matches serial's DomainDel removing the leaf)")
	assert.True(t, acc.Balance.IsZero(), "SelfDestructPath must zero Balance")
	assert.Equal(t, uint64(0), acc.Nonce, "SelfDestructPath must zero Nonce")
	assert.Equal(t, [32]byte(empty.CodeHash), acc.CodeHash, "SelfDestructPath must reset CodeHash")

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	keyVal := addr.Value()
	got := lookupKeyUpdate(t, updates, string(keyVal[:]))
	assert.Equal(t,
		commitment.DeleteUpdate,
		got.Flags,
		"Deleted+isAllZero routes through the EIP-161 DeleteUpdate branch (matches serial's DomainDel)")
}

// TestApplyWrites_BalancePathClearsDeleted verifies that a non-empty
// account write after a SelfDestructPath resets the Deleted flag — the
// same way TouchAccount drops the DeleteUpdate flag in serial when a
// non-empty value arrives after a delete.
func TestApplyWrites_BalancePathClearsDeleted(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress([20]byte{0xd1})

	writes := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, ValBool: true},
		&state.VersionedWrite{Address: addr, Path: state.BalancePath, ValU256: *uint256.NewInt(42)},
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
// end-to-end for an SD-of-pre-existing-contract scenario:
//
//	IBS.Selfdestruct (intra_block_state.go ~1430) emits via versionWritten:
//	    IncarnationPath = original.Incarnation
//	    SelfDestructPath = true
//	    BalancePath = 0
//	    StoragePath[k] = 0  for each k in stateObject.dirtyStorage
//	  → those land in blockIO.WriteSet → rawWrites
//	  → normalizeWriteSet(rawWrites, vm, txIndex, incarnation, stateReader, nil, true)
//	  → calcState.ApplyWrites(normalized)
//	  → calcState.FlushToUpdates(updates)
//
// This test populates `vm` with the same versionWritten emits IBS.Selfdestruct
// publishes in production (concern #1 from yperbasis review on PR #21032 —
// the prior version of this test had an empty vm and so the completion
// loop's vm.Read fallback never fired, masking the actual production flow).
//
// What it locks in (post-#21088 corrected semantics):
//  1. normalizeWriteSet detects SD'd addresses by scanning for
//     SelfDestructPath=true entries up front, and DROPS the raw
//     IncarnationPath / BalancePath / NoncePath / CodeHashPath / CodePath
//     writes for those addresses. The completion loop also skips them.
//     The normalized writeset for the SD'd address contains ONLY
//     SelfDestructPath=true (plus StoragePath=0 entries from vm.StorageKeys,
//     none in this scenario). Without this, applyVersionedWrites takes the
//     cleanup-before-recreate branch and writes the account back with
//     {Balance=0, Inc=preInc} encoding instead of taking the pure-delete
//     branch (DomainDel(Accounts)).
//  2. calcState.ApplyWrites ends with acc.Deleted=true, acc.Balance=0,
//     acc.Nonce=0, acc.CodeHash=empty, acc.Incarnation=0 — the
//     SelfDestructPath case zeros all account fields so FlushToUpdates
//     routes into the EIP-161 DeleteUpdate branch.
//  3. FlushToUpdates emits DeleteUpdate, matching serial's DomainDel
//     removing the leaf for a pure SD-of-pre-existing-contract.
//
// The previous expectation (default-UPDATE branch, leaf survives with
// {Balance=0, Nonce=preBlock, CodeHash=preBlock}) was based on a stale
// reading of serial; empirically serial removes the leaf, and parallel
// must do the same to produce matching trie roots in TestRecreateAndRewind.
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

	// Build the raw writeset that IBS.Selfdestruct produces in production
	// (intra_block_state.go around line 1430). LightCollector.DeleteAccount
	// also runs, but its CollectorWrites output is NOT the source for
	// rawWrites — exec3_parallel.go:2478 reads be.blockIO.WriteSet, which
	// is fed by versionWritten. So these are the writes the calc actually
	// sees.
	ver := state.Version{TxIndex: 0, Incarnation: 0}
	rawWrites := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.IncarnationPath, ValU64: original.Incarnation, Version: ver},
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, ValBool: true, Version: ver},
		&state.VersionedWrite{Address: addr, Path: state.BalancePath, ValU256: uint256.Int{}, Version: ver},
	}

	// Populate vm with the same writes — IBS.Selfdestruct calls versionWritten
	// which goes through the version map, so by the time normalizeWriteSet's
	// completion loop runs, vm.Read sees these values.
	vm := state.NewVersionMap(nil)
	vm.Write(addr, state.IncarnationPath, accounts.NilKey, ver, original.Incarnation, true)
	vm.Write(addr, state.SelfDestructPath, accounts.NilKey, ver, true, true)
	vm.Write(addr, state.BalancePath, accounts.NilKey, ver, uint256.Int{}, true)

	stateReader := &preBlockReader{addr: addr, acc: original}
	normalized := normalizeWriteSet(rawWrites, vm, 0, 0, stateReader, nil, true)

	// SD-aware filtering: only SelfDestructPath survives in the normalized
	// writeset for the SD'd address. The raw IncarnationPath/BalancePath
	// writes are dropped, and the completion loop skips this address.
	pathSeen := map[state.AccountPath]any{}
	for _, w := range normalized {
		switch w.Path {
		case state.BalancePath, state.NoncePath, state.CodeHashPath, state.IncarnationPath, state.SelfDestructPath:
			pathSeen[w.Path] = w.Val
		}
	}
	require.Contains(t, pathSeen, state.SelfDestructPath,
		"SelfDestructPath=true must survive normalize for the pure-delete branch in applyVersionedWrites")
	assert.NotContains(t, pathSeen, state.IncarnationPath,
		"IncarnationPath must be filtered for SD'd address — otherwise applyVersionedWrites takes cleanup-before-recreate")
	assert.NotContains(t, pathSeen, state.BalancePath,
		"BalancePath must be filtered for SD'd address — same reason")
	assert.NotContains(t, pathSeen, state.NoncePath,
		"NoncePath must not be filled by completion-loop fallback for SD'd address")
	assert.NotContains(t, pathSeen, state.CodeHashPath,
		"CodeHashPath must not be filled by completion-loop fallback for SD'd address")

	// Drive ApplyWrites + FlushToUpdates.
	cs := newTestCalcState()
	cs.ApplyWrites(normalized)

	acc, ok := cs.accounts[addr]
	require.True(t, ok)
	assert.True(t, acc.Deleted,
		"SelfDestructPath=true must set acc.Deleted=true")
	assert.True(t, acc.Balance.IsZero(),
		"SelfDestructPath case zeros Balance")
	assert.Equal(t, uint64(0), acc.Nonce,
		"SelfDestructPath case zeros Nonce")
	assert.Equal(t, [32]byte(empty.CodeHash), acc.CodeHash,
		"SelfDestructPath case resets CodeHash to empty")
	assert.Equal(t, uint64(0), acc.Incarnation,
		"SelfDestructPath case zeros Incarnation so FlushToUpdates routes through DeleteUpdate (EIP-161 branch), matching serial's DomainDel")

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)
	got := lookupKeyUpdate(t, updates, string(addr.Value().Bytes()))

	// EIP-161-style DeleteUpdate (matches serial's DomainDel for a pure SD).
	assert.Equal(t, commitment.DeleteUpdate, got.Flags,
		"production pipeline ends in the EIP-161 DeleteUpdate branch (Deleted+isAllZero), matching serial's DomainDel removing the leaf")
}

// TestSDStorageCascade_EmitsPerSlotDeletes locks in the load-bearing
// invariant documented at calc_state.go's SelfDestructPath case in
// ApplyWrites: when an SD'd account had storage slots recorded in the
// version map, normalizeWriteSet's `vm.StorageKeys(addr)` loop appends
// StoragePath=0 entries AFTER the SelfDestructPath entry. Those zeros
// arrive in ApplyWrites after SelfDestructPath, overwrite the pre-SD
// values that ApplyWrites' SelfDestructPath case left in cs.storageState
// (it only marks them dirty), and FlushToUpdates emits DeleteUpdate per
// slot.
//
// This addresses concern #6 from the PR review: prior to this test,
// the storage cascade was only exercised by the eest_devnet end-to-end
// suite. If someone in the future drops the vm.StorageKeys loop from
// normalizeWriteSet (or changes ApplyWrites' SelfDestructPath case to
// pre-zero the storage values), this test catches it as a unit-level
// regression: the slots would emit StorageUpdate(pre-SD value) rather
// than DeleteUpdate, and the trie would see leaked pre-SD slot values.
func TestSDStorageCascade_EmitsPerSlotDeletes(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x40, 0x55, 0xca, 0xe5})
	slot1 := accounts.InternKey(common.Hash{0x01})
	slot2 := accounts.InternKey(common.Hash{0x02})
	preSDValue1 := *uint256.NewInt(0xaaaa)
	preSDValue2 := *uint256.NewInt(0xbbbb)

	original := &accounts.Account{
		Balance:     *uint256.NewInt(1),
		Nonce:       1,
		Incarnation: 5,
	}

	// Pre-load cs.storageState with the pre-SD slot values, simulating
	// IBS having read those slots earlier in the block. ApplyWrites'
	// SelfDestructPath case marks them dirty without zeroing — so the
	// load-bearing question is whether normalizeWriteSet appends the
	// StoragePath=0 entries needed to overwrite these values.
	cs := newTestCalcState()
	cs.storageState[addr] = map[accounts.StorageKey]uint256.Int{
		slot1: preSDValue1,
		slot2: preSDValue2,
	}

	// Populate vm with StoragePath entries for both slots (this is what
	// IBS' versionWritten does when EVM SLOAD/SSTORE touches a slot).
	// Without these, vm.StorageKeys(addr) returns nil and the cascade
	// never fires.
	ver := state.Version{TxIndex: 0, Incarnation: 0}
	vm := state.NewVersionMap(nil)
	vm.Write(addr, state.StoragePath, slot1, ver, preSDValue1, true)
	vm.Write(addr, state.StoragePath, slot2, ver, preSDValue2, true)
	vm.Write(addr, state.IncarnationPath, accounts.NilKey, ver, original.Incarnation, true)
	vm.Write(addr, state.SelfDestructPath, accounts.NilKey, ver, true, true)
	vm.Write(addr, state.BalancePath, accounts.NilKey, ver, uint256.Int{}, true)

	rawWrites := state.VersionedWrites{
		&state.VersionedWrite{Address: addr, Path: state.IncarnationPath, ValU64: original.Incarnation, Version: ver},
		&state.VersionedWrite{Address: addr, Path: state.SelfDestructPath, ValBool: true, Version: ver},
		&state.VersionedWrite{Address: addr, Path: state.BalancePath, ValU256: uint256.Int{}, Version: ver},
	}

	stateReader := &preBlockReader{addr: addr, acc: original}
	normalized := normalizeWriteSet(rawWrites, vm, 0, 0, stateReader, nil, true)

	// Sanity: normalizeWriteSet should have appended one StoragePath=0
	// entry per slot in vm.StorageKeys(addr) — this is the load-bearing
	// emit. If it's gone, the assertions below will still catch the
	// effect (slots leak pre-SD values into the trie), but check it
	// here too so a regression points directly at the offending loop.
	storageZeroCount := 0
	for _, w := range normalized {
		if w.Path == state.StoragePath {
			val := w.ValU256
			assert.True(t, val.IsZero(),
				"normalizeWriteSet must emit StoragePath=0 for SD'd slots, got %v", val)
			storageZeroCount++
		}
	}
	assert.Equal(t, 2, storageZeroCount,
		"normalizeWriteSet must emit one StoragePath=0 entry per vm.StorageKeys(addr) — this is the storage cascade")

	cs.ApplyWrites(normalized)

	updates := newTestUpdates()
	cs.FlushToUpdates(updates)

	// Walk all emitted updates and assert that every storage update for
	// our SD'd address is a DeleteUpdate (not StorageUpdate with the
	// pre-SD value).
	addrBytes := addr.Value()
	storageDeletes := 0
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		if len(k) != 52 {
			return nil
		}
		if !bytes.Equal(k[:20], addrBytes[:]) {
			return nil
		}
		assert.Equal(t, commitment.DeleteUpdate, u.Flags,
			"slot %x must emit DeleteUpdate (storage cascade), not StorageUpdate with pre-SD value", k[20:])
		storageDeletes++
		return nil
	}))
	assert.Equal(t, 2, storageDeletes,
		"both pre-loaded slots must emit DeleteUpdate after the cascade")
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
