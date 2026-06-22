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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// minimalStateReader is a no-op StateReader for tests that create fresh accounts.
// All methods return zero/nil — the IBS will create new empty state objects.
type minimalStateReader struct{}

func (r *minimalStateReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return nil, nil
}
func (r *minimalStateReader) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return nil, nil
}
func (r *minimalStateReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *minimalStateReader) HasStorage(addr accounts.Address) (bool, error) { return false, nil }
func (r *minimalStateReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	return nil, nil
}
func (r *minimalStateReader) ReadAccountCodeSize(addr accounts.Address) (int, error) { return 0, nil }
func (r *minimalStateReader) ReadAccountIncarnation(addr accounts.Address) (uint64, error) {
	return 0, nil
}
func (r *minimalStateReader) SetTrace(trace bool, tracePrefix string) {}
func (r *minimalStateReader) Trace() bool                             { return false }
func (r *minimalStateReader) TracePrefix() string                     { return "" }

// TestAsBlockAccessList_SystemAddressExcludedWithoutChanges verifies that the
// system address (0xff...fe) is excluded from the BAL when it has no actual
// state changes and only revertable accesses (e.g. incidental gas-calculation
// reads during system calls).
func TestAsBlockAccessList_SystemAddressExcludedWithoutChanges(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress
	userAddr := accounts.InternAddress(common.HexToAddress("0x1111"))

	io := NewVersionedIO(1) // 2 tx slots: system call at -1, user tx at 0

	// System call (txIndex = -1): record system address as a revertable access.
	// This simulates EIP-4788 beacon root call where system address is msg.sender.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): record a normal address with a balance write.
	readSets := ReadSet{}
	readSets.Set(VersionedRead{
		Address: userAddr,
		Path:    BalancePath,
		Val:     uint64(100),
	})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: userAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(200),
		},
	})

	bal := io.AsBlockAccessList()

	// System address should be excluded (no state changes, only revertable access).
	for _, ac := range bal {
		require.NotEqual(t, sysAddr, ac.Address,
			"system address should be excluded from BAL when it has no state changes and only revertable accesses")
	}
	// User address should be present.
	found := false
	for _, ac := range bal {
		if ac.Address == userAddr {
			found = true
			break
		}
	}
	require.True(t, found, "user address should be present in BAL")
}

// TestAsBlockAccessList_SystemAddressIncludedWithNonRevertableAccess verifies
// that the system address is included in the BAL when a user tx performs a
// non-revertable access to it (e.g. BALANCE opcode, direct call, SELFDESTRUCT
// beneficiary), even without actual state changes.
func TestAsBlockAccessList_SystemAddressIncludedWithNonRevertableAccess(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(1) // system call at -1, user tx at 0

	// System call (txIndex = -1): revertable access (as usual).
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): non-revertable access (e.g. BALANCE opcode on system address).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included in BAL when a user tx has non-revertable access")
}

// TestAsBlockAccessList_SystemAddressIncludedWithStateChanges verifies that the
// system address is kept in the BAL when it has actual state changes (e.g. a
// value transfer to the system address), regardless of access type.
func TestAsBlockAccessList_SystemAddressIncludedWithStateChanges(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(1)

	// System call (txIndex = -1): revertable access only.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx (txIndex = 0): revertable access BUT with a balance change
	// (e.g. ETH transferred to system address).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: sysAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(42),
		},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included in BAL when it has actual state changes")
}

// TestAsBlockAccessList_SystemAddressRevertableFromSystemCallOnly verifies that
// a revertable access from a system call (txIndex = -1) does NOT set the
// nonRevertableUserAccess flag, so the system address is still excluded.
func TestAsBlockAccessList_SystemAddressRevertableFromSystemCallOnly(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress
	otherAddr := accounts.InternAddress(common.HexToAddress("0x2222"))

	io := NewVersionedIO(1)

	// System call (txIndex = -1): non-revertable access. Even though it's
	// non-revertable, it's from a system call (txIndex < 0) so it should
	// NOT mark the system address for inclusion.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	// User tx (txIndex = 0): touches a different address to ensure there's
	// at least one user tx in the block.
	readSets := ReadSet{}
	readSets.Set(VersionedRead{
		Address: otherAddr,
		Path:    BalancePath,
		Val:     uint64(50),
	})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{
			Address: otherAddr,
			Path:    BalancePath,
			Version: Version{TxIndex: 0},
			Val:     *uint256.NewInt(100),
		},
	})

	bal := io.AsBlockAccessList()

	for _, ac := range bal {
		require.NotEqual(t, sysAddr, ac.Address,
			"system address should be excluded: non-revertable access from system call (txIndex < 0) should not trigger inclusion")
	}
}

// TestAsBlockAccessList_NonRevertableOverridesRevertable verifies that if the
// system address first gets a revertable access from a user tx, then a
// non-revertable access from another user tx, the non-revertable wins and the
// address is included.
func TestAsBlockAccessList_NonRevertableOverridesRevertable(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(2) // 3 slots: system call at -1, user tx 0, user tx 1

	// System call: revertable.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx 0: revertable access (e.g. gas calc).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx 1: non-revertable access (e.g. BALANCE opcode).
	io.RecordAccesses(Version{TxIndex: 1}, AccessSet{
		sysAddr: &accessOptions{revertable: false},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == sysAddr {
			found = true
			break
		}
	}
	require.True(t, found,
		"system address should be included: non-revertable user access overrides earlier revertable access")

}

// TestVersionedIO_BalanceNetZeroWriteOmittedFromBAL verifies that a balance
// write that restores the exact pre-block value (with no intermediate writes)
// is treated as a net-zero change and omitted from the BAL.
func TestVersionedIO_BalanceNetZeroWriteOmittedFromBAL(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	initial := *uint256.NewInt(100)

	io := NewVersionedIO(1)

	// System tx reads the pre-block balance (establishes initialBalanceValue).
	reads := ReadSet{}
	reads.Set(VersionedRead{Address: addr, Path: BalancePath, Val: initial})
	io.RecordReads(Version{TxIndex: -1}, reads)

	// User tx writes the exact same balance back — net-zero, should be omitted.
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}, Val: initial},
	})

	bal := io.AsBlockAccessList()
	for _, ac := range bal {
		if ac.Address == addr {
			require.Empty(t, ac.BalanceChanges,
				"net-zero balance write (matches pre-block balance, no intermediates) must not appear in BAL")
		}
	}
}

// TestVersionedIO_BalanceRestoreAfterIntermediateIsRecorded verifies that
// restoring a balance to the pre-block (initial) value IS recorded in the BAL
// when intermediate balance changes exist. The restore write is needed so that
// parallel executors observing the tx see the correct value.
func TestVersionedIO_BalanceRestoreAfterIntermediateIsRecorded(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	initial := *uint256.NewInt(100)
	intermediate := *uint256.NewInt(200)

	io := NewVersionedIO(2)

	// System tx establishes pre-block balance.
	reads := ReadSet{}
	reads.Set(VersionedRead{Address: addr, Path: BalancePath, Val: initial})
	io.RecordReads(Version{TxIndex: -1}, reads)

	// tx0: intermediate write (changes balance away from initial).
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}, Val: intermediate},
	})

	// tx1: restores initial balance — must be recorded because an intermediate exists.
	io.RecordWrites(Version{TxIndex: 1}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}, Val: initial},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == addr {
			found = true
			require.Len(t, ac.BalanceChanges, 2,
				"both the intermediate write and the restore-to-initial write must appear in BAL")
		}
	}
	require.True(t, found, "address must appear in BAL")
}

// TestVersionedIO_StaleBalanceReadAfterWriteDoesNotCorruptNoOpCheck verifies
// that a stale BalancePath read arriving after a write (e.g. from a cached DB
// value that pre-dates the write) does not override balanceValue and cause a
// subsequent identical write to be incorrectly recorded as a new change.
func TestVersionedIO_StaleBalanceReadAfterWriteDoesNotCorruptNoOpCheck(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xcccc"))
	io := NewVersionedIO(2)

	// tx0: writes balance=200.
	io.RecordWrites(Version{TxIndex: 0}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}, Val: *uint256.NewInt(200)},
	})

	// tx1: stale DB read of balance=100 (DB value predates tx0's write).
	reads := ReadSet{}
	reads.Set(VersionedRead{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100)})
	io.RecordReads(Version{TxIndex: 1}, reads)

	// tx1: writes balance=200 again — same as tx0, a true no-op.
	io.RecordWrites(Version{TxIndex: 1}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}, Val: *uint256.NewInt(200)},
	})

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == addr {
			found = true
			require.Len(t, ac.BalanceChanges, 1,
				"tx1's write of 200 is a no-op (same as tx0's 200); stale read must not cause a spurious second entry")
			// blockAccessIndex = TxIndex+1, so tx0 (TxIndex=0) → Index=1.
			require.Equal(t, uint32(1), ac.BalanceChanges[0].Index,
				"the single balance change must be from tx0 (blockAccessIndex=1)")
		}
	}
	require.True(t, found, "address must appear in BAL (tx0's write is a real change)")
}

// TestVersionedIO_PostWriteBalanceReadDoesNotPoisonInitialBalance is the
// regression test for the bal-devnet-3 block-91648 BAL hash mismatch.
//
// Pattern reproduced from the production failure: a single user-tx balance
// write to an address (the EIP-1559 burnt contract on this devnet — the zero
// address) followed by a *later* BalancePath read of the same address with
// the same value. The later read is the one the parallel executor's block-end
// finalize emits when it spins up a fresh IntraBlockState — the cached state
// objects from the per-tx execution aren't shared, so the finalize hits the
// underlying state and observes the post-write value (or, in the validator
// path, the value that was pre-populated into the version map from the BAL
// sidecar).
//
// Before the fix, updateRead would seed initialBalanceValue from this
// post-write read; applyToBalance's net-zero filter would then see the very
// first recorded write equal to "initialBalanceValue" and drop it, producing
// a BAL hash that disagreed with the header.
func TestVersionedIO_PostWriteBalanceReadDoesNotPoisonInitialBalance(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000000000"))
	burned := *uint256.NewInt(0x16eaeb76) // value pulled from bal-devnet-3 block 91648

	io := NewVersionedIO(2)

	// Tx 1 burns the base fee to addr (value goes from pre-block balance to `burned`).
	io.RecordWrites(Version{TxIndex: 1}, VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}, Val: burned},
	})

	// A later BalancePath read of the same value — emitted by the fresh-IBS
	// finalize / BAL pre-pop. Before the fix, this poisoned initialBalanceValue.
	reads := ReadSet{}
	reads.Set(VersionedRead{Address: addr, Path: BalancePath, Val: burned})
	io.RecordReads(Version{TxIndex: 2}, reads)

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address == addr {
			found = true
			require.Len(t, ac.BalanceChanges, 1,
				"tx 1's burn write must remain in BAL — a post-write read with the same value must not seed initialBalanceValue and trigger the net-zero filter")
			// blockAccessIndex = TxIndex+1, so tx 1 → Index=2.
			require.Equal(t, uint32(2), ac.BalanceChanges[0].Index)
			require.True(t, ac.BalanceChanges[0].Value.Eq(&burned),
				"the surviving balance change must hold the actual burn value")
		}
	}
	require.True(t, found, "burn target address must appear in BAL")
}

// TestVersionedIO_StorageNoOpWriteAfterChangeOmittedFromBAL verifies the
// EIP-7928 rule that, for a slot written multiple times in a block, a write
// storing the value an earlier write already set is a no-op and must be
// excluded from storage_changes — only value-changing writes are recorded.
func TestVersionedIO_StorageNoOpWriteAfterChangeOmittedFromBAL(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	slot := accounts.InternKey(common.HexToHash("0x01"))
	valA := *uint256.NewInt(0x111)
	valB := *uint256.NewInt(0x222)

	io := NewVersionedIO(4)
	write := func(txIndex int, v uint256.Int) {
		io.RecordWrites(Version{TxIndex: txIndex}, VersionedWrites{
			&VersionedWrite{Address: addr, Path: StoragePath, Key: slot, Version: Version{TxIndex: txIndex}, Val: v},
		})
	}
	write(0, valA) // slot: 0 -> A  (real change)
	write(1, valB) // slot: A -> B  (real change)
	write(2, valB) // slot: B -> B  (no-op)
	write(3, valB) // slot: B -> B  (no-op)

	bal := io.AsBlockAccessList()

	found := false
	for _, ac := range bal {
		if ac.Address != addr {
			continue
		}
		found = true
		require.Lenf(t, ac.StorageChanges, 1, "one slot expected\n%s", bal.DebugString())
		changes := ac.StorageChanges[0].Changes
		require.Lenf(t, changes, 2,
			"no-op repeated-value writes (idx 3,4) must be excluded from storage_changes\n%s", bal.DebugString())
		require.Equal(t, uint32(1), changes[0].Index)
		require.True(t, changes[0].Value.Eq(&valA))
		require.Equal(t, uint32(2), changes[1].Index)
		require.True(t, changes[1].Value.Eq(&valB))
	}
	require.True(t, found, "contract must appear in BAL")
}

// fallthroughStateReader returns a fixed account and a fixed storage value, so
// a test can observe whether a versionedRead fell through to the underlying
// state (vs. returning a version-map value or aborting).
type fallthroughStateReader struct {
	minimalStateReader
	acct       *accounts.Account
	storageKey accounts.StorageKey
	storageVal uint256.Int
}

func (r *fallthroughStateReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return r.acct, nil
}

func (r *fallthroughStateReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if key == r.storageKey {
		return r.storageVal, true, nil
	}
	return uint256.Int{}, false, nil
}

// TestVersionedIO_RemovedDependencyFallsThroughToStorage is the negative test
// for the "RM DEP fallthrough" in versionedRead's MVReadResultNone branch
// (versionedio.go).
//
// Scenario: a tx has recorded a MapRead of a storage slot whose version-map
// cell was written by a prior tx. The prior tx is then re-executed and its
// cell removed (the cell reads back as MVReadResultNone), leaving a stale
// prior MapRead in the tx's readSet. A subsequent read of the same slot must
// NOT abort with ErrDependency — that cascades into re-execution livelocks in
// dense blocks. It must fall through to a storage read; the validator later
// rejects the tx if that storage value disagrees with the prior tx's settled
// value, so deferring to the validator is sound (the validator is the single
// source of truth since skipCheck was removed — issue #21319).
//
// This test pins the fallthrough: removing it (restoring panic(ErrDependency))
// makes the test panic.
func TestVersionedIO_RemovedDependencyFallsThroughToStorage(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xfa11"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	storageVal := *uint256.NewInt(0xAA) // the underlying-state value the fallthrough must surface

	acct := accounts.NewAccount()
	sr := &fallthroughStateReader{acct: &acct, storageKey: key, storageVal: storageVal}
	ibs := NewWithVersionMap(sr, NewVersionMap(nil))
	ibs.SetTxContext(2, 0)

	// Seed a stale prior MapRead of the slot — as if a now-removed prior-tx
	// write had been observed at version (1,0) with a different value.
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.Set(VersionedRead{
		Address: addr, Path: StoragePath, Key: key,
		Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0},
		Val: *uint256.NewInt(0xBB),
	})

	// The version map has no cell for this slot (the prior tx's write was
	// removed), so versionedRead sees MVReadResultNone with the stale MapRead
	// recorded — the exact RM-DEP-fallthrough condition.
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, storageVal, got,
		"a read whose version-map dependency was removed must fall through to "+
			"the underlying storage value, not abort or return the stale MapRead")
}

// TestIBSVersionedWrites_SelfdestructRetainsBalanceDropsOtherPaths verifies
// that IntraBlockState.VersionedWrites retains SelfDestructPath, BalancePath
// (including non-zero residual balances — EIP-7708 case 2), and IncarnationPath
// after selfdestruct, and drops NoncePath/CodePath which selfdestruct resets.
func TestIBSVersionedWrites_SelfdestructRetainsBalanceDropsOtherPaths(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	ibs := NewWithVersionMap(&minimalStateReader{}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)

	// Establish nonce and code before selfdestruct — these should be dropped.
	require.NoError(t, ibs.SetNonce(addr, 5, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(0), tracing.BalanceChangeUnspecified))

	// Selfdestruct: records SelfDestructPath=true, IncarnationPath, BalancePath=0.
	destructed, err := ibs.Selfdestruct(addr)
	require.NoError(t, err)
	require.True(t, destructed)

	// Simulate a non-zero post-selfdestruct balance write (EIP-7708 case 2:
	// value sent to a selfdestructed address in the same block). This overwrites
	// the zero-balance from Selfdestruct with a non-zero value that must be kept.
	require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(500), tracing.BalanceChangeUnspecified))

	writes := ibs.VersionedWrites(false)

	pathSet := map[AccountPath]bool{}
	for _, vw := range writes {
		if vw.Address == addr {
			pathSet[vw.Path] = true
		}
	}

	// Retained paths.
	require.True(t, pathSet[SelfDestructPath], "SelfDestructPath must be retained")
	require.True(t, pathSet[IncarnationPath], "IncarnationPath must be retained")
	require.True(t, pathSet[BalancePath], "BalancePath (non-zero residual) must be retained")

	// Dropped paths — selfdestruct resets nonce and code, so they must not appear.
	require.False(t, pathSet[NoncePath], "NoncePath must be dropped after selfdestruct")
	require.False(t, pathSet[CodePath], "CodePath must be dropped after selfdestruct")
}

// --- SetAccountBalanceOrDelete tests ---
// These pin the behavior of the direct finalize path's balance manipulation
// so that rationalization of the IBS finalize path cannot regress.

// TestSetAccountBalanceOrDelete_UpdateExisting verifies that when an account
// already has a BalancePath write, only the balance value is updated in-place.
func TestSetAccountBalanceOrDelete_UpdateExisting(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100)},
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(5)},
	}

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 5
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(200), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Len(t, result, 2, "no new entries should be added")
	for _, w := range result {
		if w.Path == BalancePath {
			bal := w.Val.(uint256.Int)
			require.Equal(t, uint256.NewInt(200), &bal, "balance should be updated to 200")
		}
	}
}

// TestSetAccountBalanceOrDelete_NewAccount verifies that when an account has no
// existing writes, all 4 account fields are emitted (balance, nonce, incarnation,
// codeHash) so that applyVersionedWrites can reconstruct a complete account.
func TestSetAccountBalanceOrDelete_NewAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x2000"))
	otherAddr := accounts.InternAddress(common.HexToAddress("0x3000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: otherAddr, Path: BalancePath, Val: *uint256.NewInt(50)},
	}

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(0)
	acc.Nonce = 3
	acc.Incarnation = 1
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	// Should have original write + 4 new fields for addr.
	require.Len(t, result, 5)
	pathSet := map[AccountPath]bool{}
	for _, w := range result {
		if w.Address == addr {
			pathSet[w.Path] = true
		}
	}
	require.True(t, pathSet[BalancePath], "BalancePath must be emitted")
	require.True(t, pathSet[NoncePath], "NoncePath must be emitted")
	require.True(t, pathSet[IncarnationPath], "IncarnationPath must be emitted")
	require.True(t, pathSet[CodeHashPath], "CodeHashPath must be emitted")
}

// TestSetAccountBalanceOrDelete_NilAccountCreatesEmpty verifies that passing
// nil for acc creates a fresh empty account (nonce=0, incarnation=0, emptyCodeHash).
func TestSetAccountBalanceOrDelete_NilAccountCreatesEmpty(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x4000"))
	writes := VersionedWrites{}

	result := writes.SetAccountBalanceOrDelete(addr, nil, *uint256.NewInt(100), tracing.BalanceIncreaseRewardTransactionFee, false)

	require.Len(t, result, 4)
	for _, w := range result {
		require.Equal(t, addr, w.Address)
		switch w.Path {
		case NoncePath:
			require.Equal(t, uint64(0), w.Val)
		case IncarnationPath:
			require.Equal(t, uint64(0), w.Val)
		case CodeHashPath:
			require.Equal(t, accounts.EmptyCodeHash, w.Val)
		}
	}
}

// TestSetAccountBalanceOrDelete_EIP161EmptyDeletion verifies that under
// EIP-161 (SpuriousDragon), an account with zero balance, zero nonce, and
// empty code hash is deleted: all existing writes are stripped and a
// SelfDestructPath entry is emitted.
func TestSetAccountBalanceOrDelete_EIP161EmptyDeletion(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x5000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100)},
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(0)},
	}

	acc := accounts.NewAccount() // nonce=0, emptyCodeHash
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Len(t, result, 1, "existing writes should be stripped")
	require.Equal(t, SelfDestructPath, result[0].Path)
	require.Equal(t, true, result[0].Val)
	require.Equal(t, addr, result[0].Address)
}

// TestSetAccountBalanceOrDelete_EIP161NonEmptyNotDeleted verifies that under
// EIP-161, an account with zero balance but non-zero nonce is NOT deleted.
func TestSetAccountBalanceOrDelete_EIP161NonEmptyNotDeleted(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x6000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100)},
	}

	acc := accounts.NewAccount()
	acc.Nonce = 1 // non-empty
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	// Should update in-place, not delete.
	require.Len(t, result, 1)
	require.Equal(t, BalancePath, result[0].Path)
	bal := result[0].Val.(uint256.Int)
	require.True(t, bal.IsZero(), "balance should be set to zero")
}

// TestSetAccountBalanceOrDelete_EIP161DisabledNoRemoval verifies that when
// emptyRemoval is false (pre-SpuriousDragon), empty accounts are NOT deleted.
func TestSetAccountBalanceOrDelete_EIP161DisabledNoRemoval(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x7000"))
	writes := VersionedWrites{}

	acc := accounts.NewAccount()
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, false)

	// Should emit all 4 fields, NOT a SelfDestructPath.
	require.Len(t, result, 4)
	for _, w := range result {
		require.NotEqual(t, SelfDestructPath, w.Path)
	}
}

// TestSetAccountBalanceOrDelete_OtherAddressWritesPreserved verifies that
// EIP-161 deletion only strips writes for the target address; other addresses
// in the write set are preserved.
func TestSetAccountBalanceOrDelete_OtherAddressWritesPreserved(t *testing.T) {
	t.Parallel()

	target := accounts.InternAddress(common.HexToAddress("0x8000"))
	other := accounts.InternAddress(common.HexToAddress("0x9000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: target, Path: BalancePath, Val: *uint256.NewInt(100)},
		&VersionedWrite{Address: target, Path: NoncePath, Val: uint64(0)},
		&VersionedWrite{Address: other, Path: BalancePath, Val: *uint256.NewInt(999)},
	}

	acc := accounts.NewAccount()
	result := writes.SetAccountBalanceOrDelete(target, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	// other's write + SelfDestructPath for target.
	require.Len(t, result, 2)
	otherFound := false
	selfDestructFound := false
	for _, w := range result {
		if w.Address == other && w.Path == BalancePath {
			otherFound = true
		}
		if w.Address == target && w.Path == SelfDestructPath {
			selfDestructFound = true
		}
	}
	require.True(t, otherFound, "other address write must be preserved")
	require.True(t, selfDestructFound, "target must have SelfDestructPath")
}

// TestSetAccountBalanceOrDelete_NoncePathOnly_AppendBalanceNotFullAccount
// regression-pins the addrHasAnyWrite guard added in #21017 (bug #2).
//
// Setup: the worker has already written addr's NoncePath at version V (e.g.
// a miner-self-send tx where sender == coinbase; the worker bumps the sender
// nonce). The writes slice contains the NoncePath entry but no BalancePath
// entry. finalize then calls SetAccountBalanceOrDelete to credit the tip.
//
// Pre-fix (bug #2): no BalancePath match found → fell through to the
// new-account branch and re-emitted all four account fields from the
// pre-block snapshot acc, clobbering the worker's already-bumped Nonce
// under last-wins downstream merge.
//
// Post-fix: addrHasAnyWrite=true short-circuits the new-account branch
// and appends ONLY the BalancePath; existing NoncePath is preserved.
func TestSetAccountBalanceOrDelete_NoncePathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xA000"))
	writes := VersionedWrites{
		// Worker wrote NoncePath only (e.g. nonce-bump on a sender = coinbase tx).
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(42)},
	}

	// Pre-block snapshot of the account — stale nonce that must NOT clobber
	// the worker's already-bumped value.
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 41 // stale; worker has it at 42
	acc.Incarnation = 1
	acc.CodeHash = accounts.EmptyCodeHash

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	// Should be NoncePath (worker's) + BalancePath (newly appended).
	// Must NOT re-emit Incarnation / CodeHash from the stale snapshot.
	require.Len(t, result, 2, "must append only BalancePath, not re-emit full account")

	pathSet := map[AccountPath]bool{}
	for _, w := range result {
		require.Equal(t, addr, w.Address)
		pathSet[w.Path] = true
		if w.Path == NoncePath {
			require.Equal(t, uint64(42), w.Val, "worker's nonce must NOT be clobbered by stale snapshot")
		}
		if w.Path == BalancePath {
			bal := w.Val.(uint256.Int)
			require.Equal(t, uint256.NewInt(500), &bal)
		}
	}
	require.True(t, pathSet[NoncePath], "NoncePath must be preserved")
	require.True(t, pathSet[BalancePath], "BalancePath must be appended")
	require.False(t, pathSet[IncarnationPath], "IncarnationPath must NOT be re-emitted from stale snapshot")
	require.False(t, pathSet[CodeHashPath], "CodeHashPath must NOT be re-emitted from stale snapshot")
}

// TestSetAccountBalanceOrDelete_CodeHashPathOnly_AppendBalanceNotFullAccount
// covers the same addrHasAnyWrite guard for a CodeHash-only worker write.
// Less common in practice (CREATE without balance change) but a real path
// the guard must handle for completeness.
func TestSetAccountBalanceOrDelete_CodeHashPathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB000"))
	workerCodeHash := accounts.InternCodeHash(common.HexToHash("0xcafe"))
	writes := VersionedWrites{
		// Worker wrote CodeHashPath only (e.g. CREATE installed new code).
		&VersionedWrite{Address: addr, Path: CodeHashPath, Val: workerCodeHash},
	}

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 5
	acc.Incarnation = 2
	acc.CodeHash = accounts.EmptyCodeHash // stale; worker installed real code

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Len(t, result, 2, "must append only BalancePath, not re-emit full account")

	pathSet := map[AccountPath]bool{}
	for _, w := range result {
		require.Equal(t, addr, w.Address)
		pathSet[w.Path] = true
		if w.Path == CodeHashPath {
			require.Equal(t, workerCodeHash, w.Val, "worker's CodeHash must NOT be clobbered by stale snapshot")
		}
	}
	require.True(t, pathSet[CodeHashPath], "CodeHashPath must be preserved")
	require.True(t, pathSet[BalancePath], "BalancePath must be appended")
	require.False(t, pathSet[NoncePath], "NoncePath must NOT be re-emitted from stale snapshot")
	require.False(t, pathSet[IncarnationPath], "IncarnationPath must NOT be re-emitted from stale snapshot")
}

// TestSetAccountBalanceOrDelete_IncarnationPathOnly_AppendBalanceNotFullAccount
// covers the same addrHasAnyWrite guard for an Incarnation-only worker write
// (e.g. a SELFDESTRUCT-and-recreate that bumps incarnation without changing
// other paths via this code path).
func TestSetAccountBalanceOrDelete_IncarnationPathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xC000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: IncarnationPath, Val: uint64(7)},
	}

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 3
	acc.Incarnation = 6 // stale; worker has it at 7
	acc.CodeHash = accounts.EmptyCodeHash

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Len(t, result, 2, "must append only BalancePath, not re-emit full account")

	pathSet := map[AccountPath]bool{}
	for _, w := range result {
		require.Equal(t, addr, w.Address)
		pathSet[w.Path] = true
		if w.Path == IncarnationPath {
			require.Equal(t, uint64(7), w.Val, "worker's incarnation must NOT be clobbered by stale snapshot")
		}
	}
	require.True(t, pathSet[IncarnationPath], "IncarnationPath must be preserved")
	require.True(t, pathSet[BalancePath], "BalancePath must be appended")
	require.False(t, pathSet[NoncePath], "NoncePath must NOT be re-emitted from stale snapshot")
	require.False(t, pathSet[CodeHashPath], "CodeHashPath must NOT be re-emitted from stale snapshot")
}

// --- StripBalanceWrite tests ---

// TestStripBalanceWrite_NoRead verifies that when the TX didn't read the
// address, the write is stripped but no delta is computed.
func TestStripBalanceWrite_NoRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xA000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(500)},
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(1)},
	}

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, ReadSet{})

	require.Len(t, stripped, 1, "BalancePath write should be removed")
	require.Equal(t, NoncePath, stripped[0].Path)
	require.False(t, found, "no delta when TX didn't read the address")
	require.True(t, delta.IsZero())
	require.False(t, increase)
}

// TestStripBalanceWrite_WithIncreaseDelta verifies delta computation when the
// TX wrote a higher balance than it read (increase).
func TestStripBalanceWrite_WithIncreaseDelta(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(150)},
	}

	readSet := ReadSet{}
	readSet.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Val: *uint256.NewInt(100)})

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, readSet)

	require.Len(t, stripped, 0, "BalancePath write should be removed")
	require.True(t, found)
	require.True(t, increase)
	require.Equal(t, uint256.NewInt(50), &delta, "delta should be 50 (150-100)")
}

// TestStripBalanceWrite_WithDecreaseDelta verifies delta computation when the
// TX wrote a lower balance than it read (decrease).
func TestStripBalanceWrite_WithDecreaseDelta(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xC000"))
	writes := VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(70)},
	}

	readSet := ReadSet{}
	readSet.Set(VersionedRead{Address: addr, Path: BalancePath, Key: accounts.NilKey, Val: *uint256.NewInt(100)})

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, readSet)

	require.Len(t, stripped, 0)
	require.True(t, found)
	require.False(t, increase)
	require.Equal(t, uint256.NewInt(30), &delta, "delta should be 30 (100-70)")
}

// TestStripBalanceWrite_NilAddress verifies that nil address is a no-op.
func TestStripBalanceWrite_NilAddress(t *testing.T) {
	t.Parallel()

	writes := VersionedWrites{
		&VersionedWrite{Address: accounts.InternAddress(common.HexToAddress("0xD000")), Path: BalancePath, Val: *uint256.NewInt(100)},
	}

	stripped, delta, increase, found := writes.StripBalanceWrite(accounts.Address{}, ReadSet{})

	require.Len(t, stripped, 1, "writes unchanged")
	require.False(t, found)
	require.True(t, delta.IsZero())
	require.False(t, increase)
}

// --- ApplyVersionedWrites reads generation tests ---
// These verify what reads the IBS produces when applying different write types.
// The direct finalize path must produce equivalent reads for BAL correctness.
//
// Key insight: refreshVersionedAccount (the source of BalancePath reads) only
// runs for accounts that ALREADY EXIST in the stateReader or version map.
// For newly-created accounts, GetOrNewStateObject calls createObject which
// skips refreshVersionedAccount. This is the normal case for accounts born
// in the current block (e.g. contract CREATE).

// accountStateReader returns pre-configured accounts for specific addresses.
// Used to simulate accounts that exist in the DB before execution.
type accountStateReader struct {
	minimalStateReader
	accounts map[accounts.Address]*accounts.Account
}

func (r *accountStateReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[addr]; ok {
		return acc, nil
	}
	return nil, nil
}

func newAccountStateReader(addrs ...accounts.Address) *accountStateReader {
	r := &accountStateReader{accounts: make(map[accounts.Address]*accounts.Account)}
	for _, addr := range addrs {
		a := accounts.NewAccount()
		a.Balance = *uint256.NewInt(100)
		r.accounts[addr] = &a
	}
	return r
}

// TestApplyVersionedWrites_BalanceWriteGeneratesBalanceRead verifies that a
// BalancePath write through ApplyVersionedWrites generates a BalancePath read
// (via refreshVersionedAccount in GetOrNewStateObject) for an existing account.
func TestApplyVersionedWrites_BalanceWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xE000"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, nil, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(200), BalanceChangeReason: tracing.BalanceChangeUnspecified},
	})
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	balRead := findRead(reads, addr, BalancePath)
	require.NotNil(t, balRead, "BalancePath write must generate a BalancePath read for existing accounts")
}

// TestApplyVersionedWrites_StorageWriteGeneratesBalanceRead verifies that a
// StoragePath write through ApplyVersionedWrites also generates a BalancePath
// read. This is because setState calls GetOrNewStateObject which triggers
// refreshVersionedAccount. The direct finalize path must replicate this.
func TestApplyVersionedWrites_StorageWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF000"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, nil, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	storageKey := accounts.InternKey(common.HexToHash("0x01"))
	err := ibs.ApplyVersionedWrites(VersionedWrites{
		&VersionedWrite{Address: addr, Path: StoragePath, Key: storageKey, Val: *uint256.NewInt(42)},
	})
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	balRead := findRead(reads, addr, BalancePath)
	require.NotNil(t, balRead,
		"StoragePath write must generate a BalancePath read (via refreshVersionedAccount); "+
			"direct finalize must replicate this for BAL correctness")
}

// TestApplyVersionedWrites_NonceWriteGeneratesBalanceRead verifies that a
// NoncePath write generates a BalancePath read for an existing account.
func TestApplyVersionedWrites_NonceWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF100"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, nil, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(VersionedWrites{
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(1), NonceChangeReason: tracing.NonceChangeUnspecified},
	})
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	balRead := findRead(reads, addr, BalancePath)
	require.NotNil(t, balRead,
		"NoncePath write must generate a BalancePath read (via refreshVersionedAccount)")
}

// TestApplyVersionedWrites_MultipleAccountsAllGetBalanceReads verifies that
// when multiple existing accounts have writes of different types, ALL accounts
// get BalancePath reads.
func TestApplyVersionedWrites_MultipleAccountsAllGetBalanceReads(t *testing.T) {
	t.Parallel()

	addrA := accounts.InternAddress(common.HexToAddress("0xF200"))
	addrB := accounts.InternAddress(common.HexToAddress("0xF300"))
	addrC := accounts.InternAddress(common.HexToAddress("0xF400"))
	reader := newAccountStateReader(addrA, addrB, addrC)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, nil, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	storageKey := accounts.InternKey(common.HexToHash("0x01"))
	err := ibs.ApplyVersionedWrites(VersionedWrites{
		&VersionedWrite{Address: addrA, Path: BalancePath, Val: *uint256.NewInt(200), BalanceChangeReason: tracing.BalanceChangeUnspecified},
		&VersionedWrite{Address: addrB, Path: NoncePath, Val: uint64(5), NonceChangeReason: tracing.NonceChangeUnspecified},
		&VersionedWrite{Address: addrC, Path: StoragePath, Key: storageKey, Val: *uint256.NewInt(99)},
	})
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	require.NotNil(t, findRead(reads, addrA, BalancePath), "addrA (BalancePath write) must have BalancePath read")
	require.NotNil(t, findRead(reads, addrB, BalancePath), "addrB (NoncePath write) must have BalancePath read")
	require.NotNil(t, findRead(reads, addrC, BalancePath), "addrC (StoragePath write) must have BalancePath read")
}

// TestApplyVersionedWrites_NewAccountNoBalanceRead verifies that for accounts
// that DON'T exist in the DB (newly created in this block), ApplyVersionedWrites
// does NOT generate a BalancePath read. refreshVersionedAccount is skipped
// because createObject is called instead.
func TestApplyVersionedWrites_NewAccountNoBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF500"))
	vm := NewVersionMap(nil)
	// Use minimalStateReader — returns nil for all accounts.
	ibs := New(NewVersionedStateReader(0, nil, vm, &minimalStateReader{}))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(VersionedWrites{
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100), BalanceChangeReason: tracing.BalanceChangeUnspecified},
	})
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	balRead := findRead(reads, addr, BalancePath)
	require.Nil(t, balRead,
		"newly-created account (not in DB) should NOT generate a BalancePath read")
}

// findRead searches the ReadSet for a read matching the given address and path.
func findRead(reads ReadSet, addr accounts.Address, path AccountPath) *VersionedRead {
	addrReads, ok := reads[addr]
	if !ok {
		return nil
	}
	for _, r := range addrReads {
		if r.Path == path {
			r := r // local copy for address
			return &r
		}
	}
	return nil
}

// When a prior tx wrote a sub-field (e.g. BalancePath via AddBalance) without
// writing AddressPath, refreshVersionedAccount promotes the sub-field version
// onto accountRead's AddressPath stamp. The validator's vm.Read(AddressPath)
// must not invalidate that stamp — otherwise OCC re-execution races
// identically until the retry budget exhausts.
func TestAccountRead_BalancePathPromotion_DoesNotInvalidate(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB19A240E"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Balance = *uint256.NewInt(50_000_000_000_000_000)

	postWithdrawalBalance := *uint256.NewInt(100_000_000_000_000_000)
	vm := NewVersionMap(nil)
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0},
		postWithdrawalBalance, true)

	ibs := New(NewVersionedStateReader(1, nil, vm, reader))
	ibs.SetTxContext(0, 1)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	// Simulate refreshVersionedAccount's promoted return value:
	// BalancePath at (0,0) bumped (source, version) above the
	// account-record's own.
	acc := accounts.NewAccount()
	acc.Balance = postWithdrawalBalance
	acc.CodeHash = accounts.EmptyCodeHash
	ibs.accountRead(addr, &acc, MapRead, Version{TxIndex: 0, Incarnation: 0})

	// Same call the parallel-exec scheduler makes between worker
	// completion and apply.
	io := NewVersionedIO(1)
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())

	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := vm.ValidateVersion(1, io, checkVersionEqual, true, "TestAccountRead_BalancePathPromotion")

	require.Equal(t, VersionValid, valid,
		"tx 1's account read should validate against a versionMap with only "+
			"a BalancePath write at TxIdx=0 — promoting the sub-field's "+
			"version onto an AddressPath cell that does not exist is what "+
			"makes validation fail and produces the cross-chain OCC livelock")
}

// When CreateAccount runs on an address whose only versionMap entry is a
// sub-field (e.g. BalancePath from a prior credit), the synthetic
// IncarnationPath read must default to (StorageRead, UnknownVersion), not
// the outer (MapRead, V_bal) promotion — same livelock class as the
// accountRead path.
func TestCreateAccount_SyntheticIncarnationStamp_DoesNotInvalidate(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB19A240E"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Balance = *uint256.NewInt(50_000_000_000_000_000)

	postWithdrawalBalance := *uint256.NewInt(100_000_000_000_000_000)
	vm := NewVersionMap(nil)
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0},
		postWithdrawalBalance, true)

	ibs := New(NewVersionedStateReader(1, nil, vm, reader))
	ibs.SetTxContext(0, 1)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	require.NoError(t, ibs.CreateAccount(addr, false))

	io := NewVersionedIO(1)
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.VersionedReads())

	checkVersionEqual := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := vm.ValidateVersion(1, io, checkVersionEqual, true, "TestCreateAccount_SyntheticIncarnationStamp")

	require.Equal(t, VersionValid, valid,
		"CreateAccount on an address with only a BalancePath cell must not "+
			"stamp the synthetic IncarnationPath read with the promoted "+
			"BalancePath version")
}

// getVersionedAccount must honour a prior tx's SelfDestructPath cell even
// when stateReader (which doesn't consult the versionMap) returns the pre-SD
// record. Without this gate Empty() returns false and a CALL-with-value to
// the SD'd address misses CallNewAccountGas.
func TestGetVersionedAccount_PriorTxSelfDestruct_ReturnsNil(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF1F5555B"))
	reader := newAccountStateReader(addr)
	// Pre-SD record: a deployed contract with nonce=1 and a real codeHash.
	reader.accounts[addr].Nonce = 1
	codeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader.accounts[addr].CodeHash = codeHash

	vm := NewVersionMap(nil)
	// Tx 3 self-destructs: SelfDestructPath=true only — SD doesn't write
	// Nonce/CodeHash into the versionMap.
	vm.Write(addr, SelfDestructPath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, true, true)
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, uint256.Int{}, true)
	vm.Write(addr, IncarnationPath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, uint64(1), true)

	// Tx 4's worker IBS. The reader (analogous to CachedReaderV3) returns
	// the pre-SD account directly — no versionMap-aware wrapper short-circuits
	// the SD case. Only getVersionedAccount's versionMap check should convert
	// that to nil.
	ibs := New(reader)
	ibs.SetTxContext(0, 4)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.Nil(t, account,
		"getVersionedAccount must return nil for an address self-destructed "+
			"by a prior tx with no revival, even when the underlying "+
			"stateReader still surfaces the pre-SD record")
}

// Metamorphic SD+CREATE2 in one tx: SelfDestructPath=true and a fresh
// AddressPath land at the SAME TxIdx. A strict hi > destructTxIndex
// revival check would miss this; getVersionedAccount must recognise
// AddressPath at TxIdx >= destructTxIndex as revival.
func TestGetVersionedAccount_SameTxMetamorphicRecreate_ReturnsAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x140DA4236"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Nonce = 1
	reader.accounts[addr].CodeHash = accounts.InternCodeHash(common.HexToHash("0xc8c04ce6368db80967fe4c88faa37d1a3d3becb3b9e442a62a5dc8c1df6e14ee"))

	// Tx 3: SD + CREATE2 re-deploy. All writes land at (TxIdx=3, Inc=0).
	vm := NewVersionMap(nil)
	vm.Write(addr, SelfDestructPath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, true, true)
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, uint256.Int{}, true)
	vm.Write(addr, IncarnationPath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, uint64(2), true)
	// Fresh AddressPath at the same TxIdx as the SD.
	recreatedAcc := &accounts.Account{
		Nonce:       1,
		Incarnation: 2,
		CodeHash:    accounts.InternCodeHash(common.HexToHash("0xdeadbeefcafebabe1111111111111111111111111111111111111111111111ff")),
	}
	vm.Write(addr, AddressPath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, recreatedAcc, true)

	// Tx 4 reads addr. Strict-greater on subfields wouldn't see the
	// same-TxIdx Balance/Nonce/CodeHash; the AddressPath >= destructTxIndex
	// branch is what surfaces the re-created account.
	ibs := New(NewVersionedStateReader(4, nil, vm, reader))
	ibs.SetTxContext(0, 4)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	account, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, account,
		"same-tx SD+CREATE2 (metamorphic) re-creates the contract; "+
			"getVersionedAccount must surface the re-created account, not nil")
	require.Equal(t, uint64(2), account.Incarnation,
		"the re-created account's Incarnation should reflect the CREATE2 (not the pre-SD value)")
}

func TestEIP161EmptyRemoval(t *testing.T) {
	userAddr := accounts.InternAddress(common.HexToAddress("0x1111"))

	tests := []struct {
		name           string
		spuriousDragon bool
		isAura         bool
		addr           accounts.Address
		want           bool
	}{
		{"pre-spurious-dragon user", false, false, userAddr, false},
		{"pre-spurious-dragon aura system address", false, true, params.SystemAddress, false},
		{"non-aura user", true, false, userAddr, true},
		{"non-aura system address removed", true, false, params.SystemAddress, true},
		{"aura user", true, true, userAddr, true},
		{"aura system address retained", true, true, params.SystemAddress, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, EIP161EmptyRemoval(tc.spuriousDragon, tc.isAura, tc.addr))
		})
	}
}
