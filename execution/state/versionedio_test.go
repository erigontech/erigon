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
			require.Equal(t, uint16(1), ac.BalanceChanges[0].Index,
				"the single balance change must be from tx0 (blockAccessIndex=1)")
		}
	}
	require.True(t, found, "address must appear in BAL (tx0's write is a real change)")
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
	require.NoError(t, ibs.SetNonce(addr, 5))
	require.NoError(t, ibs.SetCode(addr, []byte{0x60, 0x00}))
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
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(200), Reason: tracing.BalanceChangeUnspecified},
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
		&VersionedWrite{Address: addr, Path: NoncePath, Val: uint64(1)},
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
		&VersionedWrite{Address: addrA, Path: BalancePath, Val: *uint256.NewInt(200), Reason: tracing.BalanceChangeUnspecified},
		&VersionedWrite{Address: addrB, Path: NoncePath, Val: uint64(5)},
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
		&VersionedWrite{Address: addr, Path: BalancePath, Val: *uint256.NewInt(100), Reason: tracing.BalanceChangeUnspecified},
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
