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
// an access from a system call (txIndex = -1) does NOT set the userAccess
// flag, so the system address is still excluded when it has no state changes.
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

// TestAsBlockAccessList_RevertableUserAccessIncludesSystemAddress verifies that
// even a revertable access from a user tx is enough to include the system
// address in the BAL (matching geth's OnAccountRead behavior).
func TestAsBlockAccessList_RevertableUserAccessIncludesSystemAddress(t *testing.T) {
	t.Parallel()

	sysAddr := params.SystemAddress

	io := NewVersionedIO(1) // 2 slots: system call at -1, user tx 0

	// System call: revertable.
	io.RecordAccesses(Version{TxIndex: -1}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
	})

	// User tx 0: revertable access (e.g. gas calc versionRead).
	io.RecordAccesses(Version{TxIndex: 0}, AccessSet{
		sysAddr: &accessOptions{revertable: true},
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
		"system address should be included: any user tx access (even revertable) triggers inclusion")
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
