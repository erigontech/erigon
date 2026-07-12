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
	"fmt"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// newWriteSet builds a typed *WriteSet from heterogeneous *VersionedWrite[T]
// values, routing each into the correct per-path map by its WriteHeader. It is
// the test-side replacement for the deleted flat VersionedWrites slice literal.
func newWriteSet(writes ...any) *WriteSet {
	ws := &WriteSet{}
	for _, w := range writes {
		addWriteToSet(ws, w)
	}
	return ws
}

func addWriteToSet(ws *WriteSet, w any) {
	switch vw := w.(type) {
	case *VersionedWrite[*accounts.Account]:
		ws.SetAddress(vw.Address, vw)
	case *VersionedWrite[uint256.Int]:
		if vw.Path == StoragePath {
			ws.SetStorage(vw.Address, vw.Key, vw)
		} else {
			ws.SetBalance(vw.Address, vw)
		}
	case *VersionedWrite[uint64]:
		if vw.Path == IncarnationPath {
			ws.SetIncarnation(vw.Address, vw)
		} else {
			ws.SetNonce(vw.Address, vw)
		}
	case *VersionedWrite[bool]:
		if vw.Path == CreateContractPath {
			ws.SetCreateContract(vw.Address, vw)
		} else {
			ws.SetSelfDestruct(vw.Address, vw)
		}
	case *VersionedWrite[accounts.Code]:
		ws.SetCode(vw.Address, vw)
	case *VersionedWrite[accounts.CodeHash]:
		ws.SetCodeHash(vw.Address, vw)
	case *VersionedWrite[int]:
		ws.SetCodeSize(vw.Address, vw)
	default:
		panic(fmt.Sprintf("newWriteSet: unsupported write type %T", w))
	}
}

// writeSetVal reads the typed value for a header from the WriteSet's per-path
// maps and returns it as an any, the test-side replacement for the deleted
// Val[T] extractor.
func writeSetVal(s *WriteSet, h WriteHeader) any {
	switch h.Path {
	case AddressPath:
		vw, _ := s.GetAddress(h.Address)
		return vw.Val
	case BalancePath:
		vw, _ := s.GetBalance(h.Address)
		return vw.Val
	case StoragePath:
		vw, _ := s.GetStorage(h.Address, h.Key)
		return vw.Val
	case NoncePath:
		vw, _ := s.GetNonce(h.Address)
		return vw.Val
	case IncarnationPath:
		vw, _ := s.GetIncarnation(h.Address)
		return vw.Val
	case CodeHashPath:
		vw, _ := s.GetCodeHash(h.Address)
		return vw.Val
	case CodePath:
		vw, _ := s.GetCode(h.Address)
		return vw.Val
	case CodeSizePath:
		vw, _ := s.GetCodeSize(h.Address)
		return vw.Val
	case SelfDestructPath, CreateContractPath:
		if h.Path == CreateContractPath {
			vw, _ := s.GetCreateContract(h.Address)
			return vw.Val
		}
		vw, _ := s.GetSelfDestruct(h.Address)
		return vw.Val
	}
	return nil
}

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
	recordTouch(io, -1, sysAddr, true)

	// User tx (txIndex = 0): record a normal address with a balance write.
	readSets := ReadSet{}
	readSets.SetBalance(userAddr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: userAddr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(200)},
	))

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
	recordTouch(io, -1, sysAddr, true)

	// User tx (txIndex = 0): non-revertable access (e.g. BALANCE opcode on system address).
	recordTouch(io, 0, sysAddr, false)

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
	recordTouch(io, -1, sysAddr, true)

	// User tx (txIndex = 0): revertable access BUT with a balance change
	// (e.g. ETH transferred to system address).
	recordTouch(io, 0, sysAddr, true)
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: sysAddr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(42)},
	))

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
	recordTouch(io, -1, sysAddr, false)

	// User tx (txIndex = 0): touches a different address to ensure there's
	// at least one user tx in the block.
	readSets := ReadSet{}
	readSets.SetBalance(otherAddr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(50)})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: otherAddr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(100)},
	))

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
	recordTouch(io, -1, sysAddr, true)

	// User tx 0: revertable access (e.g. gas calc).
	recordTouch(io, 0, sysAddr, true)

	// User tx 1: non-revertable access (e.g. BALANCE opcode).
	recordTouch(io, 1, sysAddr, false)

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
	reads.SetBalance(addr, VersionedRead[uint256.Int]{Val: initial})
	io.RecordReads(Version{TxIndex: -1}, reads)

	// User tx writes the exact same balance back — net-zero, should be omitted.
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: initial},
	))

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
	reads.SetBalance(addr, VersionedRead[uint256.Int]{Val: initial})
	io.RecordReads(Version{TxIndex: -1}, reads)

	// tx0: intermediate write (changes balance away from initial).
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: intermediate},
	))

	// tx1: restores initial balance — must be recorded because an intermediate exists.
	io.RecordWrites(Version{TxIndex: 1}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: initial},
	))

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
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(200)},
	))

	// tx1: stale DB read of balance=100 (DB value predates tx0's write).
	reads := ReadSet{}
	reads.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})
	io.RecordReads(Version{TxIndex: 1}, reads)

	// tx1: writes balance=200 again — same as tx0, a true no-op.
	io.RecordWrites(Version{TxIndex: 1}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: *uint256.NewInt(200)},
	))

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
	io.RecordWrites(Version{TxIndex: 1}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: burned},
	))

	// A later BalancePath read of the same value — emitted by the fresh-IBS
	// finalize / BAL pre-pop. Before the fix, this poisoned initialBalanceValue.
	reads := ReadSet{}
	reads.SetBalance(addr, VersionedRead[uint256.Int]{Val: burned})
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
		io.RecordWrites(Version{TxIndex: txIndex}, newWriteSet(
			&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: slot, Version: Version{TxIndex: txIndex}}, Val: v},
		))
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

// TestVersionedIO_RemovedDependencyFallsThroughToStorage pins the
// MVReadResultNone fallthrough in versionedRead: a stale MapRead whose
// version-map cell has been removed must read the underlying storage value
// rather than abort with ErrDependency.
func TestVersionedIO_RemovedDependencyFallsThroughToStorage(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xfa11"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	storageVal := *uint256.NewInt(0xAA)

	acct := accounts.NewAccount()
	sr := &fallthroughStateReader{acct: &acct, storageKey: key, storageVal: storageVal}
	ibs := NewWithVersionMap(sr, NewVersionMap(nil))
	ibs.SetTxContext(2, 0)

	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.SetStorage(addr, key, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1, Incarnation: 0}},
		Val:        *uint256.NewInt(0xBB),
	})

	// Read-once (Block-STM): the repeat read is served from the read-set without
	// re-probing, so it returns the recorded MapRead value rather than eagerly
	// falling through to storage. The removed dependency (a MapRead whose
	// version-map cell is now gone) is caught at commit — validateReadImpl
	// invalidates a MapRead that resolves to MVReadResultNone (verified in
	// validateReadImpl; exercised end-to-end by the parallel exec tests + hive) —
	// which re-executes the tx and then falls through to the underlying storage.
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(0xBB), got, "read-once returns the recorded value")
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

	writes := ibs.VersionedWrites()

	pathSet := map[AccountPath]bool{}
	for h := range writes.AllHeaders() {
		if h.Address == addr {
			pathSet[h.Path] = true
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
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(100)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(5)},
	)

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 5
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(200), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Equal(t, 2, result.Count(), "no new entries should be added")
	bw, ok := result.GetBalance(addr)
	require.True(t, ok)
	require.Equal(t, uint256.NewInt(200), &bw.Val, "balance should be updated to 200")
}

// TestSetAccountBalanceOrDelete_NewAccount verifies that when an account has no
// existing writes, all 4 account fields are emitted (balance, nonce, incarnation,
// codeHash) so that applyVersionedWrites can reconstruct a complete account.
func TestSetAccountBalanceOrDelete_NewAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x2000"))
	otherAddr := accounts.InternAddress(common.HexToAddress("0x3000"))
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: otherAddr, Path: BalancePath}, Val: *uint256.NewInt(50)},
	)

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(0)
	acc.Nonce = 3
	acc.Incarnation = 1
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	// Should have original write + 4 new fields for addr.
	require.Equal(t, 5, result.Count())
	pathSet := map[AccountPath]bool{}
	for h := range result.AllHeaders() {
		if h.Address == addr {
			pathSet[h.Path] = true
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
	writes := &WriteSet{}

	result := writes.SetAccountBalanceOrDelete(addr, nil, *uint256.NewInt(100), tracing.BalanceIncreaseRewardTransactionFee, false)

	require.Equal(t, 4, result.Count())
	for h := range result.AllHeaders() {
		require.Equal(t, addr, h.Address)
		switch h.Path {
		case NoncePath:
			vw, _ := result.GetNonce(h.Address)
			require.Equal(t, uint64(0), vw.Val)
		case IncarnationPath:
			vw, _ := result.GetIncarnation(h.Address)
			require.Equal(t, uint64(0), vw.Val)
		case CodeHashPath:
			vw, _ := result.GetCodeHash(h.Address)
			require.Equal(t, accounts.EmptyCodeHash, vw.Val)
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
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(100)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(0)},
	)

	acc := accounts.NewAccount() // nonce=0, emptyCodeHash
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Equal(t, 1, result.Count(), "existing writes should be stripped")
	sd, ok := result.GetSelfDestruct(addr)
	require.True(t, ok)
	require.Equal(t, SelfDestructPath, sd.Path)
	require.Equal(t, true, sd.Val)
	require.Equal(t, addr, sd.Address)
}

// TestSetAccountBalanceOrDelete_EIP161NonEmptyNotDeleted verifies that under
// EIP-161, an account with zero balance but non-zero nonce is NOT deleted.
func TestSetAccountBalanceOrDelete_EIP161NonEmptyNotDeleted(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x6000"))
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(100)},
	)

	acc := accounts.NewAccount()
	acc.Nonce = 1 // non-empty
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	// Should update in-place, not delete.
	require.Equal(t, 1, result.Count())
	bw, ok := result.GetBalance(addr)
	require.True(t, ok)
	require.Equal(t, BalancePath, bw.Path)
	require.True(t, bw.Val.IsZero(), "balance should be set to zero")
}

// TestSetAccountBalanceOrDelete_EIP161DisabledNoRemoval verifies that when
// emptyRemoval is false (pre-SpuriousDragon), empty accounts are NOT deleted.
func TestSetAccountBalanceOrDelete_EIP161DisabledNoRemoval(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x7000"))
	writes := &WriteSet{}

	acc := accounts.NewAccount()
	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, false)

	// Should emit all 4 fields, NOT a SelfDestructPath.
	require.Equal(t, 4, result.Count())
	for h := range result.AllHeaders() {
		require.NotEqual(t, SelfDestructPath, h.Path)
	}
}

// TestSetAccountBalanceOrDelete_OtherAddressWritesPreserved verifies that
// EIP-161 deletion only strips writes for the target address; other addresses
// in the write set are preserved.
func TestSetAccountBalanceOrDelete_OtherAddressWritesPreserved(t *testing.T) {
	t.Parallel()

	target := accounts.InternAddress(common.HexToAddress("0x8000"))
	other := accounts.InternAddress(common.HexToAddress("0x9000"))
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: target, Path: BalancePath}, Val: *uint256.NewInt(100)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: target, Path: NoncePath}, Val: uint64(0)},
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: other, Path: BalancePath}, Val: *uint256.NewInt(999)},
	)

	acc := accounts.NewAccount()
	result := writes.SetAccountBalanceOrDelete(target, &acc, *uint256.NewInt(0), tracing.BalanceIncreaseRewardTransactionFee, true)

	// other's write + SelfDestructPath for target.
	require.Equal(t, 2, result.Count())
	otherFound := false
	selfDestructFound := false
	for h := range result.AllHeaders() {
		if h.Address == other && h.Path == BalancePath {
			otherFound = true
		}
		if h.Address == target && h.Path == SelfDestructPath {
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
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(500)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(1)},
	)

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, ReadSet{})

	require.Equal(t, 1, stripped.Count(), "BalancePath write should be removed")
	_, hasBalance := stripped.GetBalance(addr)
	require.False(t, hasBalance, "BalancePath write should be removed")
	_, hasNonce := stripped.GetNonce(addr)
	require.True(t, hasNonce, "NoncePath write should remain")
	require.False(t, found, "no delta when TX didn't read the address")
	require.True(t, delta.IsZero())
	require.False(t, increase)
}

// TestStripBalanceWrite_WithIncreaseDelta verifies delta computation when the
// TX wrote a higher balance than it read (increase).
func TestStripBalanceWrite_WithIncreaseDelta(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB000"))
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(150)},
	)

	readSet := ReadSet{}
	readSet.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, readSet)

	require.Equal(t, 0, stripped.Count(), "BalancePath write should be removed")
	require.True(t, found)
	require.True(t, increase)
	require.Equal(t, uint256.NewInt(50), &delta, "delta should be 50 (150-100)")
}

// TestStripBalanceWrite_WithDecreaseDelta verifies delta computation when the
// TX wrote a lower balance than it read (decrease).
func TestStripBalanceWrite_WithDecreaseDelta(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xC000"))
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(70)},
	)

	readSet := ReadSet{}
	readSet.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})

	stripped, delta, increase, found := writes.StripBalanceWrite(addr, readSet)

	require.Equal(t, 0, stripped.Count())
	require.True(t, found)
	require.False(t, increase)
	require.Equal(t, uint256.NewInt(30), &delta, "delta should be 30 (100-70)")
}

// TestStripBalanceWrite_NilAddress verifies that nil address is a no-op.
func TestStripBalanceWrite_NilAddress(t *testing.T) {
	t.Parallel()

	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: accounts.InternAddress(common.HexToAddress("0xD000")), Path: BalancePath}, Val: *uint256.NewInt(100)},
	)

	stripped, delta, increase, found := writes.StripBalanceWrite(accounts.Address{}, ReadSet{})

	require.Equal(t, 1, stripped.Count(), "writes unchanged")
	require.False(t, found)
	require.True(t, delta.IsZero())
	require.False(t, increase)
}

// --- ApplyVersionedWrites reads generation tests ---
// These verify what reads the IBS produces when applying different write types.
// The direct finalize path must produce equivalent reads for BAL correctness.
//
// Key insight: the per-field account refresh (the source of BalancePath reads) only
// runs for accounts that ALREADY EXIST in the stateReader or version map.
// For newly-created accounts, GetOrNewStateObject calls createObject which
// skips the per-field account refresh. This is the normal case for accounts born
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
// (via the per-field account refresh in GetOrNewStateObject) for an existing account.
func TestApplyVersionedWrites_BalanceWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xE000"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Reason: tracing.BalanceChangeUnspecified}, Val: *uint256.NewInt(200)},
	))
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	require.True(t, hasRead(reads, addr, BalancePath), "BalancePath write must generate a BalancePath read for existing accounts")
}

// TestApplyVersionedWrites_StorageWriteGeneratesBalanceRead verifies that a
// StoragePath write through ApplyVersionedWrites also generates a BalancePath
// read. This is because setState calls GetOrNewStateObject which triggers
// the per-field account refresh. The direct finalize path must replicate this.
func TestApplyVersionedWrites_StorageWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF000"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	storageKey := accounts.InternKey(common.HexToHash("0x01"))
	err := ibs.ApplyVersionedWrites(newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: storageKey}, Val: *uint256.NewInt(42)},
	))
	require.NoError(t, err)

	// Lean footprint: a storage write no longer drags in a whole-account
	// refresh, so it records no BalancePath read. Balance is not a BAL read
	// field (only StorageReads exist), so this is BAL-neutral.
	reads := ibs.VersionedReads()
	require.False(t, hasRead(reads, addr, BalancePath),
		"StoragePath write must not generate a spurious BalancePath read")
}

// TestApplyVersionedWrites_NonceWriteGeneratesBalanceRead verifies that a
// NoncePath write generates a BalancePath read for an existing account.
func TestApplyVersionedWrites_NonceWriteGeneratesBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF100"))
	reader := newAccountStateReader(addr)
	vm := NewVersionMap(nil)
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(newWriteSet(
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(1)},
	))
	require.NoError(t, err)

	// Lean footprint: a nonce write no longer drags in a whole-account refresh.
	reads := ibs.VersionedReads()
	require.False(t, hasRead(reads, addr, BalancePath),
		"NoncePath write must not generate a spurious BalancePath read")
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
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, reader))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	storageKey := accounts.InternKey(common.HexToHash("0x01"))
	err := ibs.ApplyVersionedWrites(newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addrA, Path: BalancePath, Reason: tracing.BalanceChangeUnspecified}, Val: *uint256.NewInt(200)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addrB, Path: NoncePath}, Val: uint64(5)},
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addrC, Path: StoragePath, Key: storageKey}, Val: *uint256.NewInt(99)},
	))
	require.NoError(t, err)

	// Lean footprint: only a balance write reads prior balance (the net-zero
	// baseline). Nonce/storage writes no longer drag in a whole-account refresh,
	// so they record no spurious BalancePath read.
	reads := ibs.VersionedReads()
	require.True(t, hasRead(reads, addrA, BalancePath), "addrA (BalancePath write) reads its prior balance (net-zero baseline)")
	require.False(t, hasRead(reads, addrB, BalancePath), "addrB (NoncePath write) must not have a spurious BalancePath read")
	require.False(t, hasRead(reads, addrC, BalancePath), "addrC (StoragePath write) must not have a spurious BalancePath read")
}

// TestApplyVersionedWrites_NewAccountNoBalanceRead verifies that for accounts
// that DON'T exist in the DB (newly created in this block), ApplyVersionedWrites
// does NOT generate a BalancePath read. The per-field account refresh is skipped
// because createObject is called instead.
func TestApplyVersionedWrites_NewAccountNoBalanceRead(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xF500"))
	vm := NewVersionMap(nil)
	// Use minimalStateReader — returns nil for all accounts.
	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, &minimalStateReader{}))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	err := ibs.ApplyVersionedWrites(newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Reason: tracing.BalanceChangeUnspecified}, Val: *uint256.NewInt(100)},
	))
	require.NoError(t, err)

	reads := ibs.VersionedReads()
	require.False(t, hasRead(reads, addr, BalancePath),
		"newly-created account (not in DB) should NOT generate a BalancePath read")
}

// recordTouch records an address-level ephemeral access for txIndex on the
// read-set (accesses feed the BAL through the read-set now).
func recordTouch(io *VersionedIO, txIndex int, addr accounts.Address, revertable bool) {
	rs := io.ReadSet(txIndex)
	if rs.access == nil {
		rs.access = make(AccessSet)
	}
	rs.access[addr] = &accessOptions{revertable: revertable}
	io.RecordReads(Version{TxIndex: txIndex}, rs)
}

// hasRead reports whether the ReadSet has a read for the given address and path.
func hasRead(reads ReadSet, addr accounts.Address, path AccountPath) bool {
	_, ok := reads.getHeader(addr, path, accounts.NilKey)
	return ok
}

// When a prior tx wrote a sub-field (e.g. BalancePath via AddBalance) without
// writing AddressPath, the per-field account refresh promotes the sub-field version
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
	vm.WriteBalance(addr,
		Version{TxIndex: 0, Incarnation: 0},
		postWithdrawalBalance, true)

	ibs := New(NewVersionedStateReader(1, ReadSet{}, vm, reader))
	ibs.SetTxContext(0, 1)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	// Simulate the per-field account refresh's promoted return value:
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
	vm.WriteBalance(addr,
		Version{TxIndex: 0, Incarnation: 0},
		postWithdrawalBalance, true)

	ibs := New(NewVersionedStateReader(1, ReadSet{}, vm, reader))
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
	vm.WriteSelfDestruct(addr,
		Version{TxIndex: 3, Incarnation: 0}, true, true)
	vm.WriteBalance(addr,
		Version{TxIndex: 3, Incarnation: 0}, uint256.Int{}, true)
	vm.WriteIncarnation(addr,
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
	vm.WriteSelfDestruct(addr,
		Version{TxIndex: 3, Incarnation: 0}, true, true)
	vm.WriteBalance(addr,
		Version{TxIndex: 3, Incarnation: 0}, uint256.Int{}, true)
	vm.WriteIncarnation(addr,
		Version{TxIndex: 3, Incarnation: 0}, uint64(2), true)
	// Fresh AddressPath at the same TxIdx as the SD.
	recreatedAcc := &accounts.Account{
		Nonce:       1,
		Incarnation: 2,
		CodeHash:    accounts.InternCodeHash(common.HexToHash("0xdeadbeefcafebabe1111111111111111111111111111111111111111111111ff")),
	}
	vm.WriteAddress(addr,
		Version{TxIndex: 3, Incarnation: 0}, recreatedAcc, true)

	// Tx 4 reads addr. Strict-greater on subfields wouldn't see the
	// same-TxIdx Balance/Nonce/CodeHash; the AddressPath >= destructTxIndex
	// branch is what surfaces the re-created account.
	ibs := New(NewVersionedStateReader(4, ReadSet{}, vm, reader))
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

// EIP-7928 net-zero guard: a slot that is read and then written back to the
// same value must stay a read in the BAL, not become a write. The filter
// lives in accountState.applyWriteStorage (the helper addStorageUpdate only
// appends). This locks the behaviour down across the typed-vio refactor.
func TestUpdateWrite_StorageReadThenWriteBackSameValue_StaysRead(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xbeef"))
	slot := accounts.InternKey(common.HexToHash("0x07"))
	orig := *uint256.NewInt(42)

	account := &accountState{changes: &types.AccountChanges{Address: addr}}

	account.updateReadStorage(slot, orig)
	require.Contains(t, account.changes.StorageReads, slot, "the read must be recorded")

	account.applyWriteStorage(slot, orig, 0)

	require.Empty(t, account.changes.StorageChanges,
		"write-back to the originally-read value is net-zero and must NOT be recorded as a storage write")
	require.Contains(t, account.changes.StorageReads, slot,
		"the net-zero write-back must remain a read")
}

// The complement: a read followed by a write to a DIFFERENT value is a real
// state change — it becomes a write and supersedes the recorded read.
func TestUpdateWrite_StorageReadThenWriteDifferentValue_BecomesWrite(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xbeef"))
	slot := accounts.InternKey(common.HexToHash("0x07"))
	orig := *uint256.NewInt(42)
	changed := *uint256.NewInt(99)

	account := &accountState{changes: &types.AccountChanges{Address: addr}}

	account.updateReadStorage(slot, orig)

	account.applyWriteStorage(slot, changed, 0)

	require.Len(t, account.changes.StorageChanges, 1,
		"a write to a different value is a real state change and must be recorded")
	require.Equal(t, slot, account.changes.StorageChanges[0].Slot)
	require.NotContains(t, account.changes.StorageReads, slot,
		"a real write supersedes the recorded read")
}

// TestAsBlockAccessList_SelfdestructedAccountRecordsBalanceToZero verifies the
// EIP-7928 rule for in-transaction SELFDESTRUCT: an account destroyed within a
// tx that had a positive pre-tx balance MUST record a balance change to zero —
// even when a later same-tx transfer leaves a non-zero final balance write.
func TestAsBlockAccessList_SelfdestructedAccountRecordsBalanceToZero(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x9e1989c1ba17e9b8fdae0b5d43a2b0c676a2070f"))
	io := NewVersionedIO(1)
	readSets := ReadSet{}
	readSets.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100000)})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(1)},
		&VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 0}}, Val: true},
	))
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Equal(t, addr, bal[0].Address)
	require.Len(t, bal[0].BalanceChanges, 1,
		"destroyed account with positive pre-tx balance must record a balance change to zero")
	require.Equal(t, uint32(1), bal[0].BalanceChanges[0].Index)
	require.True(t, bal[0].BalanceChanges[0].Value.IsZero(),
		"recorded post-balance must be zero for an account destroyed in-tx")
}

// TestAsBlockAccessList_SelfdestructedZeroPreBalanceNoBalanceChange verifies the
// EIP-7928 counterpart: same-tx SELFDESTRUCT of an account with a zero pre-tx
// balance must NOT produce a balance change entry.
func TestAsBlockAccessList_SelfdestructedZeroPreBalanceNoBalanceChange(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x2222"))
	io := NewVersionedIO(1)
	readSets := ReadSet{}
	readSets.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(0)})
	io.RecordReads(Version{TxIndex: 0}, readSets)
	io.RecordWrites(Version{TxIndex: 0}, newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(1)},
		&VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 0}}, Val: true},
	))
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Equal(t, addr, bal[0].Address)
	require.Empty(t, bal[0].BalanceChanges,
		"destroyed account with zero pre-tx balance must not record a balance change")
}

func TestEIP161EmptyRemoval(t *testing.T) {
	userAddr := accounts.InternAddress(common.HexToAddress("0x1111"))

	tests := []struct {
		name          string
		eip161Enabled bool
		isAura        bool
		addr          accounts.Address
		want          bool
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
			require.Equal(t, tc.want, EIP161EmptyRemoval(tc.eip161Enabled, tc.isAura, tc.addr))
		})
	}
}

// TestVersionedUpdates_EstimateCellConsumed pins that finalize reconstruction
// consumes an in-block subfield write sitting in an Estimate (Dependency) cell
// rather than falling back to the stale pre-block value. The per-path
// versionedUpdate helpers must gate on != MVReadResultNone (Done OR Dependency);
// gating on == MVReadResultDone drops the Estimate cell and reads stale state,
// diverging balance/nonce/codeHash/storage and the trie root.
func TestVersionedUpdates_EstimateCellConsumed(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x7667"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	vm := NewVersionMap(nil)

	ver := Version{TxIndex: 1, Incarnation: 0}
	newBalance := *uint256.NewInt(0xB0)
	newNonce := uint64(7)
	newIncarnation := uint64(3)
	newCodeHash := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x00}))
	newStorage := *uint256.NewInt(0x5707)

	// complete=false → Estimate cells (Dependency status when read from tx 5).
	vm.WriteBalance(addr, ver, newBalance, false)
	vm.WriteNonce(addr, ver, newNonce, false)
	vm.WriteIncarnation(addr, ver, newIncarnation, false)
	vm.WriteCodeHash(addr, ver, newCodeHash, false)
	vm.WriteStorage(addr, key, ver, newStorage, false)

	vr := NewVersionedStateReader(5, ReadSet{}, vm, nil)

	stale := accounts.NewAccount()
	stale.Balance = *uint256.NewInt(0x01)
	stale.Nonce = 1
	stale.Incarnation = 1
	stale.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0xFF}))

	got := vr.applyVersionedUpdates(addr, stale)
	require.Equal(t, newBalance, got.Balance, "Estimate-cell balance must be consumed, not stale")
	require.Equal(t, newNonce, got.Nonce, "Estimate-cell nonce must be consumed")
	require.Equal(t, newIncarnation, got.Incarnation, "Estimate-cell incarnation must be consumed")
	require.Equal(t, newCodeHash, got.CodeHash, "Estimate-cell codeHash must be consumed")

	storageGot, ok, err := vr.ReadAccountStorage(addr, key)
	require.NoError(t, err)
	require.True(t, ok, "Estimate-cell storage must be found")
	require.Equal(t, newStorage, storageGot, "Estimate-cell storage must be consumed, not stale")
}

// writeSetFixture builds a WriteSet covering every path, multiple addresses and
// storage keys — the input for the iteration closed-loop tests.
func writeSetFixture() (*WriteSet, []string) {
	a1 := accounts.InternAddress(common.HexToAddress("0xaa01"))
	a2 := accounts.InternAddress(common.HexToAddress("0xbb02"))
	k1 := accounts.InternKey(common.HexToHash("0x01"))
	k2 := accounts.InternKey(common.HexToHash("0x02"))
	ch := accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x00}))
	hdr := func(a accounts.Address, p AccountPath, k accounts.StorageKey) WriteHeader {
		return WriteHeader{Address: a, Path: p, Key: k, Version: Version{TxIndex: 1}}
	}
	var s WriteSet
	s.SetBalance(a1, &VersionedWrite[uint256.Int]{WriteHeader: hdr(a1, BalancePath, accounts.NilKey), Val: *uint256.NewInt(5)})
	s.SetNonce(a1, &VersionedWrite[uint64]{WriteHeader: hdr(a1, NoncePath, accounts.NilKey), Val: 7})
	s.SetIncarnation(a1, &VersionedWrite[uint64]{WriteHeader: hdr(a1, IncarnationPath, accounts.NilKey), Val: 3})
	s.SetCodeHash(a1, &VersionedWrite[accounts.CodeHash]{WriteHeader: hdr(a1, CodeHashPath, accounts.NilKey), Val: ch})
	s.SetCode(a1, &VersionedWrite[accounts.Code]{WriteHeader: hdr(a1, CodePath, accounts.NilKey), Val: accounts.NewCode([]byte{0x60, 0x00})})
	s.SetCodeSize(a1, &VersionedWrite[int]{WriteHeader: hdr(a1, CodeSizePath, accounts.NilKey), Val: 2})
	s.SetSelfDestruct(a1, &VersionedWrite[bool]{WriteHeader: hdr(a1, SelfDestructPath, accounts.NilKey), Val: true})
	s.SetCreateContract(a1, &VersionedWrite[bool]{WriteHeader: hdr(a1, CreateContractPath, accounts.NilKey), Val: true})
	s.SetStorage(a1, k1, &VersionedWrite[uint256.Int]{WriteHeader: hdr(a1, StoragePath, k1), Val: *uint256.NewInt(11)})
	s.SetStorage(a1, k2, &VersionedWrite[uint256.Int]{WriteHeader: hdr(a1, StoragePath, k2), Val: *uint256.NewInt(22)})
	s.SetBalance(a2, &VersionedWrite[uint256.Int]{WriteHeader: hdr(a2, BalancePath, accounts.NilKey), Val: *uint256.NewInt(9)})
	s.SetStorage(a2, k1, &VersionedWrite[uint256.Int]{WriteHeader: hdr(a2, StoragePath, k1), Val: *uint256.NewInt(33)})

	want := []string{
		writeKeyStr(hdr(a1, BalancePath, accounts.NilKey), (*uint256.NewInt(5)).String()),
		writeKeyStr(hdr(a1, NoncePath, accounts.NilKey), "7"),
		writeKeyStr(hdr(a1, IncarnationPath, accounts.NilKey), "3"),
		writeKeyStr(hdr(a1, CodeHashPath, accounts.NilKey), ch.Value().Hex()),
		writeKeyStr(hdr(a1, CodePath, accounts.NilKey), accounts.NewCode([]byte{0x60, 0x00}).Hash.Value().Hex()),
		writeKeyStr(hdr(a1, CodeSizePath, accounts.NilKey), "2"),
		writeKeyStr(hdr(a1, SelfDestructPath, accounts.NilKey), "true"),
		writeKeyStr(hdr(a1, CreateContractPath, accounts.NilKey), "true"),
		writeKeyStr(hdr(a1, StoragePath, k1), (*uint256.NewInt(11)).String()),
		writeKeyStr(hdr(a1, StoragePath, k2), (*uint256.NewInt(22)).String()),
		writeKeyStr(hdr(a2, BalancePath, accounts.NilKey), (*uint256.NewInt(9)).String()),
		writeKeyStr(hdr(a2, StoragePath, k1), (*uint256.NewInt(33)).String()),
	}
	sort.Strings(want)
	return &s, want
}

func writeKeyStr(h WriteHeader, val string) string {
	return fmt.Sprintf("%x|%d|%x|%s", h.Address, h.Path, h.Key, val)
}

// headerValStr reads the typed value for h from the per-path maps (the new
// iteration: AllHeaders + typed getters, no cast) and formats it.
func headerValStr(s *WriteSet, h WriteHeader) string {
	switch h.Path {
	case BalancePath:
		vw, _ := s.GetBalance(h.Address)
		return writeKeyStr(h, vw.Val.String())
	case NoncePath:
		vw, _ := s.GetNonce(h.Address)
		return writeKeyStr(h, fmt.Sprintf("%d", vw.Val))
	case IncarnationPath:
		vw, _ := s.GetIncarnation(h.Address)
		return writeKeyStr(h, fmt.Sprintf("%d", vw.Val))
	case CodeHashPath:
		vw, _ := s.GetCodeHash(h.Address)
		return writeKeyStr(h, vw.Val.Value().Hex())
	case CodePath:
		vw, _ := s.GetCode(h.Address)
		return writeKeyStr(h, vw.Val.Hash.Value().Hex())
	case CodeSizePath:
		vw, _ := s.GetCodeSize(h.Address)
		return writeKeyStr(h, fmt.Sprintf("%d", vw.Val))
	case SelfDestructPath:
		vw, _ := s.GetSelfDestruct(h.Address)
		return writeKeyStr(h, fmt.Sprintf("%t", vw.Val))
	case CreateContractPath:
		vw, _ := s.GetCreateContract(h.Address)
		return writeKeyStr(h, fmt.Sprintf("%t", vw.Val))
	case StoragePath:
		vw, _ := s.GetStorage(h.Address, h.Key)
		return writeKeyStr(h, vw.Val.String())
	}
	return ""
}

// TestWriteSet_AllHeaders_RoundTrip pins that AllHeaders + typed getters visits
// every inserted write exactly once with the correct typed value — the durable
// guard that the post-refactor iteration loses nothing.
func TestWriteSet_AllHeaders_RoundTrip(t *testing.T) {
	t.Parallel()
	s, want := writeSetFixture()
	var got []string
	for h := range s.AllHeaders() {
		got = append(got, headerValStr(s, h))
	}
	sort.Strings(got)
	require.Equal(t, want, got)
}

// TestVersionedIO_mergeTxEquivalentToMerge asserts that folding per-tx IO into
// an accumulator via mergeTx matches repeated Merge channel by channel (reads,
// writes, accesses) and as a whole BAL — including the begin-system tx at index
// -1 and two txs touching the same index.
func TestVersionedIO_mergeTxEquivalentToMerge(t *testing.T) {
	t.Parallel()

	addrs := []accounts.Address{
		accounts.InternAddress(common.HexToAddress("0x1111")),
		accounts.InternAddress(common.HexToAddress("0x2222")),
		accounts.InternAddress(common.HexToAddress("0x3333")),
	}
	slots := []accounts.StorageKey{
		accounts.InternKey(common.HexToHash("0x01")),
		accounts.InternKey(common.HexToHash("0x02")),
	}
	type txIO struct {
		txIdx int
		inc   int
		addr  accounts.Address
		slot  accounts.StorageKey
		val   uint64
	}
	// A StoragePath read on a slot the tx does not write surfaces in the BAL as
	// a StorageRead, so a dropped read also changes the hash; a balance read
	// would contribute no BAL field.
	reads := func(x txIO) ReadSet {
		rs := ReadSet{}
		rs.SetStorage(x.addr, x.slot, VersionedRead[uint256.Int]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: Version{TxIndex: x.txIdx, Incarnation: x.inc}},
			Val:        *uint256.NewInt(x.val),
		})
		// Access marks travel on the read-set now.
		rs.access = AccessSet{x.addr: &accessOptions{}}
		return rs
	}
	writes := func(x txIO) *WriteSet {
		ws := &WriteSet{}
		ws.SetBalance(x.addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: x.addr, Path: BalancePath, Version: Version{TxIndex: x.txIdx, Incarnation: x.inc}}, Val: *uint256.NewInt(x.val + 1)})
		return ws
	}

	txs := []txIO{
		{-1, 0, addrs[0], slots[0], 5}, // begin-system tx (TxIndex -1)
		{0, 0, addrs[0], slots[0], 10},
		{1, 0, addrs[1], slots[0], 20},
		{2, 1, addrs[2], slots[0], 30},
		{2, 2, addrs[0], slots[1], 40}, // same index, higher incarnation: merge-into-existing
	}

	// Oracle: build a single-tx VersionedIO per tx and fold via repeated Merge.
	merged := &VersionedIO{}
	for _, x := range txs {
		v := Version{TxIndex: x.txIdx, Incarnation: x.inc}
		io := &VersionedIO{}
		io.RecordReads(v, reads(x))
		io.RecordWrites(v, writes(x))
		merged = merged.Merge(io)
	}

	fused := &VersionedIO{}
	for _, x := range txs {
		fused.mergeTx(Version{TxIndex: x.txIdx, Incarnation: x.inc}, reads(x), writes(x))
	}

	require.Equal(t, merged.Len(), fused.Len(), "Len mismatch")
	require.Equal(t, merged.AsBlockAccessList().Hash(), fused.AsBlockAccessList().Hash(),
		"mergeTx must produce a BAL identical to repeated Merge")

	// The BAL hash does not surface a dropped access (a non-system access adds
	// no BAL field), so compare every channel at every index directly: a mergeTx
	// that overwrote instead of merged a slot passes the hash check but fails here.
	// Access marks travel on the read-set now, so the ReadSet compare covers them.
	for i := -1; i < merged.Len()-1; i++ {
		require.Equal(t, merged.ReadSet(i), fused.ReadSet(i), "reads differ at tx %d", i)
		require.Equal(t, merged.ReadSetIncarnation(i), fused.ReadSetIncarnation(i), "incarnation differs at tx %d", i)
		require.Equal(t, merged.WriteSet(i), fused.WriteSet(i), "writes differ at tx %d", i)
	}

	require.True(t, len(fused.inputs) == len(fused.outputs),
		"mergeTx must keep inputs/outputs equal length")
}

// TestSetAccountBalanceOrDelete_NoncePathOnly_AppendBalanceNotFullAccount
// regression-pins the addrHasAnyWrite guard (#21017 bug #2): when the worker
// already wrote a non-balance field, SetAccountBalanceOrDelete must append only
// Balance, not re-emit Nonce/Incarnation/CodeHash from the pre-block snapshot.
func TestSetAccountBalanceOrDelete_NoncePathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xA000"))
	writes := &WriteSet{}
	writes.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: 42})

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 41 // stale; worker has it at 42
	acc.Incarnation = 1
	acc.CodeHash = accounts.EmptyCodeHash

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Equal(t, 2, result.Count(), "must append only BalancePath, not re-emit full account")
	nw, ok := result.GetNonce(addr)
	require.True(t, ok, "NoncePath must be preserved")
	require.Equal(t, uint64(42), nw.Val, "worker's nonce must NOT be clobbered by stale snapshot")
	bw, ok := result.GetBalance(addr)
	require.True(t, ok, "BalancePath must be appended")
	require.Equal(t, *uint256.NewInt(500), bw.Val)
	_, ok = result.GetIncarnation(addr)
	require.False(t, ok, "IncarnationPath must NOT be re-emitted from stale snapshot")
	_, ok = result.GetCodeHash(addr)
	require.False(t, ok, "CodeHashPath must NOT be re-emitted from stale snapshot")
}

// TestSetAccountBalanceOrDelete_CodeHashPathOnly_AppendBalanceNotFullAccount
// covers the same guard for a CodeHash-only worker write.
func TestSetAccountBalanceOrDelete_CodeHashPathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xB000"))
	workerCodeHash := accounts.InternCodeHash(common.HexToHash("0xcafe"))
	writes := &WriteSet{}
	writes.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: workerCodeHash})

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 5
	acc.Incarnation = 2
	acc.CodeHash = accounts.EmptyCodeHash // stale; worker installed real code

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Equal(t, 2, result.Count(), "must append only BalancePath, not re-emit full account")
	cw, ok := result.GetCodeHash(addr)
	require.True(t, ok, "CodeHashPath must be preserved")
	require.Equal(t, workerCodeHash, cw.Val, "worker's CodeHash must NOT be clobbered by stale snapshot")
	_, ok = result.GetBalance(addr)
	require.True(t, ok, "BalancePath must be appended")
	_, ok = result.GetNonce(addr)
	require.False(t, ok, "NoncePath must NOT be re-emitted from stale snapshot")
	_, ok = result.GetIncarnation(addr)
	require.False(t, ok, "IncarnationPath must NOT be re-emitted from stale snapshot")
}

// TestSetAccountBalanceOrDelete_IncarnationPathOnly_AppendBalanceNotFullAccount
// covers the same guard for an Incarnation-only worker write.
func TestSetAccountBalanceOrDelete_IncarnationPathOnly_AppendBalanceNotFullAccount(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xC000"))
	writes := &WriteSet{}
	writes.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: 7})

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(100)
	acc.Nonce = 3
	acc.Incarnation = 6 // stale; worker has it at 7
	acc.CodeHash = accounts.EmptyCodeHash

	result := writes.SetAccountBalanceOrDelete(addr, &acc, *uint256.NewInt(500), tracing.BalanceIncreaseRewardTransactionFee, true)

	require.Equal(t, 2, result.Count(), "must append only BalancePath, not re-emit full account")
	iw, ok := result.GetIncarnation(addr)
	require.True(t, ok, "IncarnationPath must be preserved")
	require.Equal(t, uint64(7), iw.Val, "worker's incarnation must NOT be clobbered by stale snapshot")
	_, ok = result.GetBalance(addr)
	require.True(t, ok, "BalancePath must be appended")
	_, ok = result.GetNonce(addr)
	require.False(t, ok, "NoncePath must NOT be re-emitted from stale snapshot")
	_, ok = result.GetCodeHash(addr)
	require.False(t, ok, "CodeHashPath must NOT be re-emitted from stale snapshot")
}
