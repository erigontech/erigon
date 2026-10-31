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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A serial IBS (no versionMap, waitCommit nil) reports a usable stamp, so the vm
// frame slot cache is on; read tracing disables it, since tracing must observe
// every read.
// Not parallel: it toggles the global dbg.TraceTransactionIO, which other tests
// read (e.g. CreateAccount), so a parallel run would data-race on it.
func TestReadStamp_Gating(t *testing.T) {
	ibs := New(&emptyReader{})
	defer ibs.Close()

	_, ok := ibs.ReadStamp()
	require.True(t, ok, "serial IBS must report a usable stamp")

	prev := dbg.TraceTransactionIO
	dbg.TraceTransactionIO = true
	t.Cleanup(func() { dbg.TraceTransactionIO = prev })
	_, ok = ibs.ReadStamp()
	require.False(t, ok, "read tracing must disable the slot cache")
}

// A dep-wait resume (a read that pauses on an in-flight estimate and re-reads the
// now-committed value) must change the stamp so the vm invalidates any slot it
// cached before the pause — the lower-txIndex write that landed during the pause
// is not journalled, so wakeGen is the only thing that moves the token here.
func TestReadStamp_ResumeChangesStamp(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	// Lower-txIndex predecessor write is in flight (incomplete → ESTIMATE).
	mvhm.WriteStorage(addr, key, Version{TxIndex: 2}, uint256.Int{}, false)

	resumes := 0
	ibs.SetWaitCommit(func(int) bool {
		resumes++
		return resumes == 1 // pause once, then let the read fall through
	})

	before, ok := ibs.ReadStamp()
	require.True(t, ok)

	_, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, resumes, 1, "the read must have paused on the in-flight estimate")

	after, ok := ibs.ReadStamp()
	require.True(t, ok)
	// GetState does not journal, so wakeGen is the only field that can have moved.
	require.NotEqual(t, before, after, "a dep-wait resume must change the stamp (wakeGen) so the slot cache invalidates")
}

// A read that waits on an in-flight estimate and wakes after the writer commits
// must return the committed value and must not leave a dependency verdict.
func TestDepWait_ResumeReadsCommittedValue(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	mvhm.WriteStorage(addr, key, Version{TxIndex: 2}, uint256.Int{}, false)

	ibs.SetWaitCommit(func(int) bool {
		mvhm.WriteStorage(addr, key, Version{TxIndex: 2}, *uint256.NewInt(7), true)
		return true
	})

	v, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), v)
	require.False(t, ibs.HadInvalidRead(), "a successful wait must not force a re-execution")
}

// A SetCode whose code hash was read from a committed cell must load the previous
// code through the versioned path too: a code cell still in flight from the same
// writer must be waited on, not replaced by the pre-block storage code.
func TestDepWait_SetCodeLoadsInFlightCode(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	addrValue := addr.Value()
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], accounts.SerialiseV3(&acc), 0, nil))
	delegation := accounts.NewCode(types.AddressToDelegation(accounts.InternAddress(common.HexToAddress("0xbeef"))))
	writer := Version{TxIndex: 0}
	delegated := acc
	delegated.Nonce = 1
	delegated.CodeHash = delegation.Hash
	mvhm.WriteAddress(addr, writer, &delegated, true)
	mvhm.WriteNonce(addr, writer, 1, true)
	mvhm.WriteCodeHash(addr, writer, delegation.Hash, true)
	mvhm.WriteCodeSize(addr, writer, len(delegation.Bytes), true)
	mvhm.WriteCode(addr, writer, delegation, false)

	ibs.SetWaitCommit(func(int) bool {
		mvhm.WriteCode(addr, writer, delegation, true)
		return true
	})

	require.NoError(t, ibs.SetCode(addr, nil, tracing.CodeChangeAuthorizationClear))
	_, wrote := ibs.VersionedWrites().GetCode(addr)
	require.True(t, wrote, "clearing an in-flight delegation must write the empty code")
}

// A read that waits on a predecessor still in flight must apply that predecessor's
// self-destruct once it commits: a burned balance reads zero, not the in-flight value.
func TestDepWait_ResumeSeesSelfDestruct(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	writer := Version{TxIndex: 0}
	writes := func() *WriteSet {
		ws := &WriteSet{}
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: writer}, Val: true})
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: writer, valStatus: ValueDeleted}, Val: *uint256.NewInt(1)})
		return ws
	}
	mvhm.FlushVersionedWrites(writes(), false)
	ibs.SetWaitCommit(func(int) bool {
		mvhm.FlushVersionedWrites(writes(), true)
		return true
	})

	b, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.True(t, b.IsZero(), "a balance burned by a predecessor's self-destruct must read zero, got %s", b.String())
}

// A field read while the predecessor's self-destruct is still in flight must wait for it:
// the balance cell may already read as Done while the destruct that burns it is not.
func TestDepWait_InFlightSelfDestructIsWaitedOn(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	writer := Version{TxIndex: 0}
	destruct := &WriteSet{}
	destruct.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: writer}, Val: true})
	burn := &WriteSet{}
	burn.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: writer, valStatus: ValueDeleted}, Val: *uint256.NewInt(1)})
	mvhm.FlushVersionedWrites(destruct, false)
	mvhm.FlushVersionedWrites(burn, true)
	ibs.SetWaitCommit(func(int) bool {
		mvhm.FlushVersionedWrites(destruct, true)
		return true
	})

	b, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.True(t, b.IsZero(), "a balance burned by an in-flight self-destruct must read zero, got %s", b.String())
}
