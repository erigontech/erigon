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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// writeIndex indexes a WriteSet by (Path, Key), dropping Address — safe only
// for a single-address WriteSet (see addrWriteIndex for multiple).
func writeIndex(writes *WriteSet) map[AccountKey]any {
	idx := make(map[AccountKey]any)
	for h := range writes.AllHeaders() {
		idx[AccountKey{Path: h.Path, Key: h.Key}] = writeSetVal(writes, h)
	}
	return idx
}

func addrWriteIndex(writes *WriteSet, addr accounts.Address) map[AccountKey]any {
	idx := make(map[AccountKey]any)
	for h := range writes.AllHeaders() {
		if h.Address == addr {
			idx[AccountKey{Path: h.Path, Key: h.Key}] = writeSetVal(writes, h)
		}
	}
	return idx
}

func TestVersionedWritesMatchStateObjects(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)

	addr1 := accounts.InternAddress(common.HexToAddress("0x1111"))
	addr2 := accounts.InternAddress(common.HexToAddress("0x2222"))
	key1 := accounts.InternKey(common.HexToHash("0x0001"))
	key2 := accounts.InternKey(common.HexToHash("0x0002"))
	code1 := []byte{0xde, 0xad, 0xbe, 0xef}

	err := ibs.SetBalance(addr1, *uint256.NewInt(100), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr1, 7, tracing.NonceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetCode(addr1, code1, tracing.CodeChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetState(addr1, key1, *uint256.NewInt(42))
	require.NoError(t, err)
	err = ibs.SetState(addr1, key2, *uint256.NewInt(99))
	require.NoError(t, err)

	ibs.CreateAccount(addr2, true)
	err = ibs.SetBalance(addr2, *uint256.NewInt(200), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)

	// Capture writes before FinalizeTx: it clears journal.dirties.
	writes := ibs.VersionedWrites()

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
	require.Equal(t, gotCode1, wcode1.(accounts.Code).Bytes, "addr1: code mismatch between stateObject and VersionedWrites")

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

	idx2 := addrWriteIndex(writes, addr2)

	wbal2, ok := idx2[AccountKey{Path: BalancePath, Key: accounts.NilKey}]
	require.True(t, ok, "addr2: BalancePath write missing from VersionedWrites")
	bal2, err := ibs.GetBalance(addr2)
	require.NoError(t, err)
	require.Equal(t, bal2, wbal2.(uint256.Int), "addr2: balance mismatch between stateObject and VersionedWrites")
}

// TestSnapshotRandomWithVersionMap checks that stateObject accessors and
// VersionedWrites agree after a snapshot revert.
func TestSnapshotRandomWithVersionMap(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress(common.HexToAddress("0xAAAA"))
	key := accounts.InternKey(common.HexToHash("0x0001"))

	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)

	err := ibs.SetBalance(addr, *uint256.NewInt(50), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr, 3, tracing.NonceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetState(addr, key, *uint256.NewInt(11))
	require.NoError(t, err)

	snap := ibs.PushSnapshot()

	err = ibs.SetBalance(addr, *uint256.NewInt(999), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetNonce(addr, 42, tracing.NonceChangeUnspecified)
	require.NoError(t, err)
	err = ibs.SetState(addr, key, *uint256.NewInt(77))
	require.NoError(t, err)

	ibs.RevertToSnapshot(snap, nil)
	ibs.PopSnapshot(snap)

	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(50), &bal, "balance should be reverted to pre-snapshot value")

	nonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(3), nonce, "nonce should be reverted to pre-snapshot value")

	stor, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(11), &stor, "storage should be reverted to pre-snapshot value")

	writes := ibs.VersionedWrites()
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

// TestCommittedStateWithVersionMap checks GetCommittedState returns the
// pre-tx value (EIP-1283 "original value") even after a later tx overwrites it.
func TestCommittedStateWithVersionMap(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress(common.HexToAddress("0xBBBB"))
	key := accounts.InternKey(common.HexToHash("0x0001"))

	val1 := *uint256.NewInt(111)
	val2 := *uint256.NewInt(222)

	ibs0 := NewWithVersionMap(reader, mvhm)
	defer ibs0.Close()
	ibs0.SetTxContext(1, 0)

	err := ibs0.SetState(addr, key, val1)
	require.NoError(t, err)

	writes0 := ibs0.VersionedWrites()
	mvhm.FlushVersionedWrites(writes0, true, "")

	ibs1 := NewWithVersionMap(reader, mvhm)
	defer ibs1.Close()
	ibs1.SetTxContext(1, 1)

	committed, err := ibs1.GetCommittedState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val1, committed, "GetCommittedState must return pre-tx value (val1) before any tx1 write")

	err = ibs1.SetState(addr, key, val2)
	require.NoError(t, err)

	committed2, err := ibs1.GetCommittedState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val1, committed2, "GetCommittedState must continue to return val1 after tx1 writes val2")

	current, err := ibs1.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, val2, current, "GetState must return the current tx1 value (val2)")
}

// TestCrossBlockStateReadConsistency checks a new IBS for block N+1 reads
// state block N committed to SharedDomains.
func TestCrossBlockStateReadConsistency(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)

	addr := accounts.InternAddress(common.HexToAddress("0xDDDD"))
	key := accounts.InternKey(common.HexToHash("0x0001"))
	wantBalance := uint256.NewInt(777)
	wantNonce := uint64(13)
	wantStorage := *uint256.NewInt(555)

	{
		ibsN := New(NewReaderV3(domains.AsGetter(tx)))
		defer ibsN.Close()
		ibsN.SetTxContext(1, 0)

		err := ibsN.SetBalance(addr, *wantBalance, tracing.BalanceChangeUnspecified)
		require.NoError(t, err)
		err = ibsN.SetNonce(addr, wantNonce, tracing.NonceChangeUnspecified)
		require.NoError(t, err)
		err = ibsN.SetState(addr, key, wantStorage)
		require.NoError(t, err)

		w := NewWriter(domains.AsPutDel(tx), nil, 2)
		err = ibsN.FinalizeTx(&chain.Rules{}, w)
		require.NoError(t, err)
	}

	ibsN1 := New(NewReaderV3(domains.AsGetter(tx)))
	defer ibsN1.Close()

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

// TestDomainApplyFromVersionedWrites checks that replaying VersionedWrites
// through ApplyVersionedWrites + FinalizeTx yields the same domain state.
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

	ibsTx := NewWithVersionMap(reader, mvhm)
	defer ibsTx.Close()
	ibsTx.SetTxContext(1, 0)

	err := ibsTx.SetBalance(addr, wantBalance, tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibsTx.SetNonce(addr, wantNonce, tracing.NonceChangeUnspecified)
	require.NoError(t, err)
	err = ibsTx.SetState(addr, key, wantStorage)
	require.NoError(t, err)

	writes := ibsTx.VersionedWrites()
	require.NotEmpty(t, writes, "VersionedWrites must not be empty")

	ibsApply := New(reader)
	defer ibsApply.Close()
	err = ibsApply.ApplyVersionedWrites(writes)
	require.NoError(t, err)

	w := NewWriter(domains.AsPutDel(tx), nil, 3)
	err = ibsApply.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	ibsRead := New(NewReaderV3(domains.AsGetter(tx)))
	defer ibsRead.Close()

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
