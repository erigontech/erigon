// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/execctx"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func toAddr(a []byte) accounts.Address {
	return accounts.InternAddress(common.BytesToAddress(a))
}

func TestWriterAccountAndStorageNotifications(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		storageOriginal, storageValue uint64
		nonces                        []uint64
		action                        notifications.Action
	}{
		{"storage only", 0, 1, []uint64{1}, notifications.ActionStorage},
		{"storage cleared", 1, 0, []uint64{1}, notifications.ActionStorage},
		{"account and storage", 0, 1, []uint64{2}, notifications.ActionUpsert},
		{"account restored", 0, 1, []uint64{2, 1}, notifications.ActionUpsert},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tx, domains := NewTestRwTx(t)
			accumulator := notifications.NewAccumulator()
			accumulator.StartChange(&types.Header{Number: *uint256.NewInt(1)}, nil, false)
			writer := NewWriter(domains.AsPutDel(tx), accumulator, 1)
			address := accounts.InternAddress(common.Address{1})
			original := accounts.NewAccount()
			original.Nonce = 1
			account := original

			storageValue := uint256.NewInt(tc.storageValue)
			require.NoError(t, writer.WriteAccountStorage(address, 0, accounts.ZeroKey, *uint256.NewInt(tc.storageOriginal), *storageValue))
			for _, nonce := range tc.nonces {
				account.Nonce = nonce
				require.NoError(t, writer.UpdateAccountData(address, &original, &account))
			}
			changes := accumulator.Changes()
			require.Len(t, changes, 1)
			require.Len(t, changes[0].Changes, 1)
			require.Equal(t, tc.action, changes[0].Changes[0].Action)
			require.Len(t, changes[0].Changes[0].StorageChanges, 1)
			require.Equal(t, storageValue.Bytes(), changes[0].Changes[0].StorageChanges[0].Data)
			if tc.action == notifications.ActionUpsert {
				require.Equal(t, accounts.SerialiseV3(&account), changes[0].Changes[0].Data)
			}
		})
	}
}

func TestNull(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	txNum := uint64(1)
	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	w := NewWriter(domains.AsPutDel(tx), nil, txNum)
	state := New(r)
	defer state.Close()

	address := accounts.InternAddress(common.HexToAddress("0x823140710bf13990e4500136726d8b55"))
	require.NoError(t, state.CreateAccount(address, true))
	//value := common.FromHex("0x823140710bf13990e4500136726d8b55")
	var value uint256.Int

	require.NoError(t, state.SetState(address, accounts.ZeroKey, value))

	err = state.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	writes, err := state.FinalizedWrites(&chain.Rules{})
	require.NoError(t, err)
	require.NoError(t, ApplyWrites(writes, domains, tx, 1, txNum, false))

	value, err = state.GetCommittedState(address, accounts.ZeroKey)
	require.NoError(t, err)

	if !value.IsZero() {
		t.Errorf("expected empty hash. got %x", value)
	}
}

func TestTouchDelete(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	txNum := uint64(1)
	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	w := NewWriter(domains.AsPutDel(tx), nil, txNum)
	state := New(r)
	defer state.Close()

	_, err = state.GetOrNewStateObject(accounts.ZeroAddress)
	require.NoError(t, err)

	err = state.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	writes, err := state.FinalizedWrites(&chain.Rules{})
	require.NoError(t, err)
	require.NoError(t, ApplyWrites(writes, domains, tx, 1, txNum, false))

	state.Reset()

	snapshot := state.PushSnapshot()
	defer state.PopSnapshot(snapshot)
	err = state.AddBalance(accounts.ZeroAddress, uint256.Int{}, tracing.BalanceChangeUnspecified)
	require.NoError(t, err)

	if len(state.journal.dirties) != 1 {
		t.Fatal("expected one dirty state object")
	}
	state.RevertToSnapshot(snapshot, nil)
	if len(state.journal.dirties) != 0 {
		t.Fatal("expected no dirty state object")
	}
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	state := New(r)
	defer state.Close()

	stateobjaddr := toAddr([]byte("aa"))
	storageaddr := accounts.ZeroKey
	data1 := uint256.NewInt(42)
	data2 := uint256.NewInt(43)

	// snapshot the genesis state
	genesis := state.PushSnapshot()

	// set initial state object value
	require.NoError(t, state.SetState(stateobjaddr, storageaddr, *data1))
	snapshot := state.PushSnapshot()

	// set a new state object value, revert it and ensure correct content
	require.NoError(t, state.SetState(stateobjaddr, storageaddr, *data2))
	state.RevertToSnapshot(snapshot, nil)
	state.PopSnapshot(snapshot)
	value, err := state.GetState(stateobjaddr, storageaddr)
	require.NoError(t, err)
	require.Equal(t, *data1, value)
	value, err = state.GetCommittedState(stateobjaddr, storageaddr)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{}, value)

	// revert up to the genesis state and ensure correct content
	state.RevertToSnapshot(genesis, nil)
	state.PopSnapshot(genesis)
	value, err = state.GetState(stateobjaddr, storageaddr)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{}, value)
	value, err = state.GetCommittedState(stateobjaddr, storageaddr)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{}, value)
}

func TestSnapshotEmpty(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	state := New(r)
	defer state.Close()

	snapshot := state.PushSnapshot()
	state.RevertToSnapshot(snapshot, nil)
	state.PopSnapshot(snapshot)
}

func TestCodeResolve(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	txNum := uint64(1)
	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	rules := &chain.Rules{}

	stateobjaddr0 := toAddr([]byte("so0"))
	stateobjaddr1 := toAddr([]byte("so1"))

	seed := New(NewReaderV3(domains.AsGetter(tx)))

	del := types.AddressToDelegation(stateobjaddr1)
	require.NoError(t, seed.CreateAccount(stateobjaddr0, true))
	require.NoError(t, seed.SetCode(stateobjaddr0, del, tracing.CodeChangeUnspecified))

	target := []byte{'c', 'a', 'f', 'e'}
	require.NoError(t, seed.CreateAccount(stateobjaddr1, true))
	require.NoError(t, seed.SetCode(stateobjaddr1, target, tracing.CodeChangeUnspecified))

	writes, err := seed.FinalizedWrites(rules)
	require.NoError(t, err)
	require.NoError(t, ApplyWrites(writes, domains, tx, 0, txNum, false))
	seed.Close()

	state1 := New(NewReaderV3(domains.AsGetter(tx)))

	defer state1.Close()
	state1.SetVersionMap(&VersionMap{})
	state1.Prepare(&chain.Rules{}, accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, nil, nil)

	_, ok, err := state1.GetDelegatedDesignation(stateobjaddr0)
	require.NoError(t, err)
	require.True(t, ok)
	code, err := state1.GetCode(stateobjaddr0)
	require.NoError(t, err)
	require.Equal(t, del, code)
	resolved, err := state1.ResolveCode(stateobjaddr0, true)
	require.NoError(t, err)
	require.Equal(t, target, resolved.Bytes)
}

func NewTestRwTx(tb testing.TB) (kv.TemporalRwDB, kv.TemporalRwTx, *execctx.SharedDomains) {
	tb.Helper()

	dirs := datadir.New(tb.TempDir())

	stepSize := uint64(16)
	db := temporaltest.NewTestDB(tb, dirs, temporaltest.WithStepSize(stepSize))
	tb.Cleanup(db.Close)
	tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(tb, err)
	tb.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(tb, err)
	tb.Cleanup(domains.Close)

	return db, tx, domains
}

func TestDump(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)
	txNum, _, err := domains.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	rules := &chain.Rules{}
	addr1 := toAddr([]byte{0x01})
	addr2 := toAddr([]byte{0x01, 0x02})
	addr3 := toAddr([]byte{0x02})

	seed := New(NewReaderV3(domains.AsGetter(tx)))

	// generate a few entries
	require.NoError(t, seed.CreateAccount(addr1, true))
	require.NoError(t, seed.AddBalance(addr1, *uint256.NewInt(22), tracing.BalanceChangeUnspecified))

	require.NoError(t, seed.CreateAccount(addr2, true))
	require.NoError(t, seed.SetCode(addr2, []byte{3, 3, 3, 3, 3, 3, 3}, tracing.CodeChangeUnspecified))
	require.NoError(t, seed.SetIncarnation(addr2, 1))

	require.NoError(t, seed.CreateAccount(addr3, true))
	require.NoError(t, seed.SetBalance(addr3, *uint256.NewInt(44), tracing.BalanceChangeUnspecified))

	writes, err := seed.FinalizedWrites(rules)
	require.NoError(t, err)
	require.NoError(t, ApplyWrites(writes, domains, tx, 0, txNum, false))
	seed.Close()
	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	// check that dump contains the state objects that are in trie
	got := string(NewDumper(tx, rawdbv3.TxNums, 1).DefaultDump())
	want := `{
    "root": "0000000000000000000000000000000000000000000000000000000000000000",
    "accounts": {
        "0x0000000000000000000000000000000000000001": {
            "balance": "22",
            "nonce": 0,
            "root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
            "address": "0x0000000000000000000000000000000000000001",
            "key": "0x1468288056310c82aa4c01a7e12a10f8111a0560e72b700555479031b86c357d"
        },
        "0x0000000000000000000000000000000000000002": {
            "balance": "44",
            "nonce": 0,
            "root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
            "address": "0x0000000000000000000000000000000000000002",
            "key": "0xd52688a8f926c816ca1e079067caba944f158e764817b83fc43594370ca9cf62"
        },
        "0x0000000000000000000000000000000000000102": {
            "balance": "0",
            "nonce": 0,
            "root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "0x87874902497a5bb968da31a2998d8f22e949d1ef6214bcdedd8bae24cca4b9e3",
            "code": "0x03030303030303",
            "address": "0x0000000000000000000000000000000000000102",
            "key": "0xa17eacbc25cda025e81db9c5c62868822c73ce097cee2a63e33a2e41268358a1"
        }
    }
}`
	if got != want {
		t.Fatalf("dump mismatch:\ngot: %s\nwant: %s\n", got, want)
	}
}
