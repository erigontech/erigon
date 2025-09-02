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
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var toAddr = common.BytesToAddress

func TestNull(t *testing.T) {
	t.Parallel()
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := uint64(1)
	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	w := NewWriter(domains.AsPutDel(tx), nil, txNum)
	state := New(r)

	address := common.HexToAddress("0x823140710bf13990e4500136726d8b55")
	state.CreateAccount(address, true)
	//value := common.FromHex("0x823140710bf13990e4500136726d8b55")
	var value uint256.Int

	state.SetState(address, common.Hash{}, value)

	err = state.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	err = state.CommitBlock(&chain.Rules{}, w)
	require.NoError(t, err)

	state.GetCommittedState(address, common.Hash{}, &value)
	if !value.IsZero() {
		t.Errorf("expected empty hash. got %x", value)
	}
}

func TestTouchDelete(t *testing.T) {
	t.Parallel()
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := uint64(1)
	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	w := NewWriter(domains.AsPutDel(tx), nil, txNum)
	state := New(r)

	state.GetOrNewStateObject(common.Address{})

	err = state.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	err = state.CommitBlock(&chain.Rules{}, w)
	require.NoError(t, err)

	state.Reset()

	snapshot := state.Snapshot()
	state.AddBalance(common.Address{}, uint256.Int{}, tracing.BalanceChangeUnspecified)

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
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	state := New(r)

	stateobjaddr := toAddr([]byte("aa"))
	var storageaddr common.Hash
	data1 := uint256.NewInt(42)
	data2 := uint256.NewInt(43)

	// snapshot the genesis state
	genesis := state.Snapshot()

	// set initial state object value
	state.SetState(stateobjaddr, storageaddr, *data1)
	snapshot := state.Snapshot()

	// set a new state object value, revert it and ensure correct content
	state.SetState(stateobjaddr, storageaddr, *data2)
	state.RevertToSnapshot(snapshot, nil)

	var value uint256.Int
	state.GetState(stateobjaddr, storageaddr, &value)
	require.Equal(t, *data1, value)
	state.GetCommittedState(stateobjaddr, storageaddr, &value)
	require.Equal(t, uint256.Int{}, value)

	// revert up to the genesis state and ensure correct content
	state.RevertToSnapshot(genesis, nil)
	state.GetState(stateobjaddr, storageaddr, &value)
	require.Equal(t, uint256.Int{}, value)
	state.GetCommittedState(stateobjaddr, storageaddr, &value)
	require.Equal(t, uint256.Int{}, value)
}

func TestSnapshotEmpty(t *testing.T) {
	t.Parallel()
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	r := NewReaderV3(domains.AsGetter(tx))
	state := New(r)

	state.RevertToSnapshot(state.Snapshot(), nil)
}

// use testing instead of checker because checker does not support
// printing/logging in tests (-check.vv does not work)
func TestSnapshot2(t *testing.T) {
	//TODO: why I shouldn't recreate writer here? And why domains.SetBlockNum(1) is enough for green test?
	t.Parallel()
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum := uint64(1)
	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	w := NewWriter(domains.AsPutDel(tx), nil, txNum)

	state := New(NewReaderV3(domains.AsGetter(tx)))

	stateobjaddr0 := toAddr([]byte("so0"))
	stateobjaddr1 := toAddr([]byte("so1"))
	var storageaddr common.Hash

	data0 := uint256.NewInt(17)
	data1 := uint256.NewInt(18)

	state.SetState(stateobjaddr0, storageaddr, *data0)
	state.SetState(stateobjaddr1, storageaddr, *data1)

	// db, trie are already non-empty values
	so0, err := state.getStateObject(stateobjaddr0)
	require.NoError(t, err)
	so0.SetBalance(*uint256.NewInt(42), tracing.BalanceChangeUnspecified)
	so0.SetNonce(43)
	so0.SetCode(crypto.Keccak256Hash([]byte{'c', 'a', 'f', 'e'}), []byte{'c', 'a', 'f', 'e'})
	so0.selfdestructed = false
	so0.deleted = false
	state.setStateObject(stateobjaddr0, so0)

	err = state.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	err = state.CommitBlock(&chain.Rules{}, w)
	require.NoError(t, err)

	// and one with deleted == true
	so1, err := state.getStateObject(stateobjaddr1)
	require.NoError(t, err)
	so1.SetBalance(*uint256.NewInt(52), tracing.BalanceChangeUnspecified)
	so1.SetNonce(53)
	so1.SetCode(crypto.Keccak256Hash([]byte{'c', 'a', 'f', 'e', '2'}), []byte{'c', 'a', 'f', 'e', '2'})
	so1.selfdestructed = true
	so1.deleted = true
	state.setStateObject(stateobjaddr1, so1)

	so1, err = state.getStateObject(stateobjaddr1)
	require.NoError(t, err)
	if so1 != nil && !so1.deleted {
		t.Fatalf("deleted object not nil when getting")
	}

	snapshot := state.Snapshot()
	state.RevertToSnapshot(snapshot, nil)

	so0Restored, err := state.getStateObject(stateobjaddr0)
	require.NoError(t, err)
	// Update lazily-loaded values before comparing.
	var tmp uint256.Int
	so0Restored.GetState(storageaddr, &tmp)
	so0Restored.Code()
	// non-deleted is equal (restored)
	compareStateObjects(so0Restored, so0, t)

	// deleted should be nil, both before and after restore of state copy
	so1Restored, err := state.getStateObject(stateobjaddr1)
	require.NoError(t, err)
	if so1Restored != nil && !so1Restored.deleted {
		t.Fatalf("deleted object not nil after restoring snapshot: %+v", so1Restored)
	}
}

func compareStateObjects(so0, so1 *stateObject, t *testing.T) {
	if so0.Address() != so1.Address() {
		t.Fatalf("Address mismatch: have %v, want %v", so0.address, so1.address)
	}
	bal0 := so0.Balance()
	bal1 := so1.Balance()
	if bal0.Cmp(&bal1) != 0 {
		t.Fatalf("Balance mismatch: have %v, want %v", so0.Balance(), so1.Balance())
	}
	if so0.Nonce() != so1.Nonce() {
		t.Fatalf("Nonce mismatch: have %v, want %v", so0.Nonce(), so1.Nonce())
	}
	if so0.data.Root != so1.data.Root {
		t.Errorf("Root mismatch: have %x, want %x", so0.data.Root[:], so1.data.Root[:])
	}
	if so0.data.CodeHash != so1.data.CodeHash {
		t.Fatalf("CodeHash mismatch: have %v, want %v", so0.data.CodeHash, so1.data.CodeHash)
	}
	if !bytes.Equal(so0.code, so1.code) {
		t.Fatalf("Code mismatch: have %v, want %v", so0.code, so1.code)
	}

	if len(so1.dirtyStorage) != len(so0.dirtyStorage) {
		t.Errorf("Dirty storage size mismatch: have %d, want %d", len(so1.dirtyStorage), len(so0.dirtyStorage))
	}
	for k, v := range so1.dirtyStorage {
		if so0.dirtyStorage[k] != v {
			t.Errorf("Dirty storage key %x mismatch: have %v, want %v", k, so0.dirtyStorage[k], v)
		}
	}
	for k, v := range so0.dirtyStorage {
		if so1.dirtyStorage[k] != v {
			t.Errorf("Dirty storage key %x mismatch: have %v, want none.", k, v)
		}
	}
	if len(so1.originStorage) != len(so0.originStorage) {
		t.Errorf("Origin storage size mismatch: have %d, want %d", len(so1.originStorage), len(so0.originStorage))
	}
	for k, v := range so1.originStorage {
		if so0.originStorage[k] != v {
			t.Errorf("Origin storage key %x mismatch: have %v, want %v", k, so0.originStorage[k], v)
		}
	}
	for k, v := range so0.originStorage {
		if so1.originStorage[k] != v {
			t.Errorf("Origin storage key %x mismatch: have %v, want none.", k, v)
		}
	}
}

func NewTestTemporalDb(tb testing.TB) (kv.TemporalRwDB, kv.TemporalRwTx, *state.Aggregator) {
	tb.Helper()
	db := memdb.NewStateDB(tb.TempDir())
	tb.Cleanup(db.Close)

	dirs, logger := datadir.New(tb.TempDir()), log.New()
	salt, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(tb, err)
	agg, err := state.NewAggregator2(context.Background(), dirs, 16, salt, db, log.New())
	require.NoError(tb, err)
	tb.Cleanup(agg.Close)

	_db, err := temporal.New(db, agg)
	require.NoError(tb, err)
	tx, err := _db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(tb, err)
	tb.Cleanup(tx.Rollback)
	return _db, tx, agg
}

func TestDump(t *testing.T) {
	t.Parallel()
	_, tx, _ := NewTestTemporalDb(t)

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	st := New(NewReaderV3(domains.AsGetter(tx)))

	// generate a few entries
	obj1, err := st.GetOrNewStateObject(toAddr([]byte{0x01}))
	require.NoError(t, err)
	st.AddBalance(toAddr([]byte{0x01}), *uint256.NewInt(22), tracing.BalanceChangeUnspecified)
	obj2, err := st.GetOrNewStateObject(toAddr([]byte{0x01, 0x02}))
	require.NoError(t, err)
	obj2.SetCode(crypto.Keccak256Hash([]byte{3, 3, 3, 3, 3, 3, 3}), []byte{3, 3, 3, 3, 3, 3, 3})
	obj2.setIncarnation(1)
	obj3, err := st.GetOrNewStateObject(toAddr([]byte{0x02}))
	require.NoError(t, err)
	obj3.SetBalance(*uint256.NewInt(44), tracing.BalanceChangeUnspecified)

	w := NewWriter(domains.AsPutDel(tx), nil, domains.TxNum())
	// write some of them to the trie
	err = w.UpdateAccountData(obj1.address, &obj1.data, new(accounts.Account))
	require.NoError(t, err)
	err = w.UpdateAccountData(obj2.address, &obj2.data, new(accounts.Account))
	require.NoError(t, err)
	err = st.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	blockWriter := NewWriter(domains.AsPutDel(tx), nil, domains.TxNum())
	err = st.CommitBlock(&chain.Rules{}, blockWriter)
	require.NoError(t, err)
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
            "codeHash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        },
        "0x0000000000000000000000000000000000000002": {
            "balance": "44",
            "nonce": 0,
            "root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        },
        "0x0000000000000000000000000000000000000102": {
            "balance": "0",
            "nonce": 0,
            "root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "0x87874902497a5bb968da31a2998d8f22e949d1ef6214bcdedd8bae24cca4b9e3",
            "code": "0x03030303030303"
        }
    }
}`
	if got != want {
		t.Fatalf("dump mismatch:\ngot: %s\nwant: %s\n", got, want)
	}
}
