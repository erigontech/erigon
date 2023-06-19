// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	checker "gopkg.in/check.v1"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

var toAddr = common.BytesToAddress

type StateSuite struct {
	kv    kv.RwDB
	tx    kv.RwTx
	state *IntraBlockState
	r     StateReader
	w     StateWriter
}

var _ = checker.Suite(&StateSuite{})

func (s *StateSuite) TestDump(c *checker.C) {
	// generate a few entries
	obj1 := s.state.GetOrNewStateObject(toAddr([]byte{0x01}))
	obj1.AddBalance(uint256.NewInt(22))
	obj2 := s.state.GetOrNewStateObject(toAddr([]byte{0x01, 0x02}))
	obj2.SetCode(crypto.Keccak256Hash([]byte{3, 3, 3, 3, 3, 3, 3}), []byte{3, 3, 3, 3, 3, 3, 3})
	obj3 := s.state.GetOrNewStateObject(toAddr([]byte{0x02}))
	obj3.SetBalance(uint256.NewInt(44))

	// write some of them to the trie
	err := s.w.UpdateAccountData(obj1.address, &obj1.data, new(accounts.Account))
	c.Check(err, checker.IsNil)
	err = s.w.UpdateAccountData(obj2.address, &obj2.data, new(accounts.Account))
	c.Check(err, checker.IsNil)

	err = s.state.FinalizeTx(&chain.Rules{}, s.w)
	c.Check(err, checker.IsNil)

	err = s.state.CommitBlock(&chain.Rules{}, s.w)
	c.Check(err, checker.IsNil)

	// check that dump contains the state objects that are in trie
	tx, err1 := s.kv.BeginRo(context.Background())
	if err1 != nil {
		c.Fatalf("create tx: %v", err1)
	}
	defer tx.Rollback()

	historyV3, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		panic(err)
	}
	got := string(NewDumper(tx, 1, historyV3).DefaultDump())
	want := `{
    "root": "71edff0130dd2385947095001c73d9e28d862fc286fca2b922ca6f6f3cddfdd2",
    "accounts": {
        "0x0000000000000000000000000000000000000001": {
            "balance": "22",
            "nonce": 0,
            "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        },
        "0x0000000000000000000000000000000000000002": {
            "balance": "44",
            "nonce": 0,
            "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        },
        "0x0000000000000000000000000000000000000102": {
            "balance": "0",
            "nonce": 0,
            "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "codeHash": "87874902497a5bb968da31a2998d8f22e949d1ef6214bcdedd8bae24cca4b9e3",
            "code": "03030303030303"
        }
    }
}`
	if got != want {
		c.Errorf("DumpToCollector mismatch:\ngot: %s\nwant: %s\n", got, want)
	}
}

func (s *StateSuite) SetUpTest(c *checker.C) {
	s.kv = memdb.New("")
	tx, err := s.kv.BeginRw(context.Background()) //nolint
	if err != nil {
		panic(err)
	}
	s.tx = tx
	s.r = NewPlainState(tx, 1, nil)
	s.w = NewPlainState(tx, 1, nil)
	s.state = New(s.r)
}

func (s *StateSuite) TearDownTest(c *checker.C) {
	s.tx.Rollback()
	s.kv.Close()
}

func (s *StateSuite) TestNull(c *checker.C) {
	address := common.HexToAddress("0x823140710bf13990e4500136726d8b55")
	s.state.CreateAccount(address, true)
	//value := common.FromHex("0x823140710bf13990e4500136726d8b55")
	var value uint256.Int

	s.state.SetState(address, &common.Hash{}, value)

	err := s.state.FinalizeTx(&chain.Rules{}, s.w)
	c.Check(err, checker.IsNil)

	err = s.state.CommitBlock(&chain.Rules{}, s.w)
	c.Check(err, checker.IsNil)

	s.state.GetCommittedState(address, &common.Hash{}, &value)
	if !value.IsZero() {
		c.Errorf("expected empty hash. got %x", value)
	}
}

func (s *StateSuite) TestTouchDelete(c *checker.C) {
	s.state.GetOrNewStateObject(common.Address{})

	err := s.state.FinalizeTx(&chain.Rules{}, s.w)
	if err != nil {
		c.Fatal("error while finalize", err)
	}

	err = s.state.CommitBlock(&chain.Rules{}, s.w)
	if err != nil {
		c.Fatal("error while commit", err)
	}

	s.state.Reset()

	snapshot := s.state.Snapshot()
	s.state.AddBalance(common.Address{}, new(uint256.Int))

	if len(s.state.journal.dirties) != 1 {
		c.Fatal("expected one dirty state object")
	}
	s.state.RevertToSnapshot(snapshot)
	if len(s.state.journal.dirties) != 0 {
		c.Fatal("expected no dirty state object")
	}
}

func (s *StateSuite) TestSnapshot(c *checker.C) {
	stateobjaddr := toAddr([]byte("aa"))
	var storageaddr common.Hash
	data1 := uint256.NewInt(42)
	data2 := uint256.NewInt(43)

	// snapshot the genesis state
	genesis := s.state.Snapshot()

	// set initial state object value
	s.state.SetState(stateobjaddr, &storageaddr, *data1)
	snapshot := s.state.Snapshot()

	// set a new state object value, revert it and ensure correct content
	s.state.SetState(stateobjaddr, &storageaddr, *data2)
	s.state.RevertToSnapshot(snapshot)

	var value uint256.Int
	s.state.GetState(stateobjaddr, &storageaddr, &value)
	c.Assert(value, checker.DeepEquals, data1)
	s.state.GetCommittedState(stateobjaddr, &storageaddr, &value)
	c.Assert(value, checker.DeepEquals, common.Hash{})

	// revert up to the genesis state and ensure correct content
	s.state.RevertToSnapshot(genesis)
	s.state.GetState(stateobjaddr, &storageaddr, &value)
	c.Assert(value, checker.DeepEquals, common.Hash{})
	s.state.GetCommittedState(stateobjaddr, &storageaddr, &value)
	c.Assert(value, checker.DeepEquals, common.Hash{})
}

func (s *StateSuite) TestSnapshotEmpty(c *checker.C) {
	s.state.RevertToSnapshot(s.state.Snapshot())
}

// use testing instead of checker because checker does not support
// printing/logging in tests (-check.vv does not work)
func TestSnapshot2(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	w := NewPlainState(tx, 1, nil)
	state := New(NewPlainState(tx, 1, nil))

	stateobjaddr0 := toAddr([]byte("so0"))
	stateobjaddr1 := toAddr([]byte("so1"))
	var storageaddr common.Hash

	data0 := uint256.NewInt(17)
	data1 := uint256.NewInt(18)

	state.SetState(stateobjaddr0, &storageaddr, *data0)
	state.SetState(stateobjaddr1, &storageaddr, *data1)

	// db, trie are already non-empty values
	so0 := state.getStateObject(stateobjaddr0)
	so0.SetBalance(uint256.NewInt(42))
	so0.SetNonce(43)
	so0.SetCode(crypto.Keccak256Hash([]byte{'c', 'a', 'f', 'e'}), []byte{'c', 'a', 'f', 'e'})
	so0.selfdestructed = false
	so0.deleted = false
	state.setStateObject(stateobjaddr0, so0)

	err := state.FinalizeTx(&chain.Rules{}, w)
	if err != nil {
		t.Fatal("error while finalizing transaction", err)
	}
	w = NewPlainState(tx, 2, nil)

	err = state.CommitBlock(&chain.Rules{}, w)
	if err != nil {
		t.Fatal("error while committing state", err)
	}

	// and one with deleted == true
	so1 := state.getStateObject(stateobjaddr1)
	so1.SetBalance(uint256.NewInt(52))
	so1.SetNonce(53)
	so1.SetCode(crypto.Keccak256Hash([]byte{'c', 'a', 'f', 'e', '2'}), []byte{'c', 'a', 'f', 'e', '2'})
	so1.selfdestructed = true
	so1.deleted = true
	state.setStateObject(stateobjaddr1, so1)

	so1 = state.getStateObject(stateobjaddr1)
	if so1 != nil && !so1.deleted {
		t.Fatalf("deleted object not nil when getting")
	}

	snapshot := state.Snapshot()
	state.RevertToSnapshot(snapshot)

	so0Restored := state.getStateObject(stateobjaddr0)
	// Update lazily-loaded values before comparing.
	var tmp uint256.Int
	so0Restored.GetState(&storageaddr, &tmp)
	so0Restored.Code()
	// non-deleted is equal (restored)
	compareStateObjects(so0Restored, so0, t)

	// deleted should be nil, both before and after restore of state copy
	so1Restored := state.getStateObject(stateobjaddr1)
	if so1Restored != nil && !so1Restored.deleted {
		t.Fatalf("deleted object not nil after restoring snapshot: %+v", so1Restored)
	}
}

func compareStateObjects(so0, so1 *stateObject, t *testing.T) {
	if so0.Address() != so1.Address() {
		t.Fatalf("Address mismatch: have %v, want %v", so0.address, so1.address)
	}
	if so0.Balance().Cmp(so1.Balance()) != 0 {
		t.Fatalf("Balance mismatch: have %v, want %v", so0.Balance(), so1.Balance())
	}
	if so0.Nonce() != so1.Nonce() {
		t.Fatalf("Nonce mismatch: have %v, want %v", so0.Nonce(), so1.Nonce())
	}
	if so0.data.Root != so1.data.Root {
		t.Errorf("Root mismatch: have %x, want %x", so0.data.Root[:], so1.data.Root[:])
	}
	if !bytes.Equal(so0.CodeHash(), so1.CodeHash()) {
		t.Fatalf("CodeHash mismatch: have %v, want %v", so0.CodeHash(), so1.CodeHash())
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

func TestDump(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	w := NewPlainStateWriter(tx, tx, 0)
	state := New(NewPlainStateReader(tx))

	// generate a few entries
	obj1 := state.GetOrNewStateObject(toAddr([]byte{0x01}))
	obj1.AddBalance(uint256.NewInt(22))
	obj2 := state.GetOrNewStateObject(toAddr([]byte{0x01, 0x02}))
	obj2.SetCode(crypto.Keccak256Hash([]byte{3, 3, 3, 3, 3, 3, 3}), []byte{3, 3, 3, 3, 3, 3, 3})
	obj2.setIncarnation(1)
	obj3 := state.GetOrNewStateObject(toAddr([]byte{0x02}))
	obj3.SetBalance(uint256.NewInt(44))

	// write some of them to the trie
	err := w.UpdateAccountData(obj1.address, &obj1.data, new(accounts.Account))
	if err != nil {
		t.Fatal(err)
	}
	err = w.UpdateAccountData(obj2.address, &obj2.data, new(accounts.Account))
	if err != nil {
		t.Fatal(err)
	}

	err = state.FinalizeTx(&chain.Rules{}, w)
	if err != nil {
		t.Fatal(err)
	}

	blockWriter := NewPlainStateWriter(tx, tx, 1)
	err = state.CommitBlock(&chain.Rules{}, blockWriter)
	if err != nil {
		t.Fatal(err)
	}
	err = blockWriter.WriteChangeSets()
	if err != nil {
		t.Fatal(err)
	}
	err = blockWriter.WriteHistory()
	if err != nil {
		t.Fatal(err)
	}

	// check that dump contains the state objects that are in trie
	historyV3, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		panic(err)
	}
	got := string(NewDumper(tx, 2, historyV3).DefaultDump())
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
