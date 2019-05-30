// Copyright 2015 The go-ethereum Authors
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
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// testAccount is the data associated with an account used by the state tests.
type testAccount struct {
	address common.Address
	balance *big.Int
	nonce   uint64
	code    []byte
}

// makeTestState create a sample test state to test node-wise reconstruction.
func makeTestState() (ethdb.Database, common.Hash, []*testAccount) {
	// Create an empty state
	diskdb := ethdb.NewMemDatabase()
	tds, _ := NewTrieDbState(common.Hash{}, diskdb, 0)
	state := New(tds)

	// Fill it with some arbitrary data
	accounts := []*testAccount{}
	for i := byte(0); i < 96; i++ {
		obj := state.GetOrNewStateObject(common.BytesToAddress([]byte{i}))
		acc := &testAccount{address: common.BytesToAddress([]byte{i})}

		obj.AddBalance(big.NewInt(int64(11 * i)))
		acc.balance = big.NewInt(int64(11 * i))

		obj.SetNonce(uint64(42 * i))
		acc.nonce = uint64(42 * i)

		if i%3 == 0 {
			obj.SetCode(crypto.Keccak256Hash([]byte{i, i, i, i, i}), []byte{i, i, i, i, i})
			acc.code = []byte{i, i, i, i, i}
		}
		tds.TrieStateWriter().UpdateAccountData(obj.address, &obj.data, new(Account))
		accounts = append(accounts, acc)
	}
	root, _ := tds.IntermediateRoot(state, false)
	tds.SetBlockNr(1)
	state.Finalise(false, tds.DbStateWriter())

	// Return the generated state
	return diskdb, root, accounts
}

// checkStateAccounts cross references a reconstructed state with an expected
// account array.
func checkStateAccounts(t *testing.T, db ethdb.Database, root common.Hash, accounts []*testAccount) {
	// Check root availability and state contents
	tds, err := NewTrieDbState(root, db, 0)
	if err != nil {
		t.Fatalf("failed to create state trie at %x: %v", root, err)
	}
	state := New(tds)
	if err := checkStateConsistency(db, root); err != nil {
		t.Fatalf("inconsistent state trie at %x: %v", root, err)
	}
	for i, acc := range accounts {
		if balance := state.GetBalance(acc.address); balance.Cmp(acc.balance) != 0 {
			t.Errorf("account %d: balance mismatch: have %v, want %v", i, balance, acc.balance)
		}
		if nonce := state.GetNonce(acc.address); nonce != acc.nonce {
			t.Errorf("account %d: nonce mismatch: have %v, want %v", i, nonce, acc.nonce)
		}
		if code := state.GetCode(acc.address); !bytes.Equal(code, acc.code) {
			t.Errorf("account %d: code mismatch: have %x, want %x", i, code, acc.code)
		}
	}
}

// checkTrieConsistency checks that all nodes in a (sub-)trie are indeed present.
func checkTrieConsistency(db ethdb.Database, root common.Hash) error {
	trie := trie.New(root, AccountsBucket, nil, false)
	it := trie.NodeIterator(db, nil, 0)
	for it.Next(true) {
	}
	return it.Error()
}

// checkStateConsistency checks that all data of a state root is present.
func checkStateConsistency(db ethdb.Database, root common.Hash) error {
	// Create and iterate a state trie rooted in a sub-node
	tds, err := NewTrieDbState(root, db, 0)
	if err != nil {
		return err
	}
	it := NewNodeIterator(tds)
	for it.Next() {
	}
	return it.Error
}

// Tests that an empty state is not scheduled for syncing.
func TestEmptyStateSync(t *testing.T) {
	empty := common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	db := ethdb.NewMemDatabase()
	if req := NewStateSync(empty, db).Missing(1); len(req) != 0 {
		t.Errorf("content requested for empty state: %v", req)
	}
}
