// Copyright 2017 The go-ethereum Authors
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

package eth_test

/*
TODO: revive this tests for RPCDaemon - https://github.com/ledgerwatch/erigon/issues/939

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var dumper = spew.ConfigState{Indent: "    "}

func accountRangeTest(t *testing.T, trie *trie.Trie, db ethdb.RwKV, blockNumber uint64, sdb *state.IntraBlockState, start []byte, requestedNum int, expectedNum int) state.IteratorDump { //nolint: unparam
	result, err := state.NewDumper(db, blockNumber).IteratorDump(true, true, false, start, requestedNum)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Accounts) != expectedNum {
		t.Fatalf("expected %d results, got %d", expectedNum, len(result.Accounts))
	}
	for address := range result.Accounts {
		if address == (libcommon.Address{}) {
			t.Fatalf("empty address returned")
		}
		if !sdb.Exist(address) {
			t.Fatalf("account not found in state %s", address.Hex())
		}
	}
	return result
}

type resultHash [][]byte

func (h resultHash) Len() int           { return len(h) }
func (h resultHash) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h resultHash) Less(i, j int) bool { return bytes.Compare(h[i], h[j]) < 0 }

func TestAccountRange(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		tds   = state.NewTrieDbState(libcommon.Hash{}, db, 0)
		sdb   = state.New(tds)
		addrs = [eth.AccountRangeMaxResults * 2]libcommon.Address{}
		m     = map[libcommon.Address]bool{}
	)

	for i := range addrs {
		hash := libcommon.HexToHash(fmt.Sprintf("%x", i))
		addr := libcommon.BytesToAddress(crypto.Keccak256Hash(hash.Bytes()).Bytes())
		addrs[i] = addr
		sdb.SetBalance(addrs[i], u256.Num1)
		if _, ok := m[addr]; ok {
			t.Fatalf("bad")
		} else {
			m[addr] = true
		}
	}
	tds.StartNewBuffer()
	if err := sdb.CommitBlock(context.Background(), tds.PlainStateWriter()); err != nil {
		t.Fatal(err)
	}

	_, err := tds.ComputeTrieRoots()
	if err != nil {
		t.Fatal(err)
	}

	trie := tds.Trie()

	accountRangeTest(t, trie, db.RwKV(), 0, sdb, []byte{}, eth.AccountRangeMaxResults/2, eth.AccountRangeMaxResults/2)
	// test pagination
	firstResult := accountRangeTest(t, trie, db.RwKV(), 0, sdb, []byte{}, eth.AccountRangeMaxResults, eth.AccountRangeMaxResults)
	secondResult := accountRangeTest(t, trie, db.RwKV(), 0, sdb, firstResult.Next, eth.AccountRangeMaxResults, eth.AccountRangeMaxResults)

	hList := make(resultHash, 0)
	for addr1 := range firstResult.Accounts {
		// If address is empty, then it makes no sense to compare
		// them as they might be two different accounts.
		if addr1 == (libcommon.Address{}) {
			continue
		}
		if _, duplicate := secondResult.Accounts[addr1]; duplicate {
			t.Fatal("pagination test failed:  results should not overlap")
		}
		hList = append(hList, addr1.Bytes())
	}
	// Test to see if it's possible to recover from the middle of the previous
	// set and get an even split between the first and second sets.
	sort.Sort(hList)
	middleH := hList[eth.AccountRangeMaxResults/2]
	middleResult := accountRangeTest(t, trie, db.RwKV(), 0, sdb, middleH, eth.AccountRangeMaxResults, eth.AccountRangeMaxResults)
	missing, infirst, insecond := 0, 0, 0
	for h := range middleResult.Accounts {
		if _, ok := firstResult.Accounts[h]; ok {
			infirst++
		} else if _, ok := secondResult.Accounts[h]; ok {
			insecond++
		} else {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("%d hashes in the 'middle' set were neither in the first not the second set", missing)
	}
	if infirst != eth.AccountRangeMaxResults/2 {
		t.Fatalf("Imbalance in the number of first-test results: %d != %d", infirst, eth.AccountRangeMaxResults/2)
	}
	if insecond != eth.AccountRangeMaxResults/2 {
		t.Fatalf("Imbalance in the number of second-test results: %d != %d", insecond, eth.AccountRangeMaxResults/2)
	}
}

func TestEmptyAccountRange(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		tds = state.NewTrieDbState(libcommon.Hash{}, db, 1)
	)
	tds.StartNewBuffer()
	_, err := tds.ComputeTrieRoots()
	if err != nil {
		t.Error(err)
	}
	results, err1 := state.NewDumper(db.RwKV(), 0).IteratorDump(true, true, true, (libcommon.Hash{}).Bytes(), eth.AccountRangeMaxResults)
	if err1 != nil {
		t.Fatal(err1)
	}
	if bytes.Equal(results.Next, (libcommon.Hash{}).Bytes()) {
		t.Fatalf("Empty results should not return a second page")
	}
	if len(results.Accounts) != 0 {
		t.Fatalf("Empty state should not return addresses: %v", results.Accounts)
	}
}

func TestStorageRangeAt(t *testing.T) {
	t.Parallel()

	// Create a state where account 0x010000... has a few storage entries.
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		tds     = state.NewTrieDbState(libcommon.Hash{}, db, 0)
		statedb = state.New(tds)
		addr    = libcommon.Address{0x01}
		keys    = []libcommon.Hash{ // hashes of Keys of storage
			libcommon.HexToHash("340dd630ad21bf010b4e676dbfa9ba9a02175262d1fa356232cfde6cb5b47ef2"),
			libcommon.HexToHash("426fcb404ab2d5d8e61a3d918108006bbb0a9be65e92235bb10eefbdb6dcd053"),
			libcommon.HexToHash("48078cfed56339ea54962e72c37c7f588fc4f8e5bc173827ba75cb10a63a96a5"),
			libcommon.HexToHash("5723d2c3a83af9b735e3b7f21531e5623d183a9095a56604ead41f3582fdfb75"),
		}
		storage = commands.StorageMap{
			keys[0]: {Key: &libcommon.Hash{0x02}, Value: libcommon.Hash{0x01}},
			keys[1]: {Key: &libcommon.Hash{0x04}, Value: libcommon.Hash{0x02}},
			keys[2]: {Key: &libcommon.Hash{0x01}, Value: libcommon.Hash{0x03}},
			keys[3]: {Key: &libcommon.Hash{0x03}, Value: libcommon.Hash{0x04}},
		}
	)
	tds.StartNewBuffer()

	for _, entry := range storage {
		val := uint256.NewInt(0).SetBytes(entry.Value.Bytes())
		statedb.SetState(addr, entry.Key, *val)
	}
	//we are working with contract, so it need codehash&incarnation
	statedb.SetIncarnation(addr, state.FirstContractIncarnation)

	err := statedb.FinalizeTx(context.Background(), tds.TrieStateWriter())
	if err != nil {
		t.Fatal("error while finalising state", err)
	}

	_, err = tds.ComputeTrieRoots()
	if err != nil {
		t.Fatal("error while computing trie roots of the state", err)
	}

	tds.SetBlockNr(1)

	err = statedb.CommitBlock(context.Background(), tds.DbStateWriter())
	if err != nil {
		t.Fatal("error while committing state", err)
	}

	// Check a few combinations of limit and start/end.
	tests := []struct {
		start []byte
		limit int
		want  commands.StorageRangeResult
	}{
		{
			start: []byte{}, limit: 0,
			want: commands.StorageRangeResult{commands.StorageMap{}, &keys[0]},
		},
		{
			start: []byte{}, limit: 100,
			want: commands.StorageRangeResult{storage, nil},
		},
		{
			start: []byte{}, limit: 2,
			want: commands.StorageRangeResult{commands.StorageMap{keys[0]: storage[keys[0]], keys[1]: storage[keys[1]]}, &keys[2]},
		},
		{
			start: []byte{0x00}, limit: 4,
			want: commands.StorageRangeResult{storage, nil},
		},
		{
			start: []byte{0x40}, limit: 2,
			want: commands.StorageRangeResult{commands.StorageMap{keys[1]: storage[keys[1]], keys[2]: storage[keys[2]]}, &keys[3]},
		},
	}

	dbs := adapter.NewStateReader(db.RwKV(), 1)
	for i, test := range tests {
		test := test
		t.Run("test_"+strconv.Itoa(i), func(t *testing.T) {
			result, err := commands.StorageRangeAt(dbs, addr, test.start, test.limit)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(result, test.want) {
				t.Fatalf("wrong result for range 0x%x.., limit %d:\ngot %s\nwant %s",
					test.start, test.limit, dumper.Sdump(result), dumper.Sdump(&test.want))
			}
		})
	}
}
*/
