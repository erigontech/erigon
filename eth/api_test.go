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

package eth

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"testing"

	"context"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var dumper = spew.ConfigState{Indent: "    "}

func accountRangeTest(t *testing.T, statedb *state.DbState, start *common.Hash, requestedNum int, expectedNum int) AccountRangeResult {
	result, err := AccountRange(statedb, start, requestedNum)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Accounts) != expectedNum {
		t.Fatalf("expected %d results.  Got %d", expectedNum, len(result.Accounts))
	}

	state := state.New(statedb)

	for _, address := range result.Accounts {
		if address == nil {
			t.Fatalf("null address returned")
		}
		if !state.Exist(*address) {
			t.Fatalf("account not found in state %s", address.Hex())
		}
	}

	return result
}

type resultHash []*common.Hash

func (h resultHash) Len() int           { return len(h) }
func (h resultHash) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h resultHash) Less(i, j int) bool { return bytes.Compare(h[i].Bytes(), h[j].Bytes()) < 0 }

func TestAccountRange(t *testing.T) {
	var (
		db      = ethdb.NewMemDatabase()
		tds, _  = state.NewTrieDbState(common.Hash{}, db, 0)
		statedb = state.NewDbState(db, 0)
		state   = state.New(tds)
		addrs   = [AccountRangeMaxResults * 2]common.Address{}
		m       = map[common.Address]bool{}
	)

	for i := range addrs {
		hash := common.HexToHash(fmt.Sprintf("%x", i))
		addr := common.BytesToAddress(crypto.Keccak256Hash(hash.Bytes()).Bytes())
		addrs[i] = addr
		state.SetBalance(addrs[i], big.NewInt(1))
		if _, ok := m[addr]; ok {
			t.Fatalf("bad")
		} else {
			m[addr] = true
		}
	}
	state.CommitBlock(context.Background(), tds.DbStateWriter())

	t.Logf("test getting number of results less than max")
	accountRangeTest(t, statedb, &common.Hash{0x0}, AccountRangeMaxResults/2, AccountRangeMaxResults/2)

	t.Logf("test getting number of results greater than max %d", AccountRangeMaxResults)
	accountRangeTest(t, statedb, &common.Hash{0x0}, AccountRangeMaxResults*2, AccountRangeMaxResults)

	t.Logf("test with empty 'start' hash")
	accountRangeTest(t, statedb, nil, AccountRangeMaxResults, AccountRangeMaxResults)

	t.Logf("test pagination")

	// test pagination
	firstResult := accountRangeTest(t, statedb, &common.Hash{0x0}, AccountRangeMaxResults, AccountRangeMaxResults)

	t.Logf("test pagination 2")
	secondResult := accountRangeTest(t, statedb, &firstResult.Next, AccountRangeMaxResults, AccountRangeMaxResults)

	hList := make(resultHash, 0)
	for h1, addr1 := range firstResult.Accounts {
		h := &common.Hash{}
		h.SetBytes(h1.Bytes())
		hList = append(hList, h)
		for h2, addr2 := range secondResult.Accounts {
			// Make sure that the hashes aren't the same
			if bytes.Equal(h1.Bytes(), h2.Bytes()) {
				t.Fatalf("pagination test failed:  results should not overlap")
			}

			// If either address is nil, then it makes no sense to compare
			// them as they might be two different accounts.
			if addr1 == nil || addr2 == nil {
				continue
			}

			// Since the two hashes are different, they should not have
			// the same preimage, but let's check anyway in case there
			// is a bug in the (hash, addr) map generation code.
			if bytes.Equal(addr1.Bytes(), addr2.Bytes()) {
				t.Fatalf("pagination test failed: addresses should not repeat")
			}
		}
	}

	// Test to see if it's possible to recover from the middle of the previous
	// set and get an even split between the first and second sets.
	t.Logf("test random access pagination")
	sort.Sort(hList)
	middleH := hList[AccountRangeMaxResults/2]
	middleResult := accountRangeTest(t, statedb, middleH, AccountRangeMaxResults, AccountRangeMaxResults)
	innone, infirst, insecond := 0, 0, 0
	for h := range middleResult.Accounts {
		if _, ok := firstResult.Accounts[h]; ok {
			infirst++
		} else if _, ok := secondResult.Accounts[h]; ok {
			insecond++
		} else {
			innone++
		}
	}
	if innone != 0 {
		t.Fatalf("%d hashes in the 'middle' set were neither in the first not the second set", innone)
	}
	if infirst != AccountRangeMaxResults/2 {
		t.Fatalf("Imbalance in the number of first-test results: %d != %d", infirst, AccountRangeMaxResults/2)
	}
	if insecond != AccountRangeMaxResults/2 {
		t.Fatalf("Imbalance in the number of second-test results: %d != %d", insecond, AccountRangeMaxResults/2)
	}
}

func TestEmptyAccountRange(t *testing.T) {
	var (
		statedb = state.NewDbState(ethdb.NewMemDatabase(), 0)
	)

	results, err := AccountRange(statedb, &common.Hash{0x0}, AccountRangeMaxResults)
	if err != nil {
		t.Fatalf("Empty results should not trigger an error: %v", err)
	}
	if results.Next != common.HexToHash("0") {
		t.Fatalf("Empty results should not return a second page")
	}
	if len(results.Accounts) != 0 {
		t.Fatalf("Empty state should not return addresses: %v", results.Accounts)
	}
}

func TestStorageRangeAt(t *testing.T) {
	// Create a state where account 0x010000... has a few storage entries.
	var (
		db      = ethdb.NewMemDatabase()
		tds, _  = state.NewTrieDbState(common.Hash{}, db, 0)
		statedb = state.New(tds)
		addr    = common.Address{0x01}
		keys    = []common.Hash{ // hashes of Keys of storage
			common.HexToHash("340dd630ad21bf010b4e676dbfa9ba9a02175262d1fa356232cfde6cb5b47ef2"),
			common.HexToHash("426fcb404ab2d5d8e61a3d918108006bbb0a9be65e92235bb10eefbdb6dcd053"),
			common.HexToHash("48078cfed56339ea54962e72c37c7f588fc4f8e5bc173827ba75cb10a63a96a5"),
			common.HexToHash("5723d2c3a83af9b735e3b7f21531e5623d183a9095a56604ead41f3582fdfb75"),
		}
		storage = StorageMap{
			keys[0]: {Key: &common.Hash{0x02}, Value: common.Hash{0x01}},
			keys[1]: {Key: &common.Hash{0x04}, Value: common.Hash{0x02}},
			keys[2]: {Key: &common.Hash{0x01}, Value: common.Hash{0x03}},
			keys[3]: {Key: &common.Hash{0x03}, Value: common.Hash{0x04}},
		}
	)
	tds.StartNewBuffer()
	for _, entry := range storage {
		statedb.SetState(addr, *entry.Key, entry.Value)
	}

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
		want  StorageRangeResult
	}{
		{
			start: []byte{}, limit: 0,
			want: StorageRangeResult{StorageMap{}, &keys[0]},
		},
		{
			start: []byte{}, limit: 100,
			want: StorageRangeResult{storage, nil},
		},
		{
			start: []byte{}, limit: 2,
			want: StorageRangeResult{StorageMap{keys[0]: storage[keys[0]], keys[1]: storage[keys[1]]}, &keys[2]},
		},
		{
			start: []byte{0x00}, limit: 4,
			want: StorageRangeResult{storage, nil},
		},
		{
			start: []byte{0x40}, limit: 2,
			want: StorageRangeResult{StorageMap{keys[1]: storage[keys[1]], keys[2]: storage[keys[2]]}, &keys[3]},
		},
	}
	dbs := state.NewDbState(db, 1)
	for _, test := range tests {
		result, err := StorageRangeAt(dbs, addr, test.start, test.limit)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(result, test.want) {
			t.Fatalf("wrong result for range 0x%x.., limit %d:\ngot %s\nwant %s",
				test.start, test.limit, dumper.Sdump(result), dumper.Sdump(&test.want))
		}
	}
}
