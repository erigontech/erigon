package shards

import (
	"testing"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func TestCacheBtreeOrderAccounts(t *testing.T) {
	bt := btree.New(32)
	var aci1, aci2 AccountCacheItem
	aci1.address[0] = 1
	aci2.address[0] = 2
	bt.ReplaceOrInsert(&aci1)
	bt.ReplaceOrInsert(&aci2)
	// Specify the expected ordering
	aci1.sequence = 0
	aci2.sequence = 1
	sequence := 0
	bt.Ascend(func(item btree.Item) bool {
		if item.(CacheItem).GetSequence() != sequence {
			t.Errorf("wrong ordering: expected sequence %d, got %d", sequence, item.(CacheItem).GetSequence())
		}
		sequence++
		return true
	})
}

func TestCacheBtreeOrderAccountStorage(t *testing.T) {
	bt := btree.New(32)
	var aci1, aci2 AccountCacheItem
	aci1.address[0] = 1
	aci2.address[0] = 2
	bt.ReplaceOrInsert(&aci1)
	bt.ReplaceOrInsert(&aci2)
	var sci1, sci2, sci3 StorageCacheItem
	sci1.address[0] = 1
	sci1.location[0] = 1
	sci2.address[0] = 1
	sci2.location[0] = 2
	sci3.address[0] = 3
	sci3.location[0] = 42
	bt.ReplaceOrInsert(&aci1)
	bt.ReplaceOrInsert(&aci2)
	bt.ReplaceOrInsert(&sci1)
	bt.ReplaceOrInsert(&sci2)
	bt.ReplaceOrInsert(&sci3)
	// Specify the expected ordering
	aci1.sequence = 0
	sci1.sequence = 1
	sci2.sequence = 2
	aci2.sequence = 3
	sci3.sequence = 4
	sequence := 0
	bt.Ascend(func(item btree.Item) bool {
		if item.(CacheItem).GetSequence() != sequence {
			t.Errorf("wrong ordering: expected sequence %d, got %d", sequence, item.(CacheItem).GetSequence())
		}
		sequence++
		return true
	})
}

func TestCacheBtreeAll(t *testing.T) {
	bt := btree.New(32)
	var aci1, aci2 AccountCacheItem
	aci1.address[0] = 1
	aci2.address[0] = 2
	bt.ReplaceOrInsert(&aci1)
	bt.ReplaceOrInsert(&aci2)
	var sci1, sci2, sci3 StorageCacheItem
	sci1.address[0] = 1
	sci1.location[0] = 1
	sci2.address[0] = 1
	sci2.location[0] = 2
	sci3.address[0] = 3
	sci3.location[0] = 42
	var cci1, cci2 CodeCacheItem
	cci1.address[0] = 1
	cci2.address[0] = 3
	bt.ReplaceOrInsert(&aci1)
	bt.ReplaceOrInsert(&aci2)
	bt.ReplaceOrInsert(&sci1)
	bt.ReplaceOrInsert(&sci2)
	bt.ReplaceOrInsert(&sci3)
	bt.ReplaceOrInsert(&cci1)
	bt.ReplaceOrInsert(&cci2)
	// Specify the expected ordering
	aci1.sequence = 0
	cci1.sequence = 1
	sci1.sequence = 2
	sci2.sequence = 3
	aci2.sequence = 4
	cci2.sequence = 5
	sci3.sequence = 6
	sequence := 0
	bt.Ascend(func(item btree.Item) bool {
		if item.(CacheItem).GetSequence() != sequence {
			t.Errorf("wrong ordering: expected sequence %d, got %d", sequence, item.(CacheItem).GetSequence())
		}
		sequence++
		return true
	})
}

func TestAccountReads(t *testing.T) {
	sc := NewStateCache(32, 4, 4)
	var account1 accounts.Account
	account1.Balance.SetUint64(2)
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1.Bytes(), &account1)
	if _, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	var addr2 common.Address
	if _, ok := sc.GetAccount(addr2.Bytes()); ok {
		t.Fatalf("Did not expect account with addr2")
	}
	var addr3 common.Address
	sc.SetAccountAbsent(addr3.Bytes())
	if _, ok := sc.GetAccount(addr3.Bytes()); ok {
		t.Fatalf("Expected account with addr3 to be absent")
	}
}
