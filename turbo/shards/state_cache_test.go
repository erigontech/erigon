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
	sc := NewStateCache(32, 4)
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountRead(addr1.Bytes(), &account1)
	if _, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	var addr2 common.Address
	addr2[0] = 2
	if _, ok := sc.GetAccount(addr2.Bytes()); ok {
		t.Fatalf("Did not expect account with addr2")
	}
	var addr3 common.Address
	addr3[0] = 3
	sc.SetAccountAbsent(addr3.Bytes())
	if _, ok := sc.GetAccount(addr3.Bytes()); ok {
		t.Fatalf("Expected account with addr3 to be absent")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr common.Address
		addr[0] = byte(i)
		sc.SetAccountRead(addr.Bytes(), &account)
	}
	// Out of 6 addresses, one was not associated with an account or absence record. So 5 records would be in the cache
	// But since the limit is 4, the first addr will be evicted
	if _, ok := sc.GetAccount(addr1.Bytes()); ok {
		t.Fatalf("Expected addr1 to be evicted")
	}
	for i := 4; i <= 6; i++ {
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); !ok {
			t.Fatalf("Expected to find account with addr %x", addr)
		}
	}
}

func TestAccountReadWrites(t *testing.T) {
	sc := NewStateCache(32, 9)
	var account1 accounts.Account
	account1.Balance.SetUint64(1)
	var addr1 common.Address
	addr1[0] = 1
	sc.SetAccountWrite(addr1.Bytes(), &account1)
	if _, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	}
	if sc.writes.Len() != 1 {
		t.Fatalf("Write queue is expected to have 1 elements, got: %d", sc.writes.Len())
	}
	// Replace the existing value
	var account11 accounts.Account
	account11.Balance.SetUint64(11)
	sc.SetAccountWrite(addr1.Bytes(), &account11)
	if a, ok := sc.GetAccount(addr1.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr1")
	} else {
		if a.Balance.Uint64() != 11 {
			t.Fatalf("Expected account balance 11, got %d", a.Balance.Uint64())
		}
	}
	if sc.writes.Len() != 1 {
		t.Fatalf("Write queue is expected to have 1 elements, got: %d", sc.writes.Len())
	}
	// Add read and then replace it with the write
	var account2 accounts.Account
	account2.Balance.SetUint64(2)
	var addr2 common.Address
	addr2[0] = 2
	sc.SetAccountRead(addr2.Bytes(), &account2)
	// Check that readQueue is empty
	if sc.readQueue.Len() != 1 {
		t.Fatalf("Read queue is expected to be 1 element")
	}
	var account22 accounts.Account
	account22.Balance.SetUint64(22)
	sc.SetAccountWrite(addr2.Bytes(), &account22)
	if a, ok := sc.GetAccount(addr2.Bytes()); !ok {
		t.Fatalf("Expected to find account with addr2")
	} else {
		if a.Balance.Uint64() != 22 {
			t.Fatalf("Expected account balance 22, got %d", a.Balance.Uint64())
		}
	}
	if sc.writes.Len() != 2 {
		t.Fatalf("Write queue is expected to have 2 elements, got: %d", sc.writes.Len())
	}
	// Check that readQueue is empty
	if sc.readQueue.Len() != 0 {
		t.Fatalf("Read queue is expected to be empty")
	}
	// Deleting written account
	var account3 accounts.Account
	account3.Balance.SetUint64(3)
	var addr3 common.Address
	addr3[0] = 3
	sc.SetAccountWrite(addr3.Bytes(), &account3)
	sc.SetAccountDelete(addr3.Bytes())
	if _, ok := sc.GetAccount(addr3.Bytes()); ok {
		t.Fatalf("Expected account addr3 to be deleted")
	}
	if sc.writes.Len() != 3 {
		t.Fatalf("Write queue is expected to have 3 elements, got: %d", sc.writes.Len())
	}
	// Deleting read account
	var account4 accounts.Account
	account4.Balance.SetUint64(4)
	var addr4 common.Address
	addr4[0] = 4
	sc.SetAccountRead(addr4.Bytes(), &account4)
	sc.SetAccountDelete(addr4.Bytes())
	if _, ok := sc.GetAccount(addr4.Bytes()); ok {
		t.Fatalf("Expected account addr4 to be deleted")
	}
	if sc.writes.Len() != 4 {
		t.Fatalf("Write queue is expected to have 4 elements, got: %d", sc.writes.Len())
	}
	// Check that readQueue is empty
	if sc.readQueue.Len() != 0 {
		t.Fatalf("Read queue is expected to be empty")
	}
	// Deleting account not seen before
	var addr5 common.Address
	addr5[0] = 5
	sc.SetAccountDelete(addr5.Bytes())
	if _, ok := sc.GetAccount(addr5.Bytes()); ok {
		t.Fatalf("Expected account addr5 to be deleted")
	}
	if sc.writes.Len() != 5 {
		t.Fatalf("Write queue is expected to have 5 elements, got: %d", sc.writes.Len())
	}
}

func TestReplaceAccountReadsWithWrites(t *testing.T) {
	sc := NewStateCache(32, 6)
	for i := 1; i <= 4; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr.Bytes(), &account)
	}
	sc.TurnWritesToReads()
	if sc.writes.Len() != 0 {
		t.Fatalf("Write queue is expected to be empty, got: %d", sc.writes.Len())
	}
	if sc.readQueue.Len() != 4 {
		t.Fatalf("Read queue is expected to have 4 elements, got: %d", sc.readQueue.Len())
	}
	// Do 4 more delets
	for i := 5; i <= 8; i++ {
		var addr common.Address
		addr[0] = byte(i)
		var account accounts.Account
		account.Balance.SetUint64(uint64(i))
		sc.SetAccountWrite(addr.Bytes(), &account)
	}
	if sc.writes.Len() != 4 {
		t.Fatalf("Write queue is expected to have 4 elements, got: %d", sc.writes.Len())
	}
	if sc.readQueue.Len() != 2 {
		t.Fatalf("Read queue is expected to have 2 elements, got: %d", sc.readQueue.Len())
	}
	// Check that the first two address are evicted
	for i := 1; i <= 2; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); ok {
			t.Fatalf("Expected not to find address %d", i)
		}
	}
	// Check that the other 6 addresses are there
	for i := 3; i <= 8; i++ {
		var addr common.Address
		addr[0] = byte(i)
		if _, ok := sc.GetAccount(addr.Bytes()); !ok {
			t.Errorf("Expected to find address %d", i)
		}
	}
}
