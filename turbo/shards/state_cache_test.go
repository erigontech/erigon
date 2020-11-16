package shards

import (
	"testing"

	"github.com/google/btree"
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
