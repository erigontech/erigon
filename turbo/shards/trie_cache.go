package shards

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
)

// An optional addition to the state cache, helping to calculate state root

// Sizes of B-tree items for the purposes of keeping track of the size of reads and writes
// The sizes of the nodes of the B-tree are not accounted for, because their are private to the `btree` package
const (
	accountHashItemSize = int(unsafe.Sizeof(AccountHashItem{}))
)

type AccountHashItem struct {
	sequence       int
	queuePos       int
	flags          uint16
	hash           common.Hash
	bits           int
	addrHashPrefix []byte
}

func compare_account_accountHash(i1 *AccountItem, i2 *AccountHashItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHashPrefix)
	if c != 0 {
		return c
	}
	if 8*len(i1.addrHash) > i2.bits {
		return 1
	}
	return 0
}

func compare_storage_accountHash(i1 *StorageItem, i2 *AccountHashItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHashPrefix)
	if c != 0 {
		return c
	}
	if 8*len(i1.addrHash) > i2.bits {
		return 1
	}
	return 0
}

func compare_code_accountHash(i1 *CodeItem, i2 *AccountHashItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHashPrefix)
	if c != 0 {
		return c
	}
	if 8*len(i1.addrHash) > i2.bits {
		return 1
	}
	return 0
}

func compare_accountHash_accountHash(i1 *AccountHashItem, i2 *AccountHashItem) int {
	c := bytes.Compare(i1.addrHashPrefix, i2.addrHashPrefix)
	if c != 0 {
		return c
	}
	if i1.bits == i2.bits {
		return 0
	}
	if i1.bits < i2.bits {
		return -1
	}
	return 1
}

func compare_account_storageHash(i1 *AccountItem, i2 *StorageHashItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
}

func compare_storage_storageHash(i1 *StorageItem, i2 *StorageHashItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.incarnation == i2.incarnation {
		c = bytes.Compare(i1.locHash.Bytes(), i2.locHashPrefix)
		if c != 0 {
			return c
		}
		if 8*len(i1.locHash) > i2.bits {
			return 1
		}
		return 0
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func compare_code_storageHash(i1 *CodeItem, i2 *StorageHashItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
}

func compare_accountHash_storageHash(i1 *AccountHashItem, i2 *StorageHashItem) int {
	c := bytes.Compare(i1.addrHashPrefix, i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.bits < 8*len(i2.addrHash) {
		return -1
	}
	return 0
}

func compare_storageHash_storageHash(i1 *StorageHashItem, i2 *StorageHashItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.incarnation == i2.incarnation {
		c = bytes.Compare(i1.locHashPrefix, i2.locHashPrefix)
		if c != 0 {
			return c
		}
		if i1.bits == i2.bits {
			return 0
		}
		if i1.bits < i2.bits {
			return -1
		}
		return 1
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func (ahi *AccountHashItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		// Under equality result is "true" - account hash comes before account
		return compare_account_accountHash(i, ahi) >= 0
	case *StorageItem:
		// Under equality result is "true" - account hash comes before storage items
		return compare_storage_accountHash(i, ahi) >= 0
	case *CodeItem:
		// Under equality result is "true" - account hash comes before account code
		return compare_code_accountHash(i, ahi) >= 0
	case *AccountHashItem:
		return compare_accountHash_accountHash(ahi, i) < 0
	case *StorageHashItem:
		// Under equality result is "true" - account hash comes before storage hashes
		return compare_accountHash_storageHash(ahi, i) < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (ahi *AccountHashItem) GetSequence() int {
	return ahi.sequence
}

func (ahi *AccountHashItem) SetSequence(sequence int) {
	ahi.sequence = sequence
}

func (ahi *AccountHashItem) GetSize() int {
	return accountHashItemSize + len(ahi.addrHashPrefix)
}

func (ahi *AccountHashItem) GetQueuePos() int {
	return ahi.queuePos
}

func (ahi *AccountHashItem) SetQueuePos(pos int) {
	ahi.queuePos = pos
}

func (ahi *AccountHashItem) HasFlag(flag uint16) bool {
	return ahi.flags&flag != 0
}

func (ahi *AccountHashItem) SetFlags(flags uint16) {
	ahi.flags |= flags
}

func (ahi *AccountHashItem) ClearFlags(flags uint16) {
	ahi.flags &^= flags
}

func (ahi *AccountHashItem) String() string {
	return fmt.Sprintf("AccountHashItem(addrHashPrefix=%x,bits=%d)", ahi.addrHashPrefix, ahi.bits)
}

func (ahi *AccountHashItem) CopyValueFrom(item CacheItem) {
	otherAhi, ok := item.(*AccountHashItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountHashItem, got %T", item))
	}
	copy(ahi.hash[:], otherAhi.hash.Bytes())
}

type StorageHashItem struct {
	sequence      int
	queuePos      int
	flags         uint16
	addrHash      common.Hash
	incarnation   uint64
	hash          common.Hash
	bits          int
	locHashPrefix []byte
}

func (shi *StorageHashItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		// Under equality result is "false" - storage hash comes after account
		return compare_account_storageHash(i, shi) > 0
	case *StorageItem:
		// Under equality result is "true" - storage hash comes before storage items
		return compare_storage_storageHash(i, shi) >= 0
	case *CodeItem:
		// Under equality result is "false" - storage hash comes after account code
		return compare_code_storageHash(i, shi) > 0
	case *AccountHashItem:
		// Under equility result is "false" - storage hashes comes after account hash
		return compare_accountHash_storageHash(i, shi) > 0
	case *StorageHashItem:
		return compare_storageHash_storageHash(shi, i) < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

// UnprocessedHeap is a priority queue of items that were modified after the last recalculation of the merkle tree
type UnprocessedHeap struct {
	items []CacheItem
}

func (uh UnprocessedHeap) Len() int {
	return len(uh.items)
}

func (uh UnprocessedHeap) Less(i, j int) bool {
	return uh.items[i].Less(uh.items[j])
}

func (uh UnprocessedHeap) Swap(i, j int) {
	uh.items[i], uh.items[j] = uh.items[j], uh.items[i]
}

func (uh *UnprocessedHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	uh.items = append(uh.items, x.(CacheItem))
}

func (uh *UnprocessedHeap) Pop() interface{} {
	cacheItem := uh.items[len(uh.items)-1]
	uh.items = uh.items[:len(uh.items)-1]
	return cacheItem
}
