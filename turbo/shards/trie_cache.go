package shards

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
)

// An optional addition to the state cache, helping to calculate state root


func bytesandmask(bits int) (bytes int, mask byte) {
	wholeBytes = (fixedbits+7)/8 - 1
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return wholeBytes, mask
}

type AccountHashItem struct {
	sequence       int
	queuePos       int
	flags          uint16
	hash 		    common.Hash
	bits           int
	addrHashPrefix []byte
}

func compare_account_accountHash(i1 *AccountItem, i2 *AccountHashItem) int {
	wholeBytes, mask := bytesandmask(i2.bits)
	c := bytes.Compare(i1.addrHash[:wholeBytes], i2.addrHashPrefix[:wholeBytes])
	if c != 0 {
		return c
	}
	m1 := i1.addrHash[wholeBytes] & mask
	m2 := i2.addrHashPrefix[wholeBytes] & mask
	if m1 == m2 {
		return 0
	}
	if m1 < m2 {
		return -1
	}
	return 1
}

func compare_storage_accountHash(i1 *StorageItem, i2 *AccountHashItem) int {
	wholeBytes, mask := bytesandmask(i2.bits)
	c : = bytes.Compare(i1.addrHash[:wholeBytes], i2.addrHashPrefix[:wholeBytes])
	if c != 0 {
		return c
	}
	m1 := i1.addrHash[wholeBytes] & mask
	m2 := i2.addrHashPrefix[wholeBytes] & mask
	if m1 == m2 {
		return 0
	}
	if m1 < m2 {
		return -1
	}
	return 1
}

func compare_code_accountHash(i1 *CodeItem, i2 *AccountHashItem) int {
	wholeBytes, mask := bytesandmask(i2.bits)
	c := bytes.Compare(i1.addrHash[:wholeBytes], i2.addrHashPrefix[:wholeBytes])
	if c != 0 {
		return c
	}
	m1 := i1.addrHash[wholeBytes] & mask
	m2 := i2.addrHashPrefix[wholeBytes] & mask
	if m1 == m2 {
		return 0
	}
	if m1 < m2 {
		return -1
	}
	return 1	
}

func compare_accountHash_accountHash(i1 *AccountHashItem, i2 *AccountHashItem) int {
	var minBits int
	if i1.bits < i2.bits {
		minBits = i1.bits
	} else {
		minBits = i2.bits
	}
	wholeBytes, mask := bytesandmask(minBits)
	c : = bytes.Compare(i1.addrHashPrefix[:wholeBytes], i2.addrHashPrefix[:wholeBytes])
	if c != 0 {
		return c
	}
	m1 := i1.addrHashPrefix[wholeBytes]&mask
	m2 := i2.addrHashPrefix[wholeBytes]&mask
	if m1 < m2 {
		return -1
	}
	if m1 > m2 {
		return 1
	}
	if i1.bits == i2.bits {
		return 0
	}
	if i1.bits < i2.bits {
		return -1
	}
	return 1
}

func compare_accountHash_storageHash(i1 *AccountHashItem, i2 *StorageHashItem) {
	wholeBytes, mask := bytesandmask(i1.bits)
	c := bytes.Compare(i1.addrHashPrefix[:wholeBytes], i2.addrHash[:wholeBytes])
	if c != 0 {
		return c
	}
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
		wholeBytes, mask := bytesandmask(i2.bits)
		c = bytes.Compare(i1.locHash[:wholeBytes], i2.locHasPrefix[:wholeBytes])
		if c != 0 {
			return c
		}
		m1 := i1.locHash[wholeBytes] & mask
		m2 := i2.locaHashPrefix[wholeBytes] & mask
		if m1 == m2 {
			return 0
		}
		if m1 < m2 {
			return -1
		}
		return 1
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func compare_code_storageHash(i1 *CodeItem, i2 *StorageHashItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
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
	return accountHashItemSize
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
	return fmt.Sprintf("AccountHashItem(addrHash=%x)", ahi.addrHash)
}

func (ahi *AccountHashItem) CopyValueFrom(item CacheItem) {
	otherAhi, ok := item.(*AccountHashItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountHashItem, got %T", item))
	}
	copy(ahi.hash[:], otherAhi.Bytes())
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
	cacheItem := rh.items[len(uh.items)-1]
	uh.items = uh.items[:len(uh.items)-1]
	return cacheItem
}
