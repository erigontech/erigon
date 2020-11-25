package shards

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
)

// An optional addition to the state cache, helping to calculate state root

type AccountHashItem struct {
	sequence       int
	queuePos       int
	flags          uint16
	bits           int
	addrHashPrefix []byte
}

type StorageHashItem struct {
	sequence      int
	queuePos      int
	flags         uint16
	addrHash      common.Hash
	incarnation   uint64
	bits          int
	locHashPrefix []byte
}

func bytesandmask(bits int) (bytes int, mask byte) {
	wholeBytes = (fixedbits+7)/8 - 1
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return wholeBytes, mask
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

func (ahi *AccountHashItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		// Under equality result is "true" - account hash comes before account
		return compare_account_accountHash(i, ahi) >= 0
	case *StorageItem:
		// Under equality result is "true" - account hash comes before storage items
		return compare_storage_accountHash(i, ahi) >= 0
	case *CodeItem:
		// Under equalirt result is "true" - account hash comes before account code
		return compare_code_accountHash(i, ahi) >= 0
	case *AccountHashItem:
		return compare_accountHash_accountHash(ahi, i) < 0
	}
}

func (ai *AccountItem) GetSequence() int {
	return ai.sequence
}

func (ai *AccountItem) SetSequence(sequence int) {
	ai.sequence = sequence
}

func (ai *AccountItem) GetSize() int {
	return accountItemSize
}

func (ai *AccountItem) GetQueuePos() int {
	return ai.queuePos
}

func (ai *AccountItem) SetQueuePos(pos int) {
	ai.queuePos = pos
}

func (ai *AccountItem) HasFlag(flag uint16) bool {
	return ai.flags&flag != 0
}

func (ai *AccountItem) SetFlags(flags uint16) {
	ai.flags |= flags
}

func (ai *AccountItem) ClearFlags(flags uint16) {
	ai.flags &^= flags
}

func (ai *AccountItem) String() string {
	return fmt.Sprintf("AccountItem(addrHash=%x)", ai.addrHash)
}

func (ai *AccountItem) CopyValueFrom(item CacheItem) {
	otherAi, ok := item.(*AccountItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountItem, got %T", item))
	}
	ai.account.Copy(&otherAi.account)
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
