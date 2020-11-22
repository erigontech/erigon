package shards

import (
	"bytes"
	"container/heap"
	"fmt"
	"unsafe"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

// LRU state cache consists of two structures - B-Tree and binary heap
// Every element is marked either as Read, Updated, or Deleted via flags

const (
	ModifiedFlag uint16 = 1
	DeletedFlag  uint16 = 2
)

const (
	accountItemSize      = int(unsafe.Sizeof(AccountItem{}))
	accountWriteItemSize = int(unsafe.Sizeof(AccountWriteItem{})) + accountItemSize
	storageItemSize      = int(unsafe.Sizeof(StorageItem{}))
	storageWriteItemSize = int(unsafe.Sizeof(StorageWriteItem{})) + storageItemSize
	codeItemSize         = int(unsafe.Sizeof(CodeItem{}))
	codeWriteItemSize    = int(unsafe.Sizeof(CodeWriteItem{})) + codeItemSize
)

//nolint:maligned
type AccountItem struct {
	addrHash common.Hash
	account  accounts.Account
	sequence int
	queuePos int
	flags    uint16
}

type AccountWriteItem struct {
	address common.Address
	ai      *AccountItem
}

//nolint:maligned
type StorageItem struct {
	addrHash    common.Hash
	incarnation uint64
	locHash     common.Hash
	value       uint256.Int
	sequence    int
	queuePos    int
	flags       uint16
}

type StorageWriteItem struct {
	address  common.Address
	location common.Hash
	si       *StorageItem
}

//nolint:maligned
type CodeItem struct {
	addrHash    common.Hash
	incarnation uint64
	code        []byte
	sequence    int
	queuePos    int
	flags       uint16
}

type CodeWriteItem struct {
	address common.Address
	ci      *CodeItem
}

type CacheItem interface {
	btree.Item
	GetSequence() int
	SetSequence(sequence int)
	GetSize() int
	GetQueuePos() int
	SetQueuePos(pos int)
	HasFlag(flag uint16) bool     // Check if specified flag is set
	SetFlags(flags uint16)        // Set specified flags, but leaves other flags alone
	ClearFlags(flags uint16)      // Clear specified flags, but laves other flags alone
	CopyValueFrom(item CacheItem) // Copy value (not key) from given item
}

type CacheWriteItem interface {
	btree.Item
	GetCacheItem() CacheItem
	SetCacheItem(item CacheItem)
	GetSize() int
}

func (ai *AccountItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return bytes.Compare(ai.addrHash.Bytes(), i.addrHash.Bytes()) < 0
	case *StorageItem:
		c := bytes.Compare(ai.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			// Account comes before its storage items
			return true
		}
		return c < 0
	case *CodeItem:
		c := bytes.Compare(ai.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			// Account comes before its code
			return true
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (awi *AccountWriteItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountWriteItem:
		return bytes.Compare(awi.address.Bytes(), i.address.Bytes()) < 0
	case *StorageWriteItem:
		c := bytes.Compare(awi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			// Account comes before its storage items
			return true
		}
		return c < 0
	case *CodeWriteItem:
		c := bytes.Compare(awi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			// Account comes before its code
			return true
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (awi *AccountWriteItem) GetCacheItem() CacheItem {
	return awi.ai
}

func (awi *AccountWriteItem) SetCacheItem(item CacheItem) {
	awi.ai = item.(*AccountItem)
}

func (awi *AccountWriteItem) GetSize() int {
	return accountWriteItemSize
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

func (si *StorageItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		c := bytes.Compare(si.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			// Account comes before its storage items
			return false
		}
		return c < 0
	case *StorageItem:
		c := bytes.Compare(si.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			if si.incarnation == i.incarnation {
				return bytes.Compare(si.locHash.Bytes(), i.locHash.Bytes()) < 0
			}
			return si.incarnation < i.incarnation
		}
		return c < 0
	case *CodeItem:
		c := bytes.Compare(si.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			if si.incarnation == i.incarnation {
				// Code comes before storage items
				return false
			}
			return si.incarnation < i.incarnation
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (swi *StorageWriteItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountWriteItem:
		c := bytes.Compare(swi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			// Account comes before its storage items
			return false
		}
		return c < 0
	case *StorageWriteItem:
		c := bytes.Compare(swi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			if swi.si.incarnation == i.si.incarnation {
				return bytes.Compare(swi.location.Bytes(), i.location.Bytes()) < 0
			}
			return swi.si.incarnation < i.si.incarnation
		}
		return c < 0
	case *CodeWriteItem:
		c := bytes.Compare(swi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			if swi.si.incarnation == i.ci.incarnation {
				// Code comes before storage items
				return false
			}
			return swi.si.incarnation < i.ci.incarnation
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (swi *StorageWriteItem) GetCacheItem() CacheItem {
	return swi.si
}

func (swi *StorageWriteItem) SetCacheItem(item CacheItem) {
	swi.si = item.(*StorageItem)
}

func (swi *StorageWriteItem) GetSize() int {
	return storageWriteItemSize
}

func (si *StorageItem) GetSequence() int {
	return si.sequence
}

func (si *StorageItem) SetSequence(sequence int) {
	si.sequence = sequence
}

func (si *StorageItem) GetSize() int {
	return storageItemSize
}

func (si *StorageItem) GetQueuePos() int {
	return si.queuePos
}

func (si *StorageItem) SetQueuePos(pos int) {
	si.queuePos = pos
}

func (si *StorageItem) HasFlag(flag uint16) bool {
	return si.flags&flag != 0
}

func (si *StorageItem) SetFlags(flags uint16) {
	si.flags |= flags
}

func (si *StorageItem) ClearFlags(flags uint16) {
	si.flags &^= flags
}

func (si *StorageItem) String() string {
	return fmt.Sprintf("StorageItem(addrHash=%x,incarnation=%d,locHash=%x)", si.addrHash, si.incarnation, si.locHash)
}

func (si *StorageItem) CopyValueFrom(item CacheItem) {
	otherSi, ok := item.(*StorageItem)
	if !ok {
		panic(fmt.Sprintf("expected StorageCacheItem, got %T", item))
	}
	si.value.Set(&otherSi.value)
}

func (ci *CodeItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		c := bytes.Compare(ci.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			// Account before its code
			return false
		}
		return c < 0
	case *StorageItem:
		c := bytes.Compare(ci.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			// Code comes before storage items
			if ci.incarnation == i.incarnation {
				return true
			}
			return ci.incarnation < i.incarnation
		}
		return c < 0
	case *CodeItem:
		c := bytes.Compare(ci.addrHash.Bytes(), i.addrHash.Bytes())
		if c == 0 {
			return ci.incarnation < i.incarnation
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (cwi *CodeWriteItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountWriteItem:
		c := bytes.Compare(cwi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			// Account before its code
			return false
		}
		return c < 0
	case *StorageWriteItem:
		c := bytes.Compare(cwi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			// Code comes before storage items
			if cwi.ci.incarnation == i.si.incarnation {
				return true
			}
			return cwi.ci.incarnation < i.si.incarnation
		}
		return c < 0
	case *CodeWriteItem:
		c := bytes.Compare(cwi.address.Bytes(), i.address.Bytes())
		if c == 0 {
			return cwi.ci.incarnation < i.ci.incarnation
		}
		return c < 0
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", than))
	}
}

func (cwi *CodeWriteItem) GetCacheItem() CacheItem {
	return cwi.ci
}

func (cwi *CodeWriteItem) SetCacheItem(item CacheItem) {
	cwi.ci = item.(*CodeItem)
}

func (cwi *CodeWriteItem) GetSize() int {
	return codeWriteItemSize + len(cwi.ci.code)
}

func (ci *CodeItem) GetSequence() int {
	return ci.sequence
}

func (ci *CodeItem) SetSequence(sequence int) {
	ci.sequence = sequence
}

func (ci *CodeItem) GetSize() int {
	return codeItemSize + len(ci.code)
}

func (ci *CodeItem) GetQueuePos() int {
	return ci.queuePos
}

func (ci *CodeItem) SetQueuePos(pos int) {
	ci.queuePos = pos
}

func (ci *CodeItem) HasFlag(flag uint16) bool {
	return ci.flags&flag != 0
}

func (ci *CodeItem) SetFlags(flags uint16) {
	ci.flags |= flags
}

func (ci *CodeItem) ClearFlags(flags uint16) {
	ci.flags &^= flags
}

func (ci *CodeItem) String() string {
	return fmt.Sprintf("CodeItem(addrHash=%x,incarnation=%d)", ci.addrHash, ci.incarnation)
}

func (ci *CodeItem) CopyValueFrom(item CacheItem) {
	otherCi, ok := item.(*CodeItem)
	if !ok {
		panic(fmt.Sprintf("expected CodeCacheItem, got %T", item))
	}
	ci.code = make([]byte, len(otherCi.code))
	copy(ci.code, otherCi.code)
}

// Heap for reads
type ReadHeap struct {
	items []CacheItem
}

func (rh ReadHeap) Len() int {
	return len(rh.items)
}

func (rh ReadHeap) Less(i, j int) bool {
	return rh.items[i].GetSequence() < rh.items[j].GetSequence()
}

func (rh ReadHeap) Swap(i, j int) {
	// Swap queue positions in the B-tree leaves too
	rh.items[i].SetQueuePos(j)
	rh.items[j].SetQueuePos(i)
	rh.items[i], rh.items[j] = rh.items[j], rh.items[i]
}

func (rh *ReadHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	cacheItem := x.(CacheItem)
	cacheItem.SetQueuePos(len(rh.items))
	rh.items = append(rh.items, cacheItem)
}

func (rh *ReadHeap) Pop() interface{} {
	cacheItem := rh.items[len(rh.items)-1]
	rh.items = rh.items[:len(rh.items)-1]
	return cacheItem
}

type StateCache struct {
	readWrites *btree.BTree // Mixed reads and writes
	writes     *btree.BTree // Only writes for the effective iteration
	readQueue  ReadHeap
	limit      int //  Total sizr of reads that triggers eviction
	readSize   int
	writeSize  int
	sequence   int
}

// NewStateCache create a new state cache based on the B-trees of specific degree. The second and the third parameters are the limit on the number of reads and writes to cache, respectively
func NewStateCache(degree int, limit int) *StateCache {
	var sc StateCache
	sc.readWrites = btree.New(degree)
	sc.writes = btree.New(degree)
	sc.limit = limit
	return &sc
}

func (sc *StateCache) get(key btree.Item) (CacheItem, bool) {
	item := sc.readWrites.Get(key)
	if item == nil {
		return nil, false
	}
	cacheItem := item.(CacheItem)
	if cacheItem.HasFlag(DeletedFlag) {
		return nil, true
	}
	return cacheItem, true
}

// GetAccount searches and account with given address, without modifying any structures
// Second return value is true if such account is found
func (sc *StateCache) GetAccount(address []byte) (*accounts.Account, bool) {
	var key AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return &item.(*AccountItem).account, true
		}
		return nil, true
	}
	return nil, false
}

// GetStorage searches storage item with given address, incarnation, and location, without modifying any structures
// Second return value is true if such item is found
func (sc *StateCache) GetStorage(address []byte, incarnation uint64, location []byte) ([]byte, bool) {
	var key StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	key.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(key.locHash[:])
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return item.(*StorageItem).value.Bytes(), true
		}
		return nil, true
	}
	return nil, false
}

// GetCode searches contract code with given address, without modifying any structures
// Second return value is true if such item is found
func (sc *StateCache) GetCode(address []byte, incarnation uint64) ([]byte, bool) {
	var key CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	key.incarnation = incarnation
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return item.(*CodeItem).code, true
		}
		return nil, true
	}
	return nil, false
}

func (sc *StateCache) setRead(item CacheItem, absent bool) {
	if sc.readWrites.Get(item) != nil {
		panic(fmt.Sprintf("item must not be present in the cache before doing setRead: %s", item))
	}
	item.SetSequence(sc.sequence)
	sc.sequence++
	item.ClearFlags(ModifiedFlag)
	if absent {
		item.SetFlags(DeletedFlag)
	} else {
		item.ClearFlags(DeletedFlag)
	}
	if sc.readSize+item.GetSize() > sc.limit {
		for sc.readQueue.Len() > 0 && sc.readSize+item.GetSize() > sc.limit {
			// Read queue cannot grow anymore, need to evict one element
			cacheItem := sc.readQueue.items[0].(CacheItem)
			sc.readSize -= cacheItem.GetSize()
			sc.readWrites.Delete(cacheItem)
			sc.readQueue.items[0] = item
			item.SetQueuePos(0)
			heap.Fix(&sc.readQueue, 0)
		}
	} else {
		// Push new element on the read queue
		heap.Push(&sc.readQueue, item)
	}
	sc.readWrites.ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
}

// SetAccountRead adds given account to the cache, marking it as a read (not written)
func (sc *StateCache) SetAccountRead(address []byte, account *accounts.Account) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	ai.account.Copy(account)
	sc.setRead(&ai, false /* absent */)
}

// SetAccountRead adds given account address to the cache, marking it as a absent
func (sc *StateCache) SetAccountAbsent(address []byte) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	sc.setRead(&ai, true /* absent */)
}

func (sc *StateCache) setWrite(item CacheItem, writeItem CacheWriteItem, delete bool) {
	// Check if this is going to be modification of the existing entry
	if existing := sc.writes.Get(writeItem); existing != nil {
		cacheWriteItem := existing.(CacheWriteItem)
		cacheItem := cacheWriteItem.GetCacheItem()
		sc.readSize += item.GetSize()
		sc.readSize -= cacheItem.GetSize()
		sc.writeSize += writeItem.GetSize()
		sc.writeSize -= cacheWriteItem.GetSize()
		cacheItem.CopyValueFrom(item)
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.ClearFlags(DeletedFlag)
		}
		return
	}
	// Now see if there is such item in the readWrite B-tree - then we replace read entry with write entry
	if existing := sc.readWrites.Get(item); existing != nil {
		cacheItem := existing.(CacheItem)
		sc.readSize += item.GetSize()
		sc.readSize -= cacheItem.GetSize()
		cacheItem.CopyValueFrom(item)
		cacheItem.SetSequence(sc.sequence)
		sc.sequence++
		cacheItem.SetFlags(ModifiedFlag)
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.ClearFlags(DeletedFlag)
		}
		// Remove from the reads queue
		heap.Remove(&sc.readQueue, cacheItem.GetQueuePos())
		writeItem.SetCacheItem(cacheItem)
		sc.writes.ReplaceOrInsert(writeItem)
		sc.writeSize += writeItem.GetSize()
		return
	}
	if sc.readSize+item.GetSize() > sc.limit {
		for sc.readQueue.Len() > 0 && sc.readSize+item.GetSize() > sc.limit {
			// There is no space available, need to evict one read element
			cacheItem := heap.Pop(&sc.readQueue).(CacheItem)
			sc.readWrites.Delete(cacheItem)
			sc.readSize -= cacheItem.GetSize()
		}
	}
	item.SetSequence(sc.sequence)
	sc.sequence++
	item.SetFlags(ModifiedFlag)
	if delete {
		item.SetFlags(DeletedFlag)
	} else {
		item.ClearFlags(DeletedFlag)
	}
	sc.readWrites.ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
	writeItem.SetCacheItem(item)
	sc.writes.ReplaceOrInsert(writeItem)
	sc.writeSize += writeItem.GetSize()
}

// SetAccountWrite adds given account to the cache, marking it as written (cannot be evicted)
func (sc *StateCache) SetAccountWrite(address []byte, account *accounts.Account) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	ai.account.Copy(account)
	var awi AccountWriteItem
	copy(awi.address[:], address)
	awi.ai = &ai
	sc.setWrite(&ai, &awi, false /* delete */)
}

// SetAccountDelete is very similar to SetAccountWrite with the difference that there no set value
func (sc *StateCache) SetAccountDelete(address []byte) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	var awi AccountWriteItem
	copy(awi.address[:], address)
	awi.ai = &ai
	sc.setWrite(&ai, &awi, true /* delete */)
}

func (sc *StateCache) SetStorageRead(address []byte, incarnation uint64, location []byte, value []byte) {
	var si StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(si.addrHash[:])
	si.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(si.locHash[:])
	si.value.SetBytes(value)
	sc.setRead(&si, false /* absent */)
}

func (sc *StateCache) SetStorageAbsent(address []byte, incarnation uint64, location []byte) {
	var si StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(si.addrHash[:])
	si.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(si.locHash[:])
	sc.setRead(&si, true /* absent */)
}

func (sc *StateCache) SetStorageWrite(address []byte, incarnation uint64, location []byte, value []byte) {
	var si StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(si.addrHash[:])
	si.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(si.locHash[:])
	si.value.SetBytes(value)
	var swi StorageWriteItem
	copy(swi.address[:], address)
	copy(swi.location[:], location)
	swi.si = &si
	sc.setWrite(&si, &swi, false /* delete */)
}

func (sc *StateCache) SetStorageDelete(address []byte, incarnation uint64, location []byte) {
	var si StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(si.addrHash[:])
	si.incarnation = incarnation
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(location)
	//nolint:errcheck
	h.Sha.Read(si.locHash[:])
	var swi StorageWriteItem
	copy(swi.address[:], address)
	copy(swi.location[:], location)
	swi.si = &si
	sc.setWrite(&si, &swi, true /* delete */)
}

func (sc *StateCache) SetCodeRead(address []byte, incarnation uint64, code []byte) {
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = make([]byte, len(code))
	copy(ci.code, code)
	sc.setRead(&ci, false /* absent */)
}

func (sc *StateCache) SetCodeAbsent(address []byte, incarnation uint64) {
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	sc.setRead(&ci, true /* absent */)
}

func (sc *StateCache) SetCodeWrite(address []byte, incarnation uint64, code []byte) {
	// Check if this is going to be modification of the existing entry
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = make([]byte, len(code))
	copy(ci.code, code)
	var cwi CodeWriteItem
	copy(cwi.address[:], address)
	cwi.ci = &ci
	sc.setWrite(&ci, &cwi, false /* delete */)
}

func (sc *StateCache) SetCodeDelete(address []byte, incarnation uint64) {
	// Check if this is going to be modification of the existing entry
	var ci CodeItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ci.addrHash[:])
	ci.incarnation = incarnation
	ci.code = nil
	var cwi CodeWriteItem
	copy(cwi.address[:], address)
	cwi.ci = &ci
	sc.setWrite(&ci, &cwi, true /* delete */)
}

func (sc *StateCache) PrepareWrites() *btree.BTree {
	sc.writes.Ascend(func(i btree.Item) bool {
		cacheItem := i.(CacheWriteItem)
		cacheItem.GetCacheItem().ClearFlags(ModifiedFlag)
		return true
	})
	writes := sc.writes.Clone()
	sc.writes.Clear(true /* addNodesToFreeList */)
	sc.writeSize = 0
	return writes
}

func WalkWrites(
	writes *btree.BTree,
	accountWrite func(address []byte, account *accounts.Account) error,
	accountDelete func(address []byte) error,
	storageWrite func(address []byte, incarnation uint64, location []byte, value []byte) error,
	storageDelete func(address []byte, incarnation uint64, location []byte) error,
	codeWrite func(address []byte, incarnation uint64, code []byte) error,
	codeDelete func(address []byte, incarnation uint64) error,
) error {
	var err error
	writes.Ascend(func(i btree.Item) bool {
		switch it := i.(type) {
		case *AccountWriteItem:
			if it.ai.flags&DeletedFlag != 0 {
				if err = accountDelete(it.address.Bytes()); err != nil {
					return false
				}
			} else {
				if err = accountWrite(it.address.Bytes(), &it.ai.account); err != nil {
					return false
				}
			}
		case *StorageWriteItem:
			if it.si.flags&DeletedFlag != 0 {
				if err = storageDelete(it.address.Bytes(), it.si.incarnation, it.location.Bytes()); err != nil {
					return false
				}
			} else {
				if err = storageWrite(it.address.Bytes(), it.si.incarnation, it.location.Bytes(), it.si.value.Bytes()); err != nil {
					return false
				}
			}
		case *CodeWriteItem:
			if it.ci.flags&DeletedFlag != 0 {
				if err = codeDelete(it.address.Bytes(), it.ci.incarnation); err != nil {
					return false
				}
			} else {
				if err = codeWrite(it.address.Bytes(), it.ci.incarnation, it.ci.code); err != nil {
					return false
				}
			}
		}
		return true
	})
	return err
}

func (sc *StateCache) TurnWritesToReads(writes *btree.BTree) {
	writes.Ascend(func(i btree.Item) bool {
		cacheWriteItem := i.(CacheWriteItem)
		cacheItem := cacheWriteItem.GetCacheItem()
		if !cacheItem.HasFlag(ModifiedFlag) {
			// Cannot touch items that have been modified since we have taken away the writes
			cacheItem.ClearFlags(ModifiedFlag)
			cacheItem.SetQueuePos(len(sc.readQueue.items))
			heap.Push(&sc.readQueue, cacheItem)
		}
		return true
	})
}

func (sc *StateCache) TotalCount() int {
	return sc.readWrites.Len()
}

func (sc *StateCache) WriteCount() int {
	return sc.writes.Len()
}

func (sc *StateCache) WriteSize() int {
	return sc.writeSize
}

func (sc *StateCache) ReadSize() int {
	return sc.readSize
}
