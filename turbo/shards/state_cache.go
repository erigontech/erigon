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
	ModifiedFlag    uint16 = 1 // Set when the item is different from what is last committed to the database
	AbsentFlag      uint16 = 2 // Set when the item is absent in the state
	DeletedFlag     uint16 = 4 // Set when the item is marked for deletion, even though it might have the value in it
	UnprocessedFlag uint16 = 8 // Set when there is a modification in the item that invalidates merkle root calculated previously
)

// Sizes of B-tree items for the purposes of keeping track of the size of reads and writes
// The sizes of the nodes of the B-tree are not accounted for, because their are private to the `btree` package
const (
	accountItemSize      = int(unsafe.Sizeof(AccountItem{}))
	accountWriteItemSize = int(unsafe.Sizeof(AccountWriteItem{})) + accountItemSize
	storageItemSize      = int(unsafe.Sizeof(StorageItem{}))
	storageWriteItemSize = int(unsafe.Sizeof(StorageWriteItem{})) + storageItemSize
	codeItemSize         = int(unsafe.Sizeof(CodeItem{}))
	codeWriteItemSize    = int(unsafe.Sizeof(CodeWriteItem{})) + codeItemSize
)

// AccountItem is an element in the `readWrites` B-tree representing an Ethereum account. It can mean either value just read from the database and cache (read), or value that
// is different from what the last committed value in the DB is (write). Reads can be removed or evicted from the B-tree at any time, because this
// does not hurt the consistency. Writes cannot be removed or evicted one by one, therefore they can either be deleted all together, or
// committed all together and turned into reads.
type AccountItem struct {
	sequence int
	queuePos int
	flags    uint16
	addrHash common.Hash
	account  accounts.Account
}

// AccountWriteItem is an item in the `writes` B-tree. As can be seen, it always references a corresponding `AccountItem`. There can be `AccountItem` without corresponding `AccountWriteItem`
// (in that case `AccountItem` represents a cached read), but there cannot be `AccountWriteItem` without a corresponding `AccountItem`. Such pair represents an account that has been modified
// in the cache, but the modification has not been committed to the database yet. The correspondence of an `ai AccountItem` and an `awi AccountWriteItem` implies that
// `keccak(awi.address) == ai.addrHash`.
type AccountWriteItem struct {
	address common.Address
	ai      *AccountItem
}

type StorageItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	addrHash    common.Hash
	incarnation uint64
	locHash     common.Hash
	value       uint256.Int
}

type StorageWriteItem struct {
	address  common.Address
	location common.Hash
	si       *StorageItem
}

type CodeItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	addrHash    common.Hash
	incarnation uint64
	code        []byte
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
	HasFlag(flag uint16) bool        // Check if specified flag is set
	SetFlags(flags uint16)           // Set specified flags, but leaves other flags alone
	ClearFlags(flags uint16)         // Clear specified flags, but laves other flags alone
	CopyValueFrom(item CacheItem)    // Copy value (not key) from given item
	HasPrefix(prefix CacheItem) bool // Whether this item has specified item as a prefix
}

type CacheWriteItem interface {
	btree.Item
	GetCacheItem() CacheItem
	SetCacheItem(item CacheItem)
	GetSize() int
}

func compare_account_account(i1 *AccountItem, i2 *AccountItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
}

func compare_account_storage(i1 *AccountItem, i2 *StorageItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
}

func compare_account_code(i1 *AccountItem, i2 *CodeItem) int {
	return bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
}

func compare_storage_storage(i1 *StorageItem, i2 *StorageItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.incarnation == i2.incarnation {
		return bytes.Compare(i1.locHash.Bytes(), i2.locHash.Bytes())
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func compare_storage_code(i1 *StorageItem, i2 *CodeItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.incarnation == i2.incarnation {
		return 0
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func compare_code_code(i1 *CodeItem, i2 *CodeItem) int {
	c := bytes.Compare(i1.addrHash.Bytes(), i2.addrHash.Bytes())
	if c != 0 {
		return c
	}
	if i1.incarnation == i2.incarnation {
		return 0
	}
	if i1.incarnation < i2.incarnation {
		return -1
	}
	return 1
}

func (ai *AccountItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return compare_account_account(ai, i) < 0
	case *StorageItem:
		// Under equality the result is "true", account comes before its storage item
		return compare_account_storage(ai, i) <= 0
	case *CodeItem:
		// Under equality the result is "true", code comes before its storage item
		return compare_account_code(ai, i) <= 0
	case *AccountHashItem:
		// Under equality the result is "false" - account comes after hash
		return compare_account_accountHash(ai, i) < 0
	case *StorageHashItem:
		// Under equality the result is "true" - account comes before any storage hashes
		return compare_account_storageHash(ai, i) <= 0
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
		// Under equality the result is "false", account comes before its storage item
		return compare_account_storage(i, si) > 0
	case *StorageItem:
		return compare_storage_storage(si, i) < 0
	case *CodeItem:
		// Under equality the result is "false", code of account comes before its storage item
		return compare_storage_code(si, i) < 0
	case *AccountHashItem:
		// Under equality the result is "false", account hash comes before storage item of the account
		return compare_storage_accountHash(si, i) < 0
	case *StorageHashItem:
		// Under equality the result is "false", storage hash comes before storage item
		return compare_storage_storageHash(si, i) < 0
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
		// Under equality the result is "false" - code comes after the account itself
		return compare_account_code(i, ci) > 0
	case *StorageItem:
		// Under equality the result is "true" - code comes before the storage items
		return compare_storage_code(i, ci) >= 0
	case *CodeItem:
		return compare_code_code(ci, i) < 0
	case *AccountHashItem:
		// Under equality the result is "false" - code comes after account hash
		return compare_code_accountHash(ci, i) < 0
	case *StorageHashItem:
		// Under equality the result is "true" - code comes before the storage hashes
		return compare_code_storageHash(ci, i) <= 0
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

// StateCache is the structure containing B-trees and priority queues for the state cache
type StateCache struct {
	readWrites  *btree.BTree // Mixed reads and writes
	writes      *btree.BTree // Only writes for the effective iteration
	readQueue   ReadHeap     // Priority queue of read elements eligible for eviction (sorted by sequence)
	limit       int          // Total size of the readQueue (if new item causes the size to go over the limit, some existing items are evicted)
	readSize    int
	writeSize   int
	sequence    int             // Current sequence assigned to any item that has been "touched" (created, deleted, read). Incremented after every touch
	unprocQueue UnprocessedHeap // Priority queue of items appeared since last root calculation processing (sorted by the keys - addrHash, incarnation, locHash)
}

// NewStateCache create a new state cache based on the B-trees of specific degree. The second and the third parameters are the limit on the number of reads and writes to cache, respectively
func NewStateCache(degree int, limit int) *StateCache {
	var sc StateCache
	sc.readWrites = btree.New(degree)
	sc.writes = btree.New(degree)
	sc.limit = limit
	heap.Init(&sc.readQueue)
	heap.Init(&sc.unprocQueue)
	return &sc
}

// Clone creates a clone cache which can be modified independently, but it shares the parts of the cache that are common
func (sc *StateCache) Clone() *StateCache {
	var clone StateCache
	clone.readWrites = sc.readWrites.Clone()
	clone.writes = sc.writes.Clone()
	clone.limit = sc.limit
	heap.Init(&clone.readQueue)
	heap.Init(&clone.unprocQueue)
	return &clone
}

func (sc *StateCache) get(key btree.Item) (CacheItem, bool) {
	item := sc.readWrites.Get(key)
	if item == nil {
		return nil, false
	}
	cacheItem := item.(CacheItem)
	if cacheItem.HasFlag(DeletedFlag) || cacheItem.HasFlag(AbsentFlag) {
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

// GetDeletedAccount attempts to retrieve the last version of account before it was deleted
func (sc *StateCache) GetDeletedAccount(address []byte) *accounts.Account {
	var key AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	item := sc.readWrites.Get(&key)
	if item == nil {
		return nil
	}
	ai := item.(*AccountItem)
	if !ai.HasFlag(DeletedFlag) {
		return nil
	}
	return &ai.account
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
	item.ClearFlags(ModifiedFlag | DeletedFlag)
	if absent {
		item.SetFlags(AbsentFlag)
	} else {
		item.ClearFlags(AbsentFlag)
	}
	if sc.limit != 0 && sc.readSize+item.GetSize() > sc.limit {
		for sc.readQueue.Len() > 0 && sc.readSize+item.GetSize() > sc.limit {
			// Read queue cannot grow anymore, need to evict one element
			cacheItem := heap.Pop(&sc.readQueue).(CacheItem)
			sc.readSize -= cacheItem.GetSize()
			sc.readWrites.Delete(cacheItem)
		}
	}
	// Push new element on the read queue
	heap.Push(&sc.readQueue, item)
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
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.CopyValueFrom(item)
			cacheItem.ClearFlags(DeletedFlag)
		}
		cacheItem.SetSequence(sc.sequence)
		sc.sequence++
		return
	}
	// Now see if there is such item in the readWrite B-tree - then we replace read entry with write entry
	if existing := sc.readWrites.Get(item); existing != nil {
		cacheItem := existing.(CacheItem)
		// Remove from the reads queue
		heap.Remove(&sc.readQueue, cacheItem.GetQueuePos())
		sc.readSize += item.GetSize()
		sc.readSize -= cacheItem.GetSize()
		cacheItem.SetFlags(ModifiedFlag)
		cacheItem.ClearFlags(AbsentFlag)
		if delete {
			cacheItem.SetFlags(DeletedFlag)
		} else {
			cacheItem.CopyValueFrom(item)
			cacheItem.ClearFlags(DeletedFlag)
		}
		cacheItem.SetSequence(sc.sequence)
		sc.sequence++
		writeItem.SetCacheItem(cacheItem)
		sc.writes.ReplaceOrInsert(writeItem)
		sc.writeSize += writeItem.GetSize()
		return
	}
	if sc.limit != 0 && sc.readSize+item.GetSize() > sc.limit {
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
	item.ClearFlags(AbsentFlag)
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
		writeItem := i.(CacheWriteItem)
		cacheItem := writeItem.GetCacheItem()
		cacheItem.ClearFlags(ModifiedFlag)
		if cacheItem.HasFlag(DeletedFlag) {
			cacheItem.ClearFlags(DeletedFlag)
			cacheItem.SetFlags(AbsentFlag)
		}
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
	accountDelete func(address []byte, original *accounts.Account) error,
	storageWrite func(address []byte, incarnation uint64, location []byte, value []byte) error,
	storageDelete func(address []byte, incarnation uint64, location []byte) error,
	codeWrite func(address []byte, incarnation uint64, code []byte) error,
	codeDelete func(address []byte, incarnation uint64) error,
) error {
	var err error
	writes.Ascend(func(i btree.Item) bool {
		switch it := i.(type) {
		case *AccountWriteItem:
			if it.ai.flags&AbsentFlag != 0 {
				if err = accountDelete(it.address.Bytes(), &it.ai.account); err != nil {
					return false
				}
			} else {
				if err = accountWrite(it.address.Bytes(), &it.ai.account); err != nil {
					return false
				}
			}
		case *StorageWriteItem:
			if it.si.flags&AbsentFlag != 0 {
				if err = storageDelete(it.address.Bytes(), it.si.incarnation, it.location.Bytes()); err != nil {
					return false
				}
			} else {
				if err = storageWrite(it.address.Bytes(), it.si.incarnation, it.location.Bytes(), it.si.value.Bytes()); err != nil {
					return false
				}
			}
		case *CodeWriteItem:
			if it.ci.flags&AbsentFlag != 0 {
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
