package shards

import (
	"bytes"
	"container/heap"
	"fmt"
	"unsafe"

	"github.com/VictoriaMetrics/metrics"
	"github.com/c2h5oh/datasize"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// LRU state cache consists of two structures - B-Tree and binary heap
// Every element is marked either as Read, Updated, or Deleted via flags

// Metrics
var (
	AccRead    = metrics.GetOrCreateCounter(`cache_total{target="acc_read"}`)
	StRead     = metrics.GetOrCreateCounter(`cache_total{target="st_read"}`)
	WritesRead = metrics.GetOrCreateCounter(`cache_total{target="write"}`)
)

const (
	ModifiedFlag    uint16 = 1 // Set when the item is different seek what is last committed to the database
	AbsentFlag      uint16 = 2 // Set when the item is absent in the state
	DeletedFlag     uint16 = 4 // Set when the item is marked for deletion, even though it might have the value in it
	UnprocessedFlag uint16 = 8 // Set when there is a modification in the item that invalidates merkle root calculated previously
)

// Sizes of B-tree items for the purposes of keeping track of the size of reads and writes
// The sizes of the nodes of the B-tree are not accounted for, because their are private to the `btree` package
// +16 means - each item has 2 words overhead: 1 for interface, 1 for pointer to item.
const (
	accountItemSize      = int(unsafe.Sizeof(AccountItem{}) + 16)
	accountWriteItemSize = int(unsafe.Sizeof(AccountWriteItem{})+16) + accountItemSize
	storageItemSize      = int(unsafe.Sizeof(StorageItem{}) + 16)
	storageWriteItemSize = int(unsafe.Sizeof(StorageWriteItem{})+16) + storageItemSize
	codeItemSize         = int(unsafe.Sizeof(CodeItem{}) + 16)
	codeWriteItemSize    = int(unsafe.Sizeof(CodeWriteItem{})+16) + codeItemSize
)

// AccountSeek allows to traverse sub-tree
type AccountSeek struct {
	seek []byte
}

// StorageSeek allows to traverse sub-tree
type StorageSeek struct {
	addrHash    libcommon.Hash
	incarnation uint64
	seek        []byte
}

// AccountItem is an element in the `readWrites` B-tree representing an Ethereum account. It can mean either value
// just read seek the database and cache (read), or value that is different seek what the last committed value
// in the DB is (write). Reads can be removed or evicted seek the B-tree at any time, because this
// does not hurt the consistency. Writes cannot be removed or evicted one by one, therefore they can
// either be deleted all together, or committed all together and turned into reads.
type AccountItem struct {
	sequence int
	queuePos int
	flags    uint16
	addrHash libcommon.Hash
	account  accounts.Account
}

// AccountWriteItem is an item in the `writes` B-tree. As can be seen, it always references a corresponding
// `AccountItem`. There can be `AccountItem` without corresponding `AccountWriteItem` (in that case `AccountItem`
// represents a cached read), but there cannot be `AccountWriteItem` without a corresponding `AccountItem`.
// Such pair represents an account that has been modified in the cache, but the modification has not been committed
// to the database yet. The correspondence of an `ai AccountItem` and an `awi AccountWriteItem` implies that
// `keccak(awi.address) == ai.addrHash`.
type AccountWriteItem struct {
	address libcommon.Address
	ai      *AccountItem
}

type StorageItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	addrHash    libcommon.Hash
	incarnation uint64
	locHash     libcommon.Hash
	value       uint256.Int
}

type StorageWriteItem struct {
	address  libcommon.Address
	location libcommon.Hash
	si       *StorageItem
}

type CodeItem struct {
	sequence    int
	queuePos    int
	flags       uint16
	addrHash    libcommon.Hash
	incarnation uint64
	code        []byte
}

type CodeWriteItem struct {
	address libcommon.Address
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
	CopyValueFrom(item CacheItem)    // Copy value (not key) seek given item
	HasPrefix(prefix CacheItem) bool // Whether this item has specified item as a prefix
}

type CacheWriteItem interface {
	btree.Item
	GetCacheItem() CacheItem
	SetCacheItem(item CacheItem)
	GetSize() int
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

func (r *AccountSeek) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return bytes.Compare(r.seek, i.addrHash.Bytes()) < 0
	case *AccountWriteItem:
		return bytes.Compare(r.seek, i.ai.addrHash.Bytes()) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (r *StorageSeek) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *StorageItem:
		c := bytes.Compare(r.addrHash.Bytes(), i.addrHash.Bytes())
		if c != 0 {
			return c < 0
		}
		if r.incarnation < i.incarnation {
			return true
		}
		return bytes.Compare(r.seek, i.locHash.Bytes()) < 0
	case *StorageWriteItem:
		c := bytes.Compare(r.addrHash.Bytes(), i.si.addrHash.Bytes())
		if c != 0 {
			return c < 0
		}
		if r.incarnation < i.si.incarnation {
			return true
		}
		return bytes.Compare(r.seek, i.si.locHash.Bytes()) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (ai *AccountItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountItem:
		return bytes.Compare(ai.addrHash.Bytes(), i.addrHash.Bytes()) < 0
	case *AccountWriteItem:
		return bytes.Compare(ai.addrHash.Bytes(), i.ai.addrHash.Bytes()) < 0
	case *AccountSeek:
		return bytes.Compare(ai.addrHash.Bytes(), i.seek) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (awi *AccountWriteItem) GetCacheItem() CacheItem     { return awi.ai }
func (awi *AccountWriteItem) SetCacheItem(item CacheItem) { awi.ai = item.(*AccountItem) }
func (awi *AccountWriteItem) GetSize() int                { return accountWriteItemSize }
func (awi *AccountWriteItem) Less(than btree.Item) bool {
	return awi.ai.Less(than)
}

func (ai *AccountItem) GetSequence() int         { return ai.sequence }
func (ai *AccountItem) SetSequence(sequence int) { ai.sequence = sequence }
func (ai *AccountItem) GetSize() int             { return accountItemSize }
func (ai *AccountItem) GetQueuePos() int         { return ai.queuePos }
func (ai *AccountItem) SetQueuePos(pos int)      { ai.queuePos = pos }
func (ai *AccountItem) HasFlag(flag uint16) bool { return ai.flags&flag != 0 }
func (ai *AccountItem) SetFlags(flags uint16)    { ai.flags |= flags }
func (ai *AccountItem) ClearFlags(flags uint16)  { ai.flags &^= flags }
func (ai *AccountItem) String() string           { return fmt.Sprintf("AccountItem(addrHash=%x)", ai.addrHash) }

func (ai *AccountItem) CopyValueFrom(item CacheItem) {
	otherAi, ok := item.(*AccountItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountItem, got %T", item))
	}
	ai.account.Copy(&otherAi.account)
}

func (swi *StorageWriteItem) Less(than btree.Item) bool {
	return swi.si.Less(than.(*StorageWriteItem).si)
}
func (swi *StorageWriteItem) GetCacheItem() CacheItem     { return swi.si }
func (swi *StorageWriteItem) SetCacheItem(item CacheItem) { swi.si = item.(*StorageItem) }
func (swi *StorageWriteItem) GetSize() int                { return storageWriteItemSize }

func (si *StorageItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *StorageItem:
		c := bytes.Compare(si.addrHash.Bytes(), i.addrHash.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.incarnation {
			return true
		}
		return bytes.Compare(si.locHash.Bytes(), i.locHash.Bytes()) < 0
	case *StorageWriteItem:
		c := bytes.Compare(si.addrHash.Bytes(), i.si.addrHash.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.si.incarnation {
			return true
		}
		return bytes.Compare(si.locHash.Bytes(), i.si.locHash.Bytes()) < 0
	case *StorageSeek:
		c := bytes.Compare(si.addrHash.Bytes(), i.addrHash.Bytes())
		if c != 0 {
			return c < 0
		}
		if si.incarnation < i.incarnation {
			return true
		}
		return bytes.Compare(si.locHash.Bytes(), i.seek) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}
func (si *StorageItem) GetSequence() int         { return si.sequence }
func (si *StorageItem) SetSequence(sequence int) { si.sequence = sequence }
func (si *StorageItem) GetSize() int             { return storageItemSize }
func (si *StorageItem) GetQueuePos() int         { return si.queuePos }
func (si *StorageItem) SetQueuePos(pos int)      { si.queuePos = pos }
func (si *StorageItem) HasFlag(flag uint16) bool { return si.flags&flag != 0 }
func (si *StorageItem) SetFlags(flags uint16)    { si.flags |= flags }
func (si *StorageItem) ClearFlags(flags uint16)  { si.flags &^= flags }
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
	return compare_code_code(ci, than.(*CodeItem)) < 0
}

func (cwi *CodeWriteItem) Less(than btree.Item) bool {
	i := than.(*CodeWriteItem)
	c := bytes.Compare(cwi.address.Bytes(), i.address.Bytes())
	if c == 0 {
		return cwi.ci.incarnation < i.ci.incarnation
	}
	return c < 0
}

func (cwi *CodeWriteItem) GetCacheItem() CacheItem     { return cwi.ci }
func (cwi *CodeWriteItem) SetCacheItem(item CacheItem) { cwi.ci = item.(*CodeItem) }
func (cwi *CodeWriteItem) GetSize() int                { return codeWriteItemSize + len(cwi.ci.code) }
func (ci *CodeItem) GetSequence() int                  { return ci.sequence }
func (ci *CodeItem) SetSequence(sequence int)          { ci.sequence = sequence }
func (ci *CodeItem) GetSize() int                      { return codeItemSize + len(ci.code) }
func (ci *CodeItem) GetQueuePos() int                  { return ci.queuePos }
func (ci *CodeItem) SetQueuePos(pos int)               { ci.queuePos = pos }
func (ci *CodeItem) HasFlag(flag uint16) bool          { return ci.flags&flag != 0 }
func (ci *CodeItem) SetFlags(flags uint16)             { ci.flags |= flags }
func (ci *CodeItem) ClearFlags(flags uint16)           { ci.flags &^= flags }
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

func (rh ReadHeap) Len() int           { return len(rh.items) }
func (rh ReadHeap) Less(i, j int) bool { return rh.items[i].GetSequence() < rh.items[j].GetSequence() }
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
	readWrites  [5]*btree.BTree   // Mixed reads and writes
	writes      [5]*btree.BTree   // Only writes for the effective iteration
	readQueue   [5]ReadHeap       // Priority queue of read elements eligible for eviction (sorted by sequence)
	limit       datasize.ByteSize // Total size of the readQueue (if new item causes the size to go over the limit, some existing items are evicted)
	readSize    int
	writeSize   int
	sequence    int                // Current sequence assigned to any item that has been "touched" (created, deleted, read). Incremented after every touch
	unprocQueue [5]UnprocessedHeap // Priority queue of items appeared since last root calculation processing (sorted by the keys - addrHash, incarnation, locHash)
}

func id(a interface{}) uint8 {
	switch a.(type) {
	case *AccountItem, *AccountWriteItem, *AccountSeek:
		return 0
	case *StorageItem, *StorageWriteItem, *StorageSeek:
		return 1
	case *CodeItem, *CodeWriteItem:
		return 2
	case *AccountHashItem, *AccountHashWriteItem:
		return 3
	case *StorageHashItem, *StorageHashWriteItem:
		return 4
	default:
		panic(fmt.Sprintf("unexpected type: %T", a))
	}
}

// NewStateCache create a new state cache based on the B-trees of specific degree. The second and the third parameters are the limit on the number of reads and writes to cache, respectively
func NewStateCache(degree int, limit datasize.ByteSize) *StateCache {
	var sc StateCache
	sc.limit = limit
	for i := 0; i < len(sc.readWrites); i++ {
		sc.readWrites[i] = btree.New(degree)
	}
	for i := 0; i < len(sc.writes); i++ {
		sc.writes[i] = btree.New(degree)
	}
	for i := 0; i < len(sc.readQueue); i++ {
		heap.Init(&sc.readQueue[i])
	}
	for i := 0; i < len(sc.unprocQueue); i++ {
		heap.Init(&sc.unprocQueue[i])
	}
	return &sc
}

// Clone creates a clone cache which can be modified independently, but it shares the parts of the cache that are common
func (sc *StateCache) Clone() *StateCache {
	var clone StateCache
	for i := range clone.readWrites {
		clone.readWrites[i] = sc.readWrites[i].Clone()
		clone.writes[i] = sc.writes[i].Clone()
		clone.limit = sc.limit
		heap.Init(&clone.readQueue[i])
		heap.Init(&clone.unprocQueue[i])
	}
	return &clone
}

func (sc *StateCache) get(key btree.Item) (CacheItem, bool) {
	WritesRead.Inc()
	item := sc.readWrites[id(key)].Get(key)
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
	AccRead.Inc()
	var key AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
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

func (sc *StateCache) HasAccountWithInPrefix(addrHashPrefix []byte) bool {
	AccRead.Inc()
	seek := &AccountSeek{seek: addrHashPrefix}
	var found bool
	sc.readWrites[id(seek)].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		found = bytes.HasPrefix(i.(*AccountItem).addrHash.Bytes(), addrHashPrefix)
		return false
	})
	return found
}

// GetDeletedAccount attempts to retrieve the last version of account before it was deleted
func (sc *StateCache) GetDeletedAccount(address []byte) *accounts.Account {
	key := &AccountItem{}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(key.addrHash[:])
	item := sc.readWrites[id(key)].Get(key)
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
	StRead.Inc()
	var key StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
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
	id := id(item)
	if sc.readWrites[id].Get(item) != nil {
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

	if sc.limit != 0 && sc.readSize+item.GetSize() > int(sc.limit) {
		for sc.readQueue[id].Len() > 0 && sc.readSize+item.GetSize() > int(sc.limit) {
			// Read queue cannot grow anymore, need to evict one element
			cacheItem := heap.Pop(&sc.readQueue[id]).(CacheItem)
			sc.readSize -= cacheItem.GetSize()
			sc.readWrites[id].Delete(cacheItem)
		}
	}
	// Push new element on the read queue
	heap.Push(&sc.readQueue[id], item)
	sc.readWrites[id].ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
}

func (sc *StateCache) readQueuesLen() (res int) {
	for i := 0; i < len(sc.readQueue); i++ {
		res += sc.readQueue[i].Len()
	}
	return
}

// SetAccountRead adds given account to the cache, marking it as a read (not written)
func (sc *StateCache) SetAccountRead(address []byte, account *accounts.Account) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	ai.account.Copy(account)
	sc.setRead(&ai, false /* absent */)
}

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetAccountRead(addrHash libcommon.Hash, account *accounts.Account) {
	var ai AccountItem
	ai.addrHash.SetBytes(addrHash.Bytes())
	ai.account.Copy(account)
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) GetAccountByHashedAddress(addrHash libcommon.Hash) (*accounts.Account, bool) {
	var key AccountItem
	key.addrHash.SetBytes(addrHash.Bytes())
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return &item.(*AccountItem).account, true
		}
		return nil, true
	}
	return nil, false
}

func (sc *StateCache) GetStorageByHashedAddress(addrHash libcommon.Hash, incarnation uint64, locHash libcommon.Hash) ([]byte, bool) {
	key := StorageItem{
		addrHash:    addrHash,
		incarnation: incarnation,
		locHash:     locHash,
	}
	if item, ok := sc.get(&key); ok {
		if item != nil {
			return item.(*StorageItem).value.Bytes(), true
		}
		return nil, true
	}
	return nil, false

}

// SetAccountRead adds given account address to the cache, marking it as a absent
func (sc *StateCache) SetAccountAbsent(address []byte) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	//nolint:errcheck
	h.Sha.Write(address)
	//nolint:errcheck
	h.Sha.Read(ai.addrHash[:])
	sc.setRead(&ai, true /* absent */)
}

func (sc *StateCache) setWrite(item CacheItem, writeItem CacheWriteItem, delete bool) {
	id := id(item)
	// Check if this is going to be modification of the existing entry
	if existing := sc.writes[id].Get(writeItem); existing != nil {
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
	if existing := sc.readWrites[id].Get(item); existing != nil {
		cacheItem := existing.(CacheItem)
		// Remove seek the reads queue
		if sc.readQueue[id].Len() > 0 {
			heap.Remove(&sc.readQueue[id], cacheItem.GetQueuePos())
		}
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
		sc.writes[id].ReplaceOrInsert(writeItem)
		sc.writeSize += writeItem.GetSize()
		return
	}
	if sc.limit != 0 && sc.readSize+item.GetSize() > int(sc.limit) {
		for sc.readQueue[id].Len() > 0 && sc.readSize+item.GetSize() > int(sc.limit) {
			// There is no space available, need to evict one read element
			cacheItem := heap.Pop(&sc.readQueue[id]).(CacheItem)
			sc.readWrites[id].Delete(cacheItem)
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
	sc.readWrites[id].ReplaceOrInsert(item)
	sc.readSize += item.GetSize()
	writeItem.SetCacheItem(item)
	sc.writes[id].ReplaceOrInsert(writeItem)
	sc.writeSize += writeItem.GetSize()
}

// SetAccountWrite adds given account to the cache, marking it as written (cannot be evicted)
func (sc *StateCache) SetAccountWrite(address []byte, account *accounts.Account) {
	var ai AccountItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
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

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetStorageRead(addrHash libcommon.Hash, incarnation uint64, locHash libcommon.Hash, val []byte) {
	var i StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	copy(i.addrHash[:], addrHash.Bytes())
	i.incarnation = incarnation
	i.locHash.SetBytes(locHash.Bytes())
	i.value.SetBytes(val)
	sc.setRead(&i, false /* absent */)
}

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetAccountWrite(addrHash libcommon.Hash, account *accounts.Account) {
	var ai AccountItem
	copy(ai.addrHash[:], addrHash.Bytes())
	ai.account.Copy(account)
	var awi AccountWriteItem
	awi.ai = &ai
	sc.setWrite(&ai, &awi, false /* delete */)
}

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetAccountDelete(addrHash libcommon.Hash) {
	var ai AccountItem
	copy(ai.addrHash[:], addrHash.Bytes())
	var awi AccountWriteItem
	awi.ai = &ai
	sc.setWrite(&ai, &awi, true /* delete */)
}

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetStorageDelete(addrHash libcommon.Hash, incarnation uint64, locHash libcommon.Hash) {
	var si StorageItem
	copy(si.addrHash[:], addrHash.Bytes())
	si.incarnation = incarnation
	copy(si.locHash[:], locHash.Bytes())
	var swi StorageWriteItem
	swi.si = &si
	sc.setWrite(&si, &swi, true /* delete */)
}

// hack to set hashed addr - we don't have another one in trie stage
func (sc *StateCache) DeprecatedSetStorageWrite(addrHash libcommon.Hash, incarnation uint64, locHash libcommon.Hash, v []byte) {
	var si StorageItem
	copy(si.addrHash[:], addrHash.Bytes())
	si.incarnation = incarnation
	copy(si.locHash[:], locHash.Bytes())
	si.value.SetBytes(v)
	var swi StorageWriteItem
	swi.si = &si
	sc.setWrite(&si, &swi, false /* delete */)
}

func (sc *StateCache) SetStorageAbsent(address []byte, incarnation uint64, location []byte) {
	var si StorageItem
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
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

func (sc *StateCache) PrepareWrites() [5]*btree.BTree {
	var writes [5]*btree.BTree
	for i := 0; i < len(sc.writes); i++ {
		sc.writes[i].Ascend(func(i btree.Item) bool {
			writeItem := i.(CacheWriteItem)
			cacheItem := writeItem.GetCacheItem()
			cacheItem.ClearFlags(ModifiedFlag)
			if cacheItem.HasFlag(DeletedFlag) {
				cacheItem.ClearFlags(DeletedFlag)
				cacheItem.SetFlags(AbsentFlag)
			}
			return true
		})
		writes[i] = sc.writes[i].Clone()
		sc.writes[i].Clear(true /* addNodesToFreeList */)
		sc.writeSize = 0
	}
	return writes
}

func WalkWrites(
	writes [5]*btree.BTree,
	accountWrite func(address []byte, account *accounts.Account) error,
	accountDelete func(address []byte, original *accounts.Account) error,
	storageWrite func(address []byte, incarnation uint64, location []byte, value []byte) error,
	storageDelete func(address []byte, incarnation uint64, location []byte) error,
	codeWrite func(address []byte, incarnation uint64, code []byte) error,
	codeDelete func(address []byte, incarnation uint64) error,
) error {
	var err error
	for i := 0; i < len(writes); i++ {
		writes[i].Ascend(func(i btree.Item) bool {
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
	}

	return err
}

func (sc *StateCache) TurnWritesToReads(writes [5]*btree.BTree) {
	for i := 0; i < len(writes); i++ {
		readQueue := &sc.readQueue[i]
		writes[i].Ascend(func(it btree.Item) bool {
			cacheWriteItem := it.(CacheWriteItem)
			cacheItem := cacheWriteItem.GetCacheItem()
			if !cacheItem.HasFlag(ModifiedFlag) {
				// Cannot touch items that have been modified since we have taken away the writes
				heap.Push(readQueue, cacheItem)
			}
			return true
		})
	}
}

func (sc *StateCache) TotalCount() (res int) {
	for i := 0; i < len(sc.readWrites); i++ {
		res += sc.readWrites[i].Len()
	}
	return
}
func (sc *StateCache) WriteCount() (res int) {
	for i := 0; i < len(sc.readWrites); i++ {
		res += sc.writes[i].Len()
	}
	return
}
func (sc *StateCache) WriteSize() int { return sc.writeSize }
func (sc *StateCache) ReadSize() int  { return sc.readSize }
