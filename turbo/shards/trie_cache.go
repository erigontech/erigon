package shards

import (
	"bytes"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

// An optional addition to the state cache, helping to calculate state root

// Sizes of B-tree items for the purposes of keeping track of the size of reads and writes
// The sizes of the nodes of the B-tree are not accounted for, because their are private to the `btree` package
const (
	accountHashItemSize      = int(unsafe.Sizeof(AccountHashItem{}) + 16)
	accountHashWriteItemSize = int(unsafe.Sizeof(AccountHashWriteItem{}) + 16)
	storageHashItemSize      = int(unsafe.Sizeof(StorageHashItem{}) + 16)
	storageHashWriteItemSize = int(unsafe.Sizeof(StorageHashWriteItem{}) + 16)
)

type AccountHashItem struct {
	sequence       int
	queuePos       int
	flags          uint16
	branches       uint16
	children       uint16
	hashes         []common.Hash // TODO: store it as fixed size flat array?
	addrHashPrefix []byte
}

type AccountHashWriteItem struct {
	ai *AccountHashItem
}

func (awi *AccountHashWriteItem) GetCacheItem() CacheItem     { return awi.ai }
func (awi *AccountHashWriteItem) SetCacheItem(item CacheItem) { awi.ai = item.(*AccountHashItem) }
func (awi *AccountHashWriteItem) GetSize() int                { return accountHashWriteItemSize }
func (awi *AccountHashWriteItem) Less(than btree.Item) bool {
	return awi.ai.Less(than)
}

func (ahi *AccountHashItem) Less(than btree.Item) bool {
	switch i := than.(type) {
	case *AccountHashItem:
		return bytes.Compare(ahi.addrHashPrefix, i.addrHashPrefix) < 0
	case *AccountHashWriteItem:
		return bytes.Compare(ahi.addrHashPrefix, i.ai.addrHashPrefix) < 0
	default:
		panic(fmt.Sprintf("unexpected type: %T", than))
	}
}

func (ahi *AccountHashItem) GetSequence() int         { return ahi.sequence }
func (ahi *AccountHashItem) SetSequence(sequence int) { ahi.sequence = sequence }
func (ahi *AccountHashItem) GetSize() int             { return accountHashItemSize + len(ahi.addrHashPrefix) }
func (ahi *AccountHashItem) GetQueuePos() int         { return ahi.queuePos }
func (ahi *AccountHashItem) SetQueuePos(pos int)      { ahi.queuePos = pos }
func (ahi *AccountHashItem) HasFlag(flag uint16) bool { return ahi.flags&flag != 0 }
func (ahi *AccountHashItem) SetFlags(flags uint16)    { ahi.flags |= flags }
func (ahi *AccountHashItem) ClearFlags(flags uint16)  { ahi.flags &^= flags }
func (ahi *AccountHashItem) String() string {
	return fmt.Sprintf("AccountHashItem(addrHashPrefix=%x)", ahi.addrHashPrefix)
}

func (ahi *AccountHashItem) CopyValueFrom(item CacheItem) {
	other, ok := item.(*AccountHashItem)
	if !ok {
		panic(fmt.Sprintf("expected AccountHashItem, got %T", item))
	}
	ahi.hashes = make([]common.Hash, len(other.hashes))
	for i := 0; i < len(ahi.hashes); i++ {
		ahi.hashes[i] = other.hashes[i]
	}
	ahi.branches = other.branches
	ahi.children = other.children
}

type StorageHashWriteItem struct {
	i *StorageHashItem
}
type StorageHashItem struct {
	sequence      int
	queuePos      int
	flags         uint16
	branches      uint16
	children      uint16
	addrHash      common.Hash
	incarnation   uint64
	hashes        []common.Hash
	locHashPrefix []byte
}

func (wi *StorageHashWriteItem) GetCacheItem() CacheItem     { return wi.i }
func (wi *StorageHashWriteItem) SetCacheItem(item CacheItem) { wi.i = item.(*StorageHashItem) }
func (wi *StorageHashWriteItem) GetSize() int                { return storageHashWriteItemSize }
func (wi *StorageHashWriteItem) Less(than btree.Item) bool {
	return wi.i.Less(than.(*StorageHashWriteItem).i)
}

func (shi *StorageHashItem) Less(than btree.Item) bool {
	i := than.(*StorageHashItem)
	c := bytes.Compare(shi.addrHash.Bytes(), i.addrHash.Bytes())
	if c != 0 {
		return c < 0
	}
	if shi.incarnation != i.incarnation {
		return shi.incarnation < i.incarnation
	}
	return bytes.Compare(shi.locHashPrefix, i.locHashPrefix) < 0
}

func (shi *StorageHashItem) GetSequence() int         { return shi.sequence }
func (shi *StorageHashItem) SetSequence(sequence int) { shi.sequence = sequence }
func (shi *StorageHashItem) GetSize() int             { return storageHashItemSize + len(shi.locHashPrefix) }
func (shi *StorageHashItem) GetQueuePos() int         { return shi.queuePos }
func (shi *StorageHashItem) SetQueuePos(pos int)      { shi.queuePos = pos }
func (shi *StorageHashItem) HasFlag(flag uint16) bool { return shi.flags&flag != 0 }
func (shi *StorageHashItem) SetFlags(flags uint16)    { shi.flags |= flags }
func (shi *StorageHashItem) ClearFlags(flags uint16)  { shi.flags &^= flags }
func (shi *StorageHashItem) String() string {
	return fmt.Sprintf("StorageHashItem(addrHash=%x,incarnation=%d,locHashPrefix=%x)", shi.addrHash, shi.incarnation, shi.locHashPrefix)
}

func (shi *StorageHashItem) CopyValueFrom(item CacheItem) {
	other, ok := item.(*StorageHashItem)
	if !ok {
		panic(fmt.Sprintf("expected StorageHashItem, got %T", item))
	}
	shi.hashes = make([]common.Hash, len(other.hashes))
	for i := 0; i < len(shi.hashes); i++ {
		shi.hashes[i] = other.hashes[i]
	}
	shi.branches = other.branches
	shi.children = other.children
}

// UnprocessedHeap is a priority queue of items that were modified after the last recalculation of the merkle tree
type UnprocessedHeap struct {
	items []CacheItem
}

func (uh UnprocessedHeap) Len() int           { return len(uh.items) }
func (uh UnprocessedHeap) Less(i, j int) bool { return uh.items[i].Less(uh.items[j]) }
func (uh UnprocessedHeap) Swap(i, j int)      { uh.items[i], uh.items[j] = uh.items[j], uh.items[i] }
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

func (ai *AccountItem) HasPrefix(prefix CacheItem) bool {
	switch i := prefix.(type) {
	case *AccountItem:
		return ai.addrHash == i.addrHash && ai.account.Incarnation == i.account.Incarnation
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", prefix))
	}
}

func (si *StorageItem) HasPrefix(prefix CacheItem) bool {
	switch i := prefix.(type) {
	case *StorageItem:
		return si.addrHash == i.addrHash && si.incarnation == i.incarnation && si.locHash == i.locHash
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", prefix))
	}
}

func (ci *CodeItem) HasPrefix(prefix CacheItem) bool {
	switch i := prefix.(type) {
	case *CodeItem:
		return ci.addrHash == i.addrHash && ci.incarnation == i.incarnation
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", prefix))
	}
}

func (ahi *AccountHashItem) HasPrefix(prefix CacheItem) bool {
	switch i := prefix.(type) {
	case *AccountHashItem:
		return bytes.HasPrefix(ahi.addrHashPrefix, i.addrHashPrefix)
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", prefix))
	}
}

func (shi *StorageHashItem) HasPrefix(prefix CacheItem) bool {
	switch i := prefix.(type) {
	case *StorageHashItem:
		if shi.addrHash != i.addrHash || shi.incarnation != i.incarnation {
			return false
		}
		return bytes.HasPrefix(shi.locHashPrefix, i.locHashPrefix)
	default:
		panic(fmt.Sprintf("unrecognised type of cache item: %T", prefix))
	}
}

func (sc *StateCache) SetAccountHashesRead(prefix []byte, branches, children uint16, hashes []common.Hash) {
	if bits.OnesCount16(branches) != len(hashes) {
		panic(fmt.Errorf("invariant bits.OnesCount16(branches) == len(hashes) failed: %d, %d", bits.OnesCount16(branches), len(hashes)))
	}
	if (branches & children) != branches { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'branches is subset of children' failed: %b, %b", branches, children))
	}
	cpy := make([]common.Hash, len(hashes))
	for i := 0; i < len(hashes); i++ {
		cpy[i] = hashes[i]
	}
	ai := AccountHashItem{
		addrHashPrefix: common.CopyBytes(prefix),
		branches:       branches,
		children:       children,
		hashes:         cpy,
	}
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) SetAccountHashWrite(prefix []byte, branches, children uint16, hashes []common.Hash) {
	if bits.OnesCount16(branches) != len(hashes) {
		panic(fmt.Errorf("invariant bits.OnesCount16(branches) == len(hashes) failed: %d, %d", bits.OnesCount16(branches), len(hashes)))
	}
	if (branches & children) != branches { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'branches is subset of children' failed: %b, %b", branches, children))
	}
	cpy := make([]common.Hash, len(hashes))
	for i := 0; i < len(hashes); i++ {
		cpy[i] = hashes[i]
	}
	var ai AccountHashItem
	ai.addrHashPrefix = append(ai.addrHashPrefix[:0], prefix...)
	ai.branches = branches
	ai.children = children
	ai.hashes = cpy
	var awi AccountHashWriteItem
	awi.ai = &ai
	sc.setWrite(&ai, &awi, false /* delete */)
}

func (sc *StateCache) SetAccountHashDelete(prefix []byte) {
	var ai AccountHashItem
	var wi AccountHashWriteItem
	ai.addrHashPrefix = append(ai.addrHashPrefix[:0], prefix...)
	wi.ai = &ai
	sc.setWrite(&ai, &wi, true /* delete */)
}

func (sc *StateCache) SetStorageHashRead(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, branchChildren, children uint16, hashes []common.Hash) {
	cpy := make([]common.Hash, len(hashes))
	for i := 0; i < len(hashes); i++ {
		cpy[i] = hashes[i]
	}
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		branches:      branchChildren,
		children:      children,
		hashes:        cpy,
	}
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) SetStorageHashWrite(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, branchChildren, children uint16, hashes []common.Hash) {
	cpy := make([]common.Hash, len(hashes))
	for i := 0; i < len(hashes); i++ {
		cpy[i] = hashes[i]
	}
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		branches:      branchChildren,
		children:      children,
		hashes:        cpy,
	}
	var wi StorageHashWriteItem
	wi.i = &ai
	sc.setWrite(&ai, &wi, false /* delete */)
}

func (sc *StateCache) SetStorageHashDelete(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, branchChildren, children uint16, hashes []common.Hash) {
	cpy := make([]common.Hash, len(hashes))
	for i := 0; i < len(hashes); i++ {
		cpy[i] = hashes[i]
	}
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		branches:      branchChildren,
		children:      children,
		hashes:        cpy,
	}
	var wi StorageHashWriteItem
	wi.i = &ai
	sc.setWrite(&ai, &wi, true /* delete */)
}

func (sc *StateCache) AccountHashCount() int {
	var key AccountSeek
	return sc.readWrites[id(key)].Len()
}

func (sc *StateCache) HasAccountHashWithPrefix(addrHashPrefix []byte) bool {
	seek := &AccountHashItem{addrHashPrefix: addrHashPrefix}
	var found bool
	sc.readWrites[id(seek)].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		found = bytes.HasPrefix(i.(*AccountHashItem).addrHashPrefix, addrHashPrefix)
		return false
	})
	return found
}

func (sc *StateCache) GetAccountHash(prefix []byte) ([]byte, uint16, uint16, []common.Hash, bool) {
	var key AccountHashItem
	key.addrHashPrefix = prefix
	if item, ok := sc.get(&key); ok {
		if item != nil {
			i := item.(*AccountHashItem)
			return i.addrHashPrefix, i.branches, i.children, i.hashes, true
		}
		return nil, 0, 0, nil, true
	}
	return nil, 0, 0, nil, false
}

func (sc *StateCache) GetStorageHash(addrHash common.Hash, incarnation uint64, prefix []byte) ([]byte, uint16, uint16, []common.Hash, bool) {
	key := StorageHashItem{addrHash: addrHash, incarnation: incarnation, locHashPrefix: prefix}
	if item, ok := sc.get(&key); ok {
		if item != nil {
			i := item.(*StorageHashItem)
			return i.locHashPrefix, i.branches, i.children, i.hashes, true
		}
		return nil, 0, 0, nil, true
	}
	return nil, 0, 0, nil, false
}

func (sc *StateCache) DebugPrintAccounts() error {
	var cur *AccountHashItem
	id := id(cur)
	rw := sc.writes[id]
	rw.Ascend(func(i btree.Item) bool {
		it := i.(*AccountHashWriteItem)
		if it.ai.HasFlag(AbsentFlag) || it.ai.HasFlag(DeletedFlag) {
			fmt.Printf("deleted: %x\n", it.ai.addrHashPrefix)
		} else if it.ai.HasFlag(ModifiedFlag) {
			fmt.Printf("modified: %x\n", it.ai.addrHashPrefix)
		} else {
			fmt.Printf("normal: %x\n", it.ai.addrHashPrefix)
		}
		return true
	})

	return nil
}

func (sc *StateCache) AccountHashesTree(canUse func([]byte) bool, prefix []byte, walker func(prefix []byte, h common.Hash, skipState bool) error) error {
	var cur []byte
	seek := make([]byte, 0, 64)
	seek = append(seek, prefix...)
	var k [64][]byte
	var branch, child [64]uint16
	var id, hashID, maxID [64]int8
	var hashes [64][]common.Hash
	var lvl int
	var ok bool
	var isChild = func() bool { return (uint16(1)<<id[lvl])&child[lvl] != 0 }
	var isBranch = func() bool { return (uint16(1)<<id[lvl])&branch[lvl] != 0 }
	skipState := true

	ihK, branches, children, hashItem := sc.AccountHashesSeek(prefix)
GotItemFromCache:
	for ihK != nil { // go to sibling in cache
		lvl = len(ihK)
		k[lvl], branch[lvl], child[lvl], hashes[lvl] = ihK, branches, children, hashItem
		hashID[lvl], id[lvl], maxID[lvl] = -1, int8(bits.TrailingZeros16(children))-1, int8(bits.Len16(children))

		if prefix != nil && !bytes.HasPrefix(k[lvl], prefix) {
			return nil
		}

		for ; lvl > 0; lvl-- { // go to parent sibling in mem
			//for i := lvl; i < len(hashID); i++ {
			//	hashID[i] = 0
			//}
			cur = append(append(cur[:0], k[lvl]...), 0)
			for id[lvl]++; id[lvl] <= maxID[lvl]; id[lvl]++ { // go to sibling
				if !isChild() {
					continue
				}

				cur[len(cur)-1] = uint8(id[lvl])
				if !isBranch() {
					skipState = false
					continue
				}
				hashID[lvl]++
				if canUse(cur) {
					if err := walker(cur, hashes[lvl][hashID[lvl]], skipState); err != nil {
						return err
					}
					skipState = true
					continue // cache item can be used and exists in cache, then just go to next sibling
				}

				ihK, branches, children, hashItem, ok = sc.GetAccountHash(cur)
				if ok {
					continue GotItemFromCache
				}
				skipState = false
			}
		}

		ok = dbutils.NextNibblesSubtree(k[1], &seek)
		if !ok {
			break
		}
		ihK, branches, children, hashItem = sc.AccountHashesSeek(seek)
	}

	return nil
}

func (sc *StateCache) AccountHashesSeek(prefix []byte) ([]byte, uint16, uint16, []common.Hash) {
	var cur *AccountHashItem
	seek := &AccountHashItem{}
	id := id(seek)
	seek.addrHashPrefix = append(seek.addrHashPrefix[:0], prefix...)
	sc.readWrites[id].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		it := i.(*AccountHashItem)
		if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
			return true
		}
		cur = it // found
		return false
	})
	if cur == nil {
		return nil, 0, 0, nil
	}
	return cur.addrHashPrefix, cur.branches, cur.children, cur.hashes
}

func (sc *StateCache) StorageHashesSeek(addrHash common.Hash, incarnation uint64, prefix []byte) ([]byte, uint16, uint16, []common.Hash) {
	var cur *StorageHashItem
	seek := &StorageHashItem{}
	id := id(seek)
	seek.addrHash.SetBytes(addrHash.Bytes())
	seek.incarnation = incarnation
	seek.locHashPrefix = prefix
	sc.readWrites[id].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		it := i.(*StorageHashItem)
		if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
			return true
		}
		if it.addrHash != addrHash {
			return false
		}
		if it.incarnation != incarnation {
			return false
		}
		cur = it
		return false
	})
	if cur == nil {
		return nil, 0, 0, nil
	}
	return cur.locHashPrefix, cur.branches, cur.children, cur.hashes
}

func WalkAccountHashesWrites(writes [5]*btree.BTree, update func(prefix []byte, branchChildren, children uint16, h []common.Hash), del func(prefix []byte, branchChildren, children uint16, h []common.Hash)) {
	id := id(&AccountHashWriteItem{})
	writes[id].Ascend(func(i btree.Item) bool {
		it := i.(*AccountHashWriteItem)
		if it.ai.HasFlag(AbsentFlag) || it.ai.HasFlag(DeletedFlag) {
			del(it.ai.addrHashPrefix, it.ai.branches, it.ai.children, it.ai.hashes)
			return true
		}
		update(it.ai.addrHashPrefix, it.ai.branches, it.ai.children, it.ai.hashes)
		return true
	})
}

func (sc *StateCache) WalkStorageHashes(walker func(addrHash common.Hash, incarnation uint64, prefix []byte, branchChildren, children uint16, h []common.Hash) error) error {
	id := id(&StorageHashItem{})
	sc.readWrites[id].Ascend(func(i btree.Item) bool {
		it, ok := i.(*StorageHashItem)
		if !ok {
			return true
		}
		if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
			return true
		}
		if err := walker(it.addrHash, it.incarnation, it.locHashPrefix, it.branches, it.children, it.hashes); err != nil {
			panic(err)
		}
		return true
	})
	return nil
}

func (sc *StateCache) StorageHashes(adrHash common.Hash, incarnation uint64, walker func(prefix []byte, h common.Hash) error) error {
	var cur, prev *StorageHashItem
	next := &StorageHashItem{addrHash: adrHash, incarnation: incarnation, locHashPrefix: make([]byte, 0, 64)}
	step := func(i btree.Item) bool {
		it := i.(*StorageHashItem)
		if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
			return true
		}
		cur = it // found
		return false
	}
	rw := sc.readWrites[id(cur)]
	rw.AscendGreaterOrEqual(next, step)
	var ihK []byte
	for {
		if cur == nil {
			break
		}
		if cur.addrHash != adrHash || cur.incarnation != incarnation {
			break
		}
		hashId := 0
		if len(cur.locHashPrefix) == 0 { // root record, firstly storing root hash
			if err := walker([]byte{}, cur.hashes[0]); err != nil {
				return err
			}
			hashId++
		}
		for i := 0; i < 16; i++ {
			if ((uint16(1) << i) & cur.branches) == 0 {
				continue
			}
			ihK = append(append(ihK[:0], cur.locHashPrefix...), uint8(i))
			if err := walker(common.CopyBytes(ihK), cur.hashes[hashId]); err != nil {
				return err
			}
			hashId++
		}
		prev = cur
		cur = nil
		ok := dbutils.NextNibblesSubtree(prev.locHashPrefix, &next.locHashPrefix) // go to sibling
		if !ok {
			break
		}
		rw.AscendGreaterOrEqual(next, step)
	}
	if err := walker(nil, common.Hash{}); err != nil {
		return err
	}
	return nil
}

func WalkStorageHashesWrites(writes [5]*btree.BTree, update func(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, branchChildren, children uint16, h []common.Hash), del func(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, branchChildren, children uint16, h []common.Hash)) {
	id := id(&StorageWriteItem{})
	writes[id].Ascend(func(i btree.Item) bool {
		it := i.(*StorageHashWriteItem)
		if it.i.HasFlag(AbsentFlag) || it.i.HasFlag(DeletedFlag) {
			del(it.i.addrHash, it.i.incarnation, it.i.locHashPrefix, it.i.branches, it.i.children, it.i.hashes)
			return true
		}
		update(it.i.addrHash, it.i.incarnation, it.i.locHashPrefix, it.i.branches, it.i.children, it.i.hashes)
		return true
	})
}

func (sc *StateCache) WalkStorage(addrHash common.Hash, incarnation uint64, prefix []byte, walker func(locHash common.Hash, val []byte) error) error {
	seek := &StorageSeek{seek: prefix}
	id := id(seek)
	sc.readWrites[id].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		switch it := i.(type) {
		case *StorageItem:
			if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
				return true
			}
			if it.addrHash != addrHash || it.incarnation != incarnation {
				return false
			}
			if err := walker(it.locHash, it.value.Bytes()); err != nil {
				panic(err)
			}
		case *StorageWriteItem:
			if it.si.HasFlag(AbsentFlag) || it.si.HasFlag(DeletedFlag) {
				return true
			}
			if it.si.addrHash != addrHash || it.si.incarnation != incarnation {
				return false
			}
			if err := walker(it.si.locHash, it.si.value.Bytes()); err != nil {
				panic(err)
			}
		}
		return true
	})
	return nil
}

func (sc *StateCache) WalkAccounts(prefix []byte, walker func(addrHash common.Hash, acc *accounts.Account) (bool, error)) error {
	seek := &AccountSeek{seek: prefix}
	id := id(seek)
	sc.readWrites[id].AscendGreaterOrEqual(seek, func(i btree.Item) bool {
		switch it := i.(type) {
		case *AccountItem:
			if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
				return true
			}
			if goOn, err := walker(it.addrHash, &it.account); err != nil {
				panic(err)
			} else if !goOn {
				return false
			}
		case *AccountWriteItem:
			if it.ai.HasFlag(AbsentFlag) || it.ai.HasFlag(DeletedFlag) {
				return true
			}
			if goOn, err := walker(it.ai.addrHash, &it.ai.account); err != nil {
				panic(err)
			} else if !goOn {
				return false
			}
		}
		return true
	})
	return nil
}
