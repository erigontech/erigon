package shards

import (
	"bytes"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/google/btree"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
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
	sequence                   int
	queuePos                   int
	flags                      uint16
	hasState, hasTree, hasHash uint16
	hashes                     []libcommon.Hash // TODO: store it as fixed size flat array?
	addrHashPrefix             []byte
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
	ahi.hashes = make([]libcommon.Hash, len(other.hashes))
	for i := 0; i < len(ahi.hashes); i++ {
		ahi.hashes[i] = other.hashes[i]
	}
	ahi.hasState = other.hasState
	ahi.hasTree = other.hasTree
	ahi.hasHash = other.hasHash
}

type StorageHashWriteItem struct {
	i *StorageHashItem
}
type StorageHashItem struct {
	flags, hasState, hasTree, hasHash uint16
	sequence, queuePos                int
	addrHash                          libcommon.Hash
	incarnation                       uint64
	locHashPrefix                     []byte
	hashes                            []libcommon.Hash
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
	shi.hashes = make([]libcommon.Hash, len(other.hashes))
	for i := 0; i < len(shi.hashes); i++ {
		shi.hashes[i] = other.hashes[i]
	}
	shi.hasState = other.hasState
	shi.hasTree = other.hasTree
	shi.hasHash = other.hasHash
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

func (sc *StateCache) SetAccountHashesRead(prefix []byte, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
	if bits.OnesCount16(hasHash) != len(hashes) {
		panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)))
	}
	assertSubset(hasTree, hasState)
	assertSubset(hasHash, hasState)
	cpy := make([]libcommon.Hash, len(hashes))
	copy(cpy, hashes)
	ai := AccountHashItem{
		addrHashPrefix: common.CopyBytes(prefix),
		hasState:       hasState,
		hasTree:        hasTree,
		hasHash:        hasHash,
		hashes:         cpy,
	}
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) SetAccountHashWrite(prefix []byte, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
	if bits.OnesCount16(hasHash) != len(hashes) {
		panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)))
	}
	assertSubset(hasTree, hasState)
	assertSubset(hasHash, hasState)
	ai := AccountHashItem{
		addrHashPrefix: common.CopyBytes(prefix),
		hasState:       hasState,
		hasTree:        hasTree,
		hasHash:        hasHash,
		hashes:         make([]libcommon.Hash, len(hashes)),
	}
	copy(ai.hashes, hashes)
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

func (sc *StateCache) SetStorageHashRead(addrHash libcommon.Hash, incarnation uint64, locHashPrefix []byte, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
	cpy := make([]libcommon.Hash, len(hashes))
	copy(cpy, hashes)
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		hasState:      hasState,
		hasTree:       hasTree,
		hasHash:       hasHash,
		hashes:        cpy,
	}
	sc.setRead(&ai, false /* absent */)
}

func (sc *StateCache) SetStorageHashWrite(addrHash libcommon.Hash, incarnation uint64, locHashPrefix []byte, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
	cpy := make([]libcommon.Hash, len(hashes))
	copy(cpy, hashes)
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		hasState:      hasState,
		hasTree:       hasTree,
		hasHash:       hasHash,
		hashes:        cpy,
	}
	var wi StorageHashWriteItem
	wi.i = &ai
	sc.setWrite(&ai, &wi, false /* delete */)
}

func (sc *StateCache) SetStorageHashDelete(addrHash libcommon.Hash, incarnation uint64, locHashPrefix []byte, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
	cpy := make([]libcommon.Hash, len(hashes))
	copy(cpy, hashes)
	ai := StorageHashItem{
		addrHash:      addrHash,
		incarnation:   incarnation,
		locHashPrefix: common.CopyBytes(locHashPrefix),
		hasState:      hasState,
		hasTree:       hasTree,
		hasHash:       hasHash,
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

func (sc *StateCache) GetAccountHash(prefix []byte) ([]byte, uint16, uint16, uint16, []libcommon.Hash, bool) {
	var key AccountHashItem
	key.addrHashPrefix = prefix
	if item, ok := sc.get(&key); ok {
		if item != nil {
			i := item.(*AccountHashItem)
			return i.addrHashPrefix, i.hasState, i.hasTree, i.hasHash, i.hashes, true
		}
		return nil, 0, 0, 0, nil, true
	}
	return nil, 0, 0, 0, nil, false
}

func (sc *StateCache) GetStorageHash(addrHash libcommon.Hash, incarnation uint64, prefix []byte) ([]byte, uint16, uint16, uint16, []libcommon.Hash, bool) {
	key := StorageHashItem{addrHash: addrHash, incarnation: incarnation, locHashPrefix: prefix}
	if item, ok := sc.get(&key); ok {
		if item != nil {
			i := item.(*StorageHashItem)
			return i.locHashPrefix, i.hasState, i.hasTree, i.hasHash, i.hashes, true
		}
		return nil, 0, 0, 0, nil, true
	}
	return nil, 0, 0, 0, nil, false
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

func (sc *StateCache) AccountTree(prefix []byte, walker func(k []byte, h libcommon.Hash, hasTree, hasHash bool) (toChild bool, err error), onMiss func(k []byte)) error {
	var cur []byte
	seek := make([]byte, 0, 64)
	buf := make([]byte, 0, 64)
	next := make([]byte, 0, 64)
	seek = append(seek, prefix...)
	var k [64][]byte
	var hasTree, hasState, hasHash [64]uint16
	var id, hashID [64]int16
	var hashes [64][]libcommon.Hash
	var lvl int
	var ok bool
	var (
		ihK                                    []byte
		hasStateItem, hasTreeItem, hasHashItem uint16
		hashItem                               []libcommon.Hash
	)
	var _hasChild = func() bool { return (1<<id[lvl])&hasState[lvl] != 0 }
	var _hasTree = func() bool { return (1<<id[lvl])&hasTree[lvl] != 0 }
	var _hasHash = func() bool { return (1<<id[lvl])&hasHash[lvl] != 0 }
	var _unmarshal = func() {
		from, to := lvl+1, len(k)
		if lvl >= len(k) {
			from, to = len(k)+1, lvl+2
		}
		for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			k[i], hasState[i], hasTree[i], hasHash[i], hashID[i], id[i], hashes[i] = nil, 0, 0, 0, 0, 0, nil
		}
		lvl = len(ihK)
		k[lvl], hasState[lvl], hasTree[lvl], hasHash[lvl], hashes[lvl] = ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem
		hashID[lvl], id[lvl] = -1, int16(bits.TrailingZeros16(hasStateItem))-1
	}
	var _nextSiblingInMem = func() bool {
		for id[lvl]++; id[lvl] < int16(bits.Len16(hasState[lvl])); id[lvl]++ { // go to sibling
			if !_hasChild() {
				continue
			}

			if _hasHash() {
				hashID[lvl]++
				return true
			}
			if _hasTree() {
				return true
			}
		}
		return false
	}
	var _seek = func(seek []byte, withinPrefix []byte) bool {
		ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem = sc.AccountHashesSeek(seek)
		if ihK == nil || !bytes.HasPrefix(ihK, withinPrefix) || !bytes.HasPrefix(ihK, prefix) {
			k[lvl] = nil
			return false
		}
		_unmarshal()
		_nextSiblingInMem()
		return true
	}
	var _nextSiblingOfParentInMem = func() bool {
		for lvl > 1 { // go to parent sibling in mem
			if k[lvl-1] == nil {
				nonNilLvl := lvl - 1
				for ; k[nonNilLvl] == nil && nonNilLvl > 1; nonNilLvl-- {
				}
				next = append(append(next[:0], k[lvl]...), uint8(id[lvl]))
				buf = append(append(buf[:0], k[nonNilLvl]...), uint8(id[nonNilLvl]))
				if _seek(next, buf) {
					return true
				}
				lvl = nonNilLvl + 1
				continue
			}
			lvl--
			// END of _nextSiblingOfParentInMem
			if _nextSiblingInMem() {
				return true
			}
		}
		return false
	}
	var _nextSiblingInDB = func() bool {
		if ok = dbutils.NextNibblesSubtree(k[lvl], &seek); !ok {
			k[lvl] = nil
			return false
		}
		_seek(seek, []byte{})
		return k[lvl] != nil
	}

	_seek(prefix, []byte{})

	for k[lvl] != nil && bytes.HasPrefix(k[lvl], prefix) { // go to sibling in cache
		cur = append(append(cur[:0], k[lvl]...), uint8(id[lvl]))
		toChild, err := walker(cur, hashes[lvl][hashID[lvl]], _hasTree(), _hasHash())
		if err != nil {
			return err
		}

		// preOrderTraversalStep
		if toChild && _hasTree() {
			next = append(append(next[:0], k[lvl]...), uint8(id[lvl]))
			ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem, ok = sc.GetAccountHash(next)
			if ok {
				_unmarshal()
				_nextSiblingInMem()
				continue
			}
			onMiss(cur)
		}
		_ = _nextSiblingInMem() || _nextSiblingOfParentInMem() || _nextSiblingInDB()
	}

	if _, err := walker(nil, libcommon.Hash{}, false, false); err != nil {
		return err
	}
	return nil
}

func (sc *StateCache) StorageTree(prefix []byte, accHash libcommon.Hash, incarnation uint64, walker func(k []byte, h libcommon.Hash, hasTree, hasHash bool) (toChild bool, err error), onMiss func(k []byte)) error {
	var cur []byte
	seek := make([]byte, 0, 64)
	buf := make([]byte, 0, 64)
	next := make([]byte, 0, 64)
	seek = append(seek, prefix...)
	var k [64][]byte
	var hasTree, hasState, hasHash [64]uint16
	var id, hashID [64]int16
	var hashes [64][]libcommon.Hash
	var lvl int
	var ok bool
	var (
		ihK                                    []byte
		hasStateItem, hasTreeItem, hasHashItem uint16
		hashItem                               []libcommon.Hash
	)
	var _hasChild = func() bool { return (1<<id[lvl])&hasState[lvl] != 0 }
	var _hasBranch = func() bool { return (1<<id[lvl])&hasTree[lvl] != 0 }
	var _hasHash = func() bool { return (1<<id[lvl])&hasHash[lvl] != 0 }
	var _unmarshal = func() {
		from, to := lvl+1, len(k)
		if lvl >= len(k) {
			from, to = len(k)+1, lvl+2
		}
		for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			k[i], hasState[i], hasTree[i], hasHash[i], hashID[i], id[i], hashes[i] = nil, 0, 0, 0, 0, 0, nil
		}
		lvl = len(ihK)
		k[lvl], hasState[lvl], hasTree[lvl], hasHash[lvl], hashes[lvl] = ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem
		hashID[lvl], id[lvl] = -1, int16(bits.TrailingZeros16(hasStateItem))-1
	}
	var _nextSiblingInMem = func() bool {
		for id[lvl]++; id[lvl] < int16(bits.Len16(hasState[lvl])); id[lvl]++ { // go to sibling
			if !_hasChild() {
				continue
			}

			if _hasHash() {
				hashID[lvl]++
				return true
			}
			if _hasBranch() {
				return true
			}
		}
		return false
	}
	var _seek = func(seek []byte, withinPrefix []byte) bool {
		ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem = sc.StorageHashesSeek(accHash, incarnation, seek)
		if ihK == nil || !bytes.HasPrefix(ihK, withinPrefix) || !bytes.HasPrefix(ihK, prefix) {
			k[lvl] = nil
			return false
		}
		_unmarshal()
		_nextSiblingInMem()
		return true
	}
	var _nextSiblingOfParentInMem = func() bool {
		for lvl > 0 { // go to parent sibling in mem
			if k[lvl-1] == nil {
				nonNilLvl := lvl - 1
				for ; k[nonNilLvl] == nil && nonNilLvl > 1; nonNilLvl-- {
				}
				next = append(append(next[:0], k[lvl]...), uint8(id[lvl]))
				buf = append(append(buf[:0], k[nonNilLvl]...), uint8(id[nonNilLvl]))
				if _seek(next, buf) {
					return true
				}
				lvl = nonNilLvl + 1
				continue
			}
			lvl--
			// END of _nextSiblingOfParentInMem
			if _nextSiblingInMem() {
				return true
			}
		}
		return false
	}
	var _nextSiblingInDB = func() bool {
		if ok = dbutils.NextNibblesSubtree(k[lvl], &seek); !ok {
			k[lvl] = nil
			return false
		}
		_seek(seek, []byte{})
		return k[lvl] != nil
	}

	_seek(prefix, []byte{})

	for k[lvl] != nil && bytes.HasPrefix(k[lvl], prefix) { // go to sibling in cache
		cur = append(append(cur[:0], k[lvl]...), uint8(id[lvl]))
		toChild, err := walker(cur, hashes[lvl][hashID[lvl]], _hasBranch(), _hasHash())
		if err != nil {
			return err
		}

		if toChild && _hasBranch() {
			next = append(append(next[:0], k[lvl]...), uint8(id[lvl]))
			ihK, hasStateItem, hasTreeItem, hasHashItem, hashItem, ok = sc.GetStorageHash(accHash, incarnation, next)
			if ok {
				_unmarshal()
				_nextSiblingInMem()
				continue
			}
			onMiss(cur)
		}
		_ = _nextSiblingInMem() || _nextSiblingOfParentInMem() || _nextSiblingInDB()
	}

	if _, err := walker(nil, libcommon.Hash{}, false, false); err != nil {
		return err
	}
	return nil
}

func (sc *StateCache) AccountHashesSeek(prefix []byte) ([]byte, uint16, uint16, uint16, []libcommon.Hash) {
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
		return nil, 0, 0, 0, nil
	}
	return cur.addrHashPrefix, cur.hasState, cur.hasTree, cur.hasHash, cur.hashes
}

func (sc *StateCache) StorageHashesSeek(addrHash libcommon.Hash, incarnation uint64, prefix []byte) ([]byte, uint16, uint16, uint16, []libcommon.Hash) {
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
		return nil, 0, 0, 0, nil
	}
	return cur.locHashPrefix, cur.hasState, cur.hasTree, cur.hasHash, cur.hashes
}

func WalkAccountHashesWrites(writes [5]*btree.BTree, update func(prefix []byte, hasState, hasTree, hasHash uint16, h []libcommon.Hash), del func(prefix []byte, hasState, hasTree, hasHash uint16, h []libcommon.Hash)) {
	id := id(&AccountHashWriteItem{})
	writes[id].Ascend(func(i btree.Item) bool {
		it := i.(*AccountHashWriteItem)
		if it.ai.HasFlag(AbsentFlag) || it.ai.HasFlag(DeletedFlag) {
			del(it.ai.addrHashPrefix, it.ai.hasState, it.ai.hasTree, it.ai.hasHash, it.ai.hashes)
			return true
		}
		update(it.ai.addrHashPrefix, it.ai.hasState, it.ai.hasTree, it.ai.hasHash, it.ai.hashes)
		return true
	})
}

func (sc *StateCache) WalkStorageHashes(walker func(addrHash libcommon.Hash, incarnation uint64, prefix []byte, hasStat, hasTree, hasHash uint16, h []libcommon.Hash) error) error {
	id := id(&StorageHashItem{})
	sc.readWrites[id].Ascend(func(i btree.Item) bool {
		it, ok := i.(*StorageHashItem)
		if !ok {
			return true
		}
		if it.HasFlag(AbsentFlag) || it.HasFlag(DeletedFlag) {
			return true
		}
		if err := walker(it.addrHash, it.incarnation, it.locHashPrefix, it.hasState, it.hasTree, it.hasHash, it.hashes); err != nil {
			panic(err)
		}
		return true
	})
	return nil
}

func WalkStorageHashesWrites(writes [5]*btree.BTree, update func(addrHash libcommon.Hash, incarnation uint64, locHashPrefix []byte, hasState, hasTree, hasHash uint16, h []libcommon.Hash), del func(addrHash libcommon.Hash, incarnation uint64, locHashPrefix []byte, hasStat, hasTree, hasHash uint16, h []libcommon.Hash)) {
	id := id(&StorageWriteItem{})
	writes[id].Ascend(func(i btree.Item) bool {
		it := i.(*StorageHashWriteItem)
		if it.i.HasFlag(AbsentFlag) || it.i.HasFlag(DeletedFlag) {
			del(it.i.addrHash, it.i.incarnation, it.i.locHashPrefix, it.i.hasState, it.i.hasTree, it.i.hasHash, it.i.hashes)
			return true
		}
		update(it.i.addrHash, it.i.incarnation, it.i.locHashPrefix, it.i.hasState, it.i.hasTree, it.i.hasHash, it.i.hashes)
		return true
	})
}

func (sc *StateCache) WalkStorage(addrHash libcommon.Hash, incarnation uint64, prefix []byte, walker func(locHash libcommon.Hash, val []byte) error) error {
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

func (sc *StateCache) WalkAccounts(prefix []byte, walker func(addrHash libcommon.Hash, acc *accounts.Account) (bool, error)) error {
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

func assertSubset(a, b uint16) {
	if (a & b) != a { // a & b == a - checks whether a is subset of b
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}
