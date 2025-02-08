package state

import (
	"fmt"
	"sync"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
)

const FlagDone = 0
const FlagEstimate = 1

type AccountPath int8

func (p AccountPath) String() string {
	switch p {
	case AddressPath:
		return "Address"
	case BalancePath:
		return "Balance"
	case NoncePath:
		return "Nonce"
	case CodePath:
		return "Code"
	case CodeHashPath:
		return "Code Hash"
	case CodeSizePath:
		return "Code Size"
	case SelfDestructPath:
		return "Destruct"
	case StatePath:
		return "State"
	default:
		return fmt.Sprintf(" Unknown %d", p)
	}
}

const (
	AddressPath = iota
	BalancePath
	NoncePath
	CodePath
	CodeHashPath
	CodeSizePath
	SelfDestructPath
	StatePath
)

type VersionKey struct {
	addr    *common.Address
	key     *common.Hash
	subpath AccountPath
}

func (k VersionKey) String() string {
	var keyType string
	var subpath string
	var key string

	if k.key != nil {
		keyType = "State"
		key = fmt.Sprintf("%x %x", k.addr, k.key)
	} else {
		key = fmt.Sprintf("%x", k.addr)

		switch k.subpath {
		case AddressPath:
			keyType = "Address"
		case BalancePath:
			keyType = "Balance"
		case NoncePath:
			keyType = "Nonce"
		case CodePath:
			keyType = "Code"
		case CodeHashPath:
			keyType = "Code Hash"
		case CodeSizePath:
			keyType = "Code Size"
		case SelfDestructPath:
			keyType = "Destruct"
		default:
			keyType = "Path"
			subpath = fmt.Sprintf(" Unknown %d", k.subpath)
		}
	}

	return fmt.Sprintf("%-9s %s%s", keyType, key, subpath)
}

func (k VersionKey) IsAddress() bool {
	return k.subpath == AddressPath && k.key == nil
}

func (k VersionKey) IsState() bool {
	return k.key != nil
}

func (k VersionKey) IsSubpath() bool {
	return k.subpath != AddressPath && k.key == nil
}

func (k VersionKey) IsCode() bool {
	return k.subpath == CodePath && k.key == nil
}

func (k VersionKey) IsNonce() bool {
	return k.subpath == NoncePath && k.key == nil
}

func (k VersionKey) IsBalance() bool {
	return k.subpath == BalancePath && k.key == nil
}

func (k VersionKey) GetAddress() common.Address {
	if k.addr == nil {
		return common.Address{}
	}
	return *k.addr
}

func (k VersionKey) GetStateKey() common.Hash {
	if k.key == nil {
		return common.Hash{}
	}
	return *k.key
}

func (k VersionKey) GetSubpath() AccountPath {
	return k.subpath
}

func AddressKey(addr *common.Address) VersionKey {
	return VersionKey{
		addr: addr,
	}
}

func StateKey(addr *common.Address, hash *common.Hash) VersionKey {
	return VersionKey{
		subpath: StatePath,
		addr:    addr,
		key:     hash}
}

func SubpathKey(addr *common.Address, subpath AccountPath) VersionKey {
	return VersionKey{
		addr:    addr,
		subpath: subpath,
	}
}

func VersionKeyLess(a, b *VersionKey) bool {
	sdiff := a.subpath - b.subpath
	switch {
	case sdiff > 0:
		return true
	case sdiff < 0:
		return false
	default:
		switch a.addr.Cmp(*b.addr) {
		case 1:
			return false
		case -1:
			return true
		default:
			if a.key == nil {
				return b.key != nil
			} else if b.key == nil {
				return false
			}

			return a.key.Cmp(*b.key) < 0
		}
	}
}

type AccountKey struct {
	Path AccountPath
	Key  libcommon.Hash
}

func (k AccountKey) String() string {
	if k.Path == StatePath {
		return fmt.Sprintf("%x", k.Key)
	}

	return k.Path.String()
}

type VersionMap1 struct {
	mu    sync.RWMutex
	s     map[libcommon.Address]map[AccountKey]*TxIndexCells
	trace bool
}

func (vm *VersionMap1) getKeyCells(addr libcommon.Address, path AccountPath, key libcommon.Hash, fNoKey func(addr libcommon.Address, path AccountPath, key libcommon.Hash) *TxIndexCells) (cells *TxIndexCells) {
	it, ok := vm.s[addr]

	if ok {
		cells, ok = it[AccountKey{path, key}]
	}

	if !ok {
		cells = fNoKey(addr, path, key)
	}

	return
}

func (vm *VersionMap1) Write(addr libcommon.Address, path AccountPath, key libcommon.Hash, v Version, data interface{}, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(addr libcommon.Address, path AccountPath, key libcommon.Hash) (cells *TxIndexCells) {
		it, ok := vm.s[addr]
		cells = &TxIndexCells{
			rw: sync.RWMutex{},
			tm: &btree.Map[int, *WriteCell]{},
		}
		if ok {
			it[AccountKey{path, key}] = cells
		} else {
			vm.s[addr] = map[AccountKey]*TxIndexCells{
				{path, key}: cells,
			}
		}
		return
	})

	ci, ok := cells.tm.Get(v.TxIndex)

	var flag uint = FlagDone

	if !complete {
		flag = FlagEstimate
	}

	if ok {
		if ci.incarnation > v.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %x %s, %v", addr, AccountKey{path, key}, v.TxIndex))
		}

		ci.flag = flag
		ci.incarnation = v.Incarnation
		ci.data = data
	} else {
		if ci, ok = cells.tm.Get(v.TxIndex); !ok {
			cells.tm.Set(v.TxIndex, &WriteCell{
				flag:        flag,
				incarnation: v.Incarnation,
				data:        data,
			})
		} else {
			ci.flag = flag
			ci.incarnation = v.Incarnation
			ci.data = data
		}
	}
}

func (vm *VersionMap1) MarkEstimate(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *TxIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x %s, cells keys: %v", txIdx, addr, AccountKey{path, key}, cells.tm.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap1) MarkComplete(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *TxIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x s, cells keys: %v", txIdx, AccountKey{path, key}, cells.tm.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap1) Delete(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int, checkExists bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *TxIndexCells { return nil })

	if cells == nil {
		if !checkExists {
			return
		}

		panic(fmt.Errorf("path must already exist"))
	}

	cells.tm.Delete(txIdx)
}

type TxIndexCells struct {
	rw sync.RWMutex
	tm *btree.Map[int, *WriteCell]
}

type WriteCell struct {
	flag        uint
	incarnation int
	data        interface{}
}

type VersionMap struct {
	s     *btree.BTreeG[vmItem]
	trace bool
}

type vmItem struct {
	k VersionKey
	v *TxIndexCells
}

func vmiLess(a, b vmItem) bool {
	return VersionKeyLess(&a.k, &b.k)
}

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation int
}

func NewVersionMap() *VersionMap {
	return &VersionMap{
		s: btree.NewBTreeG[vmItem](vmiLess),
	}
}

func (vm *VersionMap) getKeyCells(k VersionKey, fNoKey func(kenc VersionKey) *TxIndexCells) (cells *TxIndexCells) {
	it, ok := vm.s.Get(vmItem{k: k})

	if !ok {
		cells = fNoKey(k)
	} else {
		cells = it.v
	}

	return
}

func (vm *VersionMap) Write(k VersionKey, v Version, data interface{}, complete bool) {
	cells := vm.getKeyCells(k, func(kenc VersionKey) (cells *TxIndexCells) {
		if it, ok := vm.s.Get(vmItem{k: k}); ok {
			cells = it.v
		} else {
			cells = &TxIndexCells{
				rw: sync.RWMutex{},
				tm: &btree.Map[int, *WriteCell]{},
			}
			vm.s.Set(vmItem{k: k, v: cells})
		}
		return
	})

	cells.rw.Lock()
	defer cells.rw.Unlock()
	ci, ok := cells.tm.Get(v.TxIndex)

	var flag uint = FlagDone

	if !complete {
		flag = FlagEstimate
	}

	if ok {
		if ci.incarnation > v.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %v, %v",
				k, v.TxIndex))
		}

		ci.flag = flag
		ci.incarnation = v.Incarnation
		ci.data = data
	} else {
		if ci, ok = cells.tm.Get(v.TxIndex); !ok {
			cells.tm.Set(v.TxIndex, &WriteCell{
				flag:        flag,
				incarnation: v.Incarnation,
				data:        data,
			})
		} else {
			ci.flag = flag
			ci.incarnation = v.Incarnation
			ci.data = data
		}
	}
}

func (vm *VersionMap) MarkEstimate(k VersionKey, txIdx int) {
	cells := vm.getKeyCells(k, func(_ VersionKey) *TxIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.RLock()
	defer cells.rw.RUnlock()
	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x, cells keys: %v", txIdx, k, cells.tm.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap) MarkComplete(k VersionKey, txIdx int) {
	cells := vm.getKeyCells(k, func(_ VersionKey) *TxIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.RLock()
	defer cells.rw.RUnlock()
	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x, cells keys: %v", txIdx, k, cells.tm.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap) Delete(k VersionKey, txIdx int, checkExists bool) {
	cells := vm.getKeyCells(k, func(_ VersionKey) *TxIndexCells { return nil })

	if cells == nil {
		if !checkExists {
			return
		}

		panic(fmt.Errorf("path must already exist"))
	}

	cells.rw.Lock()
	defer cells.rw.Unlock()
	cells.tm.Delete(txIdx)
}

const (
	MVReadResultDone       = 0
	MVReadResultDependency = 1
	MVReadResultNone       = 2
)

type ReadResult struct {
	depIdx      int
	incarnation int
	value       interface{}
}

func (res *ReadResult) DepIdx() int {
	return res.depIdx
}

func (res *ReadResult) Incarnation() int {
	return res.incarnation
}

func (res *ReadResult) Value() interface{} {
	return res.value
}

func (res *ReadResult) Version() Version {
	return Version{
		TxIndex:     res.depIdx,
		Incarnation: res.incarnation,
	}
}

func (mvr ReadResult) Status() int {
	if mvr.depIdx != -1 {
		if mvr.incarnation == -1 {
			return MVReadResultDependency
		} else {
			return MVReadResultDone
		}
	}

	return MVReadResultNone
}

func (vm *VersionMap) Read(k VersionKey, txIdx int) (res ReadResult) {
	if vm == nil {
		return res
	}

	res.depIdx = -1
	res.incarnation = -1

	cells := vm.getKeyCells(k, func(_ VersionKey) *TxIndexCells {
		return nil
	})
	if cells == nil {
		return
	}

	var floor = func(i int) (key int, val *WriteCell) {
		key = -1
		cells.tm.Descend(i, func(k int, v *WriteCell) bool {
			key = k
			val = v
			return false
		})
		return key, val
	}

	cells.rw.RLock()
	defer cells.rw.RUnlock()

	fk, fv := floor(txIdx - 1)

	if fk != -1 && fv != nil {
		c := fv
		switch c.flag {
		case FlagEstimate:
			res.depIdx = fk
			res.value = c.data
		case FlagDone:
			{
				res.depIdx = fk
				res.incarnation = c.incarnation
				res.value = c.data
			}
		default:
			panic(fmt.Errorf("should not happen - unknown flag value"))
		}
	}

	return
}

func (vm *VersionMap) FlushVersionedWrites(writes VersionedWrites, complete bool) {
	for _, v := range writes {
		if vm.trace {
			fmt.Println("WRT", v.Path, v.Version)
		}
		vm.Write(v.Path, v.Version, v.Val, complete)
	}
}

func ValidateVersion(txIdx int, lastIO *VersionedIO, versionMap *VersionMap, checkVersion func(source ReadSource, readVersion, writeVersion Version) bool) (valid bool) {
	valid = true

	if readSet := lastIO.ReadSet(txIdx); readSet != nil {
		readSet.Scan(func(vr *VersionedRead) bool {
			rr := versionMap.Read(vr.Path, txIdx)
			switch rr.Status() {
			case MVReadResultDone:
				valid = checkVersion(vr.Source, vr.Version, rr.Version())
			case MVReadResultDependency:
				valid = false
			case MVReadResultNone:
				valid = vr.Source == StorageRead
			default:
				panic(fmt.Errorf("should not happen - undefined vm read status: %v", rr.Status()))
			}

			if versionMap.trace {
				fmt.Println("RD", vr.Path, txIdx, func() string {
					switch rr.Status() {
					case MVReadResultDone:
						return "done"
					case MVReadResultDependency:
						return "dependency"
					case MVReadResultNone:
						return "none"
					default:
						return "unknown"
					}
				}(), vr.Version, rr.depIdx, rr.incarnation, valid)
			}

			return valid
		})
	}

	return
}
