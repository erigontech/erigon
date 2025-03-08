package state

import (
	"fmt"
	"sync"

	"github.com/tidwall/btree"

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

type VersionMap struct {
	mu    sync.RWMutex
	s     map[libcommon.Address]map[AccountKey]*btree.Map[int, *WriteCell]
	trace bool
}

func NewVersionMap() *VersionMap {
	return &VersionMap{
		s: map[libcommon.Address]map[AccountKey]*btree.Map[int, *WriteCell]{},
	}
}

func (vm *VersionMap) SetTrace(trace bool) {
	vm.trace = trace
}

func (vm *VersionMap) getKeyCells(addr libcommon.Address, path AccountPath, key libcommon.Hash, fNoKey func(addr libcommon.Address, path AccountPath, key libcommon.Hash) *btree.Map[int, *WriteCell]) (cells *btree.Map[int, *WriteCell]) {
	it, ok := vm.s[addr]

	if ok {
		cells, ok = it[AccountKey{path, key}]
	}

	if !ok {
		cells = fNoKey(addr, path, key)
	}

	return
}

func (vm *VersionMap) Write(addr libcommon.Address, path AccountPath, key libcommon.Hash, v Version, data interface{}, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(addr libcommon.Address, path AccountPath, key libcommon.Hash) (cells *btree.Map[int, *WriteCell]) {
		it, ok := vm.s[addr]
		cells = &btree.Map[int, *WriteCell]{}
		if ok {
			it[AccountKey{path, key}] = cells
		} else {
			vm.s[addr] = map[AccountKey]*btree.Map[int, *WriteCell]{
				{path, key}: cells,
			}
		}
		return
	})

	ci, ok := cells.Get(v.TxIndex)

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
		if ci, ok = cells.Get(v.TxIndex); !ok {
			cells.Set(v.TxIndex, &WriteCell{
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

var tra = libcommon.HexToAddress("dd03baefb005dadba5234e2954d23e5f9cbf57a9")

func (vm *VersionMap) Read(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int) (res ReadResult) {
	if vm == nil {
		return res
	}

	vm.mu.RLock()
	defer vm.mu.RUnlock()

	res.depIdx = -1
	res.incarnation = -1

	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *btree.Map[int, *WriteCell] {
		return nil
	})

	if addr == tra {
		fmt.Printf("(%d), %x: %+v\n", txIdx, addr, cells)
	}

	if cells == nil {
		return
	}

	var floor = func(i int) (key int, val *WriteCell) {
		key = -1
		cells.Descend(i, func(k int, v *WriteCell) bool {
			key = k
			val = v
			return false
		})
		return key, val
	}

	fk, fv := floor(txIdx - 1)

	if addr == tra {
		fmt.Printf("(%d), %x: fk=%d, fv=%+v\n", txIdx, addr, fk, fv)
	}

	if fk != -1 && fv != nil {
		switch fv.flag {
		case FlagEstimate:
			res.depIdx = fk
			res.value = fv.data
		case FlagDone:
			{
				res.depIdx = fk
				res.incarnation = fv.incarnation
				res.value = fv.data
			}
		default:
			panic(fmt.Errorf("should not happen - unknown flag value"))
		}
	}

	return
}

func (vm *VersionMap) FlushVersionedWrites(writes VersionedWrites, complete bool, tracePrefix string) {
	for _, v := range writes {
		if vm.trace {
			fmt.Println(tracePrefix, "FLSH", v.String())
		}
		vm.Write(v.Address, v.Path, v.Key, v.Version, v.Val, complete)
	}
}

func (vm *VersionMap) MarkEstimate(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *btree.Map[int, *WriteCell] {
		panic(fmt.Errorf("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x %s, cells keys: %v", txIdx, addr, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap) MarkComplete(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *btree.Map[int, *WriteCell] {
		panic(fmt.Errorf("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x s, cells keys: %v", txIdx, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap) Delete(addr libcommon.Address, path AccountPath, key libcommon.Hash, txIdx int, checkExists bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	cells := vm.getKeyCells(addr, path, key, func(_ libcommon.Address, _ AccountPath, _ libcommon.Hash) *btree.Map[int, *WriteCell] { return nil })

	if cells == nil {
		if !checkExists {
			return
		}

		panic(fmt.Errorf("path must already exist"))
	}

	cells.Delete(txIdx)
}

func (vm *VersionMap) DeleteAll(addr libcommon.Address, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if writes, ok := vm.s[addr]; ok {
		for _, cells := range writes {
			cells.Delete(txIdx)
		}
	}
}

type WriteCell struct {
	flag        uint
	incarnation int
	data        interface{}
}

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation int
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

func ValidateVersion(txIdx int, lastIO *VersionedIO, versionMap *VersionMap, checkVersion func(source ReadSource, readVersion, writeVersion Version) bool) (valid bool) {
	valid = true

	if readSet := lastIO.ReadSet(txIdx); readSet != nil {
		readSet.Scan(func(vr *VersionedRead) bool {
			rr := versionMap.Read(vr.Address, vr.Path, vr.Key, txIdx)
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
				fmt.Println("RD", vr.Address, AccountKey{vr.Path, vr.Key}.String(), txIdx, func() string {
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
