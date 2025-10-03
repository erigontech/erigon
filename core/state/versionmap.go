package state

import (
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/tidwall/btree"
)

type statusFlag uint

const FlagDone statusFlag = 0
const FlagEstimate statusFlag = 1
const UnknownDep = -2

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
	Key  common.Hash
}

func (k AccountKey) String() string {
	if k.Path == StatePath {
		return fmt.Sprintf("%x", k.Key)
	}

	return k.Path.String()
}

type VersionMap struct {
	mu    sync.RWMutex
	s     map[common.Address]map[AccountKey]*btree.Map[int, *WriteCell]
	trace bool
}

func NewVersionMap() *VersionMap {
	return &VersionMap{
		s: map[common.Address]map[AccountKey]*btree.Map[int, *WriteCell]{},
	}
}

func (vm *VersionMap) SetTrace(trace bool) {
	vm.trace = trace
}

func (vm *VersionMap) getKeyCells(addr common.Address, path AccountPath, key common.Hash, fNoKey func(addr common.Address, path AccountPath, key common.Hash) *btree.Map[int, *WriteCell]) (cells *btree.Map[int, *WriteCell]) {
	it, ok := vm.s[addr]

	if ok {
		cells, ok = it[AccountKey{path, key}]
	}

	if !ok && fNoKey != nil {
		cells = fNoKey(addr, path, key)
	}

	return
}

func (vm *VersionMap) Write(addr common.Address, path AccountPath, key common.Hash, v Version, data interface{}, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(addr common.Address, path AccountPath, key common.Hash) (cells *btree.Map[int, *WriteCell]) {
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

	var flag statusFlag = FlagDone

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

func (vm *VersionMap) Read(addr common.Address, path AccountPath, key common.Hash, txIdx int) (res ReadResult) {
	if vm == nil {
		return res
	}

	vm.mu.RLock()
	defer vm.mu.RUnlock()

	res.depIdx = UnknownDep
	res.incarnation = -1

	cells := vm.getKeyCells(addr, path, key, nil)

	if cells == nil {
		return
	}

	var floor = func(i int) (key int, val *WriteCell) {
		key = UnknownDep
		cells.Descend(i, func(k int, v *WriteCell) bool {
			key = k
			val = v
			return false
		})
		return key, val
	}

	fk, fv := floor(txIdx - 1)

	if fk != UnknownDep && fv != nil {
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
			panic(errors.New("should not happen - unknown flag value"))
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

func (vm *VersionMap) MarkEstimate(addr common.Address, path AccountPath, key common.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ common.Address, _ AccountPath, _ common.Hash) *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x %s, cells keys: %v", txIdx, addr, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap) MarkComplete(addr common.Address, path AccountPath, key common.Hash, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ common.Address, _ AccountPath, _ common.Hash) *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x s, cells keys: %v", txIdx, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap) Delete(addr common.Address, path AccountPath, key common.Hash, txIdx int, checkExists bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	cells := vm.getKeyCells(addr, path, key, nil)

	if cells == nil {
		if !checkExists {
			return
		}

		panic(errors.New("path must already exist"))
	}

	cells.Delete(txIdx)
}

func (vm *VersionMap) DeleteAll(addr common.Address, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if writes, ok := vm.s[addr]; ok {
		for _, cells := range writes {
			cells.Delete(txIdx)
		}
	}
}

type VersionValidity int

func (v VersionValidity) String() string {
	switch v {
	case VersionValid:
		return "valid"
	case VersionInvalid:
		return "invalid"
	case VersionTooEarly:
		return "too early"
	default:
		return "unknown"
	}
}

const (
	VersionValid VersionValidity = iota
	VersionInvalid
	VersionTooEarly
)

func (vm *VersionMap) validateRead(txIndex int, addr common.Address, path AccountPath, key common.Hash, source ReadSource, version Version,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string) VersionValidity {

	valid := VersionValid

	rr := vm.Read(addr, path, key, txIndex)
	switch rr.Status() {
	case MVReadResultDone:
		if source != MapRead {
			valid = VersionInvalid
		} else {
			valid = checkVersion(version, rr.Version())
		}
	case MVReadResultDependency:
		valid = VersionInvalid
	case MVReadResultNone:
		if source != StorageRead {
			valid = VersionInvalid
		} else {
			if valid = checkVersion(version, version); valid == VersionValid {
				if path == BalancePath || path == NoncePath || path == CodeHashPath {
					valid = vm.validateRead(txIndex, addr, AddressPath, common.Hash{}, source,
						version, checkVersion, traceInvalid, tracePrefix)
				}
			}
		}
	default:
		panic(fmt.Errorf("undefined vm read status: %v", rr.Status()))
	}

	if vm.trace || (traceInvalid && valid == VersionInvalid) {
		if len(tracePrefix) > 0 {
			tracePrefix = tracePrefix + "  RD"
		} else {
			tracePrefix = "RD"
		}
		fmt.Printf("%s %x %s, %d %s, %s (%d.%d)!=(%d.%d) %s\n", tracePrefix, addr,
			AccountKey{path, key}.String(), txIndex, func() string {
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
			}(),
			source, version.TxIndex, version.Incarnation, rr.depIdx, rr.incarnation, valid)
	}

	return valid
}

// ValidateVersion check if transaction's readSet is still valid based on the current multi-versioned memory
func (vm *VersionMap) ValidateVersion(txIdx int, lastIO *VersionedIO, checkVersion func(readVersion, writeVersion Version) VersionValidity, traceInvalid bool, tracePrefix string) (valid VersionValidity) {
	if readSet := lastIO.ReadSet(txIdx); readSet != nil {
		readSet.Scan(func(vr *VersionedRead) bool {
			valid = vm.validateRead(txIdx, vr.Address, vr.Path, vr.Key, vr.Source, vr.Version,
				checkVersion, traceInvalid, tracePrefix)
			return valid == VersionValid
		})
	}

	return
}

type WriteCell struct {
	flag        statusFlag
	incarnation int
	data        interface{}
}

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation int
}

var UnknownVersion = Version{TxIndex: UnknownDep, Incarnation: -1}

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
	if mvr.depIdx != UnknownDep {
		if mvr.incarnation == -1 {
			return MVReadResultDependency
		} else {
			return MVReadResultDone
		}
	}

	return MVReadResultNone
}
