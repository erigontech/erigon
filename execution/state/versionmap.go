package state

import (
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/tidwall/btree"
)

type statusFlag uint

const FlagDone statusFlag = 0
const FlagEstimate statusFlag = 1
const UnknownDep = -2

// numShards is the number of shards for the VersionMap. Must be a power of 2.
const numShards = 64

// shard holds a subset of the VersionMap data with its own lock for reduced contention.
type shard struct {
	mu sync.RWMutex
	s  map[accounts.Address]map[AccountKey]*btree.Map[int, *WriteCell]
}

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
	case StoragePath:
		return "Storage"
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
	StoragePath
)

type AccountKey struct {
	Path AccountPath
	Key  accounts.StorageKey
}

func (k AccountKey) String() string {
	if k.Path == StoragePath {
		return fmt.Sprintf("%x", k.Key)
	}

	return k.Path.String()
}

type VersionMap struct {
	shards [numShards]shard
	trace  bool
}

// getShard returns the shard for the given address.
func (vm *VersionMap) getShard(addr accounts.Address) *shard {
	return &vm.shards[addr.Value()[0]&(numShards-1)]
}

func NewVersionMap(changes []*types.AccountChanges) *VersionMap {
	vm := &VersionMap{}
	for i := range vm.shards {
		vm.shards[i].s = make(map[accounts.Address]map[AccountKey]*btree.Map[int, *WriteCell])
	}
	vm.WriteChanges(changes)
	return vm
}

func (vm *VersionMap) SetTrace(trace bool) {
	vm.trace = trace
}

func (sh *shard) getKeyCells(addr accounts.Address, path AccountPath, key accounts.StorageKey, fNoKey func() *btree.Map[int, *WriteCell]) (cells *btree.Map[int, *WriteCell]) {
	it, ok := sh.s[addr]

	if ok {
		cells, ok = it[AccountKey{path, key}]
	}

	if !ok && fNoKey != nil {
		cells = fNoKey()
	}

	return
}

func (vm *VersionMap) WriteChanges(changes []*types.AccountChanges) {
	for _, accountChanges := range changes {
		for _, storageChanges := range accountChanges.StorageChanges {
			for _, change := range storageChanges.Changes {
				vm.Write(accountChanges.Address, StoragePath, storageChanges.Slot, Version{TxIndex: int(change.Index) - 1}, change.Value, true)
			}
		}
		for _, balanceChange := range accountChanges.BalanceChanges {
			vm.Write(accountChanges.Address, BalancePath, accounts.NilKey, Version{TxIndex: int(balanceChange.Index) - 1}, balanceChange.Value, true)
		}
		for _, nonceChange := range accountChanges.NonceChanges {
			vm.Write(accountChanges.Address, NoncePath, accounts.NilKey, Version{TxIndex: int(nonceChange.Index) - 1}, nonceChange.Value, true)
		}
		for _, codeChange := range accountChanges.CodeChanges {
			vm.Write(accountChanges.Address, CodePath, accounts.NilKey, Version{TxIndex: int(codeChange.Index) - 1}, codeChange.Data, true)
		}
	}

}

func (vm *VersionMap) Write(addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, data interface{}, complete bool) {
	sh := vm.getShard(addr)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cells := sh.getKeyCells(addr, path, key, func() (cells *btree.Map[int, *WriteCell]) {
		it, ok := sh.s[addr]
		cells = &btree.Map[int, *WriteCell]{}
		if ok {
			it[AccountKey{path, key}] = cells
		} else {
			sh.s[addr] = map[AccountKey]*btree.Map[int, *WriteCell]{
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

func (vm *VersionMap) Read(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) (res ReadResult) {
	if vm == nil {
		return res
	}

	sh := vm.getShard(addr)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	res.depIdx = UnknownDep
	res.incarnation = -1

	cells := sh.getKeyCells(addr, path, key, nil)

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

func (vm *VersionMap) MarkEstimate(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	sh := vm.getShard(addr)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cells := sh.getKeyCells(addr, path, key, func() *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x %s, cells keys: %v", txIdx, addr, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap) MarkComplete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	sh := vm.getShard(addr)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cells := sh.getKeyCells(addr, path, key, func() *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x s, cells keys: %v", txIdx, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap) Delete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, checkExists bool) {
	sh := vm.getShard(addr)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cells := sh.getKeyCells(addr, path, key, nil)

	if cells == nil {
		if !checkExists {
			return
		}

		panic(errors.New("path must already exist"))
	}

	cells.Delete(txIdx)
}

func (vm *VersionMap) DeleteAll(addr accounts.Address, txIdx int) {
	sh := vm.getShard(addr)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if writes, ok := sh.s[addr]; ok {
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

func (vm *VersionMap) validateRead(txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
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
					valid = vm.validateRead(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
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
