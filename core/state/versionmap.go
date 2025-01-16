package state

import (
	"fmt"
	"sync"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

const FlagDone = 0
const FlagEstimate = 1

const addressType = 1
const stateType = 2
const subpathType = 3

const BalancePath = 1
const NoncePath = 2
const CodePath = 3
const CodeHashPath = 4
const CodeSizePath = 5
const SelfDestructPath = 6

const KeyLength = length.Addr + length.Hash + 2

type VersionKey [KeyLength]byte

func (k VersionKey) String() string {
	var keyType string
	var subpath string
	var key string

	switch k[KeyLength-1] {
	case addressType:
		keyType = "Address"
		key = fmt.Sprintf("%x", k[0:length.Addr])
	case stateType:
		keyType = "State"
		key = fmt.Sprintf("%x %x", k[0:length.Addr], k[length.Addr:KeyLength-2])
	case subpathType:
		switch k[KeyLength-2] {
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
		case 0:
		default:
			keyType = "Path"
			subpath = fmt.Sprintf(" Unknown %d", k[KeyLength-2])
		}
		key = fmt.Sprintf("%x", k[0:length.Addr])
	default:
		keyType = "Unkwnown"
		key = fmt.Sprintf("%x %x", k[0:length.Addr], k[length.Addr:KeyLength-2])
	}

	return fmt.Sprintf("%-9s %s%s", keyType, key, subpath)
}

func (k VersionKey) IsAddress() bool {
	return k[KeyLength-1] == addressType
}

func (k VersionKey) IsState() bool {
	return k[KeyLength-1] == stateType
}

func (k VersionKey) IsSubpath() bool {
	return k[KeyLength-1] == subpathType
}

func (k VersionKey) IsCode() bool {
	return k[KeyLength-1] == subpathType && k[KeyLength-2] == CodePath
}

func (k VersionKey) GetAddress() common.Address {
	return common.BytesToAddress(k[:length.Addr])
}

func (k VersionKey) GetStateKey() common.Hash {
	return common.BytesToHash(k[length.Addr : KeyLength-2])
}

func (k VersionKey) GetSubpath() byte {
	return k[KeyLength-2]
}

func newVersionKey(addr common.Address, hash common.Hash, subpath byte, keyType byte) VersionKey {
	var k VersionKey

	copy(k[:length.Addr], addr.Bytes())
	copy(k[length.Addr:KeyLength-2], hash.Bytes())
	k[KeyLength-2] = subpath
	k[KeyLength-1] = keyType

	return k
}

func AddressKey(addr common.Address) VersionKey {
	return newVersionKey(addr, common.Hash{}, 0, addressType)
}

func StateKey(addr common.Address, hash common.Hash) VersionKey {
	k := newVersionKey(addr, hash, 0, stateType)
	if !k.IsState() {
		panic(fmt.Errorf("key is not a state key"))
	}

	return k
}

func SubpathKey(addr common.Address, subpath byte) VersionKey {
	return newVersionKey(addr, common.Hash{}, subpath, subpathType)
}

type VersionMap struct {
	m     sync.Map
	s     sync.Map
	trace bool
}

type WriteCell struct {
	flag        uint
	incarnation int
	data        interface{}
}

type TxIndexCells struct {
	rw sync.RWMutex
	tm *btree.Map[int, *WriteCell]
}

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation int
}

func (vm *VersionMap) getKeyCells(k VersionKey, fNoKey func(kenc VersionKey) *TxIndexCells) (cells *TxIndexCells) {
	val, ok := vm.m.Load(k)

	if !ok {
		cells = fNoKey(k)
	} else {
		cells = val.(*TxIndexCells)
	}

	return
}

func (vm *VersionMap) Write(k VersionKey, v Version, data interface{}, complete bool) {
	cells := vm.getKeyCells(k, func(kenc VersionKey) (cells *TxIndexCells) {
		n := &TxIndexCells{
			rw: sync.RWMutex{},
			tm: &btree.Map[int, *WriteCell]{},
		}
		cells = n
		val, _ := vm.m.LoadOrStore(kenc, n)
		cells = val.(*TxIndexCells)
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

func (vm *VersionMap) ReadStorage(k VersionKey, fallBack func() any) any {
	data, ok := vm.s.Load(string(k[:]))
	if !ok {
		data = fallBack()
		data, _ = vm.s.LoadOrStore(string(k[:]), data)
	}

	return data
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

func (vm *VersionMap) FlushVersionedWrites(writes []VersionedWrite, complete bool) {
	for _, v := range writes {
		if vm.trace {
			fmt.Println("WRT", v.Path, v.Version)
		}
		vm.Write(v.Path, v.Version, v.Val, complete)
	}
}

func ValidateVersion(txIdx int, lastInputOutput *VersionedIO, versionMap *VersionMap) (valid bool) {
	valid = true

	for _, rd := range lastInputOutput.ReadSet(txIdx) {
		mvResult := versionMap.Read(rd.Path, txIdx)
		switch mvResult.Status() {
		case MVReadResultDone:
			// Having a write record for a path in VersionedMap doesn't necessarily mean there is a conflict,
			// because VersionedMap is a superset of the actual write set.
			// Check if the write record is actually in write set. If not, skip the key.
			// if mvResult.depIdx >= 0 && !lastInputOutput.HasWritten(mvResult.depIdx, rd.Path) {
			// 	continue
			// }

			valid = rd.Kind == ReadKindMap && rd.Version == Version{
				TxIndex:     mvResult.depIdx,
				Incarnation: mvResult.incarnation,
			}
		case MVReadResultDependency:
			valid = false
		case MVReadResultNone:
			valid = rd.Kind == ReadKindStorage
		default:
			panic(fmt.Errorf("should not happen - undefined vm read status: %ver", mvResult.Status()))
		}

		if versionMap.trace {
			fmt.Println("RD", rd.Path, txIdx, func() string {
				switch mvResult.Status() {
				case MVReadResultDone:
					return "done"
				case MVReadResultDependency:
					return "dependency"
				case MVReadResultNone:
					return "none"
				default:
					return "unknown"
				}
			}(), rd.Version, mvResult.depIdx, valid)
		}

		if !valid {
			break
		}
	}

	return
}
