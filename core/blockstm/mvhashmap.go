package blockstm

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

const KeyLength = length.Addr + length.Hash + 2

type Key [KeyLength]byte

func (k Key) IsAddress() bool {
	return k[KeyLength-1] == addressType
}

func (k Key) IsState() bool {
	return k[KeyLength-1] == stateType
}

func (k Key) IsSubpath() bool {
	return k[KeyLength-1] == subpathType
}

func (k Key) GetAddress() common.Address {
	return common.BytesToAddress(k[:length.Addr])
}

func (k Key) GetStateKey() common.Hash {
	return common.BytesToHash(k[length.Addr : KeyLength-2])
}

func (k Key) GetSubpath() byte {
	return k[KeyLength-2]
}

func newKey(addr common.Address, hash common.Hash, subpath byte, keyType byte) Key {
	var k Key

	copy(k[:length.Addr], addr.Bytes())
	copy(k[length.Addr:KeyLength-2], hash.Bytes())
	k[KeyLength-2] = subpath
	k[KeyLength-1] = keyType

	return k
}

func NewAddressKey(addr common.Address) Key {
	return newKey(addr, common.Hash{}, 0, addressType)
}

func NewStateKey(addr common.Address, hash common.Hash) Key {
	k := newKey(addr, hash, 0, stateType)
	if !k.IsState() {
		panic(fmt.Errorf("key is not a state key"))
	}

	return k
}

func NewSubpathKey(addr common.Address, subpath byte) Key {
	return newKey(addr, common.Hash{}, subpath, subpathType)
}

type VersionedMap struct {
	m sync.Map
	s sync.Map
}

type WriteCell struct {
	flag        uint
	incarnation int
	data        interface{}
}

type TxnIndexCells struct {
	rw sync.RWMutex
	tm *btree.Map[int, *WriteCell]
}

type Version struct {
	TxnIndex    int
	Incarnation int
}

func (vm *VersionedMap) getKeyCells(k Key, fNoKey func(kenc Key) *TxnIndexCells) (cells *TxnIndexCells) {
	val, ok := vm.m.Load(k)

	if !ok {
		cells = fNoKey(k)
	} else {
		cells = val.(*TxnIndexCells)
	}

	return
}

func (vm *VersionedMap) Write(k Key, v Version, data interface{}) {
	cells := vm.getKeyCells(k, func(kenc Key) (cells *TxnIndexCells) {
		n := &TxnIndexCells{
			rw: sync.RWMutex{},
			tm: &btree.Map[int, *WriteCell]{},
		}
		cells = n
		val, _ := vm.m.LoadOrStore(kenc, n)
		cells = val.(*TxnIndexCells)
		return
	})

	cells.rw.RLock()
	ci, ok := cells.tm.Get(v.TxnIndex)
	cells.rw.RUnlock()

	if ok {
		if ci.incarnation > v.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %v, %v",
				k, v.TxnIndex))
		}

		ci.flag = FlagDone
		ci.incarnation = v.Incarnation
		ci.data = data
	} else {
		func() {
			cells.rw.Lock()
			defer cells.rw.Unlock()
			if ci, ok = cells.tm.Get(v.TxnIndex); !ok {
				cells.tm.Set(v.TxnIndex, &WriteCell{
					flag:        FlagDone,
					incarnation: v.Incarnation,
					data:        data,
				})
			} else {
				ci.flag = FlagDone
				ci.incarnation = v.Incarnation
				ci.data = data
			}
		}()
	}
}

func (vm *VersionedMap) ReadStorage(k Key, fallBack func() any) any {
	data, ok := vm.s.Load(string(k[:]))
	if !ok {
		data = fallBack()
		data, _ = vm.s.LoadOrStore(string(k[:]), data)
	}

	return data
}

func (vm *VersionedMap) MarkEstimate(k Key, txIdx int) {
	cells := vm.getKeyCells(k, func(_ Key) *TxnIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.RLock()
	defer cells.rw.RUnlock()
	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIdx: %v, path, %x, cells keys: %v", txIdx, k, cells.tm.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionedMap) Delete(k Key, txIdx int) {
	cells := vm.getKeyCells(k, func(_ Key) *TxnIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.Lock()
	defer cells.rw.Unlock()
	cells.tm.Delete(txIdx)
}

const (
	MVReadResultDone       = 0
	MVReadResultDependency = 1
	MVReadResultNone       = 2
)

type MVReadResult struct {
	depIdx      int
	incarnation int
	value       interface{}
}

func (res *MVReadResult) DepIdx() int {
	return res.depIdx
}

func (res *MVReadResult) Incarnation() int {
	return res.incarnation
}

func (res *MVReadResult) Value() interface{} {
	return res.value
}

func (mvr MVReadResult) Status() int {
	if mvr.depIdx != -1 {
		if mvr.incarnation == -1 {
			return MVReadResultDependency
		} else {
			return MVReadResultDone
		}
	}

	return MVReadResultNone
}

func (vm *VersionedMap) Read(k Key, txIdx int) (res MVReadResult) {
	res.depIdx = -1
	res.incarnation = -1

	cells := vm.getKeyCells(k, func(_ Key) *TxnIndexCells {
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
	fk, fv := floor(txIdx - 1)
	cells.rw.RUnlock()

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

func (vm *VersionedMap) FlushMVWriteSet(writes []WriteDescriptor) {
	for _, v := range writes {
		vm.Write(v.Path, v.V, v.Val)
	}
}

func ValidateVersion(txIdx int, lastInputOutput *TxnInputOutput, versionedData *VersionedMap) (valid bool) {
	valid = true

	for _, rd := range lastInputOutput.ReadSet(txIdx) {
		mvResult := versionedData.Read(rd.Path, txIdx)
		switch mvResult.Status() {
		case MVReadResultDone:
			// Having a write record for a path in VersionedMap doesn't necessarily mean there is a conflict, because VersionedMap
			// is a superset of the actual write set.
			// Check if the write record is actually in write set. If not, skip the key.
			// if mvResult.depIdx >= 0 && !lastInputOutput.HasWritten(mvResult.depIdx, rd.Path) {
			// 	continue
			// }

			valid = rd.Kind == ReadKindMap && rd.V == Version{
				TxnIndex:    mvResult.depIdx,
				Incarnation: mvResult.incarnation,
			}
		case MVReadResultDependency:
			valid = false
		case MVReadResultNone:
			valid = rd.Kind == ReadKindStorage // feels like an assertion?
		default:
			panic(fmt.Errorf("should not happen - undefined vm read status: %ver", mvResult.Status()))
		}

		if !valid {
			break
		}
	}

	return
}
