// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"sort"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// NewBALReadVersionMap returns a version map whose txs read balance, nonce, code and storage
// from the block access list (the latest change with block-access index < txIdx+1, else the
// pre-block state) instead of from seeded cells. The BAL hash check after execution catches
// a list that does not match what the txs did.
func NewBALReadVersionMap(changes types.BlockAccessList) *VersionMap {
	vm := &VersionMap{HasBAL: len(changes) > 0, bal: changes}
	if vm.HasBAL {
		vm.balReads = true
		vm.balCodes = make([][]balCode, len(changes))
		for i := range changes {
			if n := len(changes[i].CodeChanges); n > 0 {
				vm.balCodes[i] = make([]balCode, n)
			}
		}
	}
	return vm
}

type balCode struct {
	once sync.Once
	code accounts.Code
}

// balReadPath reports whether a BAL-read map serves path p from the access list.
func balReadPath(p AccountPath) bool {
	switch p {
	case BalancePath, NoncePath, CodePath, CodeHashPath, CodeSizePath, StoragePath:
		return true
	}
	return false
}

func (vm *VersionMap) balAccount(addr accounts.Address) int {
	address := addr.Value()
	i := sort.Search(len(vm.bal), func(i int) bool { return bytes.Compare(vm.bal[i].Address[:], address[:]) >= 0 })
	if i == len(vm.bal) || vm.bal[i].Address != address {
		return -1
	}
	return i
}

// balLatest returns the position of the last change with index <= txIdx, or -1.
func balLatest(n int, index func(int) uint32, txIdx int) int {
	return sort.Search(n, func(k int) bool { return int(index(k)) > txIdx }) - 1
}

func balHit(index uint32) ReadResult {
	return ReadResult{depIdx: int(index) - 1}
}

func balMiss() ReadResult {
	return ReadResult{depIdx: UnknownDep, incarnation: -1}
}

func (vm *VersionMap) balBalance(addr accounts.Address, txIdx int) (uint256.Int, ReadResult, bool) {
	if i := vm.balAccount(addr); i >= 0 {
		ch := vm.bal[i].BalanceChanges
		if k := balLatest(len(ch), func(k int) uint32 { return ch[k].Index }, txIdx); k >= 0 {
			return ch[k].Value, balHit(ch[k].Index), true
		}
	}
	return uint256.Int{}, balMiss(), false
}

func (vm *VersionMap) balNonce(addr accounts.Address, txIdx int) (uint64, ReadResult, bool) {
	if i := vm.balAccount(addr); i >= 0 {
		ch := vm.bal[i].NonceChanges
		if k := balLatest(len(ch), func(k int) uint32 { return ch[k].Index }, txIdx); k >= 0 {
			return ch[k].Value, balHit(ch[k].Index), true
		}
	}
	return 0, balMiss(), false
}

func (vm *VersionMap) balCode(addr accounts.Address, txIdx int) (accounts.Code, ReadResult, bool) {
	if i := vm.balAccount(addr); i >= 0 {
		ch := vm.bal[i].CodeChanges
		if k := balLatest(len(ch), func(k int) uint32 { return ch[k].Index }, txIdx); k >= 0 {
			c := &vm.balCodes[i][k]
			c.once.Do(func() { c.code = accounts.NewCode(ch[k].Bytecode) })
			return c.code, balHit(ch[k].Index), true
		}
	}
	return accounts.Code{}, balMiss(), false
}

func (vm *VersionMap) balStorage(addr accounts.Address, key accounts.StorageKey, txIdx int) (uint256.Int, ReadResult, bool) {
	if i := vm.balAccount(addr); i >= 0 {
		slots := vm.bal[i].StorageChanges
		j := sort.Search(len(slots), func(j int) bool { return slots[j].Slot.Cmp(key) >= 0 })
		if j < len(slots) && slots[j].Slot.Cmp(key) == 0 {
			ch := slots[j].Changes
			if k := balLatest(len(ch), func(k int) uint32 { return ch[k].Index }, txIdx); k >= 0 {
				return ch[k].Value, balHit(ch[k].Index), true
			}
		}
	}
	return uint256.Int{}, balMiss(), false
}

// balExistence is the lazy form of SeedAddresses: an account the BAL creates exists from its
// first change on. The marker carries the account as of that change.
func (vm *VersionMap) balExistence(addr accounts.Address, txIdx int) (*accounts.Account, ReadResult, bool) {
	i := vm.balAccount(addr)
	if i < 0 {
		return nil, balMiss(), false
	}
	first := balNextChangeIndex(&vm.bal[i], 0)
	if first == 0 || int(first) > txIdx {
		return nil, balMiss(), false
	}
	acc := accounts.NewAccount()
	b, _, hasB := vm.balBalance(addr, int(first))
	n, _, hasN := vm.balNonce(addr, int(first))
	c, _, hasC := vm.balCode(addr, int(first))
	if !hasB && !hasN && !hasC {
		return nil, balMiss(), false
	}
	if hasB {
		acc.Balance = b
	}
	if hasN {
		acc.Nonce = n
	}
	if hasC {
		acc.CodeHash = c.Hash
	}
	res := balHit(first)
	res.flags |= readFromBAL
	return &acc, res, true
}

// balLife returns the lifecycle verdict for a BAL-path read; only a destructed entry needs the lock.
func (vm *VersionMap) balLife(addr accounts.Address, txIdx int) (life lifecycleVerdict) {
	life.state = LifecycleLive
	if e := vm.load(addr); e != nil && e.destructed.Load() {
		e.mu.RLock()
		defer e.mu.RUnlock()
		life.state, life.canonicalVer, life.destroyedAt, life.wipeInc = accountLifecycleLocked(e, txIdx, false)
	}
	return life
}

func writesAccountLifecycle(ws *WriteSet, addr accounts.Address) bool {
	_, a := ws.address[addr]
	_, sd := ws.selfDestruct[addr]
	_, inc := ws.incarnation[addr]
	_, cc := ws.createContract[addr]
	return a || sd || inc || cc
}

func (vm *VersionMap) applyBALFields(addr accounts.Address, txIdx int, account *accounts.Account) {
	if b, _, ok := vm.balBalance(addr, txIdx); ok {
		account.Balance = b
	}
	if n, _, ok := vm.balNonce(addr, txIdx); ok {
		account.Nonce = n
	}
	if c, _, ok := vm.balCode(addr, txIdx); ok {
		account.CodeHash = c.Hash
	}
	if e := vm.load(addr); e != nil {
		e.mu.RLock()
		defer e.mu.RUnlock()
		if _, fv := floorCell(e.Incarnation, txIdx); fv != nil {
			account.Incarnation = fv.Value
		}
	}
}
