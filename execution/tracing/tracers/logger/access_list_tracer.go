// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package logger

import (
	"maps"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// accessList is an accumulator for the set of accounts and storage slots an EVM
// contract execution touches.
type accessList map[common.Address]accessListSlots

// accessListSlots is an accumulator for the set of storage slots within a single
// contract that an EVM contract execution touches.
type accessListSlots struct {
	order int
	slots map[common.Hash]int
}

// newAccessList creates a new accessList.
func newAccessList() accessList {
	return make(map[common.Address]accessListSlots)
}

// set allocates the slot map on first write, so an address that is only ever
// touched as an address keeps a nil slots map.
func (s *accessListSlots) set(slot common.Hash) {
	if s.slots == nil {
		s.slots = make(map[common.Hash]int)
	}
	if _, ok := s.slots[slot]; !ok {
		s.slots[slot] = len(s.slots)
	}
}

// addAddress adds an address to the accesslist.
func (al accessList) addAddress(address common.Address) {
	// Set address if not previously present
	if _, present := al[address]; !present {
		al[address] = accessListSlots{order: len(al)}
	}
}

// addSlot adds a storage slot to the accesslist.
func (al accessList) addSlot(address common.Address, slot common.Hash) {
	storage, present := al[address]
	if !present {
		storage.order = len(al)
	}
	newSlotMap := storage.slots == nil
	storage.set(slot)
	if newSlotMap {
		al[address] = storage
	}
}

// cloneExcluding copies al without the excluded addresses, sharing no maps with
// it and renumbering order so it stays dense. The exclusion is not redundant:
// the SLOAD/SSTORE path of OnOpcode adds the executing address without
// consulting excl, so an excluded address can be in al.
func (al accessList) cloneExcluding(excl map[common.Address]struct{}) accessList {
	byOrder := make([]common.Address, len(al))
	for addr, storage := range al {
		byOrder[storage.order] = addr
	}

	cp := make(accessList, len(al))
	for _, addr := range byOrder {
		if _, ok := excl[addr]; ok {
			continue
		}
		storage := al[addr]
		storage.order = len(cp)
		storage.slots = maps.Clone(storage.slots)
		cp[addr] = storage
	}
	return cp
}

// equal checks if the content of the current access list is the same as the
// content of the other one.
func (al accessList) equal(other accessList) bool {
	// Equal sizes plus one-way containment implies set equality, so only al is walked.
	if len(al) != len(other) {
		return false
	}
	for addr, storage := range al {
		otherStorage, ok := other[addr]
		if !ok {
			return false
		}
		if len(storage.slots) != len(otherStorage.slots) {
			return false
		}
		for hash := range storage.slots {
			if _, ok := otherStorage.slots[hash]; !ok {
				return false
			}
		}
	}
	return true
}

func (al accessList) Equal(other accessList) bool {
	return al.equal(other)
}

// accesslist converts the accesslist to a types.AccessList.
func (al accessList) accessList() types.AccessList {
	acl := make(types.AccessList, len(al))
	for addr, storage := range al {
		tuple := types.AccessTuple{Address: addr, StorageKeys: make([]common.Hash, len(storage.slots))}
		for slot, pos := range storage.slots {
			tuple.StorageKeys[pos] = slot
		}
		acl[storage.order] = tuple
	}
	return acl
}

// accesslist converts the accesslist to a types.AccessList.
func (al accessList) accessListSorted() types.AccessList {
	acl := make(types.AccessList, 0, len(al))
	for addr, storage := range al {
		storageKeys := make([]common.Hash, len(storage.slots))
		for slot, pos := range storage.slots {
			storageKeys[pos] = slot
		}
		slices.SortFunc(storageKeys, func(a, b common.Hash) int {
			return a.Cmp(b)
		})
		acl = append(acl, types.AccessTuple{
			Address:     addr,
			StorageKeys: storageKeys,
		})
	}
	return acl
}

// AccessListTracer is a tracer that accumulates touched accounts and storage
// slots into an internal set.
type AccessListTracer struct {
	excl               map[common.Address]struct{} // Set of account to exclude from the list
	list               accessList                  // Set of accounts and storage slots touched
	state              *state.IntraBlockState      // State for nonce calculation of created contracts
	createdContracts   map[common.Address]struct{} // Set of all addresses of contracts created during txn execution
	usedBeforeCreation map[common.Address]struct{} // Set of all contract addresses first used before creation
}

// NewAccessListTracer creates a new tracer that can generate AccessLists.
// An optional AccessList can be specified to occupy slots and addresses in
// the resulting accesslist.
// An optional set of addresses to be excluded from the resulting accesslist can
// also be specified.
// state is borrowed for CREATE nonce lookups; the caller keeps ownership of it.
func NewAccessListTracer(acl types.AccessList, exclude map[common.Address]struct{}, state *state.IntraBlockState) *AccessListTracer {
	t := newAccessListTracer(exclude, newAccessList(), state)
	for _, al := range acl {
		if _, ok := t.excl[al.Address]; ok {
			continue
		}
		t.list.addAddress(al.Address)
		for _, slot := range al.StorageKeys {
			t.list.addSlot(al.Address, slot)
		}
	}
	return t
}

// newAccessListTracer holds the construction both entry points share, so they
// cannot drift.
func newAccessListTracer(excl map[common.Address]struct{}, list accessList, state *state.IntraBlockState) *AccessListTracer {
	if excl == nil {
		excl = make(map[common.Address]struct{})
	}
	return &AccessListTracer{
		excl:  excl,
		list:  list,
		state: state,
	}
}

// SeedNew returns a tracer that starts from a's accumulated list, for the next
// convergence iteration, copying the maps directly rather than round-tripping
// through types.AccessList.
func (a *AccessListTracer) SeedNew(state *state.IntraBlockState) *AccessListTracer {
	return newAccessListTracer(a.excl, a.list.cloneExcluding(a.excl), state)
}

// markCreated and markUsedBeforeCreation each allocate their set lazily, on
// its own first insertion.
func (a *AccessListTracer) markCreated(addr common.Address) {
	if a.createdContracts == nil {
		a.createdContracts = make(map[common.Address]struct{})
	}
	a.createdContracts[addr] = struct{}{}
}

func (a *AccessListTracer) markUsedBeforeCreation(addr common.Address) {
	if a.usedBeforeCreation == nil {
		a.usedBeforeCreation = make(map[common.Address]struct{})
	}
	a.usedBeforeCreation[addr] = struct{}{}
}

func (a *AccessListTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnOpcode: a.OnOpcode,
	}
}

func (a *AccessListTracer) OnOpcode(pc uint64, opcode byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	stackData := scope.StackData()
	stackLen := len(stackData)
	op := vm.OpCode(opcode)
	if (op == vm.SLOAD || op == vm.SSTORE) && stackLen >= 1 {
		addr := scope.Address()

		slot := common.Hash(stackData[stackLen-1].Bytes32())
		a.list.addSlot(addr.Value(), slot)
		if _, ok := a.createdContracts[addr.Value()]; !ok {
			a.markUsedBeforeCreation(addr.Value())
		}
	}
	if (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT) && stackLen >= 1 {
		addr := common.Address(stackData[stackLen-1].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
			if _, ok := a.createdContracts[addr]; !ok {
				a.markUsedBeforeCreation(addr)
			}
		}
	}
	if (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE) && stackLen >= 5 {
		addr := common.Address(stackData[stackLen-2].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
			if _, ok := a.createdContracts[addr]; !ok {
				a.markUsedBeforeCreation(addr)
			}
		}
	}
	if op == vm.CREATE {
		// contract address for CREATE can only be generated with state
		if a.state != nil {
			nonce, _ := a.state.GetNonce(scope.Address())
			addr := types.CreateAddress(scope.Address().Value(), nonce)
			if _, ok := a.excl[addr]; !ok {
				a.markCreated(addr)
			}
		}
	}
	if op == vm.CREATE2 && stackLen >= 4 {
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init, err := tracers.GetMemoryCopyPadded(scope.MemoryData(), int64(offset.Uint64()), int64(size.Uint64()))
		if err != nil {
			// t.Stop(fmt.Errorf("failed to copy CREATE2 in prestate tracer input err: %s", err))
			return
		}
		inithash := accounts.InternCodeHash(crypto.Keccak256Hash(init))
		salt := stackData[stackLen-4]
		addr := types.CreateAddress2(scope.Address().Value(), salt.Bytes32(), inithash)
		if _, ok := a.excl[addr]; !ok {
			a.markCreated(addr)
		}
	}
}

// AccessList returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessList() types.AccessList {
	return a.list.accessList()
}

// AccessListSorted returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessListSorted() types.AccessList {
	return a.list.accessListSorted()
}

// CreatedContracts returns the set of all addresses of contracts created during
// txn execution. It always returns a writable map, allocating it on first call
// if no CREATE has happened yet.
func (a *AccessListTracer) CreatedContracts() map[common.Address]struct{} {
	if a.createdContracts == nil {
		a.createdContracts = make(map[common.Address]struct{})
	}
	return a.createdContracts
}

// UsedBeforeCreation returns for a given address whether it was first used before creation.
func (a *AccessListTracer) UsedBeforeCreation(addr common.Address) bool {
	_, contained := a.usedBeforeCreation[addr]
	return contained
}

// Equal returns if the content of two access list traces are equal.
func (a *AccessListTracer) Equal(other *AccessListTracer) bool {
	return a.list.equal(other.list)
}
