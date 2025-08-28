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
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/types"
)

// accessList is an accumulator for the set of accounts and storage slots an EVM
// contract execution touches.
type accessList map[common.Address]accessListSlots

// accessListSlots is an accumulator for the set of storage slots within a single
// contract that an EVM contract execution touches.
type accessListSlots map[common.Hash]struct{}

// newAccessList creates a new accessList.
func newAccessList() accessList {
	return make(map[common.Address]accessListSlots)
}

// addAddress adds an address to the accesslist.
func (al accessList) addAddress(address common.Address) {
	// Set address if not previously present
	if _, present := al[address]; !present {
		al[address] = make(map[common.Hash]struct{})
	}
}

// addSlot adds a storage slot to the accesslist.
func (al accessList) addSlot(address common.Address, slot common.Hash) {
	// Set address if not previously present
	al.addAddress(address)

	// Set the slot on the surely existent storage set
	al[address][slot] = struct{}{}
}

// equal checks if the content of the current access list is the same as the
// content of the other one.
func (al accessList) equal(other accessList) bool {
	// Cross reference the accounts first
	if len(al) != len(other) {
		return false
	}
	for addr := range al {
		if _, ok := other[addr]; !ok {
			return false
		}
	}
	for addr := range other {
		if _, ok := al[addr]; !ok {
			return false
		}
	}
	// Accounts match, cross reference the storage slots too
	for addr, slots := range al {
		otherslots := other[addr]

		if len(slots) != len(otherslots) {
			return false
		}
		for hash := range slots {
			if _, ok := otherslots[hash]; !ok {
				return false
			}
		}
		for hash := range otherslots {
			if _, ok := slots[hash]; !ok {
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
	acl := make(types.AccessList, 0, len(al))
	for addr, slots := range al {
		tuple := types.AccessTuple{Address: addr, StorageKeys: []common.Hash{}}
		for slot := range slots {
			tuple.StorageKeys = append(tuple.StorageKeys, slot)
		}
		acl = append(acl, tuple)
	}
	return acl
}

// accesslist converts the accesslist to a types.AccessList.
func (al accessList) accessListSorted() types.AccessList {
	acl := make(types.AccessList, 0, len(al))
	for addr, slots := range al {
		storageKeys := make([]common.Hash, 0, len(slots))
		for slot := range slots {
			storageKeys = append(storageKeys, slot)
		}
		sort.Slice(storageKeys, func(i, j int) bool {
			return storageKeys[i].Cmp(storageKeys[j]) < 0
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
func NewAccessListTracer(acl types.AccessList, exclude map[common.Address]struct{}, state *state.IntraBlockState) *AccessListTracer {
	excl := make(map[common.Address]struct{})
	if exclude != nil {
		excl = exclude
	}
	list := newAccessList()
	for _, al := range acl {
		if _, ok := excl[al.Address]; !ok {
			list.addAddress(al.Address)
		}
		for _, slot := range al.StorageKeys {
			list.addSlot(al.Address, slot)
		}
	}
	return &AccessListTracer{
		excl:               excl,
		list:               list,
		state:              state,
		createdContracts:   make(map[common.Address]struct{}),
		usedBeforeCreation: make(map[common.Address]struct{}),
	}
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
		if _, ok := a.excl[addr]; !ok {
			a.list.addSlot(addr, slot)
			if _, ok := a.createdContracts[addr]; !ok {
				a.usedBeforeCreation[addr] = struct{}{}
			}
		}
	}
	if (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT) && stackLen >= 1 {
		addr := common.Address(stackData[stackLen-1].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
			if _, ok := a.createdContracts[addr]; !ok {
				a.usedBeforeCreation[addr] = struct{}{}
			}
		}
	}
	if (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE) && stackLen >= 5 {
		addr := common.Address(stackData[stackLen-2].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
			if _, ok := a.createdContracts[addr]; !ok {
				a.usedBeforeCreation[addr] = struct{}{}
			}
		}
	}
	if op == vm.CREATE {
		// contract address for CREATE can only be generated with state
		if a.state != nil {
			nonce, _ := a.state.GetNonce(scope.Address())
			addr := types.CreateAddress(scope.Address(), nonce)
			if _, ok := a.excl[addr]; !ok {
				a.createdContracts[addr] = struct{}{}
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
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		addr := types.CreateAddress2(scope.Address(), salt.Bytes32(), inithash)
		if _, ok := a.excl[addr]; !ok {
			a.createdContracts[addr] = struct{}{}
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

// CreatedContracts returns the set of all addresses of contracts created during txn execution.
func (a *AccessListTracer) CreatedContracts() map[common.Address]struct{} {
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
