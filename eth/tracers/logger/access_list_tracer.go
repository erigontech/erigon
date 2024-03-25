// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package logger

import (
	"encoding/json"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// accessList is an accumulator for the set of accounts and storage slots an EVM
// contract execution touches.
type accessList map[libcommon.Address]accessListSlots

// accessListSlots is an accumulator for the set of storage slots within a single
// contract that an EVM contract execution touches.
type accessListSlots map[libcommon.Hash]struct{}

// newAccessList creates a new accessList.
func newAccessList() accessList {
	return make(map[libcommon.Address]accessListSlots)
}

// addAddress adds an address to the accesslist.
func (al accessList) addAddress(address libcommon.Address) {
	// Set address if not previously present
	if _, present := al[address]; !present {
		al[address] = make(map[libcommon.Hash]struct{})
	}
}

// addSlot adds a storage slot to the accesslist.
func (al accessList) addSlot(address libcommon.Address, slot libcommon.Hash) {
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

// accesslist converts the accesslist to a types2.AccessList.
func (al accessList) accessList() types2.AccessList {
	acl := make(types2.AccessList, 0, len(al))
	for addr, slots := range al {
		tuple := types2.AccessTuple{Address: addr, StorageKeys: []libcommon.Hash{}}
		for slot := range slots {
			tuple.StorageKeys = append(tuple.StorageKeys, slot)
		}
		acl = append(acl, tuple)
	}
	return acl
}

// accesslist converts the accesslist to a types2.AccessList.
func (al accessList) accessListSorted() types2.AccessList {
	acl := make(types2.AccessList, 0, len(al))
	for addr, slots := range al {
		storageKeys := make([]libcommon.Hash, 0, len(slots))
		for slot := range slots {
			storageKeys = append(storageKeys, slot)
		}
		sort.Slice(storageKeys, func(i, j int) bool {
			return storageKeys[i].String() < storageKeys[j].String()
		})
		acl = append(acl, types2.AccessTuple{
			Address:     addr,
			StorageKeys: storageKeys,
		})
	}
	return acl
}

// AccessListTracer is a tracer that accumulates touched accounts and storage
// slots into an internal set.
type AccessListTracer struct {
	excl               map[libcommon.Address]struct{} // Set of account to exclude from the list
	list               accessList                     // Set of accounts and storage slots touched
	state              evmtypes.IntraBlockState       // State for nonce calculation of created contracts
	createdContracts   map[libcommon.Address]struct{} // Set of all addresses of contracts created during tx execution
	usedBeforeCreation map[libcommon.Address]struct{} // Set of all contract addresses first used before creation
}

// NewAccessListTracer creates a new tracer that can generate AccessLists.
// An optional AccessList can be specified to occupy slots and addresses in
// the resulting accesslist.
// An optional set of addresses to be excluded from the resulting accesslist can
// also be specified.
func NewAccessListTracer(acl types2.AccessList, exclude map[libcommon.Address]struct{}, state evmtypes.IntraBlockState) *AccessListTracer {
	excl := make(map[libcommon.Address]struct{})
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
		createdContracts:   make(map[libcommon.Address]struct{}),
		usedBeforeCreation: make(map[libcommon.Address]struct{}),
	}
}

func (a *AccessListTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnOpcode: a.OnOpcode,
	}
}

// OnOpcode captures all opcodes that touch storage or addresses and adds them to the accesslist.
func (a *AccessListTracer) OnOpcode(pc uint64, opcode byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	stackData := scope.StackData()
	stackLen := len(stackData)
	op := vm.OpCode(opcode)
	if (op == vm.SLOAD || op == vm.SSTORE) && stackLen >= 1 {
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		a.list.addSlot(scope.Address(), slot)
	}
	if (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT) && stackLen >= 1 {
		addr := libcommon.Address(stackData[stackLen-1].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
		}
	}
	if (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE) && stackLen >= 5 {
		addr := libcommon.Address(stackData[stackLen-2].Bytes20())
		if _, ok := a.excl[addr]; !ok {
			a.list.addAddress(addr)
		}
	}
}

// GetResult returns an empty json object.
func (a *AccessListTracer) GetResult() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (a *AccessListTracer) Stop(err error) {
}

// AccessList returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessList() types2.AccessList {
	return a.list.accessList()
}

// AccessList returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessListSorted() types2.AccessList {
	return a.list.accessListSorted()
}

// CreatedContracts returns the set of all addresses of contracts created during tx execution.
func (a *AccessListTracer) CreatedContracts() map[libcommon.Address]struct{} {
	return a.createdContracts
}

// UsedBeforeCreation returns for a given address whether it was first used before creation.
func (a *AccessListTracer) UsedBeforeCreation(addr libcommon.Address) bool {
	_, contained := a.usedBeforeCreation[addr]
	return contained
}

// Equal returns if the content of two access list traces are equal.
func (a *AccessListTracer) Equal(other *AccessListTracer) bool {
	return a.list.equal(other.list)
}
