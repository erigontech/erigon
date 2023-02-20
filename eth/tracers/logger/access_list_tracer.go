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
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/core/vm"
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

// AccessListTracer is a tracer that accumulates touched accounts and storage
// slots into an internal set.
type AccessListTracer struct {
	excl map[libcommon.Address]struct{} // Set of account to exclude from the list
	list accessList                     // Set of accounts and storage slots touched
}

// NewAccessListTracer creates a new tracer that can generate AccessLists.
// An optional AccessList can be specified to occupy slots and addresses in
// the resulting accesslist.
func NewAccessListTracer(acl types2.AccessList, from, to libcommon.Address, precompiles []libcommon.Address) *AccessListTracer {
	excl := map[libcommon.Address]struct{}{
		from: {}, to: {},
	}
	for _, addr := range precompiles {
		excl[addr] = struct{}{}
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
		excl: excl,
		list: list,
	}
}

func (a *AccessListTracer) CaptureTxStart(gasLimit uint64) {}

func (a *AccessListTracer) CaptureTxEnd(restGas uint64) {}

func (a *AccessListTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

func (a *AccessListTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

// CaptureState captures all opcodes that touch storage or addresses and adds them to the accesslist.
func (a *AccessListTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	stack := scope.Stack
	contract := scope.Contract

	stackData := stack.Data
	stackLen := len(stackData)
	if (op == vm.SLOAD || op == vm.SSTORE) && stackLen >= 1 {
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		a.list.addSlot(contract.Address(), slot)
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

func (*AccessListTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}
func (*AccessListTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
}
func (*AccessListTracer) CaptureExit(output []byte, usedGas uint64, err error) {
}

// AccessList returns the current accesslist maintained by the tracer.
func (a *AccessListTracer) AccessList() types2.AccessList {
	return a.list.accessList()
}

// Equal returns if the content of two access list traces are equal.
func (a *AccessListTracer) Equal(other *AccessListTracer) bool {
	return a.list.equal(other.list)
}
