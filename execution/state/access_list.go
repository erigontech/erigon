// Copyright 2020 The go-ethereum Authors
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

package state

import (
	"maps"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// accessList tracks addresses and storage slots accessed during a transaction.
// addresses maps each address to its index in slots (-1 if address-only, no slots).
// This layout matches go-ethereum's design: the slots slice backing array is
// reused across transactions via Reset, eliminating per-tx slot-map allocations.
type accessList struct {
	addresses map[accounts.Address]int
	slots     []map[accounts.StorageKey]struct{}
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[accounts.Address]int),
	}
}

// Reset clears the access list for reuse, keeping allocated memory.
// The slots backing array is retained; cleared inner maps are reused by
// subsequent AddSlot calls without new allocations.
func (al *accessList) Reset() {
	for _, s := range al.slots {
		clear(s)
	}
	al.slots = al.slots[:0]
	clear(al.addresses)
}

// ContainsAddress returns true if the address is in the access list.
func (al *accessList) ContainsAddress(address accounts.Address) bool {
	_, ok := al.addresses[address]
	return ok
}

// Contains checks if a slot within an account is present in the access list, returning
// separate flags for the presence of the account and the slot respectively.
func (al *accessList) Contains(address accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	idx, ok := al.addresses[address]
	if !ok {
		return false, false
	}
	if idx == -1 {
		return true, false
	}
	_, slotPresent = al.slots[idx][slot]
	return true, slotPresent
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := &accessList{
		addresses: maps.Clone(al.addresses),
		slots:     make([]map[accounts.StorageKey]struct{}, len(al.slots)),
	}
	for i, slotMap := range al.slots {
		cp.slots[i] = maps.Clone(slotMap)
	}
	return cp
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address accounts.Address) bool {
	if _, present := al.addresses[address]; present {
		return false
	}
	al.addresses[address] = -1
	return true
}

// AddSlot adds the specified (addr, slot) combo to the access list.
// Return values are:
// - address added
// - slot added
// For any 'true' value returned, a corresponding journal entry must be made.
func (al *accessList) AddSlot(address accounts.Address, slot accounts.StorageKey) (addrChange bool, slotChange bool) {
	idx, addrPresent := al.addresses[address]
	if !addrPresent || idx == -1 {
		// Address not present, or addr present but no slots yet.
		// Reuse a cleared slot map from the backing array if available.
		newIdx := len(al.slots)
		al.addresses[address] = newIdx
		var slotmap map[accounts.StorageKey]struct{}
		if newIdx < cap(al.slots) {
			slotmap = al.slots[:cap(al.slots)][newIdx]
		}
		if slotmap == nil {
			slotmap = make(map[accounts.StorageKey]struct{})
		}
		slotmap[slot] = struct{}{}
		al.slots = append(al.slots, slotmap)
		return !addrPresent, true
	}
	slotmap := al.slots[idx]
	if _, ok := slotmap[slot]; !ok {
		slotmap[slot] = struct{}{}
		return false, true
	}
	return false, false
}

// DeleteSlot removes an (address, slot)-tuple from the access list.
// This operation needs to be performed in the same order as the addition happened.
// This method is meant to be used by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteSlot(address accounts.Address, slot accounts.StorageKey) {
	idx, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting slot change, address not present in list")
	}
	if idx == -1 {
		panic("reverting slot change, address has no slots")
	}
	slotmap := al.slots[idx]
	delete(slotmap, slot)
	// Since additions and rollbacks are always in LIFO order, when a slot map
	// becomes empty it must be the last one appended — truncate the slice.
	if len(slotmap) == 0 {
		if idx != len(al.slots)-1 {
			panic("reverting slot change, LIFO violation: emptied slot map is not the last element")
		}
		al.slots = al.slots[:idx]
		al.addresses[address] = -1
	}
}

// DeleteAddress removes an address from the access list. This operation
// needs to be performed in the same order as the addition happened.
// This method is meant to be used by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteAddress(address accounts.Address) {
	idx, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting address change, address not present in list")
	}
	if idx != -1 {
		panic("reverting address change, address still has slots")
	}
	delete(al.addresses, address)
}
