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
	"github.com/erigontech/erigon-lib/common"
)

type accessList struct {
	addresses map[common.Address]map[common.Hash]struct{}
}

// ContainsAddress returns true if the address is in the access list.
func (al *accessList) ContainsAddress(address common.Address) bool {
	_, ok := al.addresses[address]
	return ok
}

// Reset
//func (al *accessList) Reset() {
//	clear(al.addresses)
//	clear(al.slots)
//	al.slots = al.slots[:0]
//}

// Contains checks if a slot within an account is present in the access list, returning
// separate flags for the presence of the account and the slot respectively.
func (al *accessList) Contains(address common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	slots, ok := al.addresses[address]
	if !ok {
		// no such address (and hence zero slots)
		return false, false
	}
	if slots == nil {
		// address yes, but no slots
		return true, false
	}
	_, slotPresent = slots[slot]
	return true, slotPresent
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: map[common.Address]map[common.Hash]struct{}{},
	}
}

//func (al *accessList) Reset() {
//	clear(al.addresses)
//	clear(al.slots)
//}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := newAccessList()
	for k, v := range al.addresses {
		if v == nil {
			cp.addresses[k] = v
		} else {
			slots := map[common.Hash]struct{}{}
			for k := range v {
				slots[k] = struct{}{}
			}
			cp.addresses[k] = slots
		}
	}

	return cp
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address common.Address) bool {
	if _, present := al.addresses[address]; present {
		return false
	}
	al.addresses[address] = nil
	return true
}

// AddSlot adds the specified (addr, slot) combo to the access list.
// Return values are:
// - address added
// - slot added
// For any 'true' value returned, a corresponding journal entry must be made.
func (al *accessList) AddSlot(address common.Address, slot common.Hash) (addrChange bool, slotChange bool) {
	slots, addrPresent := al.addresses[address]
	if !addrPresent || slots == nil {
		// Address not present, or addr present but no slots there
		al.addresses[address] = map[common.Hash]struct{}{slot: {}}
		return !addrPresent, true
	}
	if _, ok := slots[slot]; !ok {
		slots[slot] = struct{}{}
		// Journal add slot change
		return false, true
	}
	// No changes required
	return false, false
}

// DeleteSlot removes an (address, slot)-tuple from the access list.
// This operation needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteSlot(address common.Address, slot common.Hash) {
	slots, addrOk := al.addresses[address]
	// There are two ways this can fail
	if !addrOk {
		panic("reverting slot change, address not present in list")
	}
	delete(slots, slot)
	// If that was the last (first) slot, remove it
	// Since additions and rollbacks are always performed in order,
	// we can delete the item without worrying about screwing up later indices
	if len(slot) == 0 {
		al.addresses[address] = nil
	}
}

// DeleteAddress removes an address from the access list. This operation
// needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteAddress(address common.Address) {
	slots, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting address change, address not present in list")
	}
	if len(slots) > 0 {
		panic("reverting address change, address has slots")
	}
	delete(al.addresses, address)
}
