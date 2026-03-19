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
// slots[i] is the ordered list of slots for the address at index i; nil = warm, no slots.
// Slots are appended in order and removed in LIFO order by the journal.
type accessList struct {
	addresses map[accounts.Address]int
	slots     [][]accounts.StorageKey // slots[i] is the slot set for addrs[i]; nil = warm, no slots
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[accounts.Address]int),
	}
}

// Reset clears the access list for reuse, keeping allocated memory.
// Inner slices are truncated to zero; their backing arrays are reused by
// subsequent AddSlot calls without new allocations.
func (al *accessList) Reset() {
	for i := range al.slots {
		al.slots[i] = al.slots[i][:0]
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
	for _, s := range al.slots[idx] {
		if s == slot {
			return true, true
		}
	}
	return true, false
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := &accessList{
		addresses: maps.Clone(al.addresses),
		slots:     make([][]accounts.StorageKey, len(al.slots)),
	}
	for i, s := range al.slots {
		cp.slots[i] = append([]accounts.StorageKey(nil), s...)
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
		// Reuse a truncated inner slice from the backing array if available.
		newIdx := len(al.slots)
		al.addresses[address] = newIdx
		var s []accounts.StorageKey
		if newIdx < cap(al.slots) {
			s = al.slots[:cap(al.slots)][newIdx]
		}
		al.slots = append(al.slots, append(s, slot))
		return !addrPresent, true
	}
	for _, s := range al.slots[idx] {
		if s == slot {
			return false, false
		}
	}
	al.slots[idx] = append(al.slots[idx], slot)
	return false, true
}

// DeleteSlot removes an (address, slot)-tuple from the access list.
// This operation needs to be performed in the same order as the addition happened.
// This method is meant to be used by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteSlot(address accounts.Address, slot accounts.StorageKey) {
	al.PopSlot(address)
	_ = slot // journal is LIFO: the slot being deleted is always the last one added
}

// PopSlot removes the last slot added for address. Must be called in LIFO order
// matching AddSlot calls, as maintained by the journal.
func (al *accessList) PopSlot(address accounts.Address) {
	idx, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting slot change, address not present in list")
	}
	al.slots[idx] = al.slots[idx][:len(al.slots[idx])-1]
	// Since additions and rollbacks are always in LIFO order, when a slot list
	// becomes empty it must be the last one appended — truncate the slice.
	if len(al.slots[idx]) == 0 {
		al.slots = al.slots[:idx]
		al.addresses[address] = -1
	}
}

// DeleteAddress removes an address from the access list. This operation
// needs to be performed in the same order as the addition happened.
// This method is meant to be used by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteAddress(address accounts.Address) {
	delete(al.addresses, address)
}
