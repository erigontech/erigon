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
	"github.com/erigontech/erigon/execution/types/accounts"
)

// accessList tracks EIP-2929 accessed addresses and storage slots.
//
// Design: two parallel slices instead of a map.
//
//	addrs[i] is a warm address; slots[i] is its warm slot set (nil = no slots).
//
// accounts.Address is unique.Handle — an 8-byte interned pointer — so equality
// is a single pointer comparison.  For the typical 12-20 warm addresses per
// transaction a linear scan over addrs is faster than map hashing.
//
// Slot sets use maps for O(1) lookup. accounts.StorageKey is also a
// unique.Handle (8-byte pointer), so map hash/compare is cheap.
//
// PopAddress / PopSlot exploit the LIFO journal invariant for O(1) removal.
//
// Reset reuses the backing arrays of addrs and slots, and uses clear() on each
// map to preserve bucket arrays, so steady-state allocations per transaction
// are zero.
type accessList struct {
	addrs []accounts.Address                 // parallel to slots
	slots []map[accounts.StorageKey]struct{} // slots[i] is the slot set for addrs[i]; nil = warm, no slots
}

// ContainsAddress returns true if the address is in the access list.
func (al *accessList) ContainsAddress(address accounts.Address) bool {
	for _, a := range al.addrs {
		if a == address {
			return true
		}
	}
	return false
}

// Contains checks if a slot within an account is present in the access list, returning
// separate flags for the presence of the account and the slot respectively.
func (al *accessList) Contains(address accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	for i, a := range al.addrs {
		if a != address {
			continue
		}
		if al.slots[i] != nil {
			_, slotPresent = al.slots[i][slot]
		}
		return true, slotPresent
	}
	return false, false
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{}
}

// Reset clears the access list for reuse across transactions.
// It keeps both backing arrays and uses clear() on each map to preserve
// bucket arrays, so that subsequent transactions repopulate without allocating.
func (al *accessList) Reset() {
	for _, m := range al.slots {
		clear(m) // preserve bucket array
	}
	al.addrs = al.addrs[:0]
	al.slots = al.slots[:0]
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := &accessList{
		addrs: make([]accounts.Address, len(al.addrs)),
		slots: make([]map[accounts.StorageKey]struct{}, len(al.slots)),
	}
	copy(cp.addrs, al.addrs)
	for i, m := range al.slots {
		if len(m) == 0 {
			continue // keep nil for address-only entries
		}
		mc := make(map[accounts.StorageKey]struct{}, len(m))
		for k, v := range m {
			mc[k] = v
		}
		cp.slots[i] = mc
	}
	return cp
}

// appendAddr appends a new entry to both parallel slices, reusing the cleared
// map from the backing array when available (left by a prior Reset).
//
// addrs grows via append (may reallocate), while slots is resliced when possible.
// Both slices' lengths are kept in lockstep; their capacities are independent.
func (al *accessList) appendAddr(address accounts.Address) {
	n := len(al.addrs)
	al.addrs = append(al.addrs, address)
	if n < cap(al.slots) {
		al.slots = al.slots[:n+1] // reuse cleared map from backing array
	} else {
		al.slots = append(al.slots, nil)
	}
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address accounts.Address) bool {
	for _, a := range al.addrs {
		if a == address {
			return false
		}
	}
	al.appendAddr(address)
	return true
}

// AddSlot adds the specified (addr, slot) combo to the access list.
// Return values are:
// - address added
// - slot added
// For any 'true' value returned, a corresponding journal entry must be made.
func (al *accessList) AddSlot(address accounts.Address, slot accounts.StorageKey) (addrChange bool, slotChange bool) {
	for i, a := range al.addrs {
		if a != address {
			continue
		}
		m := al.slots[i]
		if m != nil {
			if _, ok := m[slot]; ok {
				return false, false
			}
		} else {
			m = make(map[accounts.StorageKey]struct{}, 4)
			al.slots[i] = m
		}
		m[slot] = struct{}{}
		return false, true
	}
	// address not present — add it together with the slot
	al.appendAddr(address)
	n := len(al.addrs) - 1
	m := al.slots[n]
	if m == nil {
		m = make(map[accounts.StorageKey]struct{}, 4)
		al.slots[n] = m
	}
	m[slot] = struct{}{}
	return true, true
}

// PopSlot removes a specific slot from address, used by journal revert.
// O(1) map delete after finding the address.
func (al *accessList) PopSlot(address accounts.Address, slot accounts.StorageKey) {
	for i, a := range al.addrs {
		if a != address {
			continue
		}
		if len(al.slots[i]) == 0 {
			panic("reverting slot change, address has no slots in access list")
		}
		delete(al.slots[i], slot)
		return
	}
	panic("reverting slot change, address not present in list")
}

// PopAddress removes the last address added to the list, relying on the LIFO
// journal invariant. O(1) pop.
func (al *accessList) PopAddress() {
	if len(al.addrs) == 0 {
		panic("reverting address change, access list is empty")
	}
	n := len(al.addrs)
	if len(al.slots[n-1]) != 0 {
		panic("reverting address change, address still has warm slots")
	}
	al.slots[n-1] = nil // release map for GC
	al.addrs = al.addrs[:n-1]
	al.slots = al.slots[:n-1]
}
