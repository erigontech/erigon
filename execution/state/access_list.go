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
// PopAddress / PopSlot exploit the LIFO journal invariant for O(1) removal.
//
// Reset reuses the backing arrays of both addrs and slots (including the inner
// slot slices), so steady-state allocations per transaction are zero.
type accessList struct {
	addrs []accounts.Address      // parallel to slots
	slots [][]accounts.StorageKey // slots[i] is the slot set for addrs[i]; nil or empty = warm, no slots
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
		for _, s := range al.slots[i] {
			if s == slot {
				return true, true
			}
		}
		return true, false
	}
	return false, false
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{}
}

// Reset clears the access list for reuse across transactions.
// It keeps both backing arrays and all inner slot-slice backing arrays alive
// so that subsequent transactions repopulate them without allocating.
func (al *accessList) Reset() {
	for i := range al.slots {
		al.slots[i] = al.slots[i][:0] // keep slot-slice backing array
	}
	al.addrs = al.addrs[:0]
	al.slots = al.slots[:0]
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := &accessList{
		addrs: make([]accounts.Address, len(al.addrs)),
		slots: make([][]accounts.StorageKey, len(al.slots)),
	}
	copy(cp.addrs, al.addrs)
	for i, s := range al.slots {
		if len(s) == 0 {
			continue // keep nil for address-only entries
		}
		sc := make([]accounts.StorageKey, len(s))
		copy(sc, s)
		cp.slots[i] = sc
	}
	return cp
}

// appendAddr appends a new entry to both parallel slices, reusing the slot-slice
// backing array when available (left by a prior Reset).
//
// addrs grows via append (may reallocate), while slots is resliced when possible.
// Both slices' lengths are kept in lockstep; their capacities are independent.
func (al *accessList) appendAddr(address accounts.Address) {
	n := len(al.addrs)
	al.addrs = append(al.addrs, address)
	if n < cap(al.slots) {
		al.slots = al.slots[:n+1] // reuse cleared slot slice from backing array
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
		for _, s := range al.slots[i] {
			if s == slot {
				return false, false
			}
		}
		al.slots[i] = append(al.slots[i], slot)
		return false, true
	}
	// address not present — add it together with the slot
	al.appendAddr(address)
	n := len(al.addrs) - 1
	al.slots[n] = append(al.slots[n], slot)
	return true, true
}

// PopSlot removes the last slot added to address, relying on the LIFO journal
// invariant. O(1) after finding the address.
func (al *accessList) PopSlot(address accounts.Address) {
	for i, a := range al.addrs {
		if a != address {
			continue
		}
		al.slots[i] = al.slots[i][:len(al.slots[i])-1]
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
	al.slots[n-1] = nil // release slot slice references for GC
	al.addrs = al.addrs[:n-1]
	al.slots = al.slots[:n-1]
}
