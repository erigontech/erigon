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

// accessList maps each address to its index in slots (-1 = address-only); the slots backing array is reused across transactions via Reset to avoid per-tx allocations.
type accessList struct {
	addresses map[accounts.Address]int
	slots     []map[accounts.StorageKey]struct{}

	// Memo of the last resolved (addr -> slot set) and its last-known-warm slot, so repeated AddSlot on the same addr/slot skips the map lookups.
	// lastSlots == nil means no memo; lastAddr alone can't say, since NilAddress is both a legal argument and that field's zero value.
	lastAddr     accounts.Address
	lastSlots    map[accounts.StorageKey]struct{}
	lastWarmSlot accounts.StorageKey
}

func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[accounts.Address]int),
	}
}

// Reset keeps the backing array and inner maps so subsequent AddSlot calls need no new allocations.
func (al *accessList) Reset() {
	for _, s := range al.slots {
		clear(s)
	}
	al.slots = al.slots[:0]
	clear(al.addresses)
	al.dropMemo()
}

func (al *accessList) dropMemo() {
	al.lastAddr = accounts.NilAddress
	al.lastSlots = nil
	al.lastWarmSlot = accounts.StorageKey{}
}

func (al *accessList) ContainsAddress(address accounts.Address) bool {
	_, ok := al.addresses[address]
	return ok
}

func (al *accessList) Contains(address accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	if al.lastSlots != nil && al.lastAddr == address {
		if slot == al.lastWarmSlot {
			return true, true
		}
		_, slotPresent = al.lastSlots[slot]
		return true, slotPresent
	}
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

func (al *accessList) AddAddress(address accounts.Address) bool {
	if _, present := al.addresses[address]; present {
		return false
	}
	al.addresses[address] = -1
	return true
}

// AddSlot returns which of address/slot are newly added; the journal must record an entry for each true result.
func (al *accessList) AddSlot(address accounts.Address, slot accounts.StorageKey) (addrChange bool, slotChange bool) {
	if al.lastSlots != nil && al.lastAddr == address {
		if slot == al.lastWarmSlot {
			return false, false
		}
		// probe-then-insert: a plain read on the warm case beats mapassign's write bookkeeping, since warm re-reads dominate cold inserts
		if _, ok := al.lastSlots[slot]; ok {
			al.lastWarmSlot = slot
			return false, false
		}
		al.lastSlots[slot] = struct{}{}
		al.lastWarmSlot = slot
		return false, true
	}
	return al.addSlotSlow(address, slot)
}

func (al *accessList) addSlotSlow(address accounts.Address, slot accounts.StorageKey) (addrChange bool, slotChange bool) {
	idx, addrPresent := al.addresses[address]
	if !addrPresent || idx == -1 {
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
		al.lastAddr, al.lastSlots, al.lastWarmSlot = address, slotmap, slot
		return !addrPresent, true
	}
	slotmap := al.slots[idx]
	al.lastAddr, al.lastSlots, al.lastWarmSlot = address, slotmap, slot
	if _, ok := slotmap[slot]; ok {
		return false, false
	}
	slotmap[slot] = struct{}{}
	return false, true
}

// DeleteSlot must be called in LIFO order matching the additions — it's meant for the journal's revert path only.
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
	al.dropMemo()
	// LIFO order guarantees an emptied slot map is always the last one appended, so truncating the slice is safe.
	if len(slotmap) == 0 {
		if idx != len(al.slots)-1 {
			panic("reverting slot change, LIFO violation: emptied slot map is not the last element")
		}
		al.slots = al.slots[:idx]
		al.addresses[address] = -1
	}
}

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
