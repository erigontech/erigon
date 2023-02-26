// Copyright 2020 The go-ethereum Authors
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

package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/exp/maps"
)

type accessList struct {
	addresses map[libcommon.Address]int
	slots     []map[libcommon.Hash]struct{}
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[libcommon.Address]int),
	}
}

func (al *accessList) Reset() {
	if ResetMapsByClean {
		maps.Clear(al.addresses)
	} else {
		al.addresses = make(map[libcommon.Address]int)
	}
	for i := range al.slots {
		al.slots[i] = nil
	}
	al.slots = al.slots[:0]
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := newAccessList()
	for k, v := range al.addresses {
		cp.addresses[k] = v
	}
	cp.slots = make([]map[libcommon.Hash]struct{}, len(al.slots))
	for i, slotMap := range al.slots {
		newSlotmap := make(map[libcommon.Hash]struct{}, len(slotMap))
		for k := range slotMap {
			newSlotmap[k] = struct{}{}
		}
		cp.slots[i] = newSlotmap
	}
	return cp
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address libcommon.Address) bool {
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
func (al *accessList) AddSlot(address libcommon.Address, slot libcommon.Hash) (addrChange bool, slotChange bool) {
	idx, addrPresent := al.addresses[address]
	if !addrPresent || idx == -1 {
		// Address not present, or addr present but no slots there
		al.addresses[address] = len(al.slots)
		slotmap := map[libcommon.Hash]struct{}{slot: {}}
		al.slots = append(al.slots, slotmap)
		return !addrPresent, true
	}
	// There is already an (address,slot) mapping
	slotmap := al.slots[idx]
	if _, ok := slotmap[slot]; !ok {
		slotmap[slot] = struct{}{}
		// Journal add slot change
		return false, true
	}
	// No changes required
	return false, false
}
