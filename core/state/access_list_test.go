// Copyright 2024 The Erigon Authors
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	stateLib "github.com/erigontech/erigon-lib/state"
)

func verifyAddrs(t *testing.T, s *IntraBlockState, astrings ...string) {
	t.Helper()
	// convert to common.Address form
	addresses := make([]common.Address, 0, len(astrings))
	var addressMap = make(map[common.Address]struct{})
	for _, astring := range astrings {
		address := common.HexToAddress(astring)
		addresses = append(addresses, address)
		addressMap[address] = struct{}{}
	}
	// Check that the given addresses are in the access list
	for _, address := range addresses {
		if !s.AddressInAccessList(address) {
			t.Fatalf("expected %x to be in access list", address)
		}
	}
	// Check that only the expected addresses are present in the acesslist
	for address := range s.accessList.addresses {
		if _, exist := addressMap[address]; !exist {
			t.Fatalf("extra address %x in access list", address)
		}
	}
}

func verifySlots(t *testing.T, s *IntraBlockState, addrString string, slotStrings ...string) {
	if !s.AddressInAccessList(common.HexToAddress(addrString)) {
		t.Fatalf("scope missing address/slots %v", addrString)
	}
	var address = common.HexToAddress(addrString)
	// convert to common.Hash form
	slots := make([]common.Hash, 0, len(slotStrings))
	var slotMap = make(map[common.Hash]struct{})
	for _, slotString := range slotStrings {
		s := common.HexToHash(slotString)
		slots = append(slots, s)
		slotMap[s] = struct{}{}
	}
	// Check that the expected items are in the access list
	for i, slot := range slots {
		if _, slotPresent := s.SlotInAccessList(address, slot); !slotPresent {
			t.Fatalf("input %d: scope missing slot %v (address %v)", i, slot, addrString)
		}
	}
	// Check that no extra elements are in the access list
	stateSlots := s.accessList.addresses[address]
	for s := range stateSlots {
		if _, slotPresent := slotMap[s]; !slotPresent {
			t.Fatalf("scope has extra slot %v (address %v)", s, addrString)
		}
	}
}

func TestAccessList(t *testing.T) {
	t.Parallel()
	// Some helpers
	addr := common.HexToAddress
	slot := common.HexToHash

	_, tx, _ := NewTestTemporalDb(t)

	domains, err := stateLib.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	state := New(NewReaderV3(domains.AsGetter(tx)))

	state.accessList = newAccessList()

	state.AddAddressToAccessList(addr("aa"))          // 1
	state.AddSlotToAccessList(addr("bb"), slot("01")) // 2,3
	state.AddSlotToAccessList(addr("bb"), slot("02")) // 4
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02")

	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02")
	if got, exp := len(state.accessList.addresses), 2; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}

	if exp, got := 4, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}

	// same again, should cause no journal entries
	state.AddSlotToAccessList(addr("bb"), slot("01"))
	state.AddSlotToAccessList(addr("bb"), slot("02"))
	state.AddAddressToAccessList(addr("aa"))
	if exp, got := 4, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}
	// some new ones
	state.AddSlotToAccessList(addr("bb"), slot("03")) // 5
	state.AddSlotToAccessList(addr("aa"), slot("01")) // 6
	state.AddSlotToAccessList(addr("cc"), slot("01")) // 7,8
	state.AddAddressToAccessList(addr("cc"))
	if exp, got := 8, state.journal.length(); exp != got {
		t.Fatalf("journal length mismatch: have %d, want %d", got, exp)
	}

	verifyAddrs(t, state, "aa", "bb", "cc")
	verifySlots(t, state, "aa", "01")
	verifySlots(t, state, "bb", "01", "02", "03")
	verifySlots(t, state, "cc", "01")

	// now start rolling back changes
	state.journal.revert(state, 7)
	if _, ok := state.SlotInAccessList(addr("cc"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb", "cc")
	verifySlots(t, state, "aa", "01")
	verifySlots(t, state, "bb", "01", "02", "03")

	state.journal.revert(state, 6)
	if state.AddressInAccessList(addr("cc")) {
		t.Fatalf("addr present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "aa", "01")
	verifySlots(t, state, "bb", "01", "02", "03")

	state.journal.revert(state, 5)
	if _, ok := state.SlotInAccessList(addr("aa"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02", "03")

	state.journal.revert(state, 4)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("03")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02")

	state.journal.revert(state, 3)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("02")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01")

	state.journal.revert(state, 2)
	if _, ok := state.SlotInAccessList(addr("bb"), slot("01")); ok {
		t.Fatalf("slot present, expected missing")
	}
	verifyAddrs(t, state, "aa", "bb")

	state.journal.revert(state, 1)
	if state.AddressInAccessList(addr("bb")) {
		t.Fatalf("addr present, expected missing")
	}
	verifyAddrs(t, state, "aa")

	state.journal.revert(state, 0)
	if state.AddressInAccessList(addr("aa")) {
		t.Fatalf("addr present, expected missing")
	}
	if got, exp := len(state.accessList.addresses), 0; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}

	require.Len(t, state.accessList.codeAccesses, 0)
	require.Len(t, state.journal.entries, 0)
	changed := state.AddCodeAddressToAccessList(addr("0x0001"))
	require.True(t, changed)
	require.Len(t, state.accessList.codeAccesses, 1)
	require.Len(t, state.journal.entries, 1)
	require.Contains(t, state.accessList.codeAccesses, addr("0x0001"))
	changed = state.AddCodeAddressToAccessList(addr("0x0002"))
	require.True(t, changed)
	require.Len(t, state.accessList.codeAccesses, 2)
	require.Len(t, state.journal.entries, 2)
	require.Contains(t, state.accessList.codeAccesses, addr("0x0001"))
	require.Contains(t, state.accessList.codeAccesses, addr("0x0002"))
	changed = state.AddCodeAddressToAccessList(addr("0x0001"))
	require.False(t, changed)
	require.Len(t, state.accessList.codeAccesses, 2)
	require.Len(t, state.journal.entries, 2)
	require.Contains(t, state.accessList.codeAccesses, addr("0x0001"))
	require.Contains(t, state.accessList.codeAccesses, addr("0x0002"))
	state.journal.revert(state, 1)
	require.Len(t, state.accessList.codeAccesses, 1)
	require.Len(t, state.journal.entries, 1)
	require.Contains(t, state.accessList.codeAccesses, addr("0x0001"))
	state.journal.revert(state, 0)
	require.Len(t, state.accessList.codeAccesses, 0)
	require.Len(t, state.journal.entries, 0)
}
