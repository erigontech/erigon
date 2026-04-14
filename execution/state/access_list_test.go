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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func verifyAddrs(t *testing.T, s *IntraBlockState, astrings ...string) {
	t.Helper()
	// convert to common.Address form
	addresses := make([]accounts.Address, 0, len(astrings))
	var addressMap = make(map[accounts.Address]struct{})
	for _, astring := range astrings {
		address := accounts.InternAddress(common.HexToAddress(astring))
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
	for _, address := range s.accessList.addrs {
		if _, exist := addressMap[address]; !exist {
			t.Fatalf("extra address %x in access list", address)
		}
	}
}

func verifySlots(t *testing.T, s *IntraBlockState, addrString string, slotStrings ...string) {
	if !s.AddressInAccessList(accounts.InternAddress(common.HexToAddress(addrString))) {
		t.Fatalf("scope missing address/slots %v", addrString)
	}
	var address = accounts.InternAddress(common.HexToAddress(addrString))

	slots := make([]accounts.StorageKey, 0, len(slotStrings))
	var slotMap = make(map[accounts.StorageKey]struct{})
	for _, slotString := range slotStrings {
		s := accounts.InternKey(common.HexToHash(slotString))
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
	for i, a := range s.accessList.addrs {
		if a == address {
			for s := range s.accessList.slots[i] {
				if _, slotPresent := slotMap[s]; !slotPresent {
					t.Fatalf("scope has extra slot %v (address %v)", s, addrString)
				}
			}
			break
		}
	}
}

func TestAccessList(t *testing.T) {
	t.Parallel()
	// Some helpers
	addr := func(a string) accounts.Address { return accounts.InternAddress(common.HexToAddress(a)) }
	slot := func(s string) accounts.StorageKey { return accounts.InternKey(common.HexToHash(s)) }

	_, tx, domains := NewTestRwTx(t)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	state := New(NewReaderV3(domains.AsGetter(tx)))

	state.accessList.Reset()

	state.AddAddressToAccessList(addr("aa"))          // 1
	state.AddSlotToAccessList(addr("bb"), slot("01")) // 2,3
	state.AddSlotToAccessList(addr("bb"), slot("02")) // 4
	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02")

	verifyAddrs(t, state, "aa", "bb")
	verifySlots(t, state, "bb", "01", "02")
	if got, exp := len(state.accessList.addrs), 2; got != exp {
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
	if got, exp := len(state.accessList.addrs), 0; got != exp {
		t.Fatalf("expected empty, got %d", got)
	}
}

// BenchmarkAccessList simulates per-transaction access list usage:
// 10 addresses with 3 slots each (realistic EVM tx pattern).
// Reset sub-benchmark uses the reuse path (real production path after Berlin).
// New sub-benchmark allocates a fresh access list per iteration (isolates struct cost).
func BenchmarkAccessList(b *testing.B) {
	const nAddr, nSlot = 10, 3
	addrs := make([]accounts.Address, nAddr)
	slots := make([]accounts.StorageKey, nSlot)
	var buf [32]byte
	for i := range addrs {
		binary.BigEndian.PutUint64(buf[12:], uint64(i+1))
		addrs[i] = accounts.InternAddress(common.BytesToAddress(buf[:]))
	}
	buf = [32]byte{} // clear address residue so slot hashes are independent
	for i := range slots {
		binary.BigEndian.PutUint64(buf[24:], uint64(i+1))
		slots[i] = accounts.InternKey(common.BytesToHash(buf[:]))
	}

	fill := func(al *accessList) {
		for _, addr := range addrs {
			al.AddAddress(addr)
			for _, slot := range slots {
				al.AddSlot(addr, slot)
			}
		}
	}

	b.Run("Reset", func(b *testing.B) {
		al := newAccessList()
		b.ReportAllocs()
		for b.Loop() {
			al.Reset()
			fill(al)
		}
	})
	b.Run("New", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			fill(newAccessList())
		}
	})
}

// BenchmarkAccessListLarge tests access list performance at EIP-2930 scale:
// 100 addresses with 10 slots each (large batch/multicall transactions).
func BenchmarkAccessListLarge(b *testing.B) {
	const nAddr, nSlot = 100, 10
	addrs := make([]accounts.Address, nAddr)
	slots := make([]accounts.StorageKey, nSlot)
	var buf [32]byte
	for i := range addrs {
		binary.BigEndian.PutUint64(buf[12:], uint64(i+1))
		addrs[i] = accounts.InternAddress(common.BytesToAddress(buf[:]))
	}
	buf = [32]byte{} // clear address residue so slot hashes are independent
	for i := range slots {
		binary.BigEndian.PutUint64(buf[24:], uint64(i+1))
		slots[i] = accounts.InternKey(common.BytesToHash(buf[:]))
	}

	fill := func(al *accessList) {
		for _, addr := range addrs {
			al.AddAddress(addr)
			for _, slot := range slots {
				al.AddSlot(addr, slot)
			}
		}
	}

	b.Run("Reset", func(b *testing.B) {
		al := newAccessList()
		b.ReportAllocs()
		for b.Loop() {
			al.Reset()
			fill(al)
		}
	})

	// Benchmark Contains (cold miss + warm hit) at scale
	b.Run("Contains", func(b *testing.B) {
		al := newAccessList()
		fill(al)
		// pick address/slot near the end to measure worst-case scan
		lastAddr := addrs[nAddr-1]
		lastSlot := slots[nSlot-1]
		missAddr := accounts.InternAddress(common.HexToAddress("0xdead"))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			al.Contains(lastAddr, lastSlot) // hit (worst position)
			al.ContainsAddress(missAddr)    // miss
		}
	})
}
