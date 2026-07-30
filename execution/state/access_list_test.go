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
	"fmt"
	"math/big"
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
	for address := range s.accessList.addresses {
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
	idx := s.accessList.addresses[address]
	if idx >= 0 {
		for s := range s.accessList.slots[idx] {
			if _, slotPresent := slotMap[s]; !slotPresent {
				t.Fatalf("scope has extra slot %v (address %v)", s, addrString)
			}
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
	defer state.Close()

	state.accessList.Reset()

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
}

func BenchmarkAccessListReset(b *testing.B) {
	sender := accounts.InternAddress(common.HexToAddress("0x1111"))
	dst := accounts.InternAddress(common.HexToAddress("0x2222"))
	precompiles := []accounts.Address{
		accounts.InternAddress(common.HexToAddress("0x0001")),
		accounts.InternAddress(common.HexToAddress("0x0002")),
		accounts.InternAddress(common.HexToAddress("0x0003")),
		accounts.InternAddress(common.HexToAddress("0x0004")),
		accounts.InternAddress(common.HexToAddress("0x0005")),
		accounts.InternAddress(common.HexToAddress("0x0006")),
		accounts.InternAddress(common.HexToAddress("0x0007")),
		accounts.InternAddress(common.HexToAddress("0x0008")),
		accounts.InternAddress(common.HexToAddress("0x0009")),
	}
	slots := []accounts.StorageKey{
		accounts.InternKey(common.HexToHash("0xabc1")),
		accounts.InternKey(common.HexToHash("0xabc2")),
		accounts.InternKey(common.HexToHash("0xabc3")),
	}

	populate := func(al *accessList) {
		al.AddAddress(sender)
		al.AddAddress(dst)
		for _, p := range precompiles {
			al.AddAddress(p)
		}
		for _, s := range slots {
			al.AddSlot(dst, s)
		}
	}

	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			al := newAccessList()
			populate(al)
		}
	})

	b.Run("reset", func(b *testing.B) {
		b.ReportAllocs()
		al := newAccessList()
		for i := 0; i < b.N; i++ {
			al.Reset()
			populate(al)
		}
	})
}

// BenchmarkAccessListSlots measures the per-opcode access-list traffic: every
// SLOAD and SSTORE calls AddSlot, and a re-read of an already-warm slot is the
// dominant case in storage-heavy contracts.
//
// runLen is how many consecutive storage ops target the same address before
// execution moves to another one. A call frame only ever touches its own
// storage, so runLen models frame length: 1 is a proxy chain doing one SLOAD
// per frame, 64 is a loop inside a single contract.
func BenchmarkAccessListSlots(b *testing.B) {
	const nAddrs, nSlots = 4, 64
	addrs := make([]accounts.Address, nAddrs)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
	}
	keys := make([]accounts.StorageKey, nSlots)
	for i := range keys {
		keys[i] = accounts.InternKey(common.BigToHash(big.NewInt(int64(i + 1))))
	}
	populated := func() *accessList {
		al := newAccessList()
		for _, a := range addrs {
			for _, k := range keys {
				al.AddSlot(a, k)
			}
		}
		return al
	}

	for _, runLen := range []int{1, 4, 16, 64} {
		name := fmt.Sprintf("run%d", runLen)

		b.Run("warm-hit/"+name, func(b *testing.B) {
			al := populated()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al.AddSlot(addrs[(i/runLen)%nAddrs], keys[i%nSlots])
			}
		})

		// Same slot re-read for the whole run: a loop hammering one storage
		// slot, the pattern SLOAD hot loops produce.
		b.Run("warm-hit-hot/"+name, func(b *testing.B) {
			al := populated()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al.AddSlot(addrs[(i/runLen)%nAddrs], keys[(i/runLen)%nSlots])
			}
		})

		b.Run("cold-insert/"+name, func(b *testing.B) {
			al := newAccessList()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Walk all nAddrs*nSlots pairs exactly once per Reset window,
				// keeping runs of runLen ops on one address. Deriving the slot
				// from i alone would repeat 64 pairs four times instead.
				w := i % (nAddrs * nSlots)
				if w == 0 {
					al.Reset()
				}
				al.AddSlot(addrs[(w/runLen)%nAddrs], keys[(w/(runLen*nAddrs))*runLen+w%runLen])
			}
		})

		b.Run("contains/"+name, func(b *testing.B) {
			al := populated()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al.Contains(addrs[(i/runLen)%nAddrs], keys[i%nSlots])
			}
		})
	}
}

// TestAccessListMemoDroppedOnSlotMapReuse pins the memo invalidation in
// DeleteSlot. Emptying a slot map truncates it out of al.slots and marks the
// address slotless, so a surviving memo would send the next AddSlot into a
// detached map — and hand that map, carrying the stray slot, to whichever
// address claims the freed slot next. Reads go through the memo too, so the
// damage only shows once another address has displaced it.
func TestAccessListMemoDroppedOnSlotMapReuse(t *testing.T) {
	t.Parallel()
	addrA := accounts.InternAddress(common.HexToAddress("0xaa"))
	addrB := accounts.InternAddress(common.HexToAddress("0xbb"))
	slot1 := accounts.InternKey(common.HexToHash("0x01"))
	slot2 := accounts.InternKey(common.HexToHash("0x02"))
	slot3 := accounts.InternKey(common.HexToHash("0x03"))

	al := newAccessList()
	al.AddSlot(addrA, slot1) // populates the memo for A
	al.DeleteSlot(addrA, slot1)
	al.AddSlot(addrA, slot2) // must re-establish A's slot map, not reuse the detached one
	al.AddSlot(addrB, slot3) // displaces the memo, so the reads below take the slow path

	addrPresent, slotPresent := al.Contains(addrA, slot2)
	require.True(t, addrPresent)
	require.True(t, slotPresent, "addrA's slot was written into a detached slot map")

	_, leaked := al.Contains(addrB, slot2)
	require.False(t, leaked, "addrA's slot leaked into addrB's reused slot map")
}

// TestAccessListWarmSlotMemoDroppedOnDelete pins lastWarmSlot invalidation for
// a DeleteSlot that leaves the slot map non-empty (no truncation): the reverted
// slot must not keep reading as warm through the memo.
func TestAccessListWarmSlotMemoDroppedOnDelete(t *testing.T) {
	t.Parallel()
	addrA := accounts.InternAddress(common.HexToAddress("0xaa"))
	slot1 := accounts.InternKey(common.HexToHash("0x01"))
	slot2 := accounts.InternKey(common.HexToHash("0x02"))

	al := newAccessList()
	al.AddSlot(addrA, slot1)
	al.AddSlot(addrA, slot2) // memoizes slot2 as the last warm slot
	al.DeleteSlot(addrA, slot2)

	_, slotPresent := al.Contains(addrA, slot2)
	require.False(t, slotPresent, "reverted slot still warm via stale lastWarmSlot memo")

	_, slotChange := al.AddSlot(addrA, slot2)
	require.True(t, slotChange, "re-adding a reverted slot must report a change for the journal")
}
