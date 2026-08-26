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

package logger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var (
	addr = common.BytesToAddress([]byte{0x01, 0x71})

	slot1 = common.BytesToHash([]byte{0x01})
	slot2 = common.BytesToHash([]byte{0x02})
	slot3 = common.BytesToHash([]byte{0x03})
	slot4 = common.BytesToHash([]byte{0x04})

	ordered = types.AccessList{{
		Address: addr,
		StorageKeys: []common.Hash{
			slot1,
			slot2,
			slot3,
			slot4,
		},
	}}
)

func TestTracer_AccessList_Order(t *testing.T) {
	al := newAccessList()
	al.addAddress(addr)
	al.addSlot(addr, slot1)
	al.addSlot(addr, slot4)
	al.addSlot(addr, slot3)
	al.addSlot(addr, slot2)
	require.NotEqual(t, ordered, al.accessList())
	require.Equal(t, ordered, al.accessListSorted())
	require.True(t, al.Equal(al)) //nolint:gocritic
}

func TestNewAccessListTracerExcludedAddress(t *testing.T) {
	excluded := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.HexToHash("0x01")
	prelude := types.AccessList{{
		Address:     excluded,
		StorageKeys: []common.Hash{slot},
	}}
	excl := map[common.Address]struct{}{excluded: {}}
	tracer := NewAccessListTracer(prelude, excl, nil)
	got := tracer.AccessList()
	if len(got) != 0 {
		t.Fatalf("excluded prelude address must not contribute tuples, got %+v", got)
	}
}

// TestTracer_AccessList_Equal pins the cases equal() must reject now that it walks
// only the receiver: a superset on either side, and same-sized sets with different
// members, at both the address and the slot level.
func TestTracer_AccessList_Equal(t *testing.T) {
	addr2 := common.BytesToAddress([]byte{0x02, 0x72})

	build := func(fill func(accessList)) accessList {
		al := newAccessList()
		fill(al)
		return al
	}

	oneAddrTwoSlots := func(al accessList) {
		al.addSlot(addr, slot1)
		al.addSlot(addr, slot2)
	}

	for _, tc := range []struct {
		name  string
		a, b  func(accessList)
		equal bool
	}{
		{"empty", func(accessList) {}, func(accessList) {}, true},
		{"same slots inserted in a different order",
			oneAddrTwoSlots,
			func(al accessList) { al.addSlot(addr, slot2); al.addSlot(addr, slot1) },
			true},
		{"other has an extra address",
			oneAddrTwoSlots,
			func(al accessList) { oneAddrTwoSlots(al); al.addAddress(addr2) },
			false},
		{"receiver has an extra address",
			func(al accessList) { oneAddrTwoSlots(al); al.addAddress(addr2) },
			oneAddrTwoSlots,
			false},
		{"same address count, different addresses",
			func(al accessList) { al.addAddress(addr) },
			func(al accessList) { al.addAddress(addr2) },
			false},
		{"same slot count, different slots",
			oneAddrTwoSlots,
			func(al accessList) { al.addSlot(addr, slot1); al.addSlot(addr, slot3) },
			false},
		{"other has an extra slot",
			func(al accessList) { al.addSlot(addr, slot1) },
			oneAddrTwoSlots,
			false},
		{"address-only vs address with a slot",
			func(al accessList) { al.addAddress(addr) },
			func(al accessList) { al.addSlot(addr, slot1) },
			false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := build(tc.a), build(tc.b)
			require.Equal(t, tc.equal, a.equal(b))
			require.Equal(t, tc.equal, b.equal(a), "equal must be symmetric")
		})
	}
}

// TestTracer_AccessList_LazySlotMap pins that an address touched only as an address
// carries no slot map, and that promoting it to a slot-carrying entry keeps its order.
func TestTracer_AccessList_LazySlotMap(t *testing.T) {
	addr2 := common.BytesToAddress([]byte{0x02, 0x72})

	al := newAccessList()
	al.addAddress(addr)
	al.addAddress(addr2)
	require.Nil(t, al[addr].slots)
	require.Nil(t, al[addr2].slots)

	al.addSlot(addr, slot1)
	al.addSlot(addr, slot2)
	require.Len(t, al[addr].slots, 2)
	require.Nil(t, al[addr2].slots)

	require.Equal(t, types.AccessList{
		{Address: addr, StorageKeys: []common.Hash{slot1, slot2}},
		{Address: addr2, StorageKeys: []common.Hash{}},
	}, al.accessList())
}

// TestTracer_AccessList_SlotFirstAddress covers addSlot on an address the list has
// never seen: it must take the next order slot rather than a zero one.
func TestTracer_AccessList_SlotFirstAddress(t *testing.T) {
	addr2 := common.BytesToAddress([]byte{0x02, 0x72})

	al := newAccessList()
	al.addAddress(addr)
	al.addSlot(addr2, slot1)

	require.Equal(t, types.AccessList{
		{Address: addr, StorageKeys: []common.Hash{}},
		{Address: addr2, StorageKeys: []common.Hash{slot1}},
	}, al.accessList())
}

// TestAccessListTracerLazyAddressSets pins that a fresh tracer leaves both
// contract-address sets nil, and that markCreated/markUsedBeforeCreation
// allocate them independently on first write.
func TestAccessListTracerLazyAddressSets(t *testing.T) {
	tracer := NewAccessListTracer(nil, nil, nil)
	require.Nil(t, tracer.createdContracts)
	require.False(t, tracer.UsedBeforeCreation(addr))

	tracer.markUsedBeforeCreation(addr)
	require.True(t, tracer.UsedBeforeCreation(addr))
	require.Nil(t, tracer.createdContracts)

	tracer.markCreated(addr)
	require.Contains(t, tracer.CreatedContracts(), addr)
}

// TestAccessListTracerCreatedContractsWritable pins that CreatedContracts always
// returns a writable map, even before any CREATE: callers writing through the
// returned set must not panic on a nil map.
func TestAccessListTracerCreatedContractsWritable(t *testing.T) {
	tracer := NewAccessListTracer(nil, nil, nil)
	got := tracer.CreatedContracts()
	require.NotNil(t, got)
	got[addr] = struct{}{}
	require.Contains(t, tracer.createdContracts, addr)
}

// TestAccessListTracerSeedNew pins that seeding directly from the accumulated maps
// is observationally the same as the types.AccessList round-trip it replaces, and
// that the two tracers share nothing.
func TestAccessListTracerSeedNew(t *testing.T) {
	excluded := common.BytesToAddress([]byte{0x09})
	excl := map[common.Address]struct{}{excluded: {}}

	prev := NewAccessListTracer(nil, excl, nil)
	prev.list.addSlot(addr, slot1)
	prev.list.addSlot(addr, slot2)
	prev.list.addAddress(common.BytesToAddress([]byte{0x02, 0x72}))

	seeded := prev.SeedNew(nil)
	roundTripped := NewAccessListTracer(prev.AccessList(), excl, nil)

	require.Equal(t, roundTripped.AccessList(), seeded.AccessList())
	require.True(t, seeded.Equal(prev))

	seeded.list.addSlot(addr, slot3)
	seeded.list.addAddress(excluded)
	require.False(t, seeded.Equal(prev), "the seeded tracer must not write through to its source")
	require.Equal(t, roundTripped.AccessList(), prev.AccessList())
}

// TestAccessListTracerSeedNewDropsExcluded pins the filtering cloneExcluding
// documents: an excluded address can reach the list, and must not survive.
func TestAccessListTracerSeedNewDropsExcluded(t *testing.T) {
	excluded := common.BytesToAddress([]byte{0x77})
	excl := map[common.Address]struct{}{excluded: {}}

	prev := NewAccessListTracer(nil, excl, nil)
	prev.list.addSlot(excluded, slot1)
	prev.list.addSlot(addr, slot2)

	seeded := prev.SeedNew(nil)
	require.Equal(t, NewAccessListTracer(prev.AccessList(), excl, nil).AccessList(), seeded.AccessList())
	require.Equal(t, types.AccessList{{Address: addr, StorageKeys: []common.Hash{slot2}}}, seeded.AccessList())
}

// TestAccessListTracerSeedNewTracesOpcodes drives a seeded tracer through the
// opcodes that write its contract sets, which the AccessList-only assertions
// above never reach.
func TestAccessListTracerSeedNewTracesOpcodes(t *testing.T) {
	prev := NewAccessListTracer(nil, nil, nil)
	prev.list.addSlot(addr, slot1)

	seeded := prev.SeedNew(nil)
	scope := &mockOpContext{
		address: accounts.InternAddress(addr),
		stack:   []uint256.Int{*new(uint256.Int).SetBytes(slot2[:])},
	}
	seeded.OnOpcode(0, byte(vm.SLOAD), 100, 3, scope, nil, 1, nil)

	require.Equal(t, types.AccessList{{Address: addr, StorageKeys: []common.Hash{slot1, slot2}}}, seeded.AccessList())
	require.True(t, seeded.UsedBeforeCreation(addr))
	require.Empty(t, seeded.CreatedContracts())
}

func BenchmarkAccessListTracerSeed(b *testing.B) {
	// Real eth_createAccessList lists are small: a handful of addresses with a
	// few slots each. The wide shapes are here for scale.
	for _, shape := range []struct{ nAddrs, nSlots int }{
		{1, 1}, {1, 5}, {1, 17}, {3, 5}, {5, 20}, {30, 20},
	} {
		prev := NewAccessListTracer(nil, nil, nil)
		for a := range shape.nAddrs {
			address := common.BytesToAddress([]byte{byte(a + 1)})
			for s := range shape.nSlots {
				prev.list.addSlot(address, common.BytesToHash([]byte{byte(s + 1)}))
			}
		}
		// AccessList() is built either way, so only the seeding half differs.
		acl := prev.AccessList()
		name := fmt.Sprintf("%dx%d", shape.nAddrs, shape.nSlots)

		b.Run(name+"/roundTrip", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = NewAccessListTracer(acl, nil, nil)
			}
		})
		b.Run(name+"/seedNew", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = prev.SeedNew(nil)
			}
		})
	}
}
