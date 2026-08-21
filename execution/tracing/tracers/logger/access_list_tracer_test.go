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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
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
	require.True(t, seeded.Equal(roundTripped))

	seeded.list.addSlot(addr, slot3)
	seeded.list.addAddress(excluded)
	require.False(t, seeded.Equal(prev), "the seeded tracer must not write through to its source")
	require.Equal(t, roundTripped.AccessList(), prev.AccessList())
}

func BenchmarkAccessListTracerSeed(b *testing.B) {
	const nAddrs, nSlots = 30, 20

	prev := NewAccessListTracer(nil, nil, nil)
	for a := range nAddrs {
		address := common.BytesToAddress([]byte{byte(a + 1)})
		for s := range nSlots {
			prev.list.addSlot(address, common.BytesToHash([]byte{byte(s + 1)}))
		}
	}

	// AccessList() is built either way (the message needs it), so only the seeding
	// half differs between the two.
	acl := prev.AccessList()
	b.Run("roundTrip", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = NewAccessListTracer(acl, nil, nil)
		}
	})
	b.Run("seedNew", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = prev.SeedNew(nil)
		}
	})
}
