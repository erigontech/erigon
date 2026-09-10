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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
