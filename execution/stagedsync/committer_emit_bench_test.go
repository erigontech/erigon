// Copyright 2026 The Erigon Authors
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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkEmitTouched(b *testing.B) {
	const n = 1024
	ver := state.Version{TxIndex: 0}
	slot := accounts.InternKey([32]byte{1})
	for _, sc := range []struct {
		name  string
		write func(w *wsb, a accounts.Address)
	}{
		{"balance-nonce", func(w *wsb, a accounts.Address) { w.bal(a, ver, *uint256.NewInt(5)).nonce(a, ver, 1) }},
		{"storage-only", func(w *wsb, a accounts.Address) { w.stor(a, slot, ver, *uint256.NewInt(1)) }},
	} {
		b.Run(sc.name, func(b *testing.B) {
			w := newWS()
			base := newMapStateReader()
			for i := range n {
				a := accounts.InternAddress([20]byte{0xbe, byte(i >> 8), byte(i)})
				sc.write(w, a)
				acc := accounts.NewAccount()
				acc.Balance = *uint256.NewInt(100)
				base.accounts[a] = &acc
			}
			ws := w.build()
			vm := state.NewVersionMap(nil)
			vm.FlushVersionedWrites(ws, true)
			cc := &commitmentCalculator{updates: newTestUpdates()}
			cc.accumulateTouched(state.NewVersionMapWriteView(ws, vm, 0), nil)
			u := newTestUpdates()
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				u.Reset()
				b.StartTimer()
				if err := cc.emitTouched(vm.View(1, base), u); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/account")
		})
	}
}
