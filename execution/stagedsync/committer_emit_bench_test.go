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
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkEmitTouched folds a block of 10k txs, each writing a sender, a receiver and one slot.
func BenchmarkEmitTouched(b *testing.B) {
	const txs = 10_000
	vm := state.NewVersionMap(nil)
	cc := &commitmentCalculator{updates: newTestUpdates()}
	addr := func(i int) accounts.Address {
		var a [20]byte
		binary.BigEndian.PutUint64(a[12:], uint64(i)+1)
		return accounts.InternAddress(a)
	}
	for tx := range txs {
		ver := state.Version{TxIndex: tx}
		var slot [32]byte
		binary.BigEndian.PutUint64(slot[24:], uint64(tx))
		ws := newWS().
			bal(addr(2*tx), ver, *uint256.NewInt(uint64(tx))).nonce(addr(2*tx), ver, 1).
			bal(addr(2*tx+1), ver, *uint256.NewInt(1)).
			stor(addr(2*tx+1), accounts.InternKey(slot), ver, *uint256.NewInt(1)).build()
		vm.FlushVersionedWrites(ws, true)
		cc.accumulateTouched(state.NewVersionMapWriteView(ws, vm, tx), nil)
	}
	u := commitment.NewUpdates(commitment.ModeParallel, b.TempDir(), commitment.KeyToHexNibbleHash)
	defer u.Close()
	b.ReportAllocs()
	for b.Loop() {
		u.Reset()
		if err := cc.emitTouched(vm.View(txs+1, nil), u); err != nil {
			b.Fatal(err)
		}
	}
}
