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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestEmitTouched_MatchesCalcState is the Phase-2 raw-view equivalence: the GetAccount
// commitment path (accumulateTouched + emitTouched) must emit the same Updates as the
// calcState accumulator (+ ApplyEIP161Removal) for the write shapes exec's FinalizedWrites
// produces. Every touched account carries field presence (a contract touched this block
// exists in the base), so GetAccount and calcState's lazy-load agree on existence. The
// self-destruct / EIP-161-empty shapes carry the write-side removal encoding, which both
// paths resolve to a leaf-removing delete.
func TestEmitTouched_MatchesCalcState(t *testing.T) {
	t.Parallel()

	const txIdx = 3
	const T = txIdx + 1 // baseTxNum 0 ⇒ GetAccount floor includes the write at txIdx
	ver := state.Version{TxIndex: txIdx}
	a := func(b byte) accounts.Address { return accounts.InternAddress([20]byte{b}) }
	k := func(b byte) accounts.StorageKey { return accounts.InternKey([32]byte{b}) }
	codeHash := accounts.NewCode([]byte{0x60, 0x00}).Hash

	scenarios := []struct {
		name  string
		build func() *state.WriteSet
	}{
		{"live account full update", func() *state.WriteSet {
			return newWS().bal(a(1), ver, *uint256.NewInt(100)).nonce(a(1), ver, 7).codeHash(a(1), ver, codeHash).build()
		}},
		{"storage plus account", func() *state.WriteSet {
			return newWS().bal(a(3), ver, *uint256.NewInt(5)).nonce(a(3), ver, 1).
				stor(a(3), k(1), ver, *uint256.NewInt(1)).stor(a(3), k(2), ver, *uint256.NewInt(2)).build()
		}},
		{"storage cleared to zero", func() *state.WriteSet {
			return newWS().bal(a(9), ver, *uint256.NewInt(5)).nonce(a(9), ver, 1).
				stor(a(9), k(3), ver, uint256.Int{}).build()
		}},
		{"self-destruct deletes leaf", func() *state.WriteSet {
			return newWS().selfDestruct(a(4), ver, true).inc(a(4), ver, 0).build()
		}},
		{"EIP-161 empty removal via self-destruct encoding", func() *state.WriteSet {
			return newWS().selfDestruct(a(5), ver, true).build()
		}},
		{"incarnation bump", func() *state.WriteSet {
			return newWS().bal(a(6), ver, *uint256.NewInt(3)).nonce(a(6), ver, 1).inc(a(6), ver, 2).build()
		}},
		{"created contract", func() *state.WriteSet {
			return newWS().createContract(a(7), ver, true).bal(a(7), ver, *uint256.NewInt(9)).nonce(a(7), ver, 1).codeHash(a(7), ver, codeHash).build()
		}},
		{"zero-balance live touch (nonce keeps it alive)", func() *state.WriteSet {
			return newWS().bal(a(8), ver, uint256.Int{}).nonce(a(8), ver, 1).build()
		}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ws := sc.build()

			vm := state.NewVersionMap(nil)
			vm.SetBaseTxNum(0)
			vm.FlushVersionedWrites(ws, true, "")
			vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

			cs := newTestCalcState()
			cs.ApplyWrites(state.NewVersionMapWriteView(ws, vm, txIdx))
			cs.ApplyEIP161Removal(true, false)
			calcUpdates := newTestUpdates()
			cs.FlushToUpdates(calcUpdates)

			cc := &commitmentCalculator{}
			cc.accumulateTouched(vm.WritesAtTxIndex(txIdx, false))
			getUpdates := newTestUpdates()
			cc.emitTouched(vm, T, nil, getUpdates)

			calc := collectUpdates(t, calcUpdates)
			require.NotEmpty(t, calc, "scenario must emit at least one update")
			require.Equal(t, calc, collectUpdates(t, getUpdates),
				"emitTouched (GetAccount) must match calcState (+ApplyEIP161Removal)")
		})
	}
}
