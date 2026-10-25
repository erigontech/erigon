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

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// collectUpdates drains every (plainKey → Update) the buffer holds into a map,
// so two independently-built Updates buffers can be compared for exact equality.
func collectUpdates(t *testing.T, updates *commitment.Updates) map[string]commitment.Update {
	t.Helper()
	out := map[string]commitment.Update{}
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		out[string(k)] = *u
		return nil
	}))
	return out
}

// TestCalcState_VersionMapViewEquivalent is increment E: the commitment input the
// calculator builds from the version map's per-tx access list (WritesAtTxIndex) must
// equal the input it builds from the retained write-set view, across the write
// shapes the per-scenario calc_state tests already prove correct against the domain
// root. Equal inputs ⇒ equal trie root, so the access-list source is a faithful
// substitute for the write-set and the executor need not keep a write-set alive.
func TestCalcState_VersionMapViewEquivalent(t *testing.T) {
	t.Parallel()

	const txIdx = 3
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
		{"storage change", func() *state.WriteSet {
			return newWS().stor(a(2), k(9), ver, *uint256.NewInt(42)).build()
		}},
		{"storage plus account", func() *state.WriteSet {
			return newWS().bal(a(3), ver, *uint256.NewInt(5)).stor(a(3), k(1), ver, *uint256.NewInt(1)).stor(a(3), k(2), ver, *uint256.NewInt(2)).build()
		}},
		{"self-destruct deletes leaf", func() *state.WriteSet {
			return newWS().selfDestruct(a(4), ver, true).inc(a(4), ver, 0).build()
		}},
		{"EIP-161 empty removal via self-destruct encoding", func() *state.WriteSet {
			return newWS().selfDestruct(a(5), ver, true).build()
		}},
		{"incarnation bump", func() *state.WriteSet {
			return newWS().bal(a(6), ver, *uint256.NewInt(3)).inc(a(6), ver, 2).build()
		}},
		{"created contract", func() *state.WriteSet {
			return newWS().createContract(a(7), ver, true).bal(a(7), ver, *uint256.NewInt(9)).nonce(a(7), ver, 1).codeHash(a(7), ver, codeHash).build()
		}},
		{"zero-balance live touch", func() *state.WriteSet {
			return newWS().bal(a(8), ver, uint256.Int{}).nonce(a(8), ver, 1).build()
		}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ws := sc.build()

			vm := state.NewVersionMap(nil)
			vm.FlushVersionedWrites(ws, true, "")
			vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

			fromWriteSet := newTestCalcState()
			fromWriteSet.ApplyWrites(state.NewVersionMapWriteView(ws, vm, txIdx))
			wsUpdates := newTestUpdates()
			fromWriteSet.FlushToUpdates(wsUpdates)

			fromAccessList := newTestCalcState()
			fromAccessList.ApplyWrites(vm.WritesAtTxIndex(txIdx, false))
			alUpdates := newTestUpdates()
			fromAccessList.FlushToUpdates(alUpdates)

			fromWS := collectUpdates(t, wsUpdates)
			require.NotEmpty(t, fromWS, "scenario must emit at least one update (else equality is vacuous)")
			require.Equal(t, fromWS, collectUpdates(t, alUpdates),
				"access-list-derived commitment input must equal write-set-derived input")
		})
	}
}
