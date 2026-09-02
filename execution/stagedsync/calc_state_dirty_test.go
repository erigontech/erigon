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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func emittedUpdates(t *testing.T, updates *commitment.Updates) map[string]commitment.Update {
	t.Helper()
	got := map[string]commitment.Update{}
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		got[string(k)] = *u
		return nil
	}))
	return got
}

func plainKeyOf(addr accounts.Address) string {
	v := addr.Value()
	return string(v[:])
}

func TestFlushToUpdates_EmitsOnlyAccountsWrittenSinceReset(t *testing.T) {
	cs := newTestCalcState()
	a := accounts.InternAddress(common.Address{0xa1})
	b := accounts.InternAddress(common.Address{0xb2})

	cs.ApplyWrites(newWS().
		bal(a, state.Version{}, *uint256.NewInt(1)).
		bal(b, state.Version{}, *uint256.NewInt(2)).
		build(), false)
	first := newTestUpdates()
	cs.FlushToUpdates(first)
	require.Len(t, emittedUpdates(t, first), 2)
	cs.ResetBlockFlags()

	cs.ApplyWrites(newWS().nonce(b, state.Version{}, 7).build(), false)
	second := newTestUpdates()
	cs.FlushToUpdates(second)
	got := emittedUpdates(t, second)
	require.Len(t, got, 1, "an account untouched since ResetBlockFlags must not be re-emitted")
	u, ok := got[plainKeyOf(b)]
	require.True(t, ok)
	require.Equal(t, uint64(2), u.Balance.Uint64(), "the accumulated balance must survive the reset")
	require.Equal(t, uint64(7), u.Nonce)
}

func TestApplyWrites_EveryPathListsTheAccountAfterReset(t *testing.T) {
	addr := accounts.InternAddress(common.Address{0xc3})
	cases := map[string]func(*wsb) *wsb{
		"balance": func(w *wsb) *wsb { return w.bal(addr, state.Version{}, *uint256.NewInt(5)) },
		"nonce":   func(w *wsb) *wsb { return w.nonce(addr, state.Version{}, 5) },
		"codeHash": func(w *wsb) *wsb {
			return w.codeHash(addr, state.Version{}, accounts.InternCodeHash(common.Hash{0x11}))
		},
		"code":         func(w *wsb) *wsb { return w.code(addr, state.Version{}, accounts.NewCode([]byte{0x60, 0x00})) },
		"incarnation":  func(w *wsb) *wsb { return w.inc(addr, state.Version{}, 1) },
		"selfDestruct": func(w *wsb) *wsb { return w.selfDestruct(addr, state.Version{}, true) },
	}
	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			cs := newTestCalcState()
			cs.ApplyWrites(newWS().bal(addr, state.Version{}, *uint256.NewInt(1)).build(), false)
			cs.ResetBlockFlags()
			cs.ApplyWrites(write(newWS()).build(), false)
			updates := newTestUpdates()
			cs.FlushToUpdates(updates)
			_, ok := emittedUpdates(t, updates)[plainKeyOf(addr)]
			require.True(t, ok, "a %s write after ResetBlockFlags must list the account for the next flush", name)
		})
	}
}

func TestApplyWrites_RepeatedWritesListTheAccountOnce(t *testing.T) {
	cs := newTestCalcState()
	addr := accounts.InternAddress(common.Address{0xd4})
	for i := uint64(1); i <= 3; i++ {
		cs.ApplyWrites(newWS().
			bal(addr, state.Version{}, *uint256.NewInt(i)).
			nonce(addr, state.Version{}, i).
			build(), false)
	}
	require.Len(t, cs.dirtyAccounts, 1)
	cs.ResetBlockFlags()
	require.Empty(t, cs.dirtyAccounts)
}

func TestFlushToUpdates_MidBlockFlushKeepsTheListUntilReset(t *testing.T) {
	cs := newTestCalcState()
	a := accounts.InternAddress(common.Address{0xe5})
	b := accounts.InternAddress(common.Address{0xf6})

	cs.ApplyWrites(newWS().bal(a, state.Version{}, *uint256.NewInt(1)).build(), false)
	checkpoint := newTestUpdates()
	cs.FlushToUpdates(checkpoint)
	require.Len(t, emittedUpdates(t, checkpoint), 1)

	cs.ApplyWrites(newWS().bal(b, state.Version{}, *uint256.NewInt(2)).build(), false)
	blockEnd := newTestUpdates()
	cs.FlushToUpdates(blockEnd)
	got := emittedUpdates(t, blockEnd)
	require.Len(t, got, 2, "a flush without ResetBlockFlags must keep earlier dirty accounts for the block-end flush")
	require.Contains(t, got, plainKeyOf(a))
	require.Contains(t, got, plainKeyOf(b))
	require.Len(t, cs.dirtyAccounts, 2, "a mid-block flush must not list an account twice")
}
