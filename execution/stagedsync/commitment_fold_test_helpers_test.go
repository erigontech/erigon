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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
)

func newTestUpdates() *commitment.Updates {
	return commitment.NewUpdates(commitment.ModeUpdate, "", commitment.KeyToHexNibbleHash)
}

func collectUpdates(t *testing.T, updates *commitment.Updates) map[string]commitment.Update {
	t.Helper()
	out := map[string]commitment.Update{}
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		out[string(k)] = *u
		return nil
	}))
	return out
}

func lookupKeyUpdate(t *testing.T, updates *commitment.Updates, plainKey string) *commitment.Update {
	t.Helper()
	var found *commitment.Update
	require.NoError(t, updates.HashSort(t.Context(), nil, func(_, k []byte, u *commitment.Update) error {
		if string(k) == plainKey {
			cp := *u
			found = &cp
		}
		return nil
	}))
	require.NotNil(t, found, "no Update emitted for plainKey %x", plainKey)
	return found
}

// emitTouchedUpdates folds writes into the version map's touched union and emits the
// commitment updates the calculator would produce from it, keyed by plain key. base is
// the committed-base reader for untouched fields (nil when the version map holds them).
func emitTouchedUpdates(t *testing.T, vm *state.VersionMap, lastTxNum uint64, base state.AccountBaseReader, writes state.WriteSetView) map[string]commitment.Update {
	t.Helper()
	cc := &commitmentCalculator{}
	cc.accumulateTouched(writes)
	u := newTestUpdates()
	cc.emitTouched(vm.View(lastTxNum, base), u)
	return collectUpdates(t, u)
}
