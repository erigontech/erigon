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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
)

// TestAddLogOnLogPointerStability pins the AddLog/OnLog contract for the value
// log buffer: the hook may mutate the log (the mutation must reach the stored
// log) and may retain the pointer beyond the callback (it must stay valid across
// later appends and Reset, which grow and reuse the internal buffer).
func TestAddLogOnLogPointerStability(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	var retained *types.Log
	ibs.SetHooks(&tracing.Hooks{
		OnLog: func(l *types.Log) {
			retained = l
			l.Data = []byte{0xbe, 0xef}
			l.Topics = []common.Hash{{0xca, 0xfe}}
		},
	})

	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x11}, Topics: []common.Hash{{0x01}}, Data: []byte{0x01}})
	require.NotNil(t, retained)
	first := retained

	// (a) the hook's Data and Topics mutations reach the stored log.
	raw := ibs.GetRawLogs(0)
	require.Len(t, raw, 1)
	require.Equal(t, []byte{0xbe, 0xef}, []byte(raw[0].Data))
	require.Equal(t, []common.Hash{{0xca, 0xfe}}, raw[0].Topics)

	// (b) churn the internal buffer: many appends (forcing growth) then Reset,
	// which reuses the backing. A pointer into the buffer would be corrupted;
	// the stable heap copy handed to OnLog must not be.
	for i := range 1000 {
		ibs.AddLog(types.Log{Address: common.Address{0x22}, Data: []byte{byte(i)}})
	}
	ibs.Reset()
	ibs.SetTxContext(2, 0)
	ibs.AddLog(types.Log{Address: common.Address{0x33}, Data: []byte{0x99}})

	require.Equal(t, common.Address{0x11}, first.Address)
	require.Equal(t, []byte{0xbe, 0xef}, []byte(first.Data))
}

// TestAllocLogPreservesCapacityAcrossRevert pins that fully reverting a tx's
// logs (which truncates the outer buffer) and then logging again reuses the
// inner buffer's capacity instead of dropping it — the same capacity Reset
// preserves.
func TestAllocLogPreservesCapacityAcrossRevert(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.SetTxContext(1, 0)
	snap := ibs.PushSnapshot()
	for i := range 8 {
		ibs.AddLog(&types.Log{Address: common.Address{byte(i)}})
	}
	require.Len(t, ibs.logs, 2)
	capBefore := cap(ibs.logs[1])
	require.GreaterOrEqual(t, capBefore, 8)

	ibs.RevertToSnapshot(snap, nil)
	require.Len(t, ibs.logs, 1) // the tx's slot was truncated off the outer buffer

	ibs.AddLog(&types.Log{Address: common.Address{0xff}})
	require.Len(t, ibs.logs, 2)
	require.Equal(t, capBefore, cap(ibs.logs[1]), "inner log buffer capacity must survive revert+relog")
}
