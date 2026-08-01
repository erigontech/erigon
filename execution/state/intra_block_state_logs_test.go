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

package state

import (
	"bytes"
	"internal/race"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
)

func TestLogsRlpHash(t *testing.T) {
	t.Parallel()

	t.Run("no logs", func(t *testing.T) {
		ibs := New(nil)
		require.Equal(t, types.RlpHash(ibs.Logs()), ibs.LogsRlpHash())
	})

	t.Run("logs across transactions", func(t *testing.T) {
		ibs := New(nil)
		ibs.SetTxContext(1, 0)
		ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1")})
		ibs.AddLog(&types.Log{
			Address: common.HexToAddress("0x2"),
			Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")},
			Data:    bytes.Repeat([]byte{0x11}, 100),
		})
		ibs.SetTxContext(1, 2)
		ibs.AddLog(&types.Log{
			Address: common.HexToAddress("0x3"),
			Topics:  []common.Hash{common.HexToHash("0xcc")},
			Data:    []byte{0x01},
		})

		require.Equal(t, types.RlpHash(ibs.Logs()), ibs.LogsRlpHash())
	})
}

func TestRevertDropsOversizedLogBuffer(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)

	snap := ibs.PushSnapshot()
	ibs.AddLog(&types.Log{
		Address: common.HexToAddress("0x1"),
		Data:    make([]byte, maxReusableLogDataCap+1),
	})
	ibs.RevertToSnapshot(snap, nil)

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	lp := ibs.AllocLog(0, 1)
	require.LessOrEqual(t, cap(lp.Data), maxReusableLogDataCap)
}

func TestRevertKeepsNormalLogBufferForReuse(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)

	snap := ibs.PushSnapshot()
	ibs.AddLog(&types.Log{
		Address: common.HexToAddress("0x1"),
		Data:    []byte{1, 2, 3},
	})
	first := ibs.logs[1][0]
	ibs.RevertToSnapshot(snap, nil)

	lp := ibs.AllocLog(0, 2)
	require.Same(t, first, lp)
}

// The OnLog hook is exported, so a tracer outside this repo may retain the
// pointer it is handed. The value must stay valid after the emit buffer is
// reused by a later block.
func TestOnLogValueSurvivesBufferReuse(t *testing.T) {
	t.Parallel()

	var retained *types.Log
	ibs := New(nil)
	ibs.SetHooks(&tracing.Hooks{OnLog: func(l *types.Log) {
		if retained == nil {
			retained = l
		}
	}})

	first := common.HexToAddress("0x1")
	firstTopic := common.HexToHash("0xaa")
	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: first, Topics: []common.Hash{firstTopic}, Data: []byte{1, 2, 3}})
	require.NotNil(t, retained)

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	ibs.AddLog(&types.Log{
		Address: common.HexToAddress("0x2"),
		Topics:  []common.Hash{common.HexToHash("0xbb")},
		Data:    []byte{9, 9, 9},
	})

	require.Equal(t, first, retained.Address)
	require.Equal(t, []common.Hash{firstTopic}, retained.Topics)
	require.Equal(t, hexutil.Bytes{1, 2, 3}, retained.Data)
}

// The untraced LOG path stays allocation-free; only a configured hook pays for
// the stable value.
func TestUntracedAddLogDoesNotAllocate(t *testing.T) {
	if race.Enabled {
		t.Skip("sync.Pool and the race detector inflate AllocsPerRun")
	}
	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa")},
		Data:    []byte{1, 2, 3},
	}
	ibs.AddLog(log) // warm the buffer so steady-state reuse is measured

	allocs := testing.AllocsPerRun(100, func() {
		ibs.Reset()
		ibs.SetTxContext(1, 0)
		ibs.AddLog(log)
	})
	require.Zero(t, allocs)
}

func BenchmarkLogsRlpHash(b *testing.B) {
	ibs := New(nil)
	for txIndex := range 200 {
		ibs.SetTxContext(1, txIndex)
		for range 3 {
			ibs.AddLog(&types.Log{
				Address: common.HexToAddress("0x1"),
				Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb"), common.HexToHash("0xcc")},
				Data:    bytes.Repeat([]byte{0x11}, 96),
			})
		}
	}

	b.Run("flatten", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = types.RlpHash(ibs.Logs())
		}
	})
	b.Run("streamed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = ibs.LogsRlpHash()
		}
	})
}
