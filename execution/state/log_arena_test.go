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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
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

// A reused entry must not carry the previous block's fields. AllocLog takes
// Address itself rather than trust the caller to clear it.
func TestAllocLogClearsReusedEntry(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	lp := ibs.AllocLog(common.HexToAddress("0xaa"), 2, 4)
	lp.Topics[0], lp.Topics[1] = common.HexToHash("0x11"), common.HexToHash("0x22")
	copy(lp.Data, []byte{1, 2, 3, 4})
	lp.TxHash, lp.BlockHash = common.HexToHash("0xde"), common.HexToHash("0xad")
	lp.BlockNumber, lp.Removed = 7, true
	ibs.NotifyLog(lp)

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	reused := ibs.AllocLog(common.HexToAddress("0xbb"), 1, 1)
	require.Same(t, lp, reused, "entry is reused")
	require.Equal(t, common.HexToAddress("0xbb"), reused.Address)
	require.Equal(t, common.Hash{}, reused.TxHash)
	require.Equal(t, common.Hash{}, reused.BlockHash)
	require.Zero(t, reused.BlockNumber)
	require.False(t, reused.Removed)
}

func TestAllocLogKeepsEarlierTxsAcrossGrowth(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	for txIndex := range 64 {
		ibs.SetTxContext(1, txIndex)
		ibs.AddLog(&types.Log{Address: common.BytesToAddress([]byte{byte(txIndex)})})
	}
	logs := ibs.Logs()
	require.Len(t, logs, 64)
	for txIndex := range 64 {
		require.Equal(t, common.BytesToAddress([]byte{byte(txIndex)}), logs[txIndex].Address, "tx %d", txIndex)
		require.Equal(t, hexutil.Uint(txIndex), logs[txIndex].TxIndex)
	}
}

func TestLogsIsNilWhenEmpty(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	require.Nil(t, ibs.Logs())

	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1")})
	ibs.Reset()
	require.Nil(t, ibs.Logs(), "the reused buffer must not surface as an empty slice")
}

func TestAddLogKeepsCallerTxAndBlockHash(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{
		Address:   common.HexToAddress("0x1"),
		TxHash:    common.HexToHash("0xde"),
		BlockHash: common.HexToHash("0xad"),
	})

	logs := ibs.GetRawLogs(0)
	require.Len(t, logs, 1)
	require.Equal(t, common.HexToHash("0xde"), logs[0].TxHash)
	require.Equal(t, common.HexToHash("0xad"), logs[0].BlockHash)
}

func TestRevertDropsOversizedLogData(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)

	snap := ibs.PushSnapshot()
	ibs.AddLog(&types.Log{
		Address: common.HexToAddress("0x1"),
		Data:    make([]byte, maxPooledLogDataCap+1),
	})
	ibs.RevertToSnapshot(snap, nil)

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 1)
	require.LessOrEqual(t, cap(lp.Data), maxPooledLogDataCap)
}

func TestResetBoundsRetainedLogMemory(t *testing.T) {
	t.Parallel()

	const blocks, burst = 32, 1024
	ibs := New(nil)
	for blockNum := range blocks {
		ibs.SetTxContext(uint64(blockNum+1), blockNum)
		for range burst {
			ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1"), Data: make([]byte, 256)})
		}
		ibs.Reset()
	}

	require.LessOrEqual(t, len(ibs.logs.pool), maxPooledLogEntries)
	require.LessOrEqual(t, ibs.logs.poolBytes, maxPooledLogBytes)

	// The run keeps its array, but every entry is in the pool.
	entries, _, dataBytes := retainedLogs(ibs)
	require.Zero(t, entries, "entries live outside the pool")
	require.Zero(t, dataBytes)
}

// The pool hands entries back in its own order, so Reset pins the set of
// reused entries, not their positions.
func TestResetKeepsLogsWithinBudget(t *testing.T) {
	t.Parallel()

	const burst = 512
	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	for range burst {
		ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1"), Data: make([]byte, 64)})
	}
	before := make(map[*types.Log]struct{}, burst)
	for _, lp := range ibs.logs.entries {
		before[lp] = struct{}{}
	}

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	for i := range burst {
		require.Contains(t, before, ibs.AllocLog(common.HexToAddress("0x2"), 0, 64), "entry %d", i)
	}
}

// poolBytes must equal the summed cap of the pooled entries' Data.
func TestLogPoolBytesMatchPooledData(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	for blockNum := range 4 {
		for txIndex := range 8 {
			ibs.SetTxContext(uint64(blockNum+1), txIndex)
			snap := ibs.PushSnapshot()
			for i := range txIndex%3 + 1 {
				ibs.AddLog(&types.Log{
					Address: common.HexToAddress("0x1"),
					Data:    make([]byte, 32*(i+1)),
				})
			}
			if txIndex%2 == 1 {
				ibs.RevertToSnapshot(snap, nil)
			}
			ibs.Reset()

			held := 0
			for _, lp := range ibs.logs.pool {
				held += cap(lp.Data)
			}
			require.Equal(t, held, ibs.logs.poolBytes, "block %d tx %d", blockNum, txIndex)
			require.LessOrEqual(t, ibs.logs.poolBytes, maxPooledLogBytes)
			require.LessOrEqual(t, len(ibs.logs.pool), maxPooledLogEntries)
		}
	}
}

func TestLogPoolReusesLargeDataAcrossTxs(t *testing.T) {
	t.Parallel()

	const dataSize = maxPooledLogDataCap / 2
	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	first := ibs.AllocLog(common.HexToAddress("0x1"), 0, dataSize)
	firstData := &first.Data[0]
	ibs.Reset()

	for txIndex := 1; txIndex < 64; txIndex++ {
		ibs.SetTxContext(1, txIndex)
		lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, dataSize)
		require.Same(t, first, lp, "tx %d", txIndex)
		require.Equal(t, firstData, &lp.Data[0], "tx %d reallocated the buffer", txIndex)
		ibs.Reset()
	}
	require.Len(t, ibs.logs.pool, 1)
}

func TestLogPoolRejectsOversizedData(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	ibs.AllocLog(common.HexToAddress("0x1"), 0, maxPooledLogDataCap+1)
	ibs.Reset()

	require.Len(t, ibs.logs.pool, 1, "the entry is small enough to keep")
	require.Zero(t, ibs.logs.poolBytes, "its Data is not")
	require.Nil(t, ibs.logs.pool[0].Data)
}

func TestResetDropsOutsizedLogSlots(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	for range maxRetainedLogSlots + 1 {
		ibs.AllocLog(common.HexToAddress("0x1"), 0, 8)
	}
	require.Greater(t, cap(ibs.logs.entries), maxRetainedLogSlots)

	ibs.Reset()
	require.Zero(t, cap(ibs.logs.entries))
}

func retainedLogs(ibs *IntraBlockState) (entries, slotCap, dataBytes int) {
	run := ibs.logs.entries
	slotCap = cap(run)
	for _, lp := range run[:cap(run)] {
		if lp == nil {
			continue
		}
		entries++
		dataBytes += cap(lp.Data)
	}
	return entries, slotCap, dataBytes
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
	first := ibs.logs.entries[0]
	ibs.RevertToSnapshot(snap, nil)

	lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 2)
	require.Same(t, first, lp)
}

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

func BenchmarkAddLog(b *testing.B) {
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")},
		Data:    bytes.Repeat([]byte{0x11}, 96),
	}
	run := func(b *testing.B, hooks *tracing.Hooks) {
		ibs := New(nil)
		ibs.SetHooks(hooks)
		b.ReportAllocs()
		for range b.N {
			ibs.Reset()
			ibs.SetTxContext(1, 0)
			ibs.AddLog(log)
		}
	}
	b.Run("untraced", func(b *testing.B) { run(b, nil) })
	b.Run("traced", func(b *testing.B) {
		run(b, &tracing.Hooks{OnLog: func(l *types.Log) { sinkLog = l }})
	})
}

var sinkLog *types.Log

func BenchmarkLogEmitWorstCaseTx(b *testing.B) {
	addr := common.HexToAddress("0x1")
	ibs := New(nil)
	b.ReportAllocs()
	for b.Loop() {
		ibs.SetTxContext(1, 0)
		for range maxLogsPerTxn {
			ibs.AllocLog(addr, 0, 0)
		}
		ibs.Reset()
	}
	b.ReportMetric(float64(len(ibs.logs.pool)), "pooled")
	b.ReportMetric(float64(ibs.logs.poolBytes)/1024, "poolKB")
}

func BenchmarkLogEmitLargeDataPerTx(b *testing.B) {
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa")},
		Data:    bytes.Repeat([]byte{0x11}, 64*1024+1),
	}
	for _, txs := range []int{16, 100} {
		b.Run(fmt.Sprintf("txs=%d", txs), func(b *testing.B) {
			ibs := New(nil)
			b.ReportAllocs()
			for b.Loop() {
				for txIndex := range txs {
					ibs.SetTxContext(1, txIndex)
					ibs.AddLog(log)
					ibs.Reset()
				}
			}
		})
	}
}

func BenchmarkLogEmitBlockLevelReset(b *testing.B) {
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")},
		Data:    bytes.Repeat([]byte{0x11}, 96),
	}
	ibs := New(nil)
	b.ReportAllocs()
	for b.Loop() {
		for txIndex := range 200 {
			ibs.SetTxContext(1, txIndex)
			for range 9 { // 1800 logs, a p99 block
				ibs.AddLog(log)
			}
		}
		ibs.Reset() // once, when the block is built
	}
	b.ReportMetric(float64(len(ibs.logs.pool)), "pooled")
}

func BenchmarkLogEmitShiftingShape(b *testing.B) {
	shapes := []struct{ txs, logsPerTx int }{{200, 3}, {60, 10}, {400, 1}, {120, 5}}
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")},
		Data:    bytes.Repeat([]byte{0x11}, 96),
	}
	ibs := New(nil)
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		s := shapes[i%len(shapes)]
		i++
		for txIndex := range s.txs {
			ibs.SetTxContext(1, txIndex)
			for range s.logsPerTx {
				ibs.AddLog(log)
			}
			ibs.Reset()
		}
	}
	entries, slotCap, dataBytes := retainedLogs(ibs)
	b.ReportMetric(float64(slotCap), "slots")
	b.ReportMetric(float64(entries), "entries")
	b.ReportMetric(float64(dataBytes)/1024, "retainedKB")
}

func BenchmarkLogEmitAndResetPerTx(b *testing.B) {
	log := &types.Log{
		Address: common.HexToAddress("0x1"),
		Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb"), common.HexToHash("0xcc")},
		Data:    bytes.Repeat([]byte{0x11}, 96),
	}
	for _, txs := range []int{16, 200} {
		b.Run(fmt.Sprintf("txs=%d", txs), func(b *testing.B) {
			ibs := New(nil)
			b.ReportAllocs()
			for b.Loop() {
				for txIndex := range txs {
					ibs.SetTxContext(1, txIndex)
					for range 3 {
						ibs.AddLog(log)
					}
					ibs.Reset()
				}
			}
		})
	}
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

func TestAddLogOnLogHookCopyContract(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	var copied []*types.Log
	ibs.SetHooks(&tracing.Hooks{
		OnLog: func(l *types.Log) { copied = append(copied, l.Copy()) },
	})

	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x11}, Topics: []common.Hash{{0x01}}, Data: []byte{0x01}})
	require.Len(t, copied, 1)

	for i := range 1000 {
		ibs.AddLog(&types.Log{Address: common.Address{0x22}, Data: []byte{byte(i)}})
	}
	ibs.Reset()
	ibs.SetTxContext(2, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x33}, Data: []byte{0x99}})

	require.Equal(t, common.Address{0x11}, copied[0].Address)
	require.Equal(t, []common.Hash{{0x01}}, copied[0].Topics)
	require.Equal(t, []byte{0x01}, []byte(copied[0].Data))
}

func TestNotifyLogHookGetsStableCopy(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	var handed []*types.Log
	ibs.SetHooks(&tracing.Hooks{
		OnLog: func(l *types.Log) { handed = append(handed, l) },
	})

	emit := func(addr common.Address, topic common.Hash, data []byte) *types.Log {
		lp := ibs.AllocLog(addr, 1, len(data))
		lp.Topics[0] = topic
		copy(lp.Data, data)
		ibs.NotifyLog(lp)
		return lp
	}

	ibs.SetTxContext(1, 0)
	lp := emit(common.Address{0xaa}, common.Hash{0x01}, []byte{0x11, 0x22})
	require.NotSame(t, lp, handed[0])

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	require.Same(t, lp, emit(common.Address{0xbb}, common.Hash{0x99}, []byte{0xde, 0xad}))

	require.Equal(t, common.Hash{0x01}, handed[0].Topics[0])
	require.Equal(t, []byte{0x11, 0x22}, []byte(handed[0].Data))
	require.Equal(t, common.Hash{0x99}, handed[1].Topics[0])
	require.Equal(t, []byte{0xde, 0xad}, []byte(handed[1].Data))
}

func TestAddLogKeepsCallerBlockNumber(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.SetTxContext(7, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x01}, BlockNumber: 42})
	ibs.AddLog(&types.Log{Address: common.Address{0x02}})

	logs := ibs.GetRawLogs(0)
	require.Len(t, logs, 2)
	require.Equal(t, hexutil.Uint64(42), logs[0].BlockNumber)
	require.Zero(t, logs[1].BlockNumber, "an unset BlockNumber must not inherit the previous log's")
}

func TestAllocLogPreservesCapacityAcrossRevert(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.SetTxContext(1, 0)
	snap := ibs.PushSnapshot()
	for i := range 8 {
		ibs.AddLog(&types.Log{Address: common.Address{byte(i)}})
	}
	capBefore := cap(ibs.logs.entries)
	require.GreaterOrEqual(t, capBefore, 8)

	ibs.RevertToSnapshot(snap, nil)
	require.Empty(t, ibs.logs.entries, "the reverted logs are gone")

	ibs.AddLog(&types.Log{Address: common.Address{0xff}})
	require.Len(t, ibs.logs.entries, 1)
	require.Equal(t, capBefore, cap(ibs.logs.entries), "the array survives revert+relog")
}

func TestLogIndexIsBlockWide(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x01}})
	ibs.AddLog(&types.Log{Address: common.Address{0x02}})

	ibs.SetTxContext(1, 1)
	snap := ibs.PushSnapshot()
	ibs.AddLog(&types.Log{Address: common.Address{0x03}})
	ibs.RevertToSnapshot(snap, nil)
	ibs.AddLog(&types.Log{Address: common.Address{0x04}})

	block := ibs.Logs()
	require.Len(t, block, 3)
	require.Equal(t, hexutil.Uint(0), block[0].Index)
	require.Equal(t, hexutil.Uint(1), block[1].Index)
	require.Equal(t, hexutil.Uint(2), block[2].Index, "index continues across txs and reuses a reverted slot")
	require.Len(t, ibs.GetRawLogs(1), 1, "the transaction in context has one")

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x05}})
	require.Equal(t, hexutil.Uint(0), ibs.GetRawLogs(0)[0].Index, "next block restarts at zero")
}

func TestGetLogsOfPastTxPanics(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1")})
	ibs.SetTxContext(1, 5)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x2")})

	require.Len(t, ibs.GetRawLogs(5), 1, "the tail")
	require.Empty(t, ibs.GetRawLogs(9), "a later transaction emitted nothing")
	require.Panics(t, func() { ibs.GetRawLogs(0) }, "the run has moved past tx 0")
}

// Not parallel: it mutates the global assert-enabled flag.
func TestGetLogsOfPastTxPanicsWithoutAsserts(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false

	ibs := New(nil)
	ibs.SetTxContext(1, 3)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1")})

	require.Panics(t, func() { ibs.GetRawLogs(2) }, "tx 2 is older than the tail")
}

func TestArenaRetentionStaysBounded(t *testing.T) {
	t.Parallel()

	shapes := []struct{ txs, logsPerTx, dataSize int }{
		{200, 3, 96},                        // ordinary
		{1500, 1, 32},                       // wide: many tx indexes
		{2, maxRetainedLogSlots + 1000, 8},  // deep: one transaction outgrows the run's array
		{20, 2, maxPooledLogDataCap + 1000}, // Data past what the pool admits
		{8, 40, maxPooledLogBytes / 32},     // Data that fits alone but not together
		{800, 2, 256},
	}
	ibs := New(nil)
	for blockNum := range 200 {
		s := shapes[blockNum%len(shapes)]
		for txIndex := range s.txs {
			ibs.SetTxContext(uint64(blockNum+1), txIndex)
			for range s.logsPerTx {
				ibs.AllocLog(common.HexToAddress("0x1"), 2, s.dataSize)
			}
			ibs.Reset()
		}
	}

	slotBytes := cap(ibs.logs.entries) * 8
	require.LessOrEqual(t, slotBytes, maxRetainedLogSlots*8, "the run's array")
	require.LessOrEqual(t, len(ibs.logs.pool), maxPooledLogEntries, "pooled entries")
	require.LessOrEqual(t, ibs.logs.poolBytes, maxPooledLogBytes, "pooled Data")

	held := slotBytes + len(ibs.logs.pool)*304 + ibs.logs.poolBytes
	require.Less(t, held, 3<<20, "an arena holds %dKB after mixed traffic", held/1024)
}
