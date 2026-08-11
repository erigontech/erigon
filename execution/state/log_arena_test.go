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

// A reused entry must not carry any field of the block that used it before.
// Address is the sharpest one: it is a consensus field with no length the
// caller has to honour, so AllocLog takes it rather than trusting the caller.
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

// Growing the run past its capacity must keep what earlier transactions wrote:
// Logs and LogsRlpHash read the whole block.
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

// A reverted entry keeps its slot, and Data past the per-entry cap does not stay
// with it.
func TestRevertDropsOversizedLogData(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)

	snap := ibs.PushSnapshot()
	ibs.AddLog(&types.Log{
		Address: common.HexToAddress("0x1"),
		Data:    make([]byte, maxRetainedLogDataCap+1),
	})
	ibs.RevertToSnapshot(snap, nil)

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 1)
	require.LessOrEqual(t, cap(lp.Data), maxRetainedLogDataCap)
}

// Slots survive Reset for reuse, so retention belongs to the IntraBlockState and
// not to one block: a burst of logs at a fresh tx index every block would
// otherwise add a high-water mark per block. The run's array is the whole of it.
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

	slotCap, dataBytes := retainedLogs(ibs)
	require.LessOrEqual(t, slotCap, maxRetainedLogSlots)
	require.LessOrEqual(t, dataBytes, maxRetainedLogBytes)
	require.Equal(t, dataBytes, ibs.logs.retainedData)
}

// A transaction that fits the budget costs no entry the next time round. Slots
// are reused where they lie, so the run walks back over the same ones in order.
func TestResetKeepsLogsWithinBudget(t *testing.T) {
	t.Parallel()

	const burst = 512
	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	for range burst {
		ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1"), Data: make([]byte, 64)})
	}
	before := make([]*types.Log, burst)
	for i := range ibs.logs.entries {
		before[i] = &ibs.logs.entries[i]
	}

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	for i := range burst {
		require.Same(t, before[i], ibs.AllocLog(common.HexToAddress("0x2"), 0, 64), "entry %d", i)
	}
}

// retainedData is what the arena admits itself against, so it has to match the
// Data the slots actually hold, through emit, revert and reset.
func TestLogRetainedDataMatchesSlots(t *testing.T) {
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

			slotCap, dataBytes := retainedLogs(ibs)
			require.Equal(t, dataBytes, ibs.logs.retainedData, "block %d tx %d", blockNum, txIndex)
			require.LessOrEqual(t, dataBytes, maxRetainedLogBytes)
			require.LessOrEqual(t, slotCap, maxRetainedLogSlots)
		}
	}
}

// A block of large logs is what the reuse is for: the transactions run one after
// another, so one buffer serves them all instead of one per transaction.
func TestLogSlotReusesLargeDataAcrossTxs(t *testing.T) {
	t.Parallel()

	const dataSize = maxRetainedLogDataCap / 2
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
	require.Equal(t, cap(first.Data), ibs.logs.retainedData)
}

// Data past the per-entry cap is not kept: one buffer that size would take the
// whole budget and leave nothing for the small entries.
func TestLogSlotRejectsOversizedData(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	ibs.AllocLog(common.HexToAddress("0x1"), 0, maxRetainedLogDataCap+1)
	ibs.Reset()

	kept := keptLogSlots(ibs)
	require.Len(t, kept, 1, "the slot is small enough to keep")
	require.Zero(t, ibs.logs.retainedData, "its Data is not")
	require.Nil(t, kept[0].Data)
}

// An outlier must not leave the array that held it behind.
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

// keptLogSlots is the run's array in full — the live entries and the slots reset
// took back, which is everything the arena holds.
func keptLogSlots(ibs *IntraBlockState) types.Logs {
	run := ibs.logs.entries
	return run[:cap(run)]
}

func retainedLogs(ibs *IntraBlockState) (slotCap, dataBytes int) {
	slots := keptLogSlots(ibs)
	for i := range slots {
		dataBytes += cap(slots[i].Data)
	}
	return len(slots), dataBytes
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
	first := &ibs.logs.entries[0]
	ibs.RevertToSnapshot(snap, nil)

	lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 2)
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

// Splits the cost of the stable OnLog value: the untraced path keeps reusing
// the buffer entry, only a configured hook pays for the copy.
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

// One transaction spending MaxTxnGasLimit entirely on logs: the most an arena
// can be asked to hold before it resets. Reports what survives the reset.
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
	slotCap, _ := retainedLogs(ibs)
	b.ReportMetric(float64(slotCap), "slots")
	b.ReportMetric(float64(ibs.logs.retainedData)/1024, "retainedKB")
}

// Blocks whose transactions each emit a 64KB log — the shape an attack sends.
// The size is fixed rather than tied to a budget, so runs stay comparable when
// the budgets move.
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

// The parallel executor resets before every transaction, so a block costs one
// Reset per transaction while the buffers hold the whole block's logs.
// The assembler and the tooling reset once a block is built, so a whole block's
// entries reach the pool in one go — the shape that decides how small the pool
// may be. p99 mainnet is ~1700 logs.
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
	slotCap, _ := retainedLogs(ibs)
	b.ReportMetric(float64(slotCap), "slots")
}

// Blocks do not repeat their shape: the same logs land at different tx indexes
// from one block to the next. An arena keyed by tx index keeps a slot for every
// shape it has seen, while what a reset-per-tx caller needs live is one
// transaction. Reports what is retained, which ns/op cannot show.
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
	slotCap, dataBytes := retainedLogs(ibs)
	b.ReportMetric(float64(slotCap), "slots")
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

// TestAddLogOnLogHookCopyContract pins the OnLog contract on the AddLog path:
// the hook sees the emitted contents during the callback, and a copy taken then
// stays intact after the buffer entry is reused by later blocks.
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

// TestNotifyLogHookGetsStableCopy pins the OnLog contract on the AllocLog path
// the EVM's makeLog uses: the hook owns what it receives, so retaining it is
// safe even though the buffer entry behind it is reused by a later block.
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

// TestAddLogKeepsCallerBlockNumber pins that BlockNumber stays a caller-assigned
// field: execution/state does not know the block number, so a reused entry must
// not carry the previous caller's value either.
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
	capBefore := cap(ibs.logs.entries)
	require.GreaterOrEqual(t, capBefore, 8)

	ibs.RevertToSnapshot(snap, nil)
	require.Empty(t, ibs.logs.entries, "the reverted logs are gone")

	ibs.AddLog(&types.Log{Address: common.Address{0xff}})
	require.Len(t, ibs.logs.entries, 1)
	require.Equal(t, capBefore, cap(ibs.logs.entries), "the array survives revert+relog")
}

// TestLogIndexIsBlockWide pins that AddLog stamps a block-wide log index:
// receipts.DeriveFields derives FirstLogIndexWithinBlock from Logs[0].Index, so
// the counter must run across transactions, roll back with a reverted log, and
// restart at zero on Reset.
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

// Reading a transaction the run has moved past answers empty, which a receipt
// records as "emitted no logs". A transaction that simply emitted nothing is
// newer than the tail, not older, so it stays legal.
func TestGetLogsOfPastTxAsserts(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = true

	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1")})
	ibs.SetTxContext(1, 5)
	ibs.AddLog(&types.Log{Address: common.HexToAddress("0x2")})

	require.Len(t, ibs.GetRawLogs(5), 1, "the tail")
	require.Empty(t, ibs.GetRawLogs(9), "a later transaction emitted nothing")
	require.Panics(t, func() { ibs.GetRawLogs(0) }, "the run has moved past tx 0")
}

// Whatever the traffic looks like, one arena holds a bounded amount: the one
// array the run left behind, and the Data its slots kept. This is the invariant
// a shape-specific leak breaks — logs parked per tx index, an array kept because
// one block was wide, Data kept because one log was large.
func TestArenaRetentionStaysBounded(t *testing.T) {
	t.Parallel()

	const sizeofLog = 168 // what a slot costs, the run's array being []types.Log
	shapes := []struct{ txs, logsPerTx, dataSize int }{
		{200, 3, 96},                          // ordinary
		{1500, 1, 32},                         // wide: many tx indexes
		{2, maxRetainedLogSlots + 1000, 8},    // deep: one transaction outgrows the run's array
		{20, 2, maxRetainedLogDataCap + 1000}, // Data past what a slot may keep
		{8, 40, maxRetainedLogBytes / 32},     // Data that fits alone but not together
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

	slotCap, dataBytes := retainedLogs(ibs)
	require.LessOrEqual(t, slotCap, maxRetainedLogSlots, "the run's array")
	require.LessOrEqual(t, dataBytes, maxRetainedLogBytes, "the Data its slots kept")
	require.Equal(t, dataBytes, ibs.logs.retainedData)

	held := slotCap*sizeofLog + dataBytes
	require.Less(t, held, 3<<20, "an arena holds %dKB after mixed traffic", held/1024)
}
