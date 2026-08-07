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

// Growing the outer buffer past its capacity must keep the groups already
// filled: Logs and LogsRlpHash read every transaction of the block.
func TestAllocLogKeepsEarlierTxsAcrossGrowth(t *testing.T) {
	t.Parallel()

	ibs := New(nil)
	for txIndex := range 64 {
		ibs.SetTxContext(1, txIndex)
		ibs.AddLog(&types.Log{Address: common.BytesToAddress([]byte{byte(txIndex)})})
	}
	for txIndex := range 64 {
		logs := ibs.GetRawLogs(txIndex)
		require.Len(t, logs, 1, "tx %d lost its logs", txIndex)
		require.Equal(t, common.BytesToAddress([]byte{byte(txIndex)}), logs[0].Address)
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

// Reverting hides an entry behind the slice length, so Reset has to scan the
// whole buffer to see that this one is too big to keep.
func TestResetDropsOversizedLogBufferHiddenByRevert(t *testing.T) {
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
	lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 1)
	require.LessOrEqual(t, cap(lp.Data), maxReusableLogDataCap)
}

// Entries survive Reset for reuse, so retention belongs to the IntraBlockState
// and not to one block: a burst of logs at a fresh tx index every block adds a
// high-water mark per block. Reset must hold the whole instance to the budget.
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

	entries, slotCap, dataBytes := retainedLogs(ibs)
	require.LessOrEqual(t, entries, maxReusableLogEntries)
	require.LessOrEqual(t, slotCap, maxReusableLogEntries)
	require.LessOrEqual(t, dataBytes, maxReusableLogBytes)
}

// The budget must not touch a block that fits inside it.
func TestResetKeepsLogsWithinBudget(t *testing.T) {
	t.Parallel()

	const burst = 512
	ibs := New(nil)
	ibs.SetTxContext(1, 0)
	for range burst {
		ibs.AddLog(&types.Log{Address: common.HexToAddress("0x1"), Data: make([]byte, 64)})
	}
	before := ibs.logs[1]
	beforeData := make([]*byte, burst)
	for i := range before {
		beforeData[i] = &before[i].Data[0]
	}

	ibs.Reset()
	ibs.SetTxContext(2, 0)
	for i := range burst {
		lp := ibs.AllocLog(common.HexToAddress("0x2"), 0, 64)
		require.Same(t, &before[i], lp, "entry %d", i)
		require.Same(t, beforeData[i], &lp.Data[0], "entry %d data", i)
	}
}

func retainedLogs(ibs *IntraBlockState) (entries, slotCap, dataBytes int) {
	for _, slot := range ibs.logs[:cap(ibs.logs)] {
		slotCap += cap(slot)
		buf := slot[:cap(slot)]
		entries += len(buf)
		for i := range buf {
			dataBytes += cap(buf[i].Data)
		}
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
	first := &ibs.logs[1][0]
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

// TestGetLogsStampsTxAndBlock pins that GetLogs stamps the tx hash, block hash
// and block number onto the logs it returns. Receipts are built straight from
// this result, so a stamp that lands on a per-iteration copy leaves every
// receipt log carrying zero identity.
func TestGetLogsStampsTxAndBlock(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.SetTxContext(0, 0)
	ibs.AddLog(&types.Log{Address: common.Address{0x11}, Topics: []common.Hash{{0x01}}, Data: []byte{0xaa}})
	ibs.AddLog(&types.Log{Address: common.Address{0x22}, Data: []byte{0xbb}})

	txnHash := common.Hash{0xde, 0xad}
	blockHash := common.Hash{0xbe, 0xef}
	const blockNumber = 4242

	logs := ibs.GetLogs(0, txnHash, blockNumber, blockHash)
	require.Len(t, logs, 2)
	for i, l := range logs {
		require.Equal(t, txnHash, l.TxHash, "log %d: TxHash", i)
		require.Equal(t, blockHash, l.BlockHash, "log %d: BlockHash", i)
		require.Equal(t, hexutil.Uint64(blockNumber), l.BlockNumber, "log %d: BlockNumber", i)
	}
}
