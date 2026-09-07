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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
)

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
	b.ReportMetric(float64(len(ibs.logs.pool)), "pooled")
	b.ReportMetric(float64(ibs.logs.poolBytes)/1024, "poolKB")
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
	b.ReportMetric(float64(len(ibs.logs.pool)), "pooled")
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
