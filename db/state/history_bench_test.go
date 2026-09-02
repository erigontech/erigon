// Copyright 2022 The Erigon Authors
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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

// BenchmarkHistoryRange benchmarks the hot path: iterating all changed keys
// across a wide txNum range from segment files (exercises HistoryChangesIterFiles.advance).
func BenchmarkHistoryRange(b *testing.B) {
	logger := log.New()
	ctx := b.Context()

	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.HistoryRange(0, int(txs), order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkRangeAsOf benchmarks iterating the full key-space at a given txNum
// from segment files (exercises HistoryRangeAsOfFiles.advanceInFiles).
func BenchmarkRangeAsOf(b *testing.B) {
	logger := log.New()
	ctx := b.Context()

	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	checkTxNum := txs / 2

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.RangeAsOf(ctx, checkTxNum, nil, nil, order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkHistoryRange_MultiFile is like BenchmarkHistoryRange but keeps all
// step-files unmerged so the heap has ~60 elements, actually exercising heap ops.
func BenchmarkHistoryRange_MultiFile(b *testing.B) {
	logger := log.New()
	ctx := b.Context()

	db, h, txs := filledHistory(b, true, logger)
	collateHistory(b, db, h, txs)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.HistoryRange(0, int(txs), order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkRangeAsOf_MultiFile is like BenchmarkRangeAsOf but keeps all
// step-files unmerged so the heap has ~60 elements, actually exercising heap ops.
func BenchmarkRangeAsOf_MultiFile(b *testing.B) {
	logger := log.New()
	ctx := b.Context()

	db, h, txs := filledHistory(b, true, logger)
	collateHistory(b, db, h, txs)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	checkTxNum := txs / 2

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.RangeAsOf(ctx, checkTxNum, nil, nil, order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkHistorySeekInFiles measures a point lookup against merged (page-compressed)
// history files. The `warm` arm reuses one HistoryRoTx, so ht.blockCompressionBuf is
// reused across seeks; `coldTx` opens a fresh HistoryRoTx per seek, which is what an
// rpcdaemon request does and what leaves the page-decode buffer cold every time.
func BenchmarkHistorySeekInFiles(b *testing.B) {
	logger := log.New()
	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	ht := h.beginForTests()
	defer ht.Close()

	requirePagedHistoryFiles(b, ht)

	keys := make([][]byte, 0, 31)
	for keyNum := uint64(1); keyNum <= 31; keyNum++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, keyNum)
		k[0] = 1
		keys = append(keys, k)
	}

	seek := func(b *testing.B, ht *HistoryRoTx, i int) {
		_, _, err := ht.historySeekInFiles(keys[i%len(keys)], uint64(i)%txs+1)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("warm", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			seek(b, ht, i)
			i++
		}
	})

	b.Run("coldBuf", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			ht.blockCompressionBuf = nil
			seek(b, ht, i)
			i++
		}
	})

	b.Run("coldTx", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			fresh := h.beginForTests()
			seek(b, fresh, i)
			fresh.Close()
			i++
		}
	})
}

// BenchmarkHistoryRangePaged walks merged (page-compressed) history files, which is
// where PagedReader decodes a page per Reset. BenchmarkHistoryRange_MultiFile keeps
// its files un-merged, so it never reaches that path.
func BenchmarkHistoryRangePaged(b *testing.B) {
	logger := log.New()
	ctx := b.Context()

	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()
	requirePagedHistoryFiles(b, ic)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.HistoryRange(0, int(txs), order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// collateHistory collates all steps into separate per-step files without merging them.
// This leaves many small files in the heap, exercising heap operations during iteration.
func collateHistory(b *testing.B, db kv.RwDB, h *History, txs uint64) {
	b.Helper()
	ctx := b.Context()
	tx, err := db.BeginRwNosync(ctx)
	require.NoError(b, err)
	defer tx.Rollback()
	for step := kv.Step(0); step < kv.Step(txs/h.stepSize)-1; step++ {
		require.NoError(b, h.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}
	require.NoError(b, tx.Commit())
}
