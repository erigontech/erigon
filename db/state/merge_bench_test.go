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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkInvertedIndexMergeFiles benchmarks InvertedIndexRoTx.mergeFiles
// with varying numbers of input files to expose the O(N²) incremental-merge cost.
func BenchmarkInvertedIndexMergeFiles(b *testing.B) {
	for _, numFiles := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("files=%d", numFiles), func(b *testing.B) {
			benchmarkIIMergeFiles(b, numFiles)
		})
	}
}

func benchmarkIIMergeFiles(b *testing.B, numFiles int) {
	b.Helper()
	// aggStep must be large enough that key-1 (which appears every txNum) produces
	// sequences longer than SIMPLE_SEQUENCE_MAX_THRESHOLD (16) per file, forcing
	// EliasFano encoding and exercising the expensive merge path.
	const aggStep = 512
	const module = 31 // number of distinct keys

	txs := uint64(numFiles) * aggStep
	ctx := context.Background()
	logger := log.New()

	db, ii, _ := filledInvIndexOfSize(b, txs, aggStep, module, logger)
	_ = db

	// Collate every step to build numFiles segment files; do NOT merge.
	tx, err := db.BeginRw(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ps := background.NewProgressSet()
	for step := kv.Step(0); step < kv.Step(numFiles); step++ {
		require.NoError(b, ii.collateBuildIntegrate(ctx, step, tx, ps))
	}

	// Determine the merge range covering all files.
	ic := ii.beginForTests()
	defer ic.Close()

	maxEndTxNum := ii.dirtyFilesEndTxNumMinimax()
	maxSpan := ii.stepSize * config3.DefaultStepsInFrozenFile
	mr := ic.findMergeRange(maxEndTxNum, maxSpan)
	require.True(b, mr.needMerge, "expected merge to be needed")

	inputFiles := ic.staticFilesInRange(mr.from, mr.to)
	require.NotEmpty(b, inputFiles)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		out, err := ic.mergeFiles(ctx, inputFiles, mr.from, mr.to, ps)
		b.StopTimer()
		require.NoError(b, err)
		out.closeFilesAndRemove()
		b.StartTimer()
	}
}
