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

package pbt

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

var parallelRoundBenchShape = parallelRoundShape{accounts: 400_000, code: 100_000, storage: 500_000, buckets: 100_000, bigBuckets: 8, bigMin: 20_000, bigMax: 50_000}

func BenchmarkParallelRoundTip(b *testing.B) {
	setParallelRoundBenchHash(b)
	corpus := newParallelRoundCorpus(b, parallelRoundBenchShape)
	benchmarkParallelRoundSchedules(b, corpus.snapshot, func(r *rand.Rand) []Op { return corpus.updates(r, 2_000, 200) })
}

func BenchmarkParallelRoundReexec(b *testing.B) {
	setParallelRoundBenchHash(b)
	corpus := newParallelRoundCorpus(b, parallelRoundBenchShape)
	benchmarkParallelRoundSchedules(b, corpus.snapshot, func(r *rand.Rand) []Op { return corpus.updates(r, 100_000, 0) })
}

func BenchmarkParallelRoundConvert(b *testing.B) {
	setParallelRoundBenchHash(b)
	benchmarkParallelRound(b, map[string][]byte{}, true, func(r *rand.Rand) []Op { return parallelRoundConvertOps(r, 100_000) })
}

func setParallelRoundBenchHash(b *testing.B) {
	commitmentflags.Restore(b)
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
}

func benchmarkParallelRound(b *testing.B, snapshot map[string][]byte, empty bool, roundOps func(*rand.Rand) []Op) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			runParallelRoundBench(b, snapshot, empty, phaseScheduleGrain, workers, roundOps)
		})
	}
}

func benchmarkParallelRoundSchedules(b *testing.B, snapshot map[string][]byte, roundOps func(*rand.Rand) []Op) {
	for _, cell := range []struct {
		name     string
		schedule phaseSchedule
		workers  int
	}{
		{"old", phaseScheduleOld, 1},
		{"old", phaseScheduleOld, 8},
		{"grain", phaseScheduleGrain, 1},
		{"grain", phaseScheduleGrain, 8},
		{"grain", phaseScheduleGrain, 16},
		{"fanout", phaseScheduleFanOut, 8},
	} {
		b.Run(fmt.Sprintf("sched=%s/workers=%d", cell.name, cell.workers), func(b *testing.B) {
			runParallelRoundBench(b, snapshot, false, cell.schedule, cell.workers, roundOps)
		})
	}
}

func runParallelRoundBench(b *testing.B, snapshot map[string][]byte, empty bool, schedule phaseSchedule, workers int, roundOps func(*rand.Rand) []Op) {
	b.StopTimer()
	for i := range b.N {
		ops := roundOps(rand.New(rand.NewSource(int64(100 + i))))
		base := newParallelRoundBase(snapshot)
		if empty {
			base.answerAbsent()
		}
		trie := base.trie()
		trie.phaseSchedule = schedule
		runtime.GC()
		b.StartTimer()
		_, err := trie.ProcessParallelContext(context.Background(), ops, workers)
		b.StopTimer()
		require.NoError(b, err)
	}
}
