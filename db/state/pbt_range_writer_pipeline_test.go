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

package state_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func selectPBinRangeWriterPipeline(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalHexBinCommitment = true
}

func TestPBinRangeWriterFoldPipelineMatchesBatchLimits(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	corpus := pbinRangeWriterCorpora()[0]
	want := runPBinRangeWriterCorpus(t, corpus.leaves, 100000)
	for _, maxOps := range []int{1000, 7} {
		run := openPBinRangeWriterRun(t, maxOps, 0, 23)
		require.Equal(t, want, pbinPipelineSnapshot(t, run, corpus.leaves), "maxOps=%d", maxOps)
	}
}

func pbinPipelineSnapshot(t *testing.T, run *pbinRangeWriterRun, leaves []state.PBinLeaf) pbinRangeWriterSnapshot {
	t.Helper()
	root, err := run.write(t.Context(), leaves)
	require.NoError(t, err)
	pbinReviewRewritePersistedStateKey(t, run.agg)
	return readPBinRangeWriterSnapshot(t, run.agg, root)
}

func TestPBinRangeWriterFoldPipelineOverlapsStages(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	corpus := pbinRangeWriterCorpora()[0]
	stage1Second := make(chan struct{})
	stage2First := make(chan struct{})
	stage3First := make(chan struct{})
	stage3Release := make(chan struct{})
	var stage3Finished atomic.Bool
	go func() {
		<-stage3First
		time.Sleep(10 * time.Millisecond)
		close(stage3Release)
	}()
	run.writer.SetPipelineHookForTest(func(stage string, index int) error {
		switch {
		case stage == "stage1" && index == 1:
			close(stage1Second)
		case stage == "stage2" && index == 0:
			close(stage2First)
			select {
			case <-stage1Second:
			case <-time.After(time.Second):
				return errors.New("stage 1 did not overlap stage 2")
			}
		case stage == "stage3" && index == 0:
			close(stage3First)
			<-stage3Release
			stage3Finished.Store(true)
		case stage == "stage1" && index == 2 && !stage3Finished.Load():
			return errors.New("stage 1 ran two batches ahead of stage 3")
		}
		return nil
	})
	_, err := run.write(t.Context(), corpus.leaves)
	require.NoError(t, err)
	require.True(t, stage3Finished.Load())
}

func TestPBinRangeWriterFoldPipelineStopsOnStageErrors(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	corpus := pbinRangeWriterCorpora()[0]
	for _, stage := range []string{"stage1", "stage2", "stage3"} {
		t.Run(stage, func(t *testing.T) {
			run := openPBinRangeWriterRun(t, 7, 0, 23)
			want := errors.New(stage + " failure")
			run.writer.SetPipelineHookForTest(func(got string, index int) error {
				if got == stage && index == 0 {
					return want
				}
				return nil
			})
			_, err := run.write(t.Context(), corpus.leaves)
			require.ErrorIs(t, err, want)
		})
	}
}
