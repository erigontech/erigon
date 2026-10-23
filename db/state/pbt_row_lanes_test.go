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
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBinRangeWriterRowsUseLanesAndCleanTemp(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	var lanes atomic.Int32
	var skips atomic.Int32
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		switch stage {
		case "lane":
			lanes.Add(1)
		case "skip":
			skips.Add(1)
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.NoError(t, err)
	require.Positive(t, lanes.Load())
	require.Positive(t, skips.Load())
	require.Empty(t, pbinRowLaneTempFiles(t, run))
}

func TestPBinRangeWriterRowsCleanTempAfterFailure(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	want := errors.New("lane build failure")
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "stage3" {
			return want
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.ErrorIs(t, err, want)
	require.Empty(t, pbinRowLaneTempFiles(t, run))
}

func pbinRowLaneTempFiles(t *testing.T, run *pbinRangeWriterRun) []string {
	t.Helper()
	lanes, err := filepath.Glob(filepath.Join(run.agg.Dirs().Tmp, "pbin-range-*-lane-*.bin"))
	require.NoError(t, err)
	return lanes
}
