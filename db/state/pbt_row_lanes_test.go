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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPBinRangeWriterRowsUseLanesAndCleanTemp(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetRowLaneSkipBufferForTest(1)
	tempDirs := run.writer.RowLaneTempDirsForTest()
	var lanes atomic.Int32
	var skips atomic.Int32
	var skipSpill atomic.Bool
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		switch stage {
		case "lane":
			lanes.Add(1)
		case "skip":
			skips.Add(1)
			for _, tempDir := range tempDirs {
				paths, err := filepath.Glob(filepath.Join(tempDir, "*"))
				if err != nil {
					return err
				}
				for _, path := range paths {
					if !strings.Contains(filepath.Base(path), "-lane-") {
						skipSpill.Store(true)
					}
				}
			}
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.NoError(t, err)
	require.Positive(t, lanes.Load())
	require.Positive(t, skips.Load())
	require.True(t, skipSpill.Load())
	requirePBinRowLaneDirsRemoved(t, tempDirs)
}

func TestPBinRangeWriterRowsCleanTempAfterFailure(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetRowLaneSkipBufferForTest(1)
	tempDirs := run.writer.RowLaneTempDirsForTest()
	want := errors.New("lane build failure")
	var skipSpill atomic.Bool
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "skip" {
			for _, tempDir := range tempDirs {
				paths, err := filepath.Glob(filepath.Join(tempDir, "*"))
				if err != nil {
					return err
				}
				for _, path := range paths {
					if !strings.Contains(filepath.Base(path), "-lane-") {
						skipSpill.Store(true)
					}
				}
			}
		}
		if stage == "range-kvi" {
			return want
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.ErrorIs(t, err, want)
	require.True(t, skipSpill.Load())
	requirePBinRowLaneDirsRemoved(t, tempDirs)
	require.Empty(t, pbinRangeWriterPartialFiles(t, run))
}

func TestPBinRangeWriterBuildsRangesConcurrently(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	var started atomic.Int32
	var overlapped atomic.Bool
	var release sync.Once
	ready := make(chan struct{})
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		switch stage {
		case "range-build-start":
			if started.Add(1) == 2 {
				overlapped.Store(true)
				release.Do(func() { close(ready) })
			}
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				return errors.New("range builds did not overlap")
			}
		case "range-build-done":
			started.Add(-1)
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.NoError(t, err)
	require.True(t, overlapped.Load())
}

func TestPBinRangeWriterWorkersBoundRangeBuilds(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetWorkersForTest(1)
	var inFlight atomic.Int32
	var maximum atomic.Int32
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "range-build-start" {
			current := inFlight.Add(1)
			for {
				old := maximum.Load()
				if old >= current || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if stage == "range-build-done" {
			inFlight.Add(-1)
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.NoError(t, err)
	require.Equal(t, int32(1), maximum.Load())
}

func requirePBinRowLaneDirsRemoved(t *testing.T, dirs []string) {
	t.Helper()
	for _, dir := range dirs {
		_, err := os.Stat(dir)
		require.ErrorIs(t, err, os.ErrNotExist, "range temp directory remains: %s", dir)
	}
}

func pbinRangeWriterPartialFiles(t *testing.T, run *pbinRangeWriterRun) []string {
	t.Helper()
	patterns := []string{
		filepath.Join(run.agg.Dirs().SnapDomain, "*-commitment-bin.*.kv"),
		filepath.Join(run.agg.Dirs().SnapDomain, "*-commitment-bin.*.kvi"),
		filepath.Join(run.agg.Dirs().Tmp, "*.kv.idt"),
	}
	var files []string
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		files = append(files, matches...)
	}
	return files
}
