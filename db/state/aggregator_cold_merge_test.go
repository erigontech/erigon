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
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

func testAggWithColdCommitment(t *testing.T, ranges []testFileRange, coldTo kv.Step) *Aggregator {
	t.Helper()
	db, agg := testDbAndAggregatorv3(t, 10)
	for _, r := range ranges {
		writeCommitmentFile(t, agg, kv.Step(r.fromStep), kv.Step(r.toStep))
	}
	require.NoError(t, agg.OpenFolder(db))
	require.NoError(t, agg.setDemoteColdDir(t.TempDir()))
	agg.commitmentHotMaxMergeSteps = 1
	for _, r := range ranges {
		if kv.Step(r.toStep) <= coldTo {
			require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, kv.Step(r.fromStep), kv.Step(r.toStep), agg.demoteColdDir))
		}
	}
	return agg
}

func writeCommitmentFile(t *testing.T, agg *Aggregator, from, to kv.Step) {
	t.Helper()
	d := agg.d[kv.CommitmentDomain]
	path := d.kvNewFilePath(from, to)
	c, err := seg.NewCompressor(t.Context(), t.Name(), path, agg.Dirs().Tmp, d.CompressCfg, log.LvlTrace, d.logger)
	require.NoError(t, err)
	defer c.Close()
	w := d.dataWriter(c, true)
	for _, k := range []string{fmt.Sprintf("k%03d", from), "shared"} {
		_, err = w.Write([]byte(k))
		require.NoError(t, err)
		_, err = w.Write(fmt.Appendf(nil, "v%03d", from))
		require.NoError(t, err)
	}
	require.NoError(t, w.Compress())
	data, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer data.Close()
	require.NoError(t, d.buildHashMapAccessor(t.Context(), from, to, data, background.NewProgressSet()))
}

func mergeAll(t *testing.T, agg *Aggregator, toTxNum uint64) {
	t.Helper()
	for {
		done, err := agg.mergeLoopStep(t.Context(), toTxNum)
		require.NoError(t, err)
		if !done {
			return
		}
	}
}

func TestAggregatorColdMergeWritesOutputToColdDir(t *testing.T) {
	t.Parallel()
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}}
	agg := testAggWithColdCommitment(t, ranges, 4)
	snapDomain, coldDir := agg.Dirs().SnapDomain, agg.demoteColdDir
	var inputs []string
	for _, s := range []string{"0-1", "1-2", "2-3", "3-4"} {
		inputs = append(inputs, domainFileBySuffix(t, snapDomain, "commitment."+s+".kv"))
	}
	hot := domainFileBySuffix(t, snapDomain, "commitment.4-5.kv")

	mergeAll(t, agg, 5*agg.StepSize())

	merged := domainFileBySuffix(t, snapDomain, "commitment.0-4.kv")
	target, err := os.Readlink(merged)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(coldDir, filepath.Base(merged)), target)
	kvi, err := filepath.Glob(filepath.Join(snapDomain, "*-commitment.0-4.kvi"))
	require.NoError(t, err)
	require.Len(t, kvi, 1)
	require.False(t, isSymlink(t, kvi[0]))
	for _, in := range inputs {
		for _, p := range []string{in, filepath.Join(coldDir, filepath.Base(in))} {
			_, err = os.Lstat(p)
			require.ErrorIs(t, err, fs.ErrNotExist, p)
		}
	}
	require.False(t, isSymlink(t, hot))
	leftovers, err := filepath.Glob(filepath.Join(snapDomain, "*.tmp"))
	require.NoError(t, err)
	require.Empty(t, leftovers)

	at := agg.BeginFilesRo()
	defer at.Close()
	require.True(t, at.d[kv.CommitmentDomain].files[0].src.cold)
}

func requireNoColdMergeOutput(t *testing.T, agg *Aggregator) {
	t.Helper()
	for _, pattern := range []string{
		filepath.Join(agg.Dirs().SnapDomain, "*-commitment.0-4.kv*"),
		filepath.Join(agg.demoteColdDir, "*-commitment.0-4.kv*"),
	} {
		left, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.Empty(t, left, pattern)
	}
}

func TestAggregatorCancelledColdMergeRemovesLinkAndTarget(t *testing.T) {
	t.Parallel()
	agg := testAggWithColdCommitment(t, []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}}, 4)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := agg.mergeLoopStep(ctx, 4*agg.StepSize())
	require.ErrorIs(t, err, context.Canceled)
	requireNoColdMergeOutput(t, agg)
}

func TestAggregatorRecoverDemotionRemovesInterruptedColdMerge(t *testing.T) {
	t.Parallel()
	agg := testAggWithColdCommitment(t, []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}}, 4)
	path := agg.d[kv.CommitmentDomain].kvNewFilePath(0, 4)
	target := filepath.Join(agg.demoteColdDir, filepath.Base(path))
	_, err := createColdMergeLink(path, target)
	require.NoError(t, err)
	for _, p := range []string{target, target + ".123.tmp"} {
		require.NoError(t, os.WriteFile(p, []byte("partial"), 0o644))
	}

	require.NoError(t, recoverDemotion(agg.Dirs().SnapDomain, agg.demoteColdDir, agg.logger))
	requireNoColdMergeOutput(t, agg)
}
