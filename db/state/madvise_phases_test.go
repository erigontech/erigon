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
	"io/fs"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/syscheck"
)

// requireAllRandom asserts that no snapshot file is left with readahead enabled.
// Snapshot data is far larger than RAM, so every mapping must stay MADV_RANDOM
// once the phase that touched it is done.
func requireAllRandom(t *testing.T, snapDir, phase string) {
	t.Helper()
	bad, err := syscheck.NonRandomUnder(snapDir)
	require.NoError(t, err)
	if len(bad) == 0 {
		return
	}
	for _, g := range bad {
		t.Errorf("after %s: %s is %s (%d mapping(s))", phase, g.Path, g.Advices(), len(g.Mappings))
	}
	t.Fatalf("after %s: %d snapshot file(s) left without MADV_RANDOM", phase, len(bad))
}

// TestMmapAdviceAcrossFilePhases pins the invariant that build, accessor
// indexing and merge each restore MADV_RANDOM on every snapshot file they map.
// A phase that madvises the shared mapping and does not put it back shows up
// here as readahead left enabled on a file far bigger than RAM.
//
// Referenced (v2.1) commitment files are built on purpose: they are what makes
// the merge take its read-through-the-shared-mmap path instead of opening a
// private sequential view.
func TestMmapAdviceAcrossFilePhases(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	snapDir := agg.Dirs().Snap

	if runtime.GOOS != "linux" {
		bad, err := syscheck.NonRandomUnder(snapDir)
		require.NoError(t, err)
		require.Empty(t, bad, "VmFlags is a Linux-only interface, nothing is observable here")
		return
	}

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	keys := mkAddrs(0x10, 12)

	writeStepsKeys(t, db, agg, keys, 0, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 32, unboundedFinalityCtx, false))
	require.Eventually(t, func() bool { return !agg.HasBackgroundFilesBuild2() }, time.Minute, 10*time.Millisecond)
	requireAllRandom(t, snapDir, "build")
	built := snapFiles(t, snapDir, ".kv")

	for _, ext := range []string{".kvi", ".bt", ".kvei", ".vi", ".efi"} {
		for _, f := range snapFiles(t, snapDir, ext) {
			require.NoError(t, dir.RemoveFile(f))
		}
	}
	db, agg = reopenAggregator(t, db, agg, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	require.NoError(t, agg.OpenFolder(db))
	require.Empty(t, snapFiles(t, snapDir, ".kvi"))
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
	require.NotEmpty(t, snapFiles(t, snapDir, ".kvi"), "indexing rebuilt no accessor")
	requireAllRandom(t, snapDir, "indexing")

	require.NoError(t, agg.MergeLoop(t.Context()))
	require.NotEmpty(t, slices.DeleteFunc(snapFiles(t, snapDir, ".kv"), func(f string) bool { return slices.Contains(built, f) }), "merge produced no file")
	requireAllRandom(t, snapDir, "merge")
}

func snapFiles(t *testing.T, snapDir, ext string) []string {
	t.Helper()
	var files []string
	require.NoError(t, filepath.WalkDir(snapDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return err
	}))
	return files
}
