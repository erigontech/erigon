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

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// These tests enforce the invariant the user articulated 2026-06-15:
// "The divergence between the inventory and the file system should
// only be temporary while components are processing files, creating
// new ones, or deleting old ones." Concretely: after a snapshot-trim
// completes (Provider.Unwind stages + FinalizeUnwind executes), every
// surviving Inventory block-file entry has a matching file on disk,
// every block-file on disk has a matching Inventory entry, and
// neither side carries anything whose FromBlock is strictly past the
// unwind target.
//
// The check lives in tests, not inline in Provider.Unwind, because a
// runtime assertion would either false-positive on the staged-but-not-
// yet-removed window (the pre-commit window where pendingTrim holds
// the to-remove list) or require expensive FS rescans on the hot
// path. Tests get to do the full walk.

// listBlockSegsOnDisk returns sorted names of `v1.1-*.seg` files at
// the top level of dir. Mirrors the filter findInventoryOrphansPastBlock
// uses; .torrent / non-seg / state-aggregator files are out of scope.
func listBlockSegsOnDisk(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "v1.1-") {
			continue
		}
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// listInventoryBlockNames returns sorted names of every block-file
// entry currently held by Inventory.
func listInventoryBlockNames(inv *snapshot.Inventory) []string {
	files := inv.BlockFiles()
	out := make([]string, 0, len(files))
	for _, e := range files {
		out = append(out, e.Name)
	}
	sort.Strings(out)
	return out
}

// seedBlockFilesAndInventory creates one v1.1-<from>-<to>-<kind>.seg
// file per (range, kind) combination in dir AND registers it in inv.
// Returns the full set of names created.
func seedBlockFilesAndInventory(t *testing.T, dir string, inv *snapshot.Inventory, ranges []struct{ from, to uint64 }) []string {
	t.Helper()
	var names []string
	kinds := []string{"headers", "bodies", "transactions"}
	for _, r := range ranges {
		for _, kind := range kinds {
			name := fmt.Sprintf("v1.1-%06d-%06d-%s.seg", r.from/1000, r.to/1000, kind)
			path := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(path, []byte("test contents"), 0o600))
			require.NoError(t, inv.AddFile(&snapshot.FileEntry{
				Name:      name,
				FromBlock: r.from,
				ToBlock:   r.to,
				Local:     true,
			}))
			names = append(names, name)
		}
	}
	return names
}

// TestProvider_FinalizeUnwind_FSAndInventoryConverge is the invariant
// enforcer the user asked for: walk both sides end-to-end and
// assert they agree post-FinalizeUnwind. Catches the exact wedge the
// inline verifier (since removed) tripped on incorrectly: in iter 1
// mode_b of the 2026-06-15 soak the trim staged 12 entries for
// removal, FinalizeUnwind never ran (the inline check forced an
// AbortUnwind), and the wedge looked like an Inventory bug when it
// was really staging working correctly. With the full Unwind →
// Commit → FinalizeUnwind sequence exercised here, both sides end up
// in lockstep.
func TestProvider_FinalizeUnwind_FSAndInventoryConverge(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	inv := snapshot.NewInventory()

	// Seed eight block ranges:
	//   in-target: 003010-003011, 003011-003012, 003012-003013, 003013-003014
	//   past-target (would be trimmed by an unwind to 3,014,500):
	//     003015-003016, 003016-003017, 003017-003018, 003018-003019
	allRanges := []struct{ from, to uint64 }{
		{3_010_000, 3_011_000},
		{3_011_000, 3_012_000},
		{3_012_000, 3_013_000},
		{3_013_000, 3_014_000},
		{3_015_000, 3_016_000},
		{3_016_000, 3_017_000},
		{3_017_000, 3_018_000},
		{3_018_000, 3_019_000},
	}
	allNames := seedBlockFilesAndInventory(t, tmpDir, inv, allRanges)
	require.Len(t, allNames, 24, "8 ranges × 3 kinds = 24 files")

	p := &Provider{snapDir: tmpDir, Inventory: inv}

	// Stage the 12 past-target entries via collectFilesPastBlock —
	// this is exactly what unwindSnapshotsPastBlock does at the end
	// of Provider.Unwind. Past-target = FromBlock > 3,014,500, so the
	// four 003015..003019 ranges qualify; the four 003010..003014
	// ranges have FromBlock ≤ 3,014,500 and are preserved.
	toBlock := uint64(3_014_500)
	pastEntries := p.collectFilesPastBlock(toBlock, 0)
	require.Len(t, pastEntries, 12, "12 entries past toBlock=3,014,500 (4 ranges × 3 kinds)")

	stageNames := make([]string, 0, len(pastEntries))
	stagePaths := make([]string, 0, len(pastEntries))
	for _, e := range pastEntries {
		stageNames = append(stageNames, e.Name)
		stagePaths = append(stagePaths, filepath.Join(tmpDir, e.Name))
	}
	sort.Strings(stageNames)
	p.pendingTrim = &pendingTrimState{names: stageNames, paths: stagePaths}

	// During the staged window, divergence is expected: Inventory
	// still holds all 24 entries, FS still has all 24 files. The
	// staging just records what FinalizeUnwind will execute.
	require.ElementsMatch(t, allNames, listBlockSegsOnDisk(t, tmpDir),
		"pre-FinalizeUnwind: FS unchanged (staged removals not yet applied)")
	require.ElementsMatch(t, allNames, listInventoryBlockNames(inv),
		"pre-FinalizeUnwind: Inventory unchanged (staged removals not yet applied)")

	// Execute the staged removals.
	require.NoError(t, p.FinalizeUnwind())

	// Post-FinalizeUnwind: the invariant must hold — FS and Inventory
	// agree, neither has anything past toBlock, and the four in-range
	// ranges (4 × 3 kinds = 12 files) survive on both sides.
	survivingNames := []string{
		"v1.1-003010-003011-bodies.seg", "v1.1-003010-003011-headers.seg", "v1.1-003010-003011-transactions.seg",
		"v1.1-003011-003012-bodies.seg", "v1.1-003011-003012-headers.seg", "v1.1-003011-003012-transactions.seg",
		"v1.1-003012-003013-bodies.seg", "v1.1-003012-003013-headers.seg", "v1.1-003012-003013-transactions.seg",
		"v1.1-003013-003014-bodies.seg", "v1.1-003013-003014-headers.seg", "v1.1-003013-003014-transactions.seg",
	}
	require.ElementsMatch(t, survivingNames, listBlockSegsOnDisk(t, tmpDir),
		"post-FinalizeUnwind: only in-target files remain on disk")
	require.ElementsMatch(t, survivingNames, listInventoryBlockNames(inv),
		"post-FinalizeUnwind: Inventory holds only in-target entries")

	// Cross-check: every Inventory entry has a file on disk and vice
	// versa. This is the user-articulated invariant in its strongest
	// form.
	require.Equal(t,
		listBlockSegsOnDisk(t, tmpDir),
		listInventoryBlockNames(inv),
		"post-FinalizeUnwind: FS and Inventory must be in lockstep — no entry without a file, no file without an entry")

	// Neither side may hold anything past toBlock.
	extras, err := p.findInventoryEntriesPastBlock(toBlock)
	require.NoError(t, err)
	require.Empty(t, extras,
		"post-FinalizeUnwind: Inventory must have no entries past toBlock")
	for _, name := range listBlockSegsOnDisk(t, tmpDir) {
		require.NotContains(t, name, "v1.1-003015-",
			"post-FinalizeUnwind: FS must have no v1.1-003015-* (past toBlock)")
		require.NotContains(t, name, "v1.1-003016-",
			"post-FinalizeUnwind: FS must have no v1.1-003016-* (past toBlock)")
		require.NotContains(t, name, "v1.1-003017-",
			"post-FinalizeUnwind: FS must have no v1.1-003017-* (past toBlock)")
		require.NotContains(t, name, "v1.1-003018-",
			"post-FinalizeUnwind: FS must have no v1.1-003018-* (past toBlock)")
	}
}

// TestProvider_FinalizeUnwind_FSAndInventoryConverge_NoOpWhenAlreadyConsistent
// pins the no-divergence path: an unwind to a target ABOVE every
// existing file's range stages no removals, FinalizeUnwind no-ops,
// and FS/Inventory stay in lockstep.
func TestProvider_FinalizeUnwind_FSAndInventoryConverge_NoOpWhenAlreadyConsistent(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	inv := snapshot.NewInventory()

	allRanges := []struct{ from, to uint64 }{
		{3_010_000, 3_011_000},
		{3_011_000, 3_012_000},
	}
	allNames := seedBlockFilesAndInventory(t, tmpDir, inv, allRanges)

	p := &Provider{snapDir: tmpDir, Inventory: inv}

	// Target ABOVE every range — nothing to trim.
	toBlock := uint64(3_020_000)
	pastEntries := p.collectFilesPastBlock(toBlock, 0)
	require.Empty(t, pastEntries, "target above every range → nothing to stage")

	require.NoError(t, p.FinalizeUnwind())

	require.ElementsMatch(t, allNames, listBlockSegsOnDisk(t, tmpDir))
	require.ElementsMatch(t, allNames, listInventoryBlockNames(inv))
	require.Equal(t,
		listBlockSegsOnDisk(t, tmpDir),
		listInventoryBlockNames(inv),
		"no-op FinalizeUnwind: FS and Inventory remain in lockstep")
}

// TestProvider_AbortUnwind_FSAndInventoryUnchanged pins the
// rollback symmetry: when an Unwind attempt errors out before
// commit, AbortUnwind drops the stage. Neither FS nor Inventory
// changes; the datadir is byte-identical to its pre-Unwind state.
func TestProvider_AbortUnwind_FSAndInventoryUnchanged(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	inv := snapshot.NewInventory()

	allRanges := []struct{ from, to uint64 }{
		{3_010_000, 3_011_000},
		{3_015_000, 3_016_000},
	}
	allNames := seedBlockFilesAndInventory(t, tmpDir, inv, allRanges)

	p := &Provider{snapDir: tmpDir, Inventory: inv}

	// Stage the past-target entries.
	toBlock := uint64(3_011_500)
	pastEntries := p.collectFilesPastBlock(toBlock, 0)
	require.Len(t, pastEntries, 3, "3 past-target entries (1 range × 3 kinds)")
	stageNames := make([]string, 0, len(pastEntries))
	stagePaths := make([]string, 0, len(pastEntries))
	for _, e := range pastEntries {
		stageNames = append(stageNames, e.Name)
		stagePaths = append(stagePaths, filepath.Join(tmpDir, e.Name))
	}
	sort.Strings(stageNames)
	p.pendingTrim = &pendingTrimState{names: stageNames, paths: stagePaths}

	// Abort — simulates a tx that rolled back after staging.
	p.AbortUnwind()

	require.Nil(t, p.pendingTrim, "AbortUnwind drops the stage")
	require.ElementsMatch(t, allNames, listBlockSegsOnDisk(t, tmpDir),
		"AbortUnwind: FS unchanged — staged FS deletes never ran")
	require.ElementsMatch(t, allNames, listInventoryBlockNames(inv),
		"AbortUnwind: Inventory unchanged — staged Inventory removals never ran")
}
