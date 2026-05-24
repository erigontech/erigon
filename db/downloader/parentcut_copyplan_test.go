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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyRange(t *testing.T) {
	cutBlock := uint64(20_000_000)
	cases := []struct {
		name string
		from uint64
		to   uint64
		want CopyClassification
	}{
		{"entirely-before", 19_998_000, 19_999_000, CopyPreCut},
		{"exact-cut-end-boundary", 19_999_000, 20_000_000, CopyPreCut}, // to == cutBlock → PreCut (cut is exclusive on to)
		{"straddles-cut", 19_999_500, 20_000_500, CopyStraddle},
		{"starts-at-cut", 20_000_000, 20_001_000, CopyStraddle}, // from == cutBlock (still spans)
		{"entirely-after", 20_001_000, 20_002_000, CopyPostCut},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyRange(tc.from, tc.to, cutBlock))
		})
	}
}

func TestClassify_BlockFile(t *testing.T) {
	cutBlock := uint64(20_000_000)
	cases := []struct {
		name string
		file string
		want CopyClassification
	}{
		// Block files require a v<N>.<M>- version prefix (snaptype.ParseFileName
		// fails fast otherwise). *1000 shorthand: `019998-019999-headers.seg`
		// covers blocks 19,998,000-19,999,000.
		{"pre-cut-headers", "v1.0-019998-019999-headers.seg", CopyPreCut},
		{"straddle-headers", "v1.0-019999-020001-headers.seg", CopyStraddle},
		{"post-cut-headers", "v1.0-020001-020002-headers.seg", CopyPostCut},
		{"pre-cut-bodies", "v1.0-019998-019999-bodies.seg", CopyPreCut},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.file, cutBlock, StepToBlock{})
			require.Equal(t, tc.want, got.Classification, "got entry: %+v", got)
		})
	}
}

func TestClassify_StateFileWithStepMapping(t *testing.T) {
	cutBlock := uint64(20_000_000)
	// State-file naming: domain.<FromStep>-<ToStep>.kv etc.
	// Step→block map: step 2520 = block 19_990_000, step 2521 = block 20_010_000.
	stepToBlock := StepToBlock{
		2519: 19_980_000,
		2520: 19_990_000, // before cut
		2521: 20_010_000, // after cut (straddles the cut on the [2520, 2521) range)
		2522: 20_020_000,
	}
	cases := []struct {
		name string
		file string
		want CopyClassification
	}{
		// File covers steps [2519, 2520) → blocks [19,980,000, 19,990,000) → PreCut
		{"pre-cut-domain", "v2.0-accounts.2519-2520.kv", CopyPreCut},
		// File covers steps [2520, 2521) → blocks [19,990,000, 20,010,000) → straddles
		{"straddle-domain", "v2.0-accounts.2520-2521.kv", CopyStraddle},
		// File covers steps [2521, 2522) → blocks [20,010,000, 20,020,000) → PostCut
		{"post-cut-domain", "v2.0-accounts.2521-2522.kv", CopyPostCut},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.file, cutBlock, stepToBlock)
			require.Equal(t, tc.want, got.Classification, "got entry: %+v", got)
		})
	}
}

func TestClassify_StateFileWithoutStepMappingDefaultsToStraddle(t *testing.T) {
	// Conservative fallback when stepToBlock is empty: state files
	// classify as Straddle so the fork retires fresh.
	got := classify("v2.0-accounts.2520-2521.kv", 20_000_000, StepToBlock{})
	require.Equal(t, CopyStraddle, got.Classification)
	require.Contains(t, got.Reason, "not in step→block map")
}

func TestClassify_UnparseableFilenameDefaultsToUnknown(t *testing.T) {
	cases := []string{
		"salt-state.txt",
		"erigondb.toml",
		"some-random-thing.dat",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			got := classify(name, 20_000_000, StepToBlock{})
			require.Equal(t, CopyUnknown, got.Classification)
			require.NotEmpty(t, got.Reason)
		})
	}
}

func TestBuildCopyPlan_EndToEndAgainstATempDir(t *testing.T) {
	dir := t.TempDir()
	// Populate a fake parent snap dir with one of each category. Block
	// files require v1.0- version prefix per the snaptype parser.
	files := []string{
		"v1.0-019998-019999-headers.seg",         // PreCut
		"v1.0-019999-020001-headers.seg",         // Straddle
		"v1.0-020001-020002-headers.seg",         // PostCut
		"salt-blocks.txt",                        // Unknown (non-range) → Copy
		"v1.0-019998-019999-headers.seg.torrent", // ignored (.torrent skip)
	}
	for _, f := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644))
	}

	plan, err := BuildCopyPlan(dir, 20_000_000, StepToBlock{})
	require.NoError(t, err)
	require.Empty(t, plan.Errors)

	copyNames := relNames(plan.Copy)
	straddleNames := relNames(plan.Straddle)
	postCutNames := relNames(plan.PostCut)

	require.Contains(t, copyNames, "v1.0-019998-019999-headers.seg", "pre-cut file copied")
	require.Contains(t, copyNames, "salt-blocks.txt", "non-range file copied conservatively")
	require.Contains(t, straddleNames, "v1.0-019999-020001-headers.seg", "straddle file excluded from copy")
	require.Contains(t, postCutNames, "v1.0-020001-020002-headers.seg", "post-cut file skipped")
	require.NotContains(t, copyNames, "v1.0-019998-019999-headers.seg.torrent",
		".torrent metadata not in copy plan (regenerated for the fork)")
}

func TestBuildCopyPlan_DeterministicOrder(t *testing.T) {
	dir := t.TempDir()
	// Out-of-order writes; result must be sorted by RelPath.
	for _, f := range []string{
		"v1.0-019999-020000-headers.seg",
		"v1.0-019998-019999-headers.seg",
		"v1.0-019997-019998-headers.seg",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte{}, 0o644))
	}
	plan, err := BuildCopyPlan(dir, 25_000_000, StepToBlock{})
	require.NoError(t, err)
	names := relNames(plan.Copy)
	require.Equal(t, []string{
		"v1.0-019997-019998-headers.seg",
		"v1.0-019998-019999-headers.seg",
		"v1.0-019999-020000-headers.seg",
	}, names)
}

func TestBuildCopyPlan_NestedSubdirsHandled(t *testing.T) {
	dir := t.TempDir()
	// Domain subdir is the canonical layout (domain/*.kv).
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "domain"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "domain", "v2.0-accounts.2519-2520.kv"), []byte{}, 0o644))

	stepToBlock := StepToBlock{2519: 19_980_000, 2520: 19_990_000}
	plan, err := BuildCopyPlan(dir, 20_000_000, stepToBlock)
	require.NoError(t, err)
	require.Empty(t, plan.Errors)

	// Subdir prefix preserved in RelPath.
	require.Contains(t, relNames(plan.Copy), "domain/v2.0-accounts.2519-2520.kv")
}

func TestBuildCopyPlan_RejectsBadInput(t *testing.T) {
	t.Run("empty parent dir", func(t *testing.T) {
		_, err := BuildCopyPlan("", 20_000_000, StepToBlock{})
		require.Error(t, err)
	})
	t.Run("nonexistent parent dir", func(t *testing.T) {
		_, err := BuildCopyPlan(filepath.Join(t.TempDir(), "does-not-exist"), 20_000_000, StepToBlock{})
		require.Error(t, err)
	})
	t.Run("parent dir is a file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(path, []byte{}, 0o644))
		_, err := BuildCopyPlan(path, 20_000_000, StepToBlock{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a directory")
	})
}

func relNames(entries []CopyPlanEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.RelPath
	}
	return out
}
