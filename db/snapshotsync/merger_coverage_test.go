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

package snapshotsync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestFindMergeRanges_SkipsRangeStraddledByWideV4 pins that a merge
// range is only proposed when the current ranges tile it.
//
// After a deep mode-C/D unwind the block side holds a wide v4 #1
// spanning many aligned chunks plus its v4 #2 tail. The widening loop
// derives aggFrom arithmetically as r.To()-span, so it can land inside
// the wide v4 #1 rather than on a segment boundary. filesByRangeOfType
// then drops that file (its from is below the merge start) and the
// merge emits a file whose name claims blocks it does not contain.
func TestFindMergeRanges_SkipsRangeStraddledByWideV4(t *testing.T) {
	t.Parallel()
	m := NewMerger("x", 1, log.LvlInfo, nil, chainspec.Mainnet.Config, log.New())

	// Wide v4 #1 [3400000, 3442048) straddles the 3440000 boundary;
	// v4 #2 tail closes it at 3443000; 1k chunks run to 3450000.
	ranges := []Range{NewRange(3_400_000, 3_442_048), NewRange(3_442_048, 3_443_000)}
	for from := uint64(3_443_000); from < 3_450_000; from += 1_000 {
		ranges = append(ranges, NewRange(from, from+1_000))
	}

	for _, r := range m.FindMergeRanges(ranges, 3_450_000) {
		require.NotEqual(t, uint64(3_440_000), r.From(),
			"[3440000, 3450000) is not tiled by the current ranges — "+
				"[3440000, 3442048) lives inside the wide v4 #1 starting at 3400000")
	}
}

// TestFindMergeRanges_ProposesFullyTiledRange is the counterpart: when
// the ranges do tile the derived span, the merge must still be proposed.
// Guards against fixing the above by suppressing merges wholesale.
func TestFindMergeRanges_ProposesFullyTiledRange(t *testing.T) {
	t.Parallel()
	m := NewMerger("x", 1, log.LvlInfo, nil, chainspec.Mainnet.Config, log.New())

	var ranges []Range
	for from := uint64(3_440_000); from < 3_450_000; from += 1_000 {
		ranges = append(ranges, NewRange(from, from+1_000))
	}

	var found bool
	for _, r := range m.FindMergeRanges(ranges, 3_450_000) {
		if r.From() == 3_440_000 && r.To() == 3_450_000 {
			found = true
		}
	}
	require.True(t, found, "ten contiguous 1k chunks tile [3440000, 3450000) — merge must be proposed")
}

// TestMerge_FailedMemberLeavesNoSiblings pins that a merge range is all
// or nothing across its types. Merge walks snapTypes in order and
// mergeSubSegment cleans up only the type that failed, so the members
// already written would otherwise survive as a range missing a type —
// which the folder scan then indexes and advertises.
func TestMerge_FailedMemberLeavesNoSiblings(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for i := range uint64(2) {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, i*10_000, (i+1)*10_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
	merger.DisableFsync()

	// Fail the last type in snapTypes order, after the earlier members
	// are already on disk. In production that failure is the
	// transactions index build rejecting a short bodies segment; here a
	// directory squatting on the destination path stands in for it, so
	// the test pins the cleanup rather than one way of reaching it.
	merged := NewRange(0, 20_000)
	last := snaptype2.BlockSnapshotTypes[len(snaptype2.BlockSnapshotTypes)-1]
	blocked := filepath.Join(dir, snaptype.SegmentFileName(last.Versions().Current, merged.From(), merged.To(), last.Enum()))
	require.NoError(t, os.Mkdir(blocked, 0o755))

	err := merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, []Range{merged}, s.Dir(), false, nil, nil)
	require.Error(t, err, "merge of %s must fail with its destination blocked", last.Name())

	for _, snT := range snaptype2.BlockSnapshotTypes {
		if snT.Enum() == last.Enum() {
			continue // its destination is the injected blocker, not a merge output
		}
		p := filepath.Join(dir, snaptype.SegmentFileName(snT.Versions().Current, merged.From(), merged.To(), snT.Enum()))
		exists, statErr := dir2.FileExist(p)
		require.NoError(t, statErr)
		require.False(t, exists,
			"%s survived a failed merge of the same range — an incomplete range must not reach disk", snT.Name())
	}
}

// TestIntegrateMergedDirtyFiles_FrozenOutputRetiresOnlyContainedSegments pins
// that integrating a frozen merge output retires only the non-frozen segments
// its range contains. A non-frozen segment of the same type that ends at or
// before the output's end but starts below it lies outside the merge, and
// retiring it unlinks a file no merge replaced.
func TestIntegrateMergedDirtyFiles_FrozenOutputRetiresOnlyContainedSegments(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	v := version.V1_1
	x := snaptype2.Transactions.Enum()

	createTestSegmentFile(t, 3_400_000, 3_500_000, x, dir, v, logger)
	for from := uint64(3_500_000); from < 3_600_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, x, dir, v, logger)
	}
	createTestSegmentFile(t, 3_500_000, 3_600_000, x, dir, v, logger)

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	var neighbour, output *DirtySegment
	var inside []*DirtySegment
	s.dirty[x].Walk(func(items []*DirtySegment) bool {
		for _, it := range items {
			switch {
			case it.from == 3_400_000 && it.to == 3_500_000:
				neighbour = it
			case it.from == 3_500_000 && it.to == 3_600_000:
				output = it
			default:
				inside = append(inside, it)
			}
		}
		return true
	})
	require.NotNil(t, neighbour)
	require.NotNil(t, output)
	require.Len(t, inside, 10)
	neighbour.frozen = false
	for _, sub := range inside {
		sub.frozen = false
	}

	// Integrate the output as a fresh merge result.
	s.dirty[x].Delete(output)
	merged := &DirtySegment{segType: snaptype2.Transactions, version: v, Range: Range{3_500_000, 3_600_000}, frozen: true}
	require.NoError(t, merged.Open(dir))

	m := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
	m.integrateMergedDirtyFiles(s, map[snaptype.Enum][]*DirtySegment{x: {merged}}, map[snaptype.Enum][]*DirtySegment{})

	dirty := map[*DirtySegment]bool{}
	s.dirty[x].Walk(func(items []*DirtySegment) bool {
		for _, it := range items {
			dirty[it] = true
		}
		return true
	})

	require.True(t, dirty[neighbour], "segment below the merged range must not be retired")
	_, err := os.Stat(filepath.Join(dir, snaptype.SegmentFileName(v, 3_400_000, 3_500_000, x)))
	require.NoError(t, err, "segment below the merged range must stay on disk")

	for _, sub := range inside {
		require.False(t, dirty[sub], "sub-segment [%d,%d) inside the merged range must be retired", sub.from, sub.to)
	}
}
