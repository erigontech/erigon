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

package snapshotsync

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/mvcc"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/ethconfig"
)

// #26989: a hidden merged file below the cutoff stays and is shown again. Main: fails.
func TestRetireFilesBelowRetiresAHiddenMergedFile(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 200_000; from += 10_000 {
		for _, typ := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	var deleted []string
	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 150_000, func(l []string) error { deleted = append(deleted, l...); return nil })
	require.NoError(t, err)
	require.True(t, retired)
	require.Contains(t, deleted, snaptype.SegmentFileName(version.V1_0, 0, 100_000, snaptype2.Transactions.Enum()))
	require.NoError(t, s.OpenFolder())
	for _, r := range visibleRanges(s, snaptype2.Transactions) {
		require.GreaterOrEqual(t, r.From(), uint64(140_000))
	}
}

// #26988: a merge range cuts a larger headers/bodies file; expects the range skipped. Main: fails (nil segment crash).
func TestMergeSkipsARangeThatCutsAnotherTypesFile(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range []snaptype.Type{snaptype2.Headers, snaptype2.Bodies} {
		createTestSegmentFile(t, 0, 200_000, typ.Enum(), dir, version.V1_0, logger)
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	createTestSegmentFile(t, 100_000, 200_000, snaptype2.Transactions.Enum(), dir, snaptype2.Transactions.Versions().Current, logger)
	for from := uint64(100_000); from < 200_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())
	before := visibleRanges(s, snaptype2.Transactions)

	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
	merger.DisableFsync()
	ranges := merger.FindMergeRanges(s.Ranges(true), snaptype.Unknown)
	require.NoError(t, (merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, ranges, dir, false, nil, nil)))
	require.Equal(t, before, visibleRanges(s, snaptype2.Transactions))
	require.NoFileExists(t, filepath.Join(dir, snaptype.SegmentFileName(snaptype2.Headers.Versions().Current, 100_000, 200_000, snaptype2.Headers.Enum())))
}

// #26988: same layout; expects the transactions range merged. Main: fails (nil segment crash).
func TestMergeRangeUnderALargerHeadersAndBodiesFile(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range []snaptype.Type{snaptype2.Headers, snaptype2.Bodies} {
		createTestSegmentFile(t, 0, 200_000, typ.Enum(), dir, version.V1_0, logger)
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	createTestSegmentFile(t, 100_000, 200_000, snaptype2.Transactions.Enum(), dir, snaptype2.Transactions.Versions().Current, logger)
	for from := uint64(100_000); from < 200_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
	merger.DisableFsync()
	ranges := merger.FindMergeRanges(s.Ranges(true), snaptype.Unknown)
	require.Equal(t, []Range{NewRange(100_000, 200_000)}, ranges)
	require.NoError(t, merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, ranges, dir, false, nil, nil))
	require.Equal(t, []Range{NewRange(0, 100_000), NewRange(100_000, 200_000)}, visibleRanges(s, snaptype2.Transactions))
}

// #26992 round 1: with nothing visible a retire must not take every file. Main: passes.
func TestRetireFilesBelowKeepsEveryFileWhenNothingVisible(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, typ.Enum(), dir, version.V1_0, logger)
		createTestSegmentFile(t, 10_000, 20_000, typ.Enum(), dir, version.V1_0, logger)
	}
	for _, r := range []Range{{0, 10_000}, {10_000, 20_000}} {
		require.NoError(t, os.Remove(filepath.Join(dir, snaptype.IdxFileName(version.V1_0, r.from, r.to, snaptype2.Headers.Name()))))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())
	require.Empty(t, visibleRanges(s, snaptype2.Transactions))

	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 30_000, func([]string) error { return nil })
	require.NoError(t, err)
	require.False(t, retired, "a cutoff past every file must not retire them all")
	require.Equal(t, 2, s.dirty[snaptype2.Transactions.Enum()].Len())
}

// #26992 round 1: a retired unindexed segment leaves its unopened index on disk. Main: fails (main never retires an unindexed segment).
func TestRetireFilesBelowRemovesTheUnopenedIndexOfAnUnindexedSegment(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 30_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 20_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	require.NoError(t, os.Remove(filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 0, 20_000, snaptype2.Transactions.Name()))))
	leftIdx := snaptype.IdxFileName(version.V1_0, 0, 20_000, snaptype2.Indexes.TxnHash2BlockNum.Name)
	require.FileExists(t, filepath.Join(dir, leftIdx))

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	var deleted []string
	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 25_000, func(l []string) error { deleted = append(deleted, l...); return nil })
	require.NoError(t, err)
	require.True(t, retired)
	require.Contains(t, deleted, snaptype.SegmentFileName(version.V1_0, 0, 20_000, snaptype2.Transactions.Enum()))
	require.NoError(t, s.OpenFolder())
	require.Contains(t, deleted, leftIdx, "the seeder keeps seeding the index of a retired range")
	require.NoFileExists(t, filepath.Join(dir, leftIdx), "the index of a retired range stays on disk")
}

// #26992 round 1: a retire during an index build orphans the new index. Main: passes.
func TestRetireFilesBelowSkipsSegmentWithIndexBuildInFlight(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 30_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 20_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	idxNames := []string{
		snaptype.IdxFileName(version.V1_0, 0, 20_000, snaptype2.Transactions.Name()),
		snaptype.IdxFileName(version.V1_0, 0, 20_000, snaptype2.Indexes.TxnHash2BlockNum.Name),
	}
	stash := t.TempDir()
	for _, n := range idxNames {
		require.NoError(t, os.Rename(filepath.Join(dir, n), filepath.Join(stash, n)))
	}

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	jobs := s.claimIndexJobs()
	require.Len(t, jobs, 1)
	require.Equal(t, uint64(20_000), jobs[0].to)

	_, err := s.RetireFilesBelow(snaptype2.Transactions, 25_000, func([]string) error { return nil })
	require.NoError(t, err)

	for _, n := range idxNames { // the index build finishes
		require.NoError(t, os.Rename(filepath.Join(stash, n), filepath.Join(dir, n)))
	}
	s.ReleaseRange(jobs[0].t, jobs[0].from, jobs[0].to)
	require.NoError(t, s.OpenFolder())

	segPath := filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 0, 20_000, snaptype2.Transactions.Enum()))
	for _, n := range idxNames {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			require.FileExists(t, segPath, "index %s is orphaned: its segment was retired during the build", n)
		}
	}
}

// #26992 round 2: a hidden file that straddles the cutoff brings back blocks below it. Main: fails.
func TestRetireFilesBelowUnderAStraddlingHiddenFile(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 200_000; from += 10_000 {
		for _, typ := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	_, err := s.RetireFilesBelow(snaptype2.Transactions, 50_000, nil)
	require.NoError(t, err)
	for _, r := range visibleRanges(s, snaptype2.Transactions) {
		require.GreaterOrEqual(t, r.From(), uint64(40_000), "transactions below the cutoff are visible again: %v", visibleRanges(s, snaptype2.Transactions))
	}
}

// #26992 round 2: a hidden file past the aligned height leaves no visible transactions. Main: fails.
func TestRetireFilesBelowKeepsTransactionsVisible(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 500_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
		if from < 300_000 {
			createTestSegmentFile(t, from, from+10_000, snaptype2.Headers.Enum(), dir, version.V1_0, logger)
			createTestSegmentFile(t, from, from+10_000, snaptype2.Bodies.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 500_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())
	require.Equal(t, uint64(300_000), s.VisibleSegmentsMaxTo(snaptype2.Transactions.Enum()))

	_, err := s.RetireFilesBelow(snaptype2.Transactions, 300_000, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(300_000), s.VisibleSegmentsMaxTo(snaptype2.Transactions.Enum()), "no transaction is visible after the retire")
}

// Copilot on #26992: retires during an index build of a hidden merged file bring back blocks below the cutoff. Main: fails.
func TestRetireConcurrentWithReadersAndIndexBuilds(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 100_000; from += 10_000 {
		for _, typ := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 50_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	idxNames := []string{
		snaptype.IdxFileName(version.V1_0, 0, 50_000, snaptype2.Transactions.Name()),
		snaptype.IdxFileName(version.V1_0, 0, 50_000, snaptype2.Indexes.TxnHash2BlockNum.Name),
	}
	stash := t.TempDir()
	for _, n := range idxNames {
		require.NoError(t, os.Rename(filepath.Join(dir, n), filepath.Join(stash, n)))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := s.View()
				for _, typ := range snaptype2.BlockSnapshotTypes {
					for _, sn := range v.Segments(typ) {
						g := sn.src.MakeGetter()
						for g.HasNext() {
							_, _ = g.Next(nil)
						}
					}
				}
				v.Close()
			}
		})
	}
	jobs := s.claimIndexJobs() // the index build of [0, 50k) starts
	require.Len(t, jobs, 1)
	built := make(chan struct{})
	release := make(chan struct{})
	wg.Go(func() {
		defer close(built)
		<-release
		for _, n := range idxNames {
			_ = os.Rename(filepath.Join(stash, n), filepath.Join(dir, n))
		}
		s.ReleaseRange(jobs[0].t, jobs[0].from, jobs[0].to)
		_ = s.OpenFolder()
	})
	for i := range 20 {
		retired, err := s.RetireFilesBelow(snaptype2.Transactions, uint64(10_000*(i%9+1)), nil)
		require.NoError(t, err)
		require.False(t, retired, "a retire below the cutoff runs while the index build is in flight")
		require.NoError(t, s.RetireFilesAbove(90_000, nil))
	}
	close(release)
	<-built

	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 30_000, nil)
	require.NoError(t, err)
	require.True(t, retired)
	close(stop)
	wg.Wait()

	require.NoError(t, s.OpenFolder())
	for _, r := range visibleRanges(s, snaptype2.Transactions) {
		require.GreaterOrEqual(t, r.From(), uint64(20_000), "transactions below the cutoff are visible again")
	}
	s.WalkDirtySegments(snaptype2.Transactions.Enum(), func(sn *DirtySegment) bool {
		require.True(t, sn.IsIndexed(), sn.FileName())
		for _, f := range sn.FilePaths(dir) {
			require.FileExists(t, filepath.Join(dir, f))
		}
		return true
	})
}

// #26992 round 3: a merge that lands between selection and detach loses blocks at the cutoff. Main: passes.
func TestRetireFilesBelowDuringAMergeKeepsTheMergeOutput(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	var small []string
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 30_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
			small = append(small, snaptype.SegmentFileName(version.V1_0, from, from+10_000, typ.Enum()))
		}
		createTestSegmentFile(t, 0, 20_000, typ.Enum(), dir, version.V1_0, logger)
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenList(small, false))
	require.True(t, visibleCovers(s, snaptype2.Transactions, 10_000, 20_000))

	s.dirtyLock.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := s.RetireFilesBelow(snaptype2.Transactions, 15_000, nil)
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for s.visible.Load().refcnt.Load() == 0 {
		require.True(t, time.Now().Before(deadline))
		time.Sleep(time.Millisecond)
	}
	// What integrateMergedDirtyFiles does for the [0, 20k) merge.
	var merged []string
	for _, typ := range snaptype2.BlockSnapshotTypes {
		merged = append(merged, snaptype.SegmentFileName(version.V1_0, 0, 20_000, typ.Enum()))
	}
	require.NoError(t, s.openSegments(merged, true, false))
	var retired retiredSegments
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for _, r := range []Range{{0, 10_000}, {10_000, 20_000}} {
			if sn := s.delete(snaptype.SegmentFileName(version.V1_0, r.from, r.to, typ.Enum())); sn != nil {
				retired = append(retired, sn)
			}
		}
	}
	retire(mvcc.RetireReasonMerged, retired)
	s.recalcVisibleFiles(s.alignMin, retired)
	s.dirtyLock.Unlock()
	require.NoError(t, <-done)

	require.True(t, visibleCovers(s, snaptype2.Transactions, 15_000, 20_000),
		"blocks at or above the cutoff were retired: visible %v", visibleRanges(s, snaptype2.Transactions))
}

// #26992 round 3: an unindexed file below the cutoff with no build in flight stays on disk. Main: fails.
func TestRetireFilesBelowRetiresAnUnindexedFileWithNoIndexBuild(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 30_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	for _, n := range []string{
		snaptype.IdxFileName(version.V1_0, 0, 10_000, snaptype2.Transactions.Name()),
		snaptype.IdxFileName(version.V1_0, 0, 10_000, snaptype2.Indexes.TxnHash2BlockNum.Name),
	} {
		require.NoError(t, os.Remove(filepath.Join(dir, n)))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())
	require.False(t, s.isInProgress(snaptype2.Transactions.Enum(), 0, 10_000))

	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 25_000, nil)
	require.NoError(t, err)
	require.True(t, retired, "no index build is in flight, yet the retire waits")
	s.dirtyLock.RLock()
	defer s.dirtyLock.RUnlock()
	s.dirty[snaptype2.Transactions.Enum()].Walk(func(segs []*DirtySegment) bool {
		for _, sn := range segs {
			require.GreaterOrEqual(t, sn.to, uint64(25_000), "a file below the cutoff stays: %s", sn.FileName())
		}
		return true
	})
}

// #26992 round 3: an unindexed straddling file is shown again from block 0 once indexed. Main: fails.
func TestRetireFilesBelowRetiresAnUnindexedStraddlingFile(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 100_000; from += 10_000 {
		for _, typ := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	idxNames := []string{
		snaptype.IdxFileName(version.V1_0, 0, 100_000, snaptype2.Transactions.Name()),
		snaptype.IdxFileName(version.V1_0, 0, 100_000, snaptype2.Indexes.TxnHash2BlockNum.Name),
	}
	stash := t.TempDir()
	for _, n := range idxNames {
		require.NoError(t, os.Rename(filepath.Join(dir, n), filepath.Join(stash, n)))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 50_000, nil)
	require.NoError(t, err)
	require.True(t, retired)
	for _, n := range idxNames {
		require.NoError(t, os.Rename(filepath.Join(stash, n), filepath.Join(dir, n)))
	}
	require.NoError(t, s.OpenFolder())
	for _, r := range visibleRanges(s, snaptype2.Transactions) {
		require.GreaterOrEqual(t, r.From(), uint64(40_000), "transactions below the cutoff are visible again: %v", visibleRanges(s, snaptype2.Transactions))
	}
}

// Copilot on #26992: retiring the older version of a range deletes the index the newer one reads. Main: fails (main never retires the older version).
func TestRetireFilesBelowKeepsTheIndexOfASameRangeSurvivor(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 200_000, typ.Enum(), dir, version.V1_0, logger)
		createTestSegmentFile(t, 200_000, 300_000, typ.Enum(), dir, version.V1_0, logger)
	}
	createTestSegmentFile(t, 0, 200_000, snaptype2.Transactions.Enum(), dir, version.V1_1, logger)
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())
	require.Equal(t, 3, s.dirty[snaptype2.Transactions.Enum()].Len())

	var deleted []string
	retired, err := s.RetireFilesBelow(snaptype2.Transactions, 200_000, func(l []string) error { deleted = append(deleted, l...); return nil })
	require.NoError(t, err)
	require.True(t, retired)
	for _, f := range deleted {
		require.Equal(t, ".seg", filepath.Ext(f), "an index the kept segment reads is handed to the seeder: %s", f)
	}
	s.reclaimRetired()
	s.WalkDirtySegments(snaptype2.Transactions.Enum(), func(sn *DirtySegment) bool {
		for _, f := range sn.FilePaths(dir) {
			require.FileExists(t, filepath.Join(dir, f), "the kept segment lost its index")
		}
		return true
	})
	require.NoError(t, s.OpenFolder())
	require.True(t, visibleCovers(s, snaptype2.Transactions, 0, 200_000))
}

// #26992 round 4: IndexMissed releases its claim before OpenFolder, so a retire orphans the new index. Main: passes.
func TestRetireFilesBelowAfterAnIndexJobKeepsItsIndex(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 40_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	createTestSegmentFile(t, 0, 20_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	idxOf := func(from, to uint64) []string {
		return []string{
			snaptype.IdxFileName(version.V1_0, from, to, snaptype2.Transactions.Name()),
			snaptype.IdxFileName(version.V1_0, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name),
		}
	}
	stash := t.TempDir()
	for _, n := range append(idxOf(0, 20_000), idxOf(30_000, 40_000)...) {
		require.NoError(t, os.Rename(filepath.Join(dir, n), filepath.Join(stash, n)))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	jobs := s.claimIndexJobs()
	require.Len(t, jobs, 2)
	// job [0, 20k) ends and releases its claim; job [30k, 40k) is still building.
	for _, n := range idxOf(0, 20_000) {
		require.NoError(t, os.Rename(filepath.Join(stash, n), filepath.Join(dir, n)))
	}
	s.ReleaseRange(snaptype2.Transactions.Enum(), 0, 20_000)

	var deleted []string
	_, err := s.RetireFilesBelow(snaptype2.Transactions, 25_000, func(l []string) error { deleted = append(deleted, l...); return nil })
	require.NoError(t, err)

	for _, n := range idxOf(30_000, 40_000) {
		require.NoError(t, os.Rename(filepath.Join(stash, n), filepath.Join(dir, n)))
	}
	s.ReleaseRange(snaptype2.Transactions.Enum(), 30_000, 40_000)
	require.NoError(t, s.OpenFolder())

	segPath := filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 0, 20_000, snaptype2.Transactions.Enum()))
	for _, n := range idxOf(0, 20_000) {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			require.FileExists(t, segPath, "index %s is orphaned on disk; onDelete got %v", n, deleted)
		}
	}
}

// #26992 round 4: files detached before onDelete are not retried when it fails. Main: passes.
func TestRetireFilesBelowRetriesAfterAnOnDeleteFailure(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for from := uint64(0); from < 30_000; from += 10_000 {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	injected := errors.New("seeder down")
	_, err := s.RetireFilesBelow(snaptype2.Transactions, 15_000, func([]string) error { return injected })
	require.ErrorIs(t, err, injected)

	var deleted []string
	_, err = s.RetireFilesBelow(snaptype2.Transactions, 15_000, func(l []string) error { deleted = append(deleted, l...); return nil })
	require.NoError(t, err)
	require.Contains(t, deleted, snaptype.SegmentFileName(version.V1_0, 0, 10_000, snaptype2.Transactions.Enum()), "the seeder is never told to drop the retired file")
}

// #26992 round 4: an unindexed file past the visible top is retired with blocks no other file holds. Main: passes.
func TestRetireFilesBelowKeepsBlocksOnlyAnUnindexedFileHolds(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	for from := uint64(0); from < 30_000; from += 10_000 {
		for _, typ := range []snaptype.Type{snaptype2.Headers, snaptype2.Bodies} {
			createTestSegmentFile(t, from, from+10_000, typ.Enum(), dir, version.V1_0, logger)
		}
	}
	for from := uint64(0); from < 20_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	}
	createTestSegmentFile(t, 10_000, 30_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	for _, n := range []string{
		snaptype.IdxFileName(version.V1_0, 10_000, 30_000, snaptype2.Transactions.Name()),
		snaptype.IdxFileName(version.V1_0, 10_000, 30_000, snaptype2.Indexes.TxnHash2BlockNum.Name),
	} {
		require.NoError(t, os.Remove(filepath.Join(dir, n)))
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	_, err := s.RetireFilesBelow(snaptype2.Transactions, 15_000, nil)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 10_000, 30_000, snaptype2.Transactions.Enum())),
		"transactions [20k, 30k) are only in this file")
}

func visibleCovers(s *BaseRoSnapshots, typ snaptype.Type, from, to uint64) bool {
	for _, r := range visibleRanges(s, typ) {
		if r.From() <= from && r.To() >= to {
			return true
		}
	}
	return false
}
