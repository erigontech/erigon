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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/ethconfig"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	err = c.AddWord([]byte{1})
	require.NoError(t, err)
	err = c.Compress()
	require.NoError(t, err)
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, name.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(t.Context())
	require.NoError(t, err)
	if name == snaptype2.Transactions.Enum() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name)),
			LeafSize:   8,
		}, logger)
		require.NoError(t, err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build(t.Context())
		require.NoError(t, err)
		defer idx.Close()
	}
}

func createTestSegmentOnlyFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())
}

func BenchmarkFindMergeRange(t *testing.B) {
	merger := NewMerger("x", 1, log.LvlInfo, nil, chainspec.Mainnet.Config, nil)
	merger.DisableFsync()
	t.Run("big", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var RangesOld []Range
			for i := 0; i < 24; i++ {
				RangesOld = append(RangesOld, NewRange(uint64(i*100_000), uint64((i+1)*100_000)))
			}
			merger.FindMergeRanges(RangesOld, uint64(24*100_000))

			var RangesNew []Range
			start := uint64(19_000_000)
			for i := uint64(0); i < 24; i++ {
				RangesNew = append(RangesNew, NewRange(start+(i*100_000), start+((i+1)*100_000)))
			}
			merger.FindMergeRanges(RangesNew, uint64(24*100_000))
		}
	})

	t.Run("small", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var RangesOld Ranges
			for i := uint64(0); i < 240; i++ {
				RangesOld = append(RangesOld, NewRange(i*10_000, (i+1)*10_000))
			}
			merger.FindMergeRanges(RangesOld, uint64(240*10_000))

			var RangesNew Ranges
			start := uint64(19_000_000)
			for i := uint64(0); i < 240; i++ {
				RangesNew = append(RangesNew, NewRange(start+i*10_000, start+(i+1)*10_000))
			}
			merger.FindMergeRanges(RangesNew, uint64(240*10_000))
		}
	})

}

func TestFindMergeRange(t *testing.T) {
	merger := NewMerger("x", 1, log.LvlInfo, nil, chainspec.Mainnet.Config, nil)
	merger.DisableFsync()
	t.Run("big", func(t *testing.T) {
		var RangesOld []Range
		for i := 0; i < 24; i++ {
			RangesOld = append(RangesOld, NewRange(uint64(i*100_000), uint64((i+1)*100_000)))
		}
		found := merger.FindMergeRanges(RangesOld, uint64(24*100_000))

		expect := Ranges{
			NewRange(0, 500000),
			NewRange(500000, 1000000),
			NewRange(1000000, 1500000),
			NewRange(1500000, 2000000)}
		require.Equal(t, expect.String(), Ranges(found).String())

		var RangesNew []Range
		start := uint64(99_000_000)
		for i := uint64(0); i < 24; i++ {
			RangesNew = append(RangesNew, NewRange(start+(i*100_000), start+((i+1)*100_000)))
		}
		found = merger.FindMergeRanges(RangesNew, uint64(24*100_000))

		expect = Ranges{}
		require.Equal(t, expect.String(), Ranges(found).String())
	})

	t.Run("small", func(t *testing.T) {
		var RangesOld Ranges
		for i := uint64(0); i < 240; i++ {
			RangesOld = append(RangesOld, NewRange(i*10_000, (i+1)*10_000))
		}
		found := merger.FindMergeRanges(RangesOld, uint64(240*10_000))
		var expect Ranges
		for i := uint64(0); i < 4; i++ {
			expect = append(expect, NewRange(i*snaptype.Erigon2OldMergeLimit, (i+1)*snaptype.Erigon2OldMergeLimit))
		}
		for i := uint64(0); i < 4; i++ {
			expect = append(expect, NewRange(2_000_000+i*snaptype.Erigon2MergeLimit, 2_000_000+(i+1)*snaptype.Erigon2MergeLimit))
		}

		require.Equal(t, expect.String(), Ranges(found).String())

		var RangesNew Ranges
		start := uint64(99_000_000)
		for i := uint64(0); i < 240; i++ {
			RangesNew = append(RangesNew, NewRange(start+i*10_000, start+(i+1)*10_000))
		}
		found = merger.FindMergeRanges(RangesNew, uint64(240*10_000))
		expect = nil
		for i := uint64(0); i < 24; i++ {
			expect = append(expect, NewRange(start+i*snaptype.Erigon2MergeLimit, start+(i+1)*snaptype.Erigon2MergeLimit))
		}

		require.Equal(t, expect.String(), Ranges(found).String())
	})

}

func TestMergeSnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for i, snT := range snaptype2.BlockSnapshotTypes {
			ver := version.V1_0
			if i%2 == 1 {
				ver = version.V1_1
			}
			createTestSegmentFile(t, from, to, snT.Enum(), dir, ver, logger)
		}
	}

	N := uint64(70)

	for i := uint64(0); i < N; i++ {
		createFile(i*10_000, (i+1)*10_000)
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
		merger.DisableFsync()
		require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, true))
		Ranges := merger.FindMergeRanges(s.Ranges(false), s.SegmentsMax())
		require.Len(Ranges, 3)
		// NOTE: TestMergeSnapshots calls Merge with doIndex=false.
		// Since the merged segment is not indexed, RecalcVisibleSegments will not promote it,
		// and the merge already removed the subsumed sub-segments from dirtyFiles, causing visible
		// segments to drop to 0 until BuildMissedIndices/OpenFolder runs. This is expected
		// for doIndex=false merges and is only done in this test to skip redundant index rebuilding.
		err := merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	expectedFileName := snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, 0, 500_000, snaptype2.Transactions.Enum())
	d, err := seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a := d.Count()
	require.Equal(50, a)

	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
		merger.DisableFsync()
		s.OpenFolder()
		Ranges := merger.FindMergeRanges(s.Ranges(false), s.SegmentsMax())
		require.Empty(Ranges)
		// doIndex=false, same rationale as above
		err := merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	// [0; N] merges are not supported anymore

	// expectedFileName = snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, 600_000, 700_000, snaptype2.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)

	// start := uint64(19_000_000)
	// for i := uint64(0); i < N; i++ {
	// 	createFile(start+i*10_000, start+(i+1)*10_000)
	// }
	// s = NewRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, start, logger)
	// defer s.Close()
	// require.NoError(s.OpenFolder())
	// {
	// 	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.MainnetChainConfig, logger)
	// 	merger.DisableFsync()
	// 	fmt.Println(s.Ranges(), s.SegmentsMax())
	// 	fmt.Println(s.Ranges(), s.SegmentsMax())
	// 	Ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
	// 	require.True(len(Ranges) > 0)
	// 	err := merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
	// 	require.NoError(err)
	// }

	// expectedFileName = snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, start+100_000, start+200_000, snaptype2.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)

	// {
	// 	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.MainnetChainConfig, logger)
	// 	merger.DisableFsync()
	// 	s.OpenSegments(snaptype2.BlockSnapshotTypes, false)
	// 	Ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
	// 	require.True(len(Ranges) == 0)
	// 	err := merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
	// 	require.NoError(err)
	// }

	// expectedFileName = snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, start+600_000, start+700_000, snaptype2.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)
}

func TestMergeSkipsPreClaimedRange(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	for i := uint64(0); i < 4; i++ {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, i*10_000, (i+1)*10_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	// pre-claim one type of the first range: Merge must skip that whole range,
	// roll back its own partial claims, and leave the pre-claim intact
	claimed, free := NewRange(0, 20_000), NewRange(20_000, 40_000)
	require.True(s.TryAcquireRange(snaptype2.Transactions.Enum(), claimed.From(), claimed.To()))

	merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
	merger.DisableFsync()
	require.NoError(merger.Merge(t.Context(), s, snaptype2.BlockSnapshotTypes, []Range{claimed, free}, s.Dir(), false, nil, nil))

	skippedFile := filepath.Join(dir, snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, claimed.From(), claimed.To(), snaptype2.Transactions.Enum()))
	exists, err := dir2.FileExist(skippedFile)
	require.NoError(err)
	require.False(exists)
	require.False(s.TryAcquireRange(snaptype2.Transactions.Enum(), claimed.From(), claimed.To()))
	require.True(s.TryAcquireRange(snaptype2.Headers.Enum(), claimed.From(), claimed.To()))
	s.ReleaseRange(snaptype2.Headers.Enum(), claimed.From(), claimed.To())

	for _, snT := range snaptype2.BlockSnapshotTypes {
		mergedFile := filepath.Join(dir, snaptype.SegmentFileName(snT.Versions().Current, free.From(), free.To(), snT.Enum()))
		exists, err := dir2.FileExist(mergedFile)
		require.NoError(err)
		require.True(exists)
		require.True(s.TryAcquireRange(snT.Enum(), free.From(), free.To()))
		s.ReleaseRange(snT.Enum(), free.From(), free.To())
	}
}

func TestDeleteSnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, version.V1_0, logger)
		}
	}

	N := uint64(70)

	for i := uint64(0); i < N; i++ {
		createFile(i*10_000, (i+1)*10_000)
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	retireFiles := []string{
		"v1.0-000000-000010-bodies.seg",
		"v1.0-000000-000010-headers.seg",
		"v1.0-000000-000010-transactions.seg",
	}
	require.NoError(s.OpenFolder())
	for _, f := range retireFiles {
		require.NoError(s.Delete(f))
		require.False(slices.Contains(s.Files(), f))
	}
}

func TestDeleteSnapshotsIsIdempotent(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir := t.TempDir()
	require := require.New(t)

	for _, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, version.V1_0, logger)
	}

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	fileName := snaptype.SegmentFileName(version.V1_0, 0, 10_000, snaptype2.Bodies.Enum())

	require.NoError(s.Delete(fileName))
	require.False(slices.Contains(s.Files(), fileName))

	require.NotPanics(func() {
		require.NoError(s.Delete(fileName))
	})
	require.NotPanics(func() {
		require.NoError(s.Delete("v1.0-999999-1000000-bodies.seg"))
	})
}

func TestRemoveOverlaps(t *testing.T) {
	mustSeeFile := func(files []string, fileNameWithoutVersion string) bool { //file-version agnostic
		for _, f := range files {
			if strings.HasSuffix(f, fileNameWithoutVersion) {
				return true
			}
		}
		return false
	}

	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, version.V1_0, logger)
		}
	}

	// 0 - 10_000, ... , 40_000 - 50_000 => 5 files
	// 0 - 100_000 => 1 file
	// 130_000 - 140_000, ... , 180_000 - 190_000 => 5 files
	// 100_000 - 200_000 => 1 file
	// 200_000 - 210_000, ... , 220_000 - 230_000 => 3 files

	for i := uint64(0); i < 5; i++ {
		createFile(i*10_000, (i+1)*10_000)
	}

	createFile(0, 100_000)

	for i := uint64(3); i < 8; i++ {
		createFile(100_000+i*10_000, 100_000+(i+1)*10_000)
	}

	createFile(100_000, 200_000)

	for i := uint64(0); i < 3; i++ {
		createFile(200_000+i*10_000, 200_000+(i+1)*10_000)
	}

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	list, err := snaptype.Segments(s.Dir())
	require.NoError(err)
	require.Len(list, 45)

	list, err = snaptype.IdxFiles(s.Dir())
	require.NoError(err)
	require.Len(list, 60)

	//corner case: small header.seg was removed, but header.idx left as garbage. such garbage must be cleaned.
	dir2.RemoveFile(filepath.Join(s.Dir(), list[15].Name()))

	require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, true))
	require.NoError(s.RemoveOverlaps(func(delFiles []string) error {
		require.Len(delFiles, 69)
		mustSeeFile(delFiles, "000000-000010-bodies.seg")
		mustSeeFile(delFiles, "000000-000010-bodies.idx")
		mustSeeFile(delFiles, "000000-000010-headers.seg")
		mustSeeFile(delFiles, "000000-000010-transactions.seg")
		mustSeeFile(delFiles, "000000-000010-transactions.seg")
		mustSeeFile(delFiles, "000000-000010-transactions-to-block.idx")
		mustSeeFile(delFiles, "000170-000180-transactions-to-block.idx")
		require.False(filepath.IsAbs(delFiles[0])) // expecting non-absolute paths (relative as of snapshots dir)
		return nil
	}))

	list, err = snaptype.Segments(s.Dir())
	require.NoError(err)
	require.Len(list, 15)

	for i, info := range list {
		if i%5 < 2 {
			require.Equal(100_000, int(info.Len()), info.Name())
		} else {
			require.Equal(10_000, int(info.Len()), info.Name())
		}
	}

	list, err = snaptype.IdxFiles(s.Dir())
	require.NoError(err)
	require.Len(list, 20)
}

func TestRemoveOverlaps_CrossingTypeString(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, version.V1_0, logger)
		}
	}

	// 0 - 10_000 => 1 file

	createFile(0, 10000)

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	list, err := snaptype.Segments(s.Dir())
	require.NoError(err)
	require.Equal(3, len(list))

	list, err = snaptype.IdxFiles(s.Dir())
	require.NoError(err)
	require.Equal(4, len(list))

	require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, true))
	require.NoError(s.RemoveOverlaps(func(delList []string) error {
		require.Len(delList, 0)
		return nil
	}))

	list, err = snaptype.Segments(s.Dir())
	require.NoError(err)
	require.Equal(3, len(list))

	list, err = snaptype.IdxFiles(s.Dir())
	require.NoError(err)
	require.Equal(4, len(list))

}

func TestCanRetire(t *testing.T) {
	require := require.New(t)
	cases := []struct {
		inFrom, inTo, outFrom, outTo uint64
		can                          bool
	}{
		{0, 1234, 0, 1000, true},
		{1_000_000, 1_120_000, 1_000_000, 1_100_000, true},
		{2_500_000, 4_100_000, 2_500_000, 2_600_000, true},
		{2_500_000, 2_500_100, 2_500_000, 2_500_000, false},
		{1_001_000, 2_000_000, 1_001_000, 1_002_000, true},
	}
	snCfg := snapcfg.KnownCfgOrDevnet(networkname.Mainnet)
	for i, tc := range cases {
		from, to, can := CanRetire(tc.inFrom, tc.inTo, snaptype.Unknown, snCfg)
		require.Equal(int(tc.outFrom), int(from), i)
		require.Equal(int(tc.outTo), int(to), i)
		require.Equal(tc.can, can, tc.inFrom, tc.inTo, i)
	}
}
func TestOpenAllSnapshot(t *testing.T) {
	logger := log.New()
	baseDir, require := t.TempDir(), require.New(t)

	steps := []uint64{500_000, 100_000}

	for i, chain := range []string{networkname.Mainnet, networkname.Amoy} {
		step := steps[i]
		dir := filepath.Join(baseDir, chain)
		chainSnapshotCfg, _ := snapcfg.KnownCfg(chain)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		cfg := ethconfig.BlocksFreezing{ChainName: chain}
		createFile := func(from, to uint64, name snaptype.Type) {
			createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
		}
		s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()
		err := s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible.Load().segments[snaptype2.Enums.Headers])
		require.Empty(s.visible.Load().segments[snaptype2.Enums.Headers])
		s.Close()

		createFile(step, step*2, snaptype2.Bodies)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()
		require.NotNil(s.visible.Load().segments[snaptype2.Enums.Bodies])
		require.Empty(s.visible.Load().segments[snaptype2.Enums.Bodies])
		s.Close()

		createFile(step, step*2, snaptype2.Headers)
		createFile(step, step*2, snaptype2.Transactions)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible.Load().segments[snaptype2.Enums.Headers])
		require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, true))
		// require.Equal(1, len(getSegs(snaptype2.Enums.Headers]))
		s.Close()

		createFile(0, step, snaptype2.Bodies)
		createFile(0, step, snaptype2.Headers)
		createFile(0, step, snaptype2.Transactions)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()

		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible.Load().segments[snaptype2.Enums.Headers])
		require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 2)

		view := s.View()
		defer view.Close()

		seg, ok := view.Segment(snaptype2.Transactions, 10)
		require.True(ok)
		require.Equal(seg.to, step)

		seg, ok = view.Segment(snaptype2.Transactions, step)
		require.True(ok)
		require.Equal(seg.to, step*2)

		_, ok = view.Segment(snaptype2.Transactions, step*2)
		require.False(ok)

		// Erigon may create new snapshots by itself - with high bigger than hardcoded ExpectedBlocks
		// ExpectedBlocks - says only how much block must come from Torrent
		chainSnapshotCfg.ExpectBlocks = 500_000 - 1
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		err = s.OpenFolder()
		require.NoError(err)
		defer s.Close()
		require.NotNil(s.visible.Load().segments[snaptype2.Enums.Headers])
		require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 2)

		createFile(step, step*2-step/5, snaptype2.Headers)
		createFile(step, step*2-step/5, snaptype2.Bodies)
		createFile(step, step*2-step/5, snaptype2.Transactions)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()
		err = s.OpenFolder()
		require.NoError(err)
	}
}

func TestParseCompressedFileName(t *testing.T) {
	require := require.New(t)
	fs := fstest.MapFS{
		"a":                      &fstest.MapFile{},
		"1-a":                    &fstest.MapFile{},
		"1-2-a":                  &fstest.MapFile{},
		"1-2-bodies.info":        &fstest.MapFile{},
		"1-2-bodies.seg":         &fstest.MapFile{},
		"v2-1-2-bodies.seg":      &fstest.MapFile{},
		"v0-1-2-bodies.seg":      &fstest.MapFile{},
		"v1-1-2-bodies.seg":      &fstest.MapFile{},
		"v1.0-1-2-bodies.seg":    &fstest.MapFile{},
		"v1-accounts.24-28.ef":   &fstest.MapFile{},
		"v1.0-accounts.24-28.ef": &fstest.MapFile{},
		"salt-blocks.txt":        &fstest.MapFile{},
		"v1.0-022695-022696-transactions-to-block.idx":                     &fstest.MapFile{},
		"v1-022695-022696-transactions-to-block.idx":                       &fstest.MapFile{},
		"preverified.toml":                                                 &fstest.MapFile{},
		"idx/v1-tracesto.40-44.ef":                                         &fstest.MapFile{},
		"v1.0-021700-021800-bodies.seg.torrent":                            &fstest.MapFile{},
		"caplin/v1.0-021150-021200-BlockRoot.seg":                          &fstest.MapFile{},
		"v1.0-accounts.0-128.bt.torrent":                                   &fstest.MapFile{},
		"v1.0-022695-022696-transactions-to-block.idx.torrent":             &fstest.MapFile{},
		"v1.0-022695-022696-transactions-to-block.idx.tmp.tmp.torrent.tmp": &fstest.MapFile{},
		"v1.0-accounts.24-28.ef.torrent":                                   &fstest.MapFile{},
		"v1.0-accounts.24-28.ef.torrent.tmp.tmp.tmp":                       &fstest.MapFile{},
		"v1.0-070200-070300-bodies.seg.torrent4014494284":                  &fstest.MapFile{},
		"caplin/v1.1-013050-013100-ValidatorEffectiveBalance.seg":          &fstest.MapFile{},
	}
	stat := func(name string) string {
		s, err := fs.Stat(name)
		require.NoError(err)
		return s.Name()
	}
	_, _, ok := snaptype.ParseFileName("", stat("a"))
	require.False(ok)
	_, _, ok = snaptype.ParseFileName("", stat("1-a"))
	require.False(ok)
	_, _, ok = snaptype.ParseFileName("", stat("1-2-a"))
	require.False(ok)
	_, _, ok = snaptype.ParseFileName("", stat("1-2-bodies.info"))
	require.False(ok)
	_, _, ok = snaptype.ParseFileName("", stat("1-2-bodies.seg"))
	require.False(ok)
	_, _, ok = snaptype.ParseFileName("", stat("v2-1-2-bodies.seg"))
	require.True(ok)
	_, _, ok = snaptype.ParseFileName("", stat("v0-1-2-bodies.seg"))
	require.True(ok)
	f, _, ok := snaptype.ParseFileName("", stat("v1-1-2-bodies.seg"))
	require.True(ok)
	require.Equal(f.Type.Enum(), snaptype2.Bodies.Enum())
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
	require.Equal("bodies", f.TypeString)

	var e3 bool
	f, e3, ok = snaptype.ParseFileName("", "caplin/v1.0-021150-021200-BlockRoot.seg")
	require.True(ok)
	require.False(e3)
	require.Equal(21150000, int(f.From))
	require.Equal(21200000, int(f.To))
	require.Equal("BlockRoot", f.TypeString)
	require.Equal("BlockRoot", f.CaplinTypeString)
	require.Nil(f.Type) // caplin state snapshot types don't have a registered snaptype.Type

	f, e3, ok = snaptype.ParseFileName("", "caplin/v1.1-013050-013100-ValidatorEffectiveBalance.seg")
	require.True(ok)
	require.False(e3)
	require.Equal(13050000, int(f.From))
	require.Equal(13100000, int(f.To))
	require.Equal("ValidatorEffectiveBalance", f.TypeString)
	require.Equal("ValidatorEffectiveBalance", f.CaplinTypeString)
	require.Nil(f.Type) // caplin state snapshot types don't have a registered snaptype.Type

	f, e3, ok = snaptype.ParseFileName("caplin", "v1.1-013050-013100-ValidatorEffectiveBalance.seg")
	require.True(ok)
	require.False(e3)
	require.Equal(13050000, int(f.From))
	require.Equal(13100000, int(f.To))
	require.Equal("ValidatorEffectiveBalance", f.TypeString)
	require.Equal("ValidatorEffectiveBalance", f.CaplinTypeString)
	require.Nil(f.Type) // caplin state snapshot types don't have a registered snaptype.Type

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-022695-022696-transactions-to-block.idx"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.TypeString, snaptype2.Indexes.TxnHash2BlockNum.Name)
	require.Equal(22695000, int(f.From))
	require.Equal(22696000, int(f.To))

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-022695-022696-transactions-to-block.idx.torrent"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.TypeString, snaptype2.Indexes.TxnHash2BlockNum.Name)
	require.Equal(22695000, int(f.From))
	require.Equal(22696000, int(f.To))

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-022695-022696-transactions-to-block.idx.tmp.tmp.torrent.tmp"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.TypeString, snaptype2.Indexes.TxnHash2BlockNum.Name)
	require.Equal(22695000, int(f.From))
	require.Equal(22696000, int(f.To))

	f, e3, ok = snaptype.ParseFileName("", stat("v1-022695-022696-transactions-to-block.idx"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.TypeString, snaptype2.Indexes.TxnHash2BlockNum.Name)
	require.Equal(22695000, int(f.From))
	require.Equal(22696000, int(f.To))

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-1-2-bodies.seg"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.Type.Enum(), snaptype2.Bodies.Enum())
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
	require.Equal("bodies", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-070200-070300-bodies.seg.torrent4014494284"))
	require.True(ok)
	require.False(e3)
	require.Equal(f.Type.Enum(), snaptype2.Bodies.Enum())
	require.Equal(70200_000, int(f.From))
	require.Equal(70300_000, int(f.To))
	require.Equal("bodies", f.TypeString)
	require.Equal(".torrent4014494284", f.Ext)

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-accounts.24-28.ef"))
	require.True(ok)
	require.True(e3)
	require.Equal(24, int(f.From))
	require.Equal(28, int(f.To))
	require.Equal("accounts", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-accounts.24-28.ef.torrent"))
	require.True(ok)
	require.True(e3)
	require.Equal(24, int(f.From))
	require.Equal(28, int(f.To))
	require.Equal("accounts", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("", stat("v1.0-accounts.24-28.ef.torrent.tmp.tmp.tmp"))
	require.True(ok)
	require.True(e3)
	require.Equal(24, int(f.From))
	require.Equal(28, int(f.To))
	require.Equal("accounts", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("", stat("v1-accounts.24-28.ef"))
	require.True(ok)
	require.True(e3)
	require.Equal(24, int(f.From))
	require.Equal(28, int(f.To))
	require.Equal("accounts", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("", stat("salt-blocks.txt"))
	require.True(ok)
	require.False(e3)
	require.Equal("salt", f.TypeString)
	require.Equal("salt", f.Type.Name())

	f, e3, ok = snaptype.ParseFileName("", stat("idx/v1-tracesto.40-44.ef"))
	require.True(ok)
	require.True(e3)
	require.Equal("tracesto", f.TypeString)

	f, e3, ok = snaptype.ParseFileName("caplin/", stat("idx/v1-tracesto.40-44.ef"))
	require.True(ok)
	require.True(e3)
	require.Equal("tracesto", f.TypeString)
	//require.Equal("tracesto", f.Type.Name())
}

func TestCalculateVisibleSegments(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}

	for i := uint64(0); i < 7; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Headers)
	}
	for i := uint64(0); i < 6; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Bodies)
	}
	for i := uint64(0); i < 5; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Transactions)
	}
	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	{
		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Transactions], 5)

		require.Equal(7, s.dirtyFiles[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirtyFiles[snaptype2.Enums.Bodies].Len())
		require.Equal(5, s.dirtyFiles[snaptype2.Enums.Transactions].Len())
	}

	// gap in transactions: [5*500_000 - 6*500_000]
	{
		createFile(6*500_000, 7*500_000, snaptype2.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Transactions], 5)

		// dirtyFiles retains gapped files; visible is still filtered by RecalcVisibleSegments.
		require.Equal(7, s.dirtyFiles[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirtyFiles[snaptype2.Enums.Bodies].Len())
		require.Equal(6, s.dirtyFiles[snaptype2.Enums.Transactions].Len())
	}

	// overlap in transactions: [4*500_000 - 4.5*500_000]
	{
		createFile(4*500_000, 4*500_000+250_000, snaptype2.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible.Load().segments[snaptype2.Enums.Transactions], 5)

		// dirtyFiles retains overlapping files; visible is still filtered by RecalcVisibleSegments.
		require.Equal(7, s.dirtyFiles[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirtyFiles[snaptype2.Enums.Bodies].Len())
		require.Equal(7, s.dirtyFiles[snaptype2.Enums.Transactions].Len())
	}
}

func TestCalculateVisibleSegmentsWhenGapsInIdx(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}

	for i := uint64(0); i < 3; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Headers)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Bodies)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Transactions)
	}

	missingIdxFile := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 500_000, 1_000_000, snaptype2.Headers.Name()))
	err := dir2.RemoveFile(missingIdxFile)
	require.NoError(err)

	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	require.NoError(s.OpenFolder())
	idx := s.idxAvailability()
	require.Equal(500_000-1, int(idx))

	require.Len(s.visible.Load().segments[snaptype2.Enums.Headers], 1)
	require.Equal(3, s.dirtyFiles[snaptype2.Enums.Headers].Len())
}

func TestSegmentsMaxDerivedFromVisible(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}
	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}

	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	require.NoError(s.OpenFolder())
	require.Equal(uint64(0), s.SegmentsMax())
	s.Close()

	for i := uint64(0); i < 3; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Headers)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Bodies)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Transactions)
	}
	s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	require.NoError(s.OpenFolder())
	require.Equal(uint64(1_500_000-1), s.SegmentsMax())

	// Regression: an unindexed trailing .seg would previously advance
	// segmentsMax because it was set from dirtyFiles files in openSegments.
	// Now it must not, because it never becomes visible.
	createFile(1_500_000, 2_000_000, snaptype2.Headers)
	missingIdx := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 1_500_000, 2_000_000, snaptype2.Headers.Name()))
	require.NoError(dir2.RemoveFile(missingIdx))

	require.NoError(s.OpenFolder())
	require.Equal(uint64(1_500_000-1), s.SegmentsMax())
	s.Close()
}

func TestViewPinsGeneration(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}
	for i := uint64(0); i < 2; i++ {
		createFile(i*500_000, (i+1)*500_000, snaptype2.Headers)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Bodies)
		createFile(i*500_000, (i+1)*500_000, snaptype2.Transactions)
	}

	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	v := s.View()
	defer v.Close()
	pinned := v.Segments(snaptype2.Headers)
	require.Len(pinned, 2)
	oldSrc0, oldSrc1 := pinned[0].src, pinned[1].src

	// Drop the [500k-1m) segment from visible by introducing a larger [0-1m)
	// that subsumes it (the recalc pass removes subsumed entries). The view
	// captured before this change must still reference the old generation.
	createFile(0, 1_000_000, snaptype2.Headers)
	createFile(0, 1_000_000, snaptype2.Bodies)
	createFile(0, 1_000_000, snaptype2.Transactions)
	require.NoError(s.OpenFolder())

	// New view sees the subsumed generation: a single [0-1m) segment.
	v2 := s.View()
	defer v2.Close()
	require.Len(v2.Segments(snaptype2.Headers), 1)

	// Old view still pins the pre-recalc generation.
	require.Len(pinned, 2)
	require.Same(oldSrc0, pinned[0].src)
	require.Same(oldSrc1, pinned[1].src)
}

// TestCloseWhatNotInListVsLiveViewDoesNotCrash verifies that a reopen which drops
// segments a live View still holds does not close them out from under the reader.
// A covering [0,10000) segment lands on disk and OpenFolder's CloseWhatNotInList
// drops the subsumed 1k sub-segments; because a View still pins that generation,
// their descriptors are released only once the View drains, so Close must not crash.
func TestCloseWhatNotInListVsLiveViewDoesNotCrash(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)

	verOf := func(i int) snaptype.Version {
		if i%2 == 1 {
			return version.V1_1
		}
		return version.V1_0
	}

	// Ten 1k sub-segments per type covering [0, 10000).
	for from := uint64(0); from < 10_000; from += 1_000 {
		for i, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, verOf(i), logger)
		}
	}

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	// A live reader pins the current bundle, as a merge's View does.
	v := s.View()

	// A covering [0,10000) segment lands on disk, subsuming the 1k sub-segments.
	for i, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, verOf(i), logger)
	}

	// Reopen: NoOverlaps drops the subsumed sub-segments from the list, so
	// CloseWhatNotInList retires them while the live View still pins them.
	require.NoError(s.OpenFolder())

	// Closing the View must not crash.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("View.Close crashed (use-after-close of a live-held segment): %v", r)
		}
	}()
	v.Close()
}

// TestRoSnapshots_BundleRefcountReclamation pins the lock-free reclamation model:
// a reader pins the whole visible bundle with a single refcount (not per file),
// and a file retired while a reader is live survives until that reader drains,
// then is physically removed and the generation chain collapses.
func TestRoSnapshots_BundleRefcountReclamation(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)

	// Ten 1k sub-segments per type covering [0, 10000) — small, non-frozen files.
	for from := uint64(0); from < 10_000; from += 1_000 {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	// A reader pins the bundle exactly once.
	v := s.View()
	require.Equal(int32(1), s.visible.Load().refcnt.Load())

	// Pick the last visible (non-frozen) Headers file.
	headers := v.Segments(snaptype2.Headers)
	require.NotEmpty(headers)
	victim := headers[len(headers)-1].src
	require.False(victim.isFrozen(s.snCfg))
	victimPath := victim.FilePath()
	require.FileExists(victimPath)

	// Retire it while the reader is live: an older generation still references
	// it, so the file survives and the chain no longer collapses to one node.
	require.NoError(s.Delete(victim.FileName()))
	require.FileExists(victimPath)
	require.NotEqual(s.visible.Load(), s.oldestVisible, "retired generation still pinned by live reader")

	// Last reader drains → reclaim removes the file and the chain collapses.
	v.Close()
	require.NoFileExists(victimPath)
	require.Equal(s.visible.Load(), s.oldestVisible, "chain must collapse to a single node once drained")
}

// TestRoTxClose_ReleasesPinWhenTypeEmpty guards against RoTx.Close leaking the
// generation pin when the requested type has no visible slice. Such a RoTx has a
// nil Segments slice, and Close must still release the pin (keyed off the pin,
// not Segments) — otherwise the generation never drains and reclamation wedges.
func TestRoTxClose_ReleasesPinWhenTypeEmpty(t *testing.T) {
	logger := log.New()
	require := require.New(t)
	dir := t.TempDir()

	// Configure only Headers, then query Bodies: a valid enum with no visible slice.
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, []snaptype.Type{snaptype2.Headers}, true, logger)
	defer s.Close()
	require.Nil(s.visible.Load().segments[snaptype2.Bodies.Enum()], "precondition: Bodies has no visible slice")

	rt := s.ViewType(snaptype2.Bodies)
	require.Nil(rt.Segments)
	require.Equal(int32(1), s.visible.Load().refcnt.Load())
	rt.Close()
	require.Zero(s.visible.Load().refcnt.Load(), "Close must release the pin even when the type slice is nil")
}

// TestRoSnapshots_ConcurrentViewsAndRepublish stresses the pin/publish machinery
// under -race: many readers pin and drain generations while a publisher keeps
// republishing the visible set. It guards the Load→pin window (a reader must
// never use a generation being reclaimed) and the generation chain (which must
// collapse once every reader has drained).
func TestRoSnapshots_ConcurrentViewsAndRepublish(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)

	for from := uint64(0); from < 20_000; from += 1_000 {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	const readers, iters = 8, 300
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				v := s.View()
				for _, seg := range v.Segments(snaptype2.Headers) {
					if seg.Src() != nil {
						_ = seg.Src().FilePath()
					}
				}
				v.Close()

				rt := s.ViewType(snaptype2.Bodies)
				_ = len(rt.Segments)
				rt.Close()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			_ = s.update(s.alignMin, nil)
		}
	}()
	wg.Wait()

	require.Zero(s.visible.Load().refcnt.Load(), "no reader pins must leak")
	require.Equal(s.visible.Load(), s.oldestVisible, "chain must collapse once all readers drain")
}

// TestCloseWhatNotInList_DropsUnopenedSegment pins the unified FileSet behavior:
// an unopened (nil-Decompressor) segment is always dropped, even when its name is
// in the keep-list — a later reopen re-creates it, so keeping it would only leave
// a duplicate.
func TestCloseWhatNotInList_DropsUnopenedSegment(t *testing.T) {
	logger := log.New()
	require := require.New(t)
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, t.TempDir(), snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	sn := NewDirtySegment(snaptype2.Headers, version.V1_0, 0, 1_000)
	require.Nil(sn.Decompressor)
	s.dirtyFiles[snaptype2.Headers.Enum()].Set(sn)

	s.dirtyLock.Lock()
	removed := CloseWhatNotInList(s.dirtyFiles, []string{sn.FileName()}) // its name IS in the keep-list
	s.dirtyLock.Unlock()

	require.Contains(removed, RetiredSegment{seg: sn}, "unopened segment must be dropped even when kept-by-name")
	_, stillInDirty := s.dirtyFiles[snaptype2.Headers.Enum()].Get(sn)
	require.False(stillInDirty)
}

func createTestIdxFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, name.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(t.Context())
	require.NoError(t, err)
	if name == snaptype2.Transactions.Enum() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name)),
			LeafSize:   8,
		}, logger)
		require.NoError(t, err)
		defer idx.Close()
		idx.DisableFsync()
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build(t.Context())
		require.NoError(t, err)
	}
}

// Unindexed covering segment must not hide indexed subsegments. Once the covering
// segment's index is built, it should be promoted to visible and the subsegments dropped.
func TestOpenFolderPromotesCovering(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}

	createFile(0, 500_000, snaptype2.Headers)
	createFile(0, 500_000, snaptype2.Bodies)
	createFile(0, 500_000, snaptype2.Transactions)
	createFile(500_000, 1_000_000, snaptype2.Headers)
	createFile(500_000, 1_000_000, snaptype2.Bodies)
	createFile(500_000, 1_000_000, snaptype2.Transactions)

	createTestSegmentOnlyFile(t, 0, 1_000_000, snaptype2.Enums.Transactions, dir, version.V1_0, logger)

	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	require.NoError(s.OpenFolder())
	// Here, we verify that all files (both subsegments and unindexed covering segments)
	// are loaded into dirtyFiles, but only indexed segments are visible.
	require.Equal(3, s.dirtyFiles[snaptype2.Enums.Transactions].Len())
	require.Equal(2, s.dirtyFiles[snaptype2.Enums.Headers].Len())
	require.Equal(2, s.dirtyFiles[snaptype2.Enums.Bodies].Len())

	visibleTxn := s.visible.Load().segments[snaptype2.Enums.Transactions]
	require.Len(visibleTxn, 2)
	require.Equal(uint64(0), visibleTxn[0].from)
	require.Equal(uint64(500_000), visibleTxn[0].to)
	require.Equal(uint64(500_000), visibleTxn[1].from)
	require.Equal(uint64(1_000_000), visibleTxn[1].to)
	require.Equal(uint64(1_000_000-1), s.SegmentsMax())

	// Build the index for the covering segment now to promote it.
	createTestIdxFile(t, 0, 1_000_000, snaptype2.Enums.Transactions, dir, version.V1_0, logger)

	require.NoError(s.OpenFolder())
	visibleTxnAfter := s.visible.Load().segments[snaptype2.Enums.Transactions]
	require.Len(visibleTxnAfter, 1)
	require.Equal(uint64(0), visibleTxnAfter[0].from)
	require.Equal(uint64(1_000_000), visibleTxnAfter[0].to)
	require.Equal(uint64(1_000_000-1), s.SegmentsMax())
}

// TestOverlapNoTruncation checks that having a fully indexed covering segment
// alongside its subsegments (both present in the dirtyFiles list) does not trigger
// gap/overlap protection for the next contiguous segment. Previously, the subsegments
// were appended after the covering segment in the visible list, causing the gap detector
// to truncate the entire remainder of the visible chain (fixes issue #21472).
func TestOverlapNoTruncation(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, version.V1_0, logger)
	}

	createFile(0, 500_000, snaptype2.Headers)
	createFile(0, 500_000, snaptype2.Bodies)
	createFile(0, 500_000, snaptype2.Transactions)

	createFile(500_000, 1_000_000, snaptype2.Headers)
	createFile(500_000, 1_000_000, snaptype2.Bodies)
	createFile(500_000, 1_000_000, snaptype2.Transactions)

	createFile(0, 1_000_000, snaptype2.Headers)
	createFile(0, 1_000_000, snaptype2.Bodies)
	createFile(0, 1_000_000, snaptype2.Transactions)

	createFile(1_000_000, 1_500_000, snaptype2.Headers)
	createFile(1_000_000, 1_500_000, snaptype2.Bodies)
	createFile(1_000_000, 1_500_000, snaptype2.Transactions)

	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	require.NoError(s.OpenFolder())
	visibleTxn := s.visible.Load().segments[snaptype2.Enums.Transactions]

	require.Len(visibleTxn, 2)
	require.Equal(uint64(0), visibleTxn[0].from)
	require.Equal(uint64(1_000_000), visibleTxn[0].to)
	require.Equal(uint64(1_000_000), visibleTxn[1].from)
	require.Equal(uint64(1_500_000), visibleTxn[1].to)
	require.Equal(uint64(1_500_000-1), s.SegmentsMax())
}
