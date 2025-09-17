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
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(context.Background(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
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
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, name.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(context.Background())
	require.NoError(t, err)
	if name == snaptype2.Transactions.Enum() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name)),
			LeafSize:   8,
		}, logger)
		require.NoError(t, err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build(context.Background())
		require.NoError(t, err)
		defer idx.Close()
	}
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
			found := merger.FindMergeRanges(RangesOld, uint64(24*100_000))

			expect := Ranges{
				NewRange(0, 500000),
				NewRange(500000, 1000000),
				NewRange(1000000, 1500000),
				NewRange(1500000, 2000000)}
			require.Equal(t, expect.String(), Ranges(found).String())

			var RangesNew []Range
			start := uint64(19_000_000)
			for i := uint64(0); i < 24; i++ {
				RangesNew = append(RangesNew, NewRange(start+(i*100_000), start+((i+1)*100_000)))
			}
			found = merger.FindMergeRanges(RangesNew, uint64(24*100_000))

			expect = Ranges{}
			require.Equal(t, expect.String(), Ranges(found).String())
		}
	})

	t.Run("small", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
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
			start := uint64(19_000_000)
			for i := uint64(0); i < 240; i++ {
				RangesNew = append(RangesNew, NewRange(start+i*10_000, start+(i+1)*10_000))
			}
			found = merger.FindMergeRanges(RangesNew, uint64(240*10_000))
			expect = nil
			for i := uint64(0); i < 24; i++ {
				expect = append(expect, NewRange(start+i*snaptype.Erigon2MergeLimit, start+(i+1)*snaptype.Erigon2MergeLimit))
			}

			require.Equal(t, expect.String(), Ranges(found).String())
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
		start := uint64(19_000_000)
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
		start := uint64(19_000_000)
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
	require.NoError(s.OpenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, chainspec.Mainnet.Config, logger)
		merger.DisableFsync()
		s.OpenSegments(snaptype2.BlockSnapshotTypes, false, true)
		Ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.Len(Ranges, 3)
		err := merger.Merge(context.Background(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
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
		Ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.Empty(Ranges)
		err := merger.Merge(context.Background(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
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
	// 	err := merger.Merge(context.Background(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
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
	// 	err := merger.Merge(context.Background(), s, snaptype2.BlockSnapshotTypes, Ranges, s.Dir(), false, nil, nil)
	// 	require.NoError(err)
	// }

	// expectedFileName = snaptype.SegmentFileName(snaptype2.Transactions.Versions().Current, start+600_000, start+700_000, snaptype2.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)
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

func TestRemoveOverlaps(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
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

	require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, false, true))
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

	require.NoError(s.OpenSegments(snaptype2.BlockSnapshotTypes, false, true))
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
	for i, tc := range cases {
		from, to, can := CanRetire(tc.inFrom, tc.inTo, snaptype.Unknown, nil)
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
		require.NotNil(s.visible[snaptype2.Enums.Headers])
		require.Empty(s.visible[snaptype2.Enums.Headers])
		s.Close()

		createFile(step, step*2, snaptype2.Bodies)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()
		require.NotNil(s.visible[snaptype2.Enums.Bodies])
		require.Empty(s.visible[snaptype2.Enums.Bodies])
		s.Close()

		createFile(step, step*2, snaptype2.Headers)
		createFile(step, step*2, snaptype2.Transactions)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible[snaptype2.Enums.Headers])
		s.OpenSegments(snaptype2.BlockSnapshotTypes, false, true)
		// require.Equal(1, len(getSegs(snaptype2.Enums.Headers]))
		s.Close()

		createFile(0, step, snaptype2.Bodies)
		createFile(0, step, snaptype2.Headers)
		createFile(0, step, snaptype2.Transactions)
		s = NewRoSnapshots(cfg, dir, snaptype2.BlockSnapshotTypes, true, logger)
		defer s.Close()

		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible[snaptype2.Enums.Headers])
		require.Len(s.visible[snaptype2.Enums.Headers], 2)

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
		require.NotNil(s.visible[snaptype2.Enums.Headers])
		require.Len(s.visible[snaptype2.Enums.Headers], 2)

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
	require.Equal("domain", f.Type.Name())

	f, e3, ok = snaptype.ParseFileName("", stat("idx/v1-tracesto.40-44.ef"))
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

		require.Len(s.visible[snaptype2.Enums.Headers], 5)
		require.Len(s.visible[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible[snaptype2.Enums.Transactions], 5)

		require.Equal(7, s.dirty[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirty[snaptype2.Enums.Bodies].Len())
		require.Equal(5, s.dirty[snaptype2.Enums.Transactions].Len())
	}

	// gap in transactions: [5*500_000 - 6*500_000]
	{
		createFile(6*500_000, 7*500_000, snaptype2.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Len(s.visible[snaptype2.Enums.Headers], 5)
		require.Len(s.visible[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible[snaptype2.Enums.Transactions], 5)

		require.Equal(7, s.dirty[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirty[snaptype2.Enums.Bodies].Len())
		require.Equal(5, s.dirty[snaptype2.Enums.Transactions].Len())
	}

	// overlap in transactions: [4*500_000 - 4.5*500_000]
	{
		createFile(4*500_000, 4*500_000+250_000, snaptype2.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Len(s.visible[snaptype2.Enums.Headers], 5)
		require.Len(s.visible[snaptype2.Enums.Bodies], 5)
		require.Len(s.visible[snaptype2.Enums.Transactions], 5)

		require.Equal(7, s.dirty[snaptype2.Enums.Headers].Len())
		require.Equal(6, s.dirty[snaptype2.Enums.Bodies].Len())
		require.Equal(5, s.dirty[snaptype2.Enums.Transactions].Len())
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

	require.Len(s.visible[snaptype2.Enums.Headers], 1)
	require.Equal(3, s.dirty[snaptype2.Enums.Headers].Len())
}
