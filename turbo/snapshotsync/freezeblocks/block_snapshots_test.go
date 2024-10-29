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

package freezeblocks

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"

	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/testlog"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, version snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(context.Background(), "test", filepath.Join(dir, snaptype.SegmentFileName(version, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
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
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(1, from, to, name.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(context.Background())
	require.NoError(t, err)
	if name == coresnaptype.Transactions.Enum() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(1, from, to, coresnaptype.Indexes.TxnHash2BlockNum.Name)),
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
	merger := NewMerger("x", 1, log.LvlInfo, nil, params.MainnetChainConfig, nil)
	merger.DisableFsync()
	t.Run("big", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var rangesOld []Range
			for i := 0; i < 24; i++ {
				rangesOld = append(rangesOld, Range{from: uint64(i * 100_000), to: uint64((i + 1) * 100_000)})
			}
			found := merger.FindMergeRanges(rangesOld, uint64(24*100_000))

			expect := Ranges{{0, 500000}, {500000, 1000000}, {1000000, 1500000}, {1500000, 2000000}}
			require.Equal(t, expect.String(), Ranges(found).String())

			var rangesNew []Range
			start := uint64(19_000_000)
			for i := uint64(0); i < 24; i++ {
				rangesNew = append(rangesNew, Range{from: start + (i * 100_000), to: start + ((i + 1) * 100_000)})
			}
			found = merger.FindMergeRanges(rangesNew, uint64(24*100_000))

			expect = Ranges{}
			require.Equal(t, expect.String(), Ranges(found).String())
		}
	})

	t.Run("small", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var rangesOld Ranges
			for i := uint64(0); i < 240; i++ {
				rangesOld = append(rangesOld, Range{from: i * 10_000, to: (i + 1) * 10_000})
			}
			found := merger.FindMergeRanges(rangesOld, uint64(240*10_000))
			var expect Ranges
			for i := uint64(0); i < 4; i++ {
				expect = append(expect, Range{from: i * snaptype.Erigon2OldMergeLimit, to: (i + 1) * snaptype.Erigon2OldMergeLimit})
			}
			for i := uint64(0); i < 4; i++ {
				expect = append(expect, Range{from: 2_000_000 + i*snaptype.Erigon2MergeLimit, to: 2_000_000 + (i+1)*snaptype.Erigon2MergeLimit})
			}

			require.Equal(t, expect.String(), Ranges(found).String())

			var rangesNew Ranges
			start := uint64(19_000_000)
			for i := uint64(0); i < 240; i++ {
				rangesNew = append(rangesNew, Range{from: start + i*10_000, to: start + (i+1)*10_000})
			}
			found = merger.FindMergeRanges(rangesNew, uint64(240*10_000))
			expect = nil
			for i := uint64(0); i < 24; i++ {
				expect = append(expect, Range{from: start + i*snaptype.Erigon2MergeLimit, to: start + (i+1)*snaptype.Erigon2MergeLimit})
			}

			require.Equal(t, expect.String(), Ranges(found).String())
		}

	})

}

func TestFindMergeRange(t *testing.T) {
	merger := NewMerger("x", 1, log.LvlInfo, nil, params.MainnetChainConfig, nil)
	merger.DisableFsync()
	t.Run("big", func(t *testing.T) {
		var rangesOld []Range
		for i := 0; i < 24; i++ {
			rangesOld = append(rangesOld, Range{from: uint64(i * 100_000), to: uint64((i + 1) * 100_000)})
		}
		found := merger.FindMergeRanges(rangesOld, uint64(24*100_000))

		expect := Ranges{{0, 500000}, {500000, 1000000}, {1000000, 1500000}, {1500000, 2000000}}
		require.Equal(t, expect.String(), Ranges(found).String())

		var rangesNew []Range
		start := uint64(19_000_000)
		for i := uint64(0); i < 24; i++ {
			rangesNew = append(rangesNew, Range{from: start + (i * 100_000), to: start + ((i + 1) * 100_000)})
		}
		found = merger.FindMergeRanges(rangesNew, uint64(24*100_000))

		expect = Ranges{}
		require.Equal(t, expect.String(), Ranges(found).String())
	})

	t.Run("small", func(t *testing.T) {
		var rangesOld Ranges
		for i := uint64(0); i < 240; i++ {
			rangesOld = append(rangesOld, Range{from: i * 10_000, to: (i + 1) * 10_000})
		}
		found := merger.FindMergeRanges(rangesOld, uint64(240*10_000))
		var expect Ranges
		for i := uint64(0); i < 4; i++ {
			expect = append(expect, Range{from: i * snaptype.Erigon2OldMergeLimit, to: (i + 1) * snaptype.Erigon2OldMergeLimit})
		}
		for i := uint64(0); i < 4; i++ {
			expect = append(expect, Range{from: 2_000_000 + i*snaptype.Erigon2MergeLimit, to: 2_000_000 + (i+1)*snaptype.Erigon2MergeLimit})
		}

		require.Equal(t, expect.String(), Ranges(found).String())

		var rangesNew Ranges
		start := uint64(19_000_000)
		for i := uint64(0); i < 240; i++ {
			rangesNew = append(rangesNew, Range{from: start + i*10_000, to: start + (i+1)*10_000})
		}
		found = merger.FindMergeRanges(rangesNew, uint64(240*10_000))
		expect = nil
		for i := uint64(0); i < 24; i++ {
			expect = append(expect, Range{from: start + i*snaptype.Erigon2MergeLimit, to: start + (i+1)*snaptype.Erigon2MergeLimit})
		}

		require.Equal(t, expect.String(), Ranges(found).String())
	})

}

func TestMergeSnapshots(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range coresnaptype.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, 1, logger)
		}
	}

	N := uint64(70)

	for i := uint64(0); i < N; i++ {
		createFile(i*10_000, (i+1)*10_000)
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, 0, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
		merger.DisableFsync()
		s.OpenSegments(coresnaptype.BlockSnapshotTypes, false)
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.Equal(3, len(ranges))
		err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	expectedFileName := snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, 0, 500_000, coresnaptype.Transactions.Enum())
	d, err := seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a := d.Count()
	require.Equal(50, a)

	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
		merger.DisableFsync()
		s.OpenFolder()
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.Equal(0, len(ranges))
		err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	// [0; N] merges are not supported anymore

	// expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, 600_000, 700_000, coresnaptype.Transactions.Enum())
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
	// 	merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
	// 	merger.DisableFsync()
	// 	fmt.Println(s.Ranges(), s.SegmentsMax())
	// 	fmt.Println(s.Ranges(), s.SegmentsMax())
	// 	ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
	// 	require.True(len(ranges) > 0)
	// 	err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
	// 	require.NoError(err)
	// }

	// expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, start+100_000, start+200_000, coresnaptype.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)

	// {
	// 	merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
	// 	merger.DisableFsync()
	// 	s.OpenSegments(coresnaptype.BlockSnapshotTypes, false)
	// 	ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
	// 	require.True(len(ranges) == 0)
	// 	err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
	// 	require.NoError(err)
	// }

	// expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, start+600_000, start+700_000, coresnaptype.Transactions.Enum())
	// d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	// require.NoError(err)
	// defer d.Close()
	// a = d.Count()
	// require.Equal(10, a)
}

func TestDeleteSnapshots(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range coresnaptype.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, 1, logger)
		}
	}

	N := uint64(70)

	for i := uint64(0); i < N; i++ {
		createFile(i*10_000, (i+1)*10_000)
	}
	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, 0, logger)
	defer s.Close()
	retireFiles := []string{
		"v1-000000-000010-bodies.seg",
		"v1-000000-000010-headers.seg",
		"v1-000000-000010-transactions.seg",
	}
	require.NoError(s.OpenFolder())
	for _, f := range retireFiles {
		require.NoError(s.Delete(f))
		require.False(slices.Contains(s.Files(), f))
	}
}

func TestRemoveOverlaps(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range coresnaptype.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, 1, logger)
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

	s := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, 0, logger)

	defer s.Close()
	require.NoError(s.OpenSegments(coresnaptype.BlockSnapshotTypes, false))

	list, err := snaptype.Segments(s.dir)
	require.NoError(err)
	require.Equal(45, len(list))

	s.removeOverlapsAfterMerge()

	list, err = snaptype.Segments(s.dir)
	require.NoError(err)

	require.Equal(15, len(list))

	for i, info := range list {
		if i%5 < 2 {
			require.Equal(100_000, int(info.Len()))
		} else {
			require.Equal(10_000, int(info.Len()))
		}
	}
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
		from, to, can := canRetire(tc.inFrom, tc.inTo, snaptype.Unknown, nil)
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
		chainSnapshotCfg := snapcfg.KnownCfg(chain)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		cfg := ethconfig.BlocksFreezing{ChainName: chain}
		createFile := func(from, to uint64, name snaptype.Type) {
			createTestSegmentFile(t, from, to, name.Enum(), dir, 1, logger)
		}
		s := NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		err := s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible[coresnaptype.Enums.Headers])
		require.Equal(0, len(s.visible[coresnaptype.Enums.Headers]))
		s.Close()

		createFile(step, step*2, coresnaptype.Bodies)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		require.NotNil(s.visible[coresnaptype.Enums.Bodies])
		require.Equal(0, len(s.visible[coresnaptype.Enums.Bodies]))
		s.Close()

		createFile(step, step*2, coresnaptype.Headers)
		createFile(step, step*2, coresnaptype.Transactions)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible[coresnaptype.Enums.Headers])
		s.OpenSegments(coresnaptype.BlockSnapshotTypes, false)
		// require.Equal(1, len(getSegs(coresnaptype.Enums.Headers]))
		s.Close()

		createFile(0, step, coresnaptype.Bodies)
		createFile(0, step, coresnaptype.Headers)
		createFile(0, step, coresnaptype.Transactions)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()

		err = s.OpenFolder()
		require.NoError(err)
		require.NotNil(s.visible[coresnaptype.Enums.Headers])
		require.Equal(2, len(s.visible[coresnaptype.Enums.Headers]))

		view := s.View()
		defer view.Close()

		seg, ok := view.TxsSegment(10)
		require.True(ok)
		require.Equal(seg.to, step)

		seg, ok = view.TxsSegment(step)
		require.True(ok)
		require.Equal(seg.to, step*2)

		_, ok = view.TxsSegment(step * 2)
		require.False(ok)

		// Erigon may create new snapshots by itself - with high bigger than hardcoded ExpectedBlocks
		// ExpectedBlocks - says only how much block must come from Torrent
		chainSnapshotCfg.ExpectBlocks = 500_000 - 1
		s = NewRoSnapshots(cfg, dir, 0, logger)
		err = s.OpenFolder()
		require.NoError(err)
		defer s.Close()
		require.NotNil(s.visible[coresnaptype.Enums.Headers])
		require.Equal(2, len(s.visible[coresnaptype.Enums.Headers]))

		createFile(step, step*2-step/5, coresnaptype.Headers)
		createFile(step, step*2-step/5, coresnaptype.Bodies)
		createFile(step, step*2-step/5, coresnaptype.Transactions)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		err = s.OpenFolder()
		require.NoError(err)
	}
}

func TestParseCompressedFileName(t *testing.T) {
	require := require.New(t)
	fs := fstest.MapFS{
		"a":                 &fstest.MapFile{},
		"1-a":               &fstest.MapFile{},
		"1-2-a":             &fstest.MapFile{},
		"1-2-bodies.info":   &fstest.MapFile{},
		"1-2-bodies.seg":    &fstest.MapFile{},
		"v2-1-2-bodies.seg": &fstest.MapFile{},
		"v0-1-2-bodies.seg": &fstest.MapFile{},
		"v1-1-2-bodies.seg": &fstest.MapFile{},
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
	require.Equal(f.Type.Enum(), coresnaptype.Bodies.Enum())
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
}

func TestCalculateVisibleSegments(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, 1, logger)
	}

	for i := uint64(0); i < 7; i++ {
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Headers)
	}
	for i := uint64(0); i < 6; i++ {
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Bodies)
	}
	for i := uint64(0); i < 5; i++ {
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Transactions)
	}
	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, 0, logger)
	defer s.Close()

	{
		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Equal(5, len(s.visible[coresnaptype.Enums.Headers]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Bodies]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Transactions]))

		require.Equal(7, s.dirty[coresnaptype.Enums.Headers].Len())
		require.Equal(6, s.dirty[coresnaptype.Enums.Bodies].Len())
		require.Equal(5, s.dirty[coresnaptype.Enums.Transactions].Len())
	}

	// gap in transactions: [5*500_000 - 6*500_000]
	{
		createFile(6*500_000, 7*500_000, coresnaptype.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Equal(5, len(s.visible[coresnaptype.Enums.Headers]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Bodies]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Transactions]))

		require.Equal(7, s.dirty[coresnaptype.Enums.Headers].Len())
		require.Equal(6, s.dirty[coresnaptype.Enums.Bodies].Len())
		require.Equal(5, s.dirty[coresnaptype.Enums.Transactions].Len())
	}

	// overlap in transactions: [4*500_000 - 4.5*500_000]
	{
		createFile(4*500_000, 4*500_000+250_000, coresnaptype.Transactions)

		require.NoError(s.OpenFolder())
		idx := s.idxAvailability()
		require.Equal(2_500_000-1, int(idx))

		require.Equal(5, len(s.visible[coresnaptype.Enums.Headers]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Bodies]))
		require.Equal(5, len(s.visible[coresnaptype.Enums.Transactions]))

		require.Equal(7, s.dirty[coresnaptype.Enums.Headers].Len())
		require.Equal(6, s.dirty[coresnaptype.Enums.Bodies].Len())
		require.Equal(5, s.dirty[coresnaptype.Enums.Transactions].Len())
	}
}

func TestCalculateVisibleSegmentsWhenGapsInIdx(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64, name snaptype.Type) {
		createTestSegmentFile(t, from, to, name.Enum(), dir, 1, logger)
	}

	for i := uint64(0); i < 3; i++ {
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Headers)
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Bodies)
		createFile(i*500_000, (i+1)*500_000, coresnaptype.Transactions)
	}

	missingIdxFile := filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, coresnaptype.Headers.Name()))
	err := os.Remove(missingIdxFile)
	require.NoError(err)

	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}
	s := NewRoSnapshots(cfg, dir, 0, logger)
	defer s.Close()

	require.NoError(s.OpenFolder())
	idx := s.idxAvailability()
	require.Equal(500_000-1, int(idx))

	require.Equal(1, len(s.visible[coresnaptype.Enums.Headers]))
	require.Equal(3, s.dirty[coresnaptype.Enums.Headers].Len())
}
