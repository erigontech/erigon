package freezeblocks

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"

	"github.com/ledgerwatch/erigon/common/math"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, version snaptype.Version, logger log.Logger) {
	c, err := seg.NewCompressor(context.Background(), "test", filepath.Join(dir, snaptype.SegmentFileName(version, from, to, name)), dir, 100, 1, log.LvlDebug, logger)
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
	s := NewRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer s.Close()
	require.NoError(s.ReopenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
		merger.DisableFsync()
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.True(len(ranges) > 0)
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
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.True(len(ranges) == 0)
		err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, 600_000, 700_000, coresnaptype.Transactions.Enum())
	d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a = d.Count()
	require.Equal(10, a)

	start := uint64(19_000_000)
	for i := uint64(0); i < N; i++ {
		createFile(start+i*10_000, start+(i+1)*10_000)
	}
	s = NewRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, start, logger)
	defer s.Close()
	require.NoError(s.ReopenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
		merger.DisableFsync()
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.True(len(ranges) > 0)
		err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, start+100_000, start+200_000, coresnaptype.Transactions.Enum())
	d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a = d.Count()
	require.Equal(10, a)

	{
		merger := NewMerger(dir, 1, log.LvlInfo, nil, params.MainnetChainConfig, logger)
		merger.DisableFsync()
		ranges := merger.FindMergeRanges(s.Ranges(), s.SegmentsMax())
		require.True(len(ranges) == 0)
		err := merger.Merge(context.Background(), s, coresnaptype.BlockSnapshotTypes, ranges, s.Dir(), false, nil, nil)
		require.NoError(err)
	}

	expectedFileName = snaptype.SegmentFileName(coresnaptype.Transactions.Versions().Current, start+600_000, start+700_000, coresnaptype.Transactions.Enum())
	d, err = seg.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a = d.Count()
	require.Equal(10, a)
}

func TestRemoveOverlaps(t *testing.T) {
	logger := log.New()
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range coresnaptype.BlockSnapshotTypes {
			createTestSegmentFile(t, from, to, snT.Enum(), dir, 1, logger)
		}
	}

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

	s := NewRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)

	defer s.Close()
	require.NoError(s.ReopenFolder())

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

	for _, chain := range []string{networkname.MainnetChainName, networkname.MumbaiChainName} {
		dir := filepath.Join(baseDir, chain)
		chainSnapshotCfg := snapcfg.KnownCfg(chain)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		cfg := ethconfig.BlocksFreezing{Enabled: true}
		createFile := func(from, to uint64, name snaptype.Type) {
			createTestSegmentFile(t, from, to, name.Enum(), dir, 1, logger)
		}
		s := NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		err := s.ReopenFolder()
		require.NoError(err)
		require.NotNil(s.segments.Get(coresnaptype.Enums.Headers))
		getSegs := func(e snaptype.Enum) *segments {
			res, _ := s.segments.Get(e)
			return res
		}
		require.Equal(0, len(getSegs(coresnaptype.Enums.Headers).segments))
		s.Close()

		createFile(500_000, 1_000_000, coresnaptype.Bodies)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		require.NotNil(getSegs(coresnaptype.Enums.Bodies))
		require.Equal(0, len(getSegs(coresnaptype.Enums.Bodies).segments))
		s.Close()

		createFile(500_000, 1_000_000, coresnaptype.Headers)
		createFile(500_000, 1_000_000, coresnaptype.Transactions)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		err = s.ReopenFolder()
		require.NoError(err)
		require.NotNil(getSegs(coresnaptype.Enums.Headers))
		require.Equal(0, len(getSegs(coresnaptype.Enums.Headers).segments))
		s.Close()

		createFile(0, 500_000, coresnaptype.Bodies)
		createFile(0, 500_000, coresnaptype.Headers)
		createFile(0, 500_000, coresnaptype.Transactions)
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()

		err = s.ReopenFolder()
		require.NoError(err)
		require.NotNil(getSegs(coresnaptype.Enums.Headers))
		require.Equal(2, len(getSegs(coresnaptype.Enums.Headers).segments))

		view := s.View()
		defer view.Close()

		seg, ok := view.TxsSegment(10)
		require.True(ok)
		require.Equal(int(seg.to), 500_000)

		seg, ok = view.TxsSegment(500_000)
		require.True(ok)
		require.Equal(int(seg.to), 1_000_000)

		_, ok = view.TxsSegment(1_000_000)
		require.False(ok)

		// Erigon may create new snapshots by itself - with high bigger than hardcoded ExpectedBlocks
		// ExpectedBlocks - says only how much block must come from Torrent
		chainSnapshotCfg.ExpectBlocks = 500_000 - 1
		s = NewRoSnapshots(cfg, dir, 0, logger)
		err = s.ReopenFolder()
		require.NoError(err)
		defer s.Close()
		require.NotNil(getSegs(coresnaptype.Enums.Headers))
		require.Equal(2, len(getSegs(coresnaptype.Enums.Headers).segments))

		createFile(500_000, 900_000, coresnaptype.Headers)
		createFile(500_000, 900_000, coresnaptype.Bodies)
		createFile(500_000, 900_000, coresnaptype.Transactions)
		chainSnapshotCfg.ExpectBlocks = math.MaxUint64
		s = NewRoSnapshots(cfg, dir, 0, logger)
		defer s.Close()
		err = s.ReopenFolder()
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
