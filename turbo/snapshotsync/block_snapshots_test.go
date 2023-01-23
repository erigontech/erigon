package snapshotsync

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Type, dir string) {
	c, err := compress.NewCompressor(context.Background(), "test", filepath.Join(dir, snaptype.SegmentFileName(from, to, name)), dir, 100, 1, log.LvlDebug)
	require.NoError(t, err)
	defer c.Close()
	err = c.AddWord([]byte{1})
	require.NoError(t, err)
	err = c.Compress()
	require.NoError(t, err)
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(from, to, name.String())),
		LeafSize:   8,
	})
	require.NoError(t, err)
	defer idx.Close()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build()
	require.NoError(t, err)
	if name == snaptype.Transactions {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(from, to, snaptype.Transactions2Block.String())),
			LeafSize:   8,
		})
		require.NoError(t, err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build()
		require.NoError(t, err)
		defer idx.Close()
	}
}

func TestMergeSnapshots(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range snaptype.AllSnapshotTypes {
			createTestSegmentFile(t, from, to, snT, dir)
		}
	}

	N := uint64(7)
	createFile(0, 500_000)
	for i := uint64(500_000); i < 500_000+N*100_000; i += 100_000 {
		createFile(i, i+100_000)
	}
	cfg := ethconfig.Snapshot{Enabled: true}
	s := NewRoSnapshots(cfg, dir)
	defer s.Close()
	require.NoError(s.ReopenFolder())
	{
		merger := NewMerger(dir, 1, log.LvlInfo, uint256.Int{}, nil)
		ranges := merger.FindMergeRanges(s.Ranges())
		require.True(len(ranges) > 0)
		err := merger.Merge(context.Background(), s, ranges, s.Dir(), false)
		require.NoError(err)
	}

	expectedFileName := snaptype.SegmentFileName(500_000, 1_000_000, snaptype.Transactions)
	d, err := compress.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a := d.Count()
	require.Equal(5, a)

	{
		merger := NewMerger(dir, 1, log.LvlInfo, uint256.Int{}, nil)
		ranges := merger.FindMergeRanges(s.Ranges())
		require.True(len(ranges) == 0)
		err := merger.Merge(context.Background(), s, ranges, s.Dir(), false)
		require.NoError(err)
	}

	expectedFileName = snaptype.SegmentFileName(1_100_000, 1_200_000, snaptype.Transactions)
	d, err = compress.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a = d.Count()
	require.Equal(1, a)
}

func TestCanRetire(t *testing.T) {
	require := require.New(t)
	cases := []struct {
		inFrom, inTo, outFrom, outTo uint64
		can                          bool
	}{
		{0, 1234, 0, 1000, true},
		{1_000_000, 1_120_000, 1_000_000, 1_100_000, true},
		{2_500_000, 4_100_000, 2_500_000, 3_000_000, true},
		{2_500_000, 2_500_100, 2_500_000, 2_500_000, false},
		{1_001_000, 2_000_000, 1_001_000, 1_002_000, true},
	}
	for _, tc := range cases {
		from, to, can := canRetire(tc.inFrom, tc.inTo)
		require.Equal(int(tc.outFrom), int(from))
		require.Equal(int(tc.outTo), int(to))
		require.Equal(tc.can, can, tc.inFrom, tc.inTo)
	}
}
func TestOpenAllSnapshot(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	chainSnapshotCfg := snapcfg.KnownCfg(networkname.MainnetChainName, nil, nil)
	chainSnapshotCfg.ExpectBlocks = math.MaxUint64
	cfg := ethconfig.Snapshot{Enabled: true}
	createFile := func(from, to uint64, name snaptype.Type) { createTestSegmentFile(t, from, to, name, dir) }
	s := NewRoSnapshots(cfg, dir)
	defer s.Close()
	err := s.ReopenFolder()
	require.NoError(err)
	require.Equal(0, len(s.Headers.segments))
	s.Close()

	createFile(500_000, 1_000_000, snaptype.Bodies)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	require.Equal(0, len(s.Bodies.segments)) //because, no headers and transactions snapshot files are created
	s.Close()

	createFile(500_000, 1_000_000, snaptype.Headers)
	createFile(500_000, 1_000_000, snaptype.Transactions)
	s = NewRoSnapshots(cfg, dir)
	err = s.ReopenFolder()
	require.NoError(err)
	require.Equal(0, len(s.Headers.segments))
	s.Close()

	createFile(0, 500_000, snaptype.Bodies)
	createFile(0, 500_000, snaptype.Headers)
	createFile(0, 500_000, snaptype.Transactions)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()

	err = s.ReopenFolder()
	require.NoError(err)
	require.Equal(2, len(s.Headers.segments))

	ok, err := s.ViewTxs(10, func(sn *TxnSegment) error {
		require.Equal(int(sn.ranges.to), 500_000)
		return nil
	})
	require.NoError(err)
	require.True(ok)

	ok, err = s.ViewTxs(500_000, func(sn *TxnSegment) error {
		require.Equal(int(sn.ranges.to), 1_000_000) // [from:to)
		return nil
	})
	require.NoError(err)
	require.True(ok)

	ok, err = s.ViewTxs(1_000_000, func(sn *TxnSegment) error {
		return nil
	})
	require.NoError(err)
	require.False(ok)

	// Erigon may create new snapshots by itself - with high bigger than hardcoded ExpectedBlocks
	// ExpectedBlocks - says only how much block must come from Torrent
	chainSnapshotCfg.ExpectBlocks = 500_000 - 1
	s = NewRoSnapshots(cfg, dir)
	err = s.ReopenFolder()
	require.NoError(err)
	defer s.Close()
	require.Equal(2, len(s.Headers.segments))

	createFile(500_000, 900_000, snaptype.Headers)
	createFile(500_000, 900_000, snaptype.Bodies)
	createFile(500_000, 900_000, snaptype.Transactions)
	chainSnapshotCfg.ExpectBlocks = math.MaxUint64
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	err = s.ReopenFolder()
	require.NoError(err)
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
	_, err := snaptype.ParseFileName("", stat("a"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("1-a"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("1-2-a"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("1-2-bodies.info"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("1-2-bodies.seg"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("v2-1-2-bodies.seg"))
	require.Error(err)
	_, err = snaptype.ParseFileName("", stat("v0-1-2-bodies.seg"))
	require.Error(err)

	f, err := snaptype.ParseFileName("", stat("v1-1-2-bodies.seg"))
	require.NoError(err)
	require.Equal(f.T, snaptype.Bodies)
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
}
