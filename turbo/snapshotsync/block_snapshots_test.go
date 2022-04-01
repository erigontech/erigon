package snapshotsync

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/holiman/uint256"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name Type, dir string) {
	c, err := compress.NewCompressor(context.Background(), "test", filepath.Join(dir, SegmentFileName(from, to, name)), dir, 100, 1, log.LvlDebug)
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
		IndexFile:  filepath.Join(dir, IdxFileName(from, to, name.String())),
		LeafSize:   8,
	})
	require.NoError(t, err)
	defer idx.Close()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build()
	require.NoError(t, err)
	if name == Transactions {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, IdxFileName(from, to, Transactions2Block.String())),
			LeafSize:   8,
		})
		require.NoError(t, err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build()
		require.NoError(t, err)
		defer idx.Close()

		idx2, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, IdxFileName(from, to, TransactionsId.String())),
			LeafSize:   8,
		})
		require.NoError(t, err)
		err = idx2.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx2.Build()
		require.NoError(t, err)
		defer idx2.Close()
	}
}

func TestMergeSnapshots(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range AllSnapshotTypes {
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
	require.NoError(s.Reopen())

	{
		merger := NewMerger(dir, 1, log.LvlInfo, uint256.Int{})
		ranges := merger.FindMergeRanges(s)
		require.True(len(ranges) > 0)
		err := merger.Merge(context.Background(), s, ranges, &dir2.Rw{Path: s.Dir()}, false)
		require.NoError(err)
	}

	expectedFileName := SegmentFileName(500_000, 1_000_000, Transactions)
	d, err := compress.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a := d.Count()
	require.Equal(5, a)

	{
		merger := NewMerger(dir, 1, log.LvlInfo, uint256.Int{})
		ranges := merger.FindMergeRanges(s)
		require.True(len(ranges) == 0)
		err := merger.Merge(context.Background(), s, ranges, &dir2.Rw{Path: s.Dir()}, false)
		require.NoError(err)
	}

	expectedFileName = SegmentFileName(1_100_000, 1_200_000, Transactions)
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
func TestRecompress(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) { createTestSegmentFile(t, from, to, Headers, dir) }

	createFile(0, 1_000)
	err := RecompressSegments(context.Background(), &dir2.Rw{Path: dir}, dir)
	require.NoError(err)

	d, err := compress.NewDecompressor(filepath.Join(dir, SegmentFileName(0, 1_000, Headers)))
	require.NoError(err)
	defer d.Close()
	assert.Equal(t, 1, d.Count())
}

func TestOpenAllSnapshot(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	chainSnapshotCfg := snapshothashes.KnownConfig(networkname.MainnetChainName)
	chainSnapshotCfg.ExpectBlocks = math.MaxUint64
	cfg := ethconfig.Snapshot{Enabled: true}
	createFile := func(from, to uint64, name Type) { createTestSegmentFile(t, from, to, name, dir) }
	s := NewRoSnapshots(cfg, dir)
	defer s.Close()
	err := s.Reopen()
	require.NoError(err)
	require.Equal(0, len(s.Headers.segments))
	s.Close()

	createFile(500_000, 1_000_000, Bodies)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	require.Equal(0, len(s.Bodies.segments)) //because, no headers and transactions snapshot files are created
	s.Close()

	createFile(500_000, 1_000_000, Headers)
	createFile(500_000, 1_000_000, Transactions)
	s = NewRoSnapshots(cfg, dir)
	err = s.Reopen()
	require.Error(err)
	require.Equal(0, len(s.Headers.segments)) //because, no gaps are allowed (expect snapshots from block 0)
	s.Close()

	createFile(0, 500_000, Bodies)
	createFile(0, 500_000, Headers)
	createFile(0, 500_000, Transactions)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()

	err = s.Reopen()
	require.NoError(err)
	require.Equal(2, len(s.Headers.segments))

	ok, err := s.ViewTxs(10, func(sn *TxnSegment) error {
		require.Equal(int(sn.To), 500_000)
		return nil
	})
	require.NoError(err)
	require.True(ok)

	ok, err = s.ViewTxs(500_000, func(sn *TxnSegment) error {
		require.Equal(int(sn.To), 1_000_000) // [from:to)
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
	err = s.Reopen()
	require.NoError(err)
	defer s.Close()
	require.Equal(2, len(s.Headers.segments))

	createFile(500_000, 900_000, Headers)
	createFile(500_000, 900_000, Bodies)
	createFile(500_000, 900_000, Transactions)
	chainSnapshotCfg.ExpectBlocks = math.MaxUint64
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	err = s.Reopen()
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
	_, err := ParseFileName("", stat("a"))
	require.Error(err)
	_, err = ParseFileName("", stat("1-a"))
	require.Error(err)
	_, err = ParseFileName("", stat("1-2-a"))
	require.Error(err)
	_, err = ParseFileName("", stat("1-2-bodies.info"))
	require.Error(err)
	_, err = ParseFileName("", stat("1-2-bodies.seg"))
	require.Error(err)
	_, err = ParseFileName("", stat("v2-1-2-bodies.seg"))
	require.Error(err)
	_, err = ParseFileName("", stat("v0-1-2-bodies.seg"))
	require.Error(err)

	f, err := ParseFileName("", stat("v1-1-2-bodies.seg"))
	require.NoError(err)
	require.Equal(f.T, Bodies)
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
}
