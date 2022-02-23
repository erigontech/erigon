package snapshotsync

import (
	"context"
	"path/filepath"
	"testing"

	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestSegmentFile(t *testing.T, from, to uint64, name FType, dir string) {
	c, err := compress.NewCompressor(context.Background(), "test", filepath.Join(dir, SegmentFileName(from, to, name)), dir, 100, 1)
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
		IndexFile:  filepath.Join(dir, IdxFileName(from, to, name)),
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
			IndexFile:  filepath.Join(dir, IdxFileName(from, to, Transactions2Block)),
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

func TestMerge(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	createFile := func(from, to uint64) {
		for _, snT := range AllSnapshotTypes {
			createTestSegmentFile(t, from, to, snT, dir)
		}
	}

	N := uint64(15)
	createFile(0, 500_000)
	for i := uint64(500_000); i < 500_000+N*50_000; i += 50_000 {
		createFile(i, i+50_000)
	}
	cfg := ethconfig.Snapshot{Enabled: true}
	s := NewRoSnapshots(cfg, dir)
	defer s.Close()

	require.NoError(s.ReopenSegments())
	err := findAndMergeBlockSegments(context.Background(), s, dir)
	require.NoError(err)
	require.NoError(s.ReopenSegments())

	expectedFileName := SegmentFileName(500_000, 1_000_000, Transactions)
	d, err := compress.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a := d.Count()
	require.Equal(10, a)

	err = findAndMergeBlockSegments(context.Background(), s, dir)
	require.NoError(err)
	require.NoError(s.ReopenSegments())

	expectedFileName = SegmentFileName(1_000_000, 1_250_000, Transactions)
	d, err = compress.NewDecompressor(filepath.Join(dir, expectedFileName))
	require.NoError(err)
	defer d.Close()
	a = d.Count()
	require.Equal(5, a)
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
	createFile := func(from, to uint64, name FType) { createTestSegmentFile(t, from, to, name, dir) }
	s := NewRoSnapshots(cfg, dir)
	defer s.Close()
	err := s.ReopenSegments()
	require.NoError(err)
	require.Equal(0, len(s.blocks))
	s.Close()

	createFile(500_000, 1_000_000, Bodies)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	require.Equal(0, len(s.blocks)) //because, no headers and transactions snapshot files are created
	s.Close()

	createFile(500_000, 1_000_000, Headers)
	createFile(500_000, 1_000_000, Transactions)
	s = NewRoSnapshots(cfg, dir)
	err = s.ReopenSegments()
	require.Error(err)
	require.Equal(0, len(s.blocks)) //because, no gaps are allowed (expect snapshots from block 0)
	s.Close()

	createFile(0, 500_000, Bodies)
	createFile(0, 500_000, Headers)
	createFile(0, 500_000, Transactions)
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()

	err = s.ReopenSegments()
	require.NoError(err)
	s.indicesReady.Store(true)
	require.Equal(2, len(s.blocks))

	sn, ok := s.Blocks(10)
	require.True(ok)
	require.Equal(int(sn.To), 500_000)

	sn, ok = s.Blocks(500_000)
	require.True(ok)
	require.Equal(int(sn.To), 1_000_000) // [from:to)

	_, ok = s.Blocks(1_000_000)
	require.False(ok)

	// Erigon may create new snapshots by itself - with high bigger than hardcoded ExpectedBlocks
	// ExpectedBlocks - says only how much block must come from Torrent
	chainSnapshotCfg.ExpectBlocks = 500_000 - 1
	s = NewRoSnapshots(cfg, dir)
	err = s.ReopenSegments()
	require.NoError(err)
	defer s.Close()
	require.Equal(2, len(s.blocks))

	createFile(500_000, 900_000, Headers)
	createFile(500_000, 900_000, Bodies)
	createFile(500_000, 900_000, Transactions)
	chainSnapshotCfg.ExpectBlocks = math.MaxUint64
	s = NewRoSnapshots(cfg, dir)
	defer s.Close()
	err = s.ReopenSegments()
	require.NoError(err)
}

func TestParseCompressedFileName(t *testing.T) {
	require := require.New(t)
	_, err := ParseFileName("a")
	require.Error(err)
	_, err = ParseFileName("1-a")
	require.Error(err)
	_, err = ParseFileName("1-2-a")
	require.Error(err)
	_, err = ParseFileName("1-2-bodies.info")
	require.Error(err)
	_, err = ParseFileName("1-2-bodies.idx")
	require.Error(err)
	_, err = ParseFileName("1-2-bodies.seg")
	require.Error(err)
	_, err = ParseFileName("v2-1-2-bodies.seg")
	require.Error(err)
	_, err = ParseFileName("v0-1-2-bodies.seg")
	require.Error(err)

	f, err := ParseFileName("v1-1-2-bodies.seg")
	require.NoError(err)
	require.Equal(f.T, Bodies)
	require.Equal(1_000, int(f.From))
	require.Equal(2_000, int(f.To))
}
