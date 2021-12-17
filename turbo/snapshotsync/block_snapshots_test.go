package snapshotsync

import (
	"path"
	"testing"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/stretchr/testify/require"
)

func TestOpenAllSnapshot(t *testing.T) {
	dir, require := t.TempDir(), require.New(t)
	cfg := snapshothashes.KnownConfig(networkname.MainnetChainName)
	cfg.ExpectBlocks = math.MaxUint64
	createFile := func(from, to uint64, name SnapshotType) {
		c, err := compress.NewCompressor("test", path.Join(dir, SegmentFileName(from, to, name)), dir, 100)
		require.NoError(err)
		defer c.Close()
		err = c.AddWord([]byte{1})
		require.NoError(err)
		err = c.Compress()
		require.NoError(err)
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  path.Join(dir, IdxFileName(from, to, name)),
			LeafSize:   8,
		})
		require.NoError(err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(err)
		err = idx.Build()
		require.NoError(err)
	}
	s := NewAllSnapshots(dir, cfg)
	err := s.ReopenSegments()
	require.NoError(err)
	require.Equal(0, len(s.blocks))
	s.Close()

	createFile(500_000, 1_000_000, Bodies)
	s = NewAllSnapshots(dir, cfg)
	require.Equal(0, len(s.blocks)) //because, no headers and transactions snapshot files are created
	s.Close()

	createFile(500_000, 1_000_000, Headers)
	createFile(500_000, 1_000_000, Transactions)
	s = NewAllSnapshots(dir, cfg)
	err = s.ReopenSegments()
	require.NoError(err)
	require.Equal(0, len(s.blocks)) //because, no gaps are allowed (expect snapshots from block 0)
	s.Close()

	createFile(0, 500_000, Bodies)
	createFile(0, 500_000, Headers)
	createFile(0, 500_000, Transactions)
	s = NewAllSnapshots(dir, cfg)
	err = s.ReopenSegments()
	require.NoError(err)
	defer s.Close()
	require.Equal(2, len(s.blocks))

	sn, ok := s.Blocks(10)
	require.True(ok)
	require.Equal(int(sn.To), 500_000)

	sn, ok = s.Blocks(500_000)
	require.True(ok)
	require.Equal(int(sn.To), 1_000_000) // [from:to)

	_, ok = s.Blocks(1_000_000)
	require.False(ok)

	// user must be able to limit amount of blocks which read from snapshot
	cfg.ExpectBlocks = 500_000 - 1
	s = NewAllSnapshots(dir, cfg)
	err = s.ReopenSegments()
	require.NoError(err)
	defer s.Close()
	require.Equal(1, len(s.blocks))
}

func TestParseCompressedFileName(t *testing.T) {
	require := require.New(t)
	_, _, _, err := ParseFileName("a", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("1-a", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("1-2-a", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("1-2-bodies.info", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("1-2-bodies.idx", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("1-2-bodies.seg", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("v2-1-2-bodies.seg", ".seg")
	require.Error(err)
	_, _, _, err = ParseFileName("v0-1-2-bodies.seg", ".seg")
	require.Error(err)

	from, to, tt, err := ParseFileName("v1-1-2-bodies.seg", ".seg")
	require.NoError(err)
	require.Equal(tt, Bodies)
	require.Equal(1_000, int(from))
	require.Equal(2_000, int(to))
}
