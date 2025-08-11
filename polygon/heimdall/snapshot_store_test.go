package heimdall

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/chain/networkname"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon-lib/version"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/eth/ethconfig"

	"github.com/stretchr/testify/require"
)

// Span tests

func TestHeimdallStoreLastFrozenSpanIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, Enums.Spans, dir, version.V1_0, logger)
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	require.Equal(t, uint64(78), heimdallStore.spans.LastFrozenEntityId())
}

func TestHeimdallStoreLastFrozenSpanIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	require.Equal(t, uint64(0), heimdallStore.spans.LastFrozenEntityId())
}
func TestHeimdallStoreLastFrozenSpanIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, Enums.Spans, dir, version.V1_0, logger)
	// delete idx file for last bor span segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 1_000_000, 1_500_000, Spans.Name()))
	err := dir2.RemoveFile(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	require.Equal(t, uint64(156), heimdallStore.spans.LastFrozenEntityId())
}

func TestBlockReaderLastFrozenSpanIdReturnsZeroWhenAllSegmentsDoNotHaveIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, Enums.Spans, dir, version.V1_0, logger)
	// delete idx file for all bor span segments to simulate segments with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 1, 500_000, Spans.Name()))
	err := dir2.RemoveFile(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 500_000, 1_000_000, Spans.Name()))
	err = dir2.RemoveFile(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 1_000_000, 1_500_000, Spans.Name()))
	err = dir2.RemoveFile(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	require.Equal(t, uint64(0), heimdallStore.spans.LastFrozenEntityId())
}

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver version.Version, logger log.Logger) {
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

func createTestBorEventSegmentFile(t *testing.T, from, to, eventId uint64, dir string, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	compressor, err := seg.NewCompressor(
		context.Background(),
		"test",
		filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, from, to, Enums.Events)),
		dir,
		compressCfg,
		log.LvlDebug,
		logger,
	)
	require.NoError(t, err)
	defer compressor.Close()
	compressor.DisableFsync()
	data := make([]byte, length.Hash+length.BlockNum+8)
	binary.BigEndian.PutUint64(data[length.Hash+length.BlockNum:length.Hash+length.BlockNum+8], eventId)
	err = compressor.AddWord(data)
	require.NoError(t, err)
	err = compressor.Compress()
	require.NoError(t, err)
	idx, err := recsplit.NewRecSplit(
		recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, Events.Name())),
			LeafSize:   8,
		},
		logger,
	)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(context.Background())
	require.NoError(t, err)
}
