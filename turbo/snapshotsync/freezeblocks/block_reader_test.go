package freezeblocks

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(78), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for last bor span segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, snaptype.BorSpans.String()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(156), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdReturnsZeroWhenAllSegmentsDoNotHaveIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for all bor span segments to simulate segments with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1, 500_000, snaptype.BorSpans.String()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, snaptype.BorSpans.String()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, snaptype.BorSpans.String()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(132), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for last bor events segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, snaptype.BorEvents.String()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(264), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdReturnsZeroWhenAllSegmentsDoNotHaveIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, snaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, snaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx files for all bor events segment to simulate segment files with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 0, 500_000, snaptype.BorEvents.String()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, snaptype.BorEvents.String()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, snaptype.BorEvents.String()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{Enabled: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.ReopenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenEventId())
}

func createTestBorEventSegmentFile(t *testing.T, from, to, eventId uint64, dir string, logger log.Logger) {
	compressor, err := compress.NewCompressor(
		context.Background(),
		"test",
		filepath.Join(dir, snaptype.SegmentFileName(1, from, to, snaptype.Enums.BorEvents)),
		dir,
		100,
		1,
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
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(1, from, to, snaptype.BorEvents.String())),
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
