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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	coresnaptype "github.com/erigontech/erigon/v3/core/snaptype"
	"github.com/erigontech/erigon/v3/eth/ethconfig"
	"github.com/erigontech/erigon/v3/polygon/bridge"
	"github.com/erigontech/erigon/v3/polygon/heimdall"
	"github.com/erigontech/erigon/v3/turbo/testlog"
)

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:         borRoSnapshots,
		heimdallStore: heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dataDir, 1), borRoSnapshots)}
	require.Equal(t, uint64(78), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:         borRoSnapshots,
		heimdallStore: heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dataDir, 1), borRoSnapshots)}
	require.Equal(t, uint64(0), blockReader.LastFrozenSpanId())
}

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

func TestBlockReaderLastFrozenSpanIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, heimdall.Enums.Spans, dir, 1, logger)
	// delete idx file for last bor span segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, heimdall.Spans.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:         borRoSnapshots,
		heimdallStore: heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dataDir, 1), borRoSnapshots)}
	require.Equal(t, uint64(156), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdReturnsZeroWhenAllSegmentsDoNotHaveIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, heimdall.Enums.Spans, dir, 1, logger)
	// delete idx file for all bor span segments to simulate segments with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1, 500_000, heimdall.Spans.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, heimdall.Spans.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, heimdall.Spans.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:         borRoSnapshots,
		heimdallStore: heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dataDir, 1), borRoSnapshots)}
	require.Equal(t, uint64(0), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:          borRoSnapshots,
		borBridgeStore: bridge.NewSnapshotStore(bridge.NewMdbxStore(dataDir, logger, false, 1), borRoSnapshots, nil)}
	require.Equal(t, uint64(132), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:          borRoSnapshots,
		borBridgeStore: bridge.NewSnapshotStore(bridge.NewMdbxStore(dataDir, logger, false, 1), borRoSnapshots, nil)}
	require.Equal(t, uint64(0), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, heimdall.Enums.Spans, dir, 1, logger)
	// delete idx file for last bor events segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, heimdall.Events.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:          borRoSnapshots,
		borBridgeStore: bridge.NewSnapshotStore(bridge.NewMdbxStore(dataDir, logger, false, 1), borRoSnapshots, nil)}
	require.Equal(t, uint64(264), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdReturnsZeroWhenAllSegmentsDoNotHaveIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 500_000, 1_000_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 1_000_000, 1_500_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 500_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, heimdall.Enums.Spans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, heimdall.Enums.Spans, dir, 1, logger)
	// delete idx files for all bor events segment to simulate segment files with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 0, 500_000, heimdall.Events.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, heimdall.Events.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, heimdall.Events.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := heimdall.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	blockReader := &BlockReader{
		borSn:          borRoSnapshots,
		borBridgeStore: bridge.NewSnapshotStore(bridge.NewMdbxStore(dataDir, logger, false, 1), borRoSnapshots, nil)}
	require.Equal(t, uint64(0), blockReader.LastFrozenEventId())
}

func createTestBorEventSegmentFile(t *testing.T, from, to, eventId uint64, dir string, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	compressor, err := seg.NewCompressor(
		context.Background(),
		"test",
		filepath.Join(dir, snaptype.SegmentFileName(1, from, to, heimdall.Enums.Events)),
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
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(1, from, to, heimdall.Events.Name())),
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
