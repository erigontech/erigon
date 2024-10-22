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
	"github.com/erigontech/erigon/eth/ethconfig"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(78), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenSpanIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
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
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for last bor span segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, borsnaptype.BorSpans.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
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
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for all bor span segments to simulate segments with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1, 500_000, borsnaptype.BorSpans.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, borsnaptype.BorSpans.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, borsnaptype.BorSpans.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenSpanId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 500_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(132), blockReader.LastFrozenEventId())
}

func TestBlockReaderLastFrozenEventIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
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
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx file for last bor events segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, borsnaptype.BorEvents.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
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
	createTestSegmentFile(t, 0, 500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 500_000, 1_000_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	createTestSegmentFile(t, 1_000_000, 1_500_000, borsnaptype.Enums.BorSpans, dir, 1, logger)
	// delete idx files for all bor events segment to simulate segment files with missing idx files
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(1, 0, 500_000, borsnaptype.BorEvents.Name()))
	err := os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 500_000, 1_000_000, borsnaptype.BorEvents.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	idxFileToDelete = filepath.Join(dir, snaptype.IdxFileName(1, 1_000_000, 1_500_000, borsnaptype.BorEvents.Name()))
	err = os.Remove(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewBorRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnetChainName}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	blockReader := &BlockReader{borSn: borRoSnapshots}
	require.Equal(t, uint64(0), blockReader.LastFrozenEventId())
}

func createTestBorEventSegmentFile(t *testing.T, from, to, eventId uint64, dir string, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	compressor, err := seg.NewCompressor(
		context.Background(),
		"test",
		filepath.Join(dir, snaptype.SegmentFileName(1, from, to, borsnaptype.Enums.BorEvents)),
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
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(1, from, to, borsnaptype.BorEvents.Name())),
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
