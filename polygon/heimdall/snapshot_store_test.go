package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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
	"github.com/erigontech/erigon/execution/chain/networkname"
)

// Span tests

func TestHeimdallStoreLastFrozenSpanIdWhenSegmentFilesArePresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 5_000, 132, dir, logger)
	createTestSegmentFile(t, 0, 5_000, Enums.Spans, dir, version.V1_0, logger)
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet, NoDownloader: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	err = heimdallStore.Prepare(t.Context())
	require.NoError(t, err)
	lastFrozenSpanId, err := heimdallStore.spans.LastFrozenEntityId()
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastFrozenSpanId)
}

func TestHeimdallStoreLastFrozenSpanIdWhenSegmentFilesAreNotPresent(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet, NoDownloader: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err := borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)

	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	lastFrozenSpanId, err := heimdallStore.spans.LastFrozenEntityId()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastFrozenSpanId)
}
func TestHeimdallStoreLastFrozenSpanIdReturnsLastSegWithIdx(t *testing.T) {
	t.Parallel()

	logger := testlog.Logger(t, log.LvlInfo)
	dir := t.TempDir()
	createTestBorEventSegmentFile(t, 0, 4_000, 132, dir, logger)
	createTestBorEventSegmentFile(t, 4_000, 6_000, 264, dir, logger)
	createTestBorEventSegmentFile(t, 6_000, 10_000, 528, dir, logger)
	createTestSegmentFile(t, 0, 4_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 4_000, 6_000, Enums.Spans, dir, version.V1_0, logger)
	createTestSegmentFile(t, 6_000, 10_000, Enums.Spans, dir, version.V1_0, logger)
	// delete idx file for last bor span segment to simulate segment with missing idx file
	idxFileToDelete := filepath.Join(dir, snaptype.IdxFileName(version.V1_0, 0, 4_000, Spans.Name()))
	err := dir2.RemoveFile(idxFileToDelete)
	require.NoError(t, err)
	borRoSnapshots := NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.BorMainnet, NoDownloader: true}, dir, 0, logger)
	defer borRoSnapshots.Close()
	err = borRoSnapshots.OpenFolder()
	require.NoError(t, err)

	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	heimdallStore := NewSnapshotStore(NewMdbxStore(logger, dataDir, false, 1), borRoSnapshots)
	err = heimdallStore.Prepare(t.Context())
	require.NoError(t, err)
	lastFrozenSpanid, err := heimdallStore.spans.LastFrozenEntityId()
	require.NoError(t, err)
	require.Equal(t, uint64(9), lastFrozenSpanid)
}

func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver version.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	segFileName := filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name))
	c, err := seg.NewCompressor(context.Background(), "test", segFileName, dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	// use from and to to determine which spans go inside this .seg file from the spansForTesting
	// it is not a requirement, but a handy convention for testing purposes
	for i := from / 1000; i < to/1000; i++ {
		span := spanDataForTesting[i]
		buf, err := json.Marshal(span)
		require.NoError(t, err)
		err = c.AddWord(buf)
		require.NoError(t, err)
	}
	err = c.Compress()
	require.NoError(t, err)
	d, err := seg.NewDecompressor(segFileName)
	require.NoError(t, err)
	_ = d
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   c.Count(),
		Enums:      c.Count() > 0,
		BucketSize: recsplit.DefaultBucketSize,
		TmpDir:     dir,
		BaseDataID: 0,
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, name.String())),
		LeafSize:   recsplit.DefaultLeafSize,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	getter := d.MakeGetter()
	//
	var i, offset, nextPos uint64
	var key [8]byte
	for getter.HasNext() {
		nextPos, _ = getter.Skip()
		binary.BigEndian.PutUint64(key[:], i)
		i++
		err = idx.AddKey(key[:], offset)
		require.NoError(t, err)
		offset = nextPos
	}
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

var spanDataForTesting = []Span{
	Span{
		Id:         0,
		StartBlock: 0,
		EndBlock:   999,
	},
	Span{
		Id:         1,
		StartBlock: 1000,
		EndBlock:   1999,
	},
	Span{
		Id:         2,
		StartBlock: 2000,
		EndBlock:   2999,
	},
	Span{
		Id:         3,
		StartBlock: 3000,
		EndBlock:   3999,
	},
	Span{
		Id:         4,
		StartBlock: 4000,
		EndBlock:   4999,
	},
	Span{
		Id:         5,
		StartBlock: 5000,
		EndBlock:   5999,
	},
	Span{
		Id:         6,
		StartBlock: 6000,
		EndBlock:   6999,
	},
	Span{
		Id:         7,
		StartBlock: 7000,
		EndBlock:   7999,
	},
	Span{
		Id:         8,
		StartBlock: 8000,
		EndBlock:   8999,
	},
	Span{
		Id:         9,
		StartBlock: 9000,
		EndBlock:   9999,
	},
}
