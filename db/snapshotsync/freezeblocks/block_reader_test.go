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
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// createTestSegmentFile creates a minimal snapshot segment file for testing
func createTestSegmentFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
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
		IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, name.String())),
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	err = idx.AddKey([]byte{1}, 0)
	require.NoError(t, err)
	err = idx.Build(t.Context())
	require.NoError(t, err)
	if name == snaptype2.Transactions.Enum() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(ver, from, to, snaptype2.Indexes.TxnHash2BlockNum.Name)),
			LeafSize:   8,
		}, logger)
		require.NoError(t, err)
		err = idx.AddKey([]byte{1}, 0)
		require.NoError(t, err)
		err = idx.Build(t.Context())
		require.NoError(t, err)
		defer idx.Close()
	}
}

func createTestSegmentOnlyFile(t *testing.T, from, to uint64, name snaptype.Enum, dir string, ver snaptype.Version, logger log.Logger) {
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)), dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())
}

func requireSegmentFilesExist(t *testing.T, dir string, ver snaptype.Version, from, to uint64, names ...snaptype.Enum) {
	t.Helper()
	for _, name := range names {
		_, err := os.Stat(filepath.Join(dir, snaptype.SegmentFileName(ver, from, to, name)))
		require.NoError(t, err)
	}
}

// TestBlockRetireSkipsOnGap verifies that the block retirement
// logic correctly prevents freezing when there is a gap between the last block available
// in the snapshots and the first block still present in the database. If this gap exists,
// we cannot retire blocks because the history is not contiguous.
func TestBlockRetireSkipsOnGap(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()

	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(999), snapshots.SegmentsMax())

	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	genesisHeader := &types.Header{}
	require.NoError(t, rawdb.WriteHeader(rwTx, genesisHeader))
	prunedBoundaryHeader := &types.Header{Number: *uint256.NewInt(1001)}
	require.NoError(t, rawdb.WriteHeader(rwTx, prunedBoundaryHeader))
	require.NoError(t, rwTx.Commit())

	blockReader := NewBlockReader(snapshots)
	br := &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}

	hasEnough, err := br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.False(t, hasEnough)
}

// TestBlockRetireContiguous ensures that block retirement is allowed
// to proceed when the database block history starts exactly where the snapshots end.
// This is the correct, contiguous state where we can transition retired blocks.
func TestBlockRetireContiguous(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()

	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)
	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(999), snapshots.SegmentsMax())

	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	genesisHeader := &types.Header{}
	require.NoError(t, rawdb.WriteHeader(rwTx, genesisHeader))
	nextHeader := &types.Header{Number: *uint256.NewInt(1000)}
	require.NoError(t, rawdb.WriteHeader(rwTx, nextHeader))
	require.NoError(t, rwTx.Commit())

	blockReader := NewBlockReader(snapshots)
	br := &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}

	hasEnough, err := br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.True(t, hasEnough)
}

// TestBlockRetireFallback verifies that if a merged segment is written
// to disk but its index is not generated yet, the node restart will not hide the smaller
// subsegments. These subsegments must remain visible so that block retirement can keep
// running without getting stuck (fixes issue #21472). Once the unindexed covering segment
// is deleted or indexed, the visibility should remain stable.
func TestBlockRetireFallback(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()

	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1000, 2000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1000, 2000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1000, 2000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(1999), snapshots.SegmentsMax())

	requireSegmentFilesExist(t, dirs.Snap, ver, 1, 1000, snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions)
	requireSegmentFilesExist(t, dirs.Snap, ver, 1000, 2000, snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions)

	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	genesisHeader := &types.Header{}
	require.NoError(t, rawdb.WriteHeader(rwTx, genesisHeader))
	nextHeader := &types.Header{Number: *uint256.NewInt(2000)}
	require.NoError(t, rawdb.WriteHeader(rwTx, nextHeader))
	require.NoError(t, rwTx.Commit())

	// DB starts right after snapshots, retirement should be allowed.
	blockReader := NewBlockReader(snapshots)
	br := &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}
	hasEnough, err := br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.True(t, hasEnough)

	// Simulate a restart after a merged transaction segment landed on disk but before
	// its indexes were built: the smaller indexed subsegments must stay visible until
	// the covering segment is indexed. reopenedSnapshots below is the fresh open.
	createTestSegmentOnlyFile(t, 1, 2000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)

	reopenedSnapshots := blocksnapshots.NewRoSnapshots(snapshots.Cfg(), dirs.Snap, logger)
	defer reopenedSnapshots.Close() // fallback safety guard in case of early test failure
	require.NoError(t, reopenedSnapshots.OpenFolder())
	require.Equal(t, uint64(1999), reopenedSnapshots.SegmentsMax())
	requireSegmentFilesExist(t, dirs.Snap, ver, 1, 1000, snaptype2.Enums.Transactions)
	requireSegmentFilesExist(t, dirs.Snap, ver, 1000, 2000, snaptype2.Enums.Transactions)

	blockReader = NewBlockReader(reopenedSnapshots)
	br = &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}
	hasEnough, err = br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.True(t, hasEnough)
	// Close reopenedSnapshots before removing the unindexed overlap to start the restore phase.
	reopenedSnapshots.Close()

	// Removing the unindexed overlap leaves the same indexed subsegments visible.
	unindexedOverlap := filepath.Join(dirs.Snap, snaptype.SegmentFileName(ver, 1, 2000, snaptype2.Enums.Transactions))
	require.NoError(t, dir.RemoveFile(unindexedOverlap))

	restoredSnapshots := blocksnapshots.NewRoSnapshots(snapshots.Cfg(), dirs.Snap, logger)
	require.NoError(t, restoredSnapshots.OpenFolder())
	defer restoredSnapshots.Close()
	require.Equal(t, uint64(1999), restoredSnapshots.SegmentsMax())

	blockReader = NewBlockReader(restoredSnapshots)
	br = &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}
	hasEnough, err = br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.True(t, hasEnough)
}

// TestBlockRetireAllOverlapped tests a scenario where all block
// snapshot types (Headers, Bodies, and Transactions) have unindexed covering segments
// on disk. Under the alignMin setting, we must verify that all three types correctly
// fall back to their indexed subsegments and maintain the correct visible range, allowing
// block retirement to proceed (related to issue #21472).
func TestBlockRetireAllOverlapped(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	ver := version.V1_0

	// Create indexed subsegments for all types.
	for _, enum := range []snaptype.Enum{snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions} {
		createTestSegmentFile(t, 1, 1000, enum, dirs.Snap, ver, logger)
		createTestSegmentFile(t, 1000, 2000, enum, dirs.Snap, ver, logger)
	}

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(1999), snapshots.SegmentsMax())

	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	genesisHeader := &types.Header{}
	require.NoError(t, rawdb.WriteHeader(rwTx, genesisHeader))
	nextHeader := &types.Header{Number: *uint256.NewInt(2000)}
	require.NoError(t, rawdb.WriteHeader(rwTx, nextHeader))
	require.NoError(t, rwTx.Commit())

	// Add unindexed covering segments for ALL types. With alignMin=true,
	// RecalcVisibleSegments must fall back to indexed subsegments for every
	// type, and SegmentsMax must take the correct minimum.
	for _, enum := range []snaptype.Enum{snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions} {
		createTestSegmentOnlyFile(t, 1, 2000, enum, dirs.Snap, ver, logger)
	}

	reopened := blocksnapshots.NewRoSnapshots(cfg, dirs.Snap, logger)
	require.NoError(t, reopened.OpenFolder())
	defer reopened.Close()
	require.Equal(t, uint64(1999), reopened.SegmentsMax())

	blockReader := NewBlockReader(reopened)
	br := &BlockRetire{
		db:          db,
		blockReader: blockReader,
		logger:      logger,
	}
	hasEnough, err := br.dbHasEnoughDataForBlocksRetire(t.Context())
	require.NoError(t, err)
	require.True(t, hasEnough)
}

// TestBlockReaderGenesisBlockWithSnapshots tests that the genesis block is always read from the database, even when snapshots exist
func TestBlockReaderGenesisBlockWithSnapshots(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	logger := log.New()

	// Snapshot segments (blocks 1..1000) must exist before the temporal DB opens
	// them, so its block-files view (shared with the reader) includes them.
	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)

	db := temporaltest.NewTestDB(t, dirs)
	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	blockReader := NewBlockReader(snapshots)
	require.Greater(t, snapshots.BlocksAvailable(), uint64(0))

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	require.NoError(t, err)
	assert.Equal(t, genesisHash, common.Hash{}) // genesis hash should be empty
	tx.Rollback()

	// create minimal genesis block for testing
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	genesisHeader := &types.Header{}
	genesisHash = genesisHeader.Hash()
	require.NoError(t, rawdb.WriteHeader(rwTx, genesisHeader))
	require.NoError(t, rawdb.WriteCanonicalHash(rwTx, genesisHash, 0))
	require.NoError(t, rawdb.WriteHeadHeaderHash(rwTx, genesisHash))
	require.NoError(t, rwTx.Commit())

	// Read genesis (block 0) with snapshots present: must come from the DB, not snapshots.
	tx, err = db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	hash, ok, err := blockReader.CanonicalHash(t.Context(), tx, 0)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, genesisHash, hash)

	block, senders, err := blockReader.BlockWithSenders(t.Context(), tx, genesisHash, 0)
	assert.NoError(t, err)
	// should be nil because genesis block does not have transactions
	assert.Nil(t, block)
	assert.Nil(t, senders)

	header, err := blockReader.Header(t.Context(), tx, genesisHash, 0)
	require.NoError(t, err)
	assert.NotNil(t, header)
	assert.Equal(t, uint64(0), header.Number.Uint64())

	// HasSenders should work for genesis
	hasSenders, err := blockReader.HasSenders(t.Context(), tx, genesisHash, 0)
	assert.NoError(t, err)
	assert.False(t, hasSenders) // should be false because genesis block does not have senders
}

func TestCanonicalHashCache_DBHit(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	blockReader := NewBlockReader(db.(HasBlockFiles).DebugBlockFiles())

	// Write a canonical hash to the DB
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()
	header := &types.Header{Number: *uint256.NewInt(0)}
	expectedHash := header.Hash()
	require.NoError(t, rawdb.WriteCanonicalHash(rwTx, expectedHash, 0))
	require.NoError(t, rwTx.Commit())

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// First call: should read from DB (DB results are not cached, only snapshot results are)
	hash, ok, err := blockReader.CanonicalHash(context.Background(), tx, 0)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, expectedHash, hash)

	// DB results should NOT be cached (only snapshot data is immutable and cacheable)
	_, found := blockReader.canonicalHashCache.Get(uint64(0))
	assert.False(t, found)

	// Second call: should still return correct result from DB
	hash2, ok2, err := blockReader.CanonicalHash(context.Background(), tx, 0)
	require.NoError(t, err)
	assert.True(t, ok2)
	assert.Equal(t, expectedHash, hash2)
}

func TestCanonicalHashCache_Miss(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	blockReader := NewBlockReader(db.(HasBlockFiles).DebugBlockFiles())

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Block 999 doesn't exist in DB or snapshots
	hash, ok, err := blockReader.CanonicalHash(context.Background(), tx, 999)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, common.Hash{}, hash)

	// Should not be cached
	_, found := blockReader.canonicalHashCache.Get(uint64(999))
	assert.False(t, found)
}

func TestCanonicalHashCache_MultipleBlocks(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	blockReader := NewBlockReader(db.(HasBlockFiles).DebugBlockFiles())

	// Write multiple canonical hashes
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	hashes := make([]common.Hash, 5)
	for i := range uint64(5) {
		header := &types.Header{Number: *uint256.NewInt(i)}
		hashes[i] = header.Hash()
		require.NoError(t, rawdb.WriteCanonicalHash(rwTx, hashes[i], i))
	}
	require.NoError(t, rwTx.Commit())
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Read all blocks — results come from DB, not snapshots, so the cache stays empty.
	for i := range uint64(5) {
		hash, ok, err := blockReader.CanonicalHash(context.Background(), tx, i)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, hashes[i], hash)
	}

	// DB results should NOT be cached (only snapshot data is immutable and cacheable)
	for i := range uint64(5) {
		_, found := blockReader.canonicalHashCache.Get(i)
		assert.False(t, found, "block %d should not be cached (DB data)", i)
	}
}

// TestCanonicalHashCache_SnapshotPath verifies that CanonicalHash populates
// canonicalHashCache when the hash is read from a snapshot segment (not from DB),
// and that subsequent calls are served from the cache without touching the snapshot.
func TestCanonicalHashCache_SnapshotPath(t *testing.T) {
	// Use the same from/to range as the other snapshot tests so OpenFolder
	// recognises the segment (naming convention: v1.0-000000-000001-headers.seg).
	const (
		from     = uint64(1)
		to       = uint64(1000)
		blockNum = from // first block in the segment; OrdinalLookup(from-from)=OrdinalLookup(0)
	)
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	ver := version.V1_0

	// Build a header and RLP-encode it.
	// Snapshot word format: 1 prefix byte (skipped by the decoder) + RLP bytes.
	header := &types.Header{Number: *uint256.NewInt(blockNum)}
	rlpBytes, err := rlp.EncodeToBytes(header)
	require.NoError(t, err)
	word := append([]byte{0}, rlpBytes...)

	// Write the headers segment with a single valid entry.
	segPath := filepath.Join(dirs.Snap, snaptype.SegmentFileName(ver, from, to, snaptype2.Enums.Headers))
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, dirs.Snap, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	c.DisableFsync()
	require.NoError(t, c.AddWord(word))
	require.NoError(t, c.Compress())
	c.Close()

	// Build index with BaseDataID=from so OrdinalLookup(blockNum-from)=OrdinalLookup(0).
	idxPath := filepath.Join(dirs.Snap, snaptype.IdxFileName(ver, from, to, snaptype2.Enums.Headers.String()))
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dirs.Snap,
		IndexFile:  idxPath,
		LeafSize:   8,
		BaseDataID: from,
		Enums:      true,
	}, logger)
	require.NoError(t, err)
	idx.DisableFsync()
	require.NoError(t, idx.AddKey([]byte{0}, 0))
	require.NoError(t, idx.Build(t.Context()))
	idx.Close()

	// Bodies and Transactions segments are required for OpenFolder to recognise the range.
	createTestSegmentFile(t, from, to, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, from, to, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)

	db := temporaltest.NewTestDB(t, dirs)
	blockReader := NewBlockReader(db.(HasBlockFiles).DebugBlockFiles())

	// No canonical hash written to DB → CanonicalHash must fall through to snapshot path.
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// First call: DB miss → snapshot read → cache populated.
	hash1, ok, err := blockReader.CanonicalHash(context.Background(), tx, blockNum)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, header.Hash(), hash1)

	cached, found := blockReader.canonicalHashCache.Get(blockNum)
	assert.True(t, found, "canonicalHashCache must be populated after a snapshot read")
	assert.Equal(t, header.Hash(), cached)

	// Second call: must be served from cache (no snapshot I/O).
	hash2, ok2, err := blockReader.CanonicalHash(context.Background(), tx, blockNum)
	require.NoError(t, err)
	assert.True(t, ok2)
	assert.Equal(t, header.Hash(), hash2)
}

// TestTxBlockView_StaleUntilReopen reproduces the minimal-mode history-download
// regression: a temporal tx pins its block-files view at begin-time, so body
// segments opened afterwards stay invisible to reads through that tx until the
// tx reopens its underlying-files view. When they stayed invisible, the
// snapshots stage's download filter walked an empty body view, couldn't compute
// the history prune cutoff, and downloaded every history file.
func TestTxBlockView_StaleUntilReopen(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	blockReader := NewBlockReader(snapshots)

	// Begin the tx BEFORE any block segments exist: it pins an empty view.
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	bodiesInTxView := func() int {
		return len(blockReader.view(rwTx).Bodies())
	}
	require.Equal(t, 0, bodiesInTxView())

	// Create and open header/body/tx segments after the tx began.
	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, dirs.Snap, ver, logger)
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, uint64(999), snapshots.SegmentsMax())

	// Regression: the tx still sees its stale, empty view.
	require.Equal(t, 0, bodiesInTxView())

	// Fix: reopening the tx's underlying-files view exposes the bodies.
	rwTx.(kv.CanReopenUnderlyingFilesTx).ForceReopenUnderlyingFilesTx()
	require.Positive(t, bodiesInTxView())
}

// frozenBlocksBackendClient stubs the one call under test; every other method of the
// embedded interface stays nil and panics if reached.
type frozenBlocksBackendClient struct {
	remoteproto.ETHBACKENDClient
	reply *remoteproto.FrozenBlocksReply
	err   error
}

func (c frozenBlocksBackendClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	return c.reply, c.err
}

// TestRemoteBlockReaderFrozenBlocks pins that the remote reader answers FrozenBlocks
// through the backend instead of panicking: the receipt gates and eth_capabilities
// reach it on a remote rpcdaemon. The signature leaves no way to surface an error, so
// a failed call reports zero, the conservative answer for every caller.
func TestRemoteBlockReaderFrozenBlocks(t *testing.T) {
	t.Parallel()

	reader := NewRemoteBlockReader(frozenBlocksBackendClient{reply: &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}})
	require.NotPanics(t, func() {
		require.Equal(t, uint64(42), reader.FrozenBlocks())
	})

	failing := NewRemoteBlockReader(frozenBlocksBackendClient{err: errors.New("backend down")})
	require.NotPanics(t, func() {
		require.Zero(t, failing.FrozenBlocks())
	})
}

type countingFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	err   error
	calls atomic.Int64
}

func (c *countingFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	c.calls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}, nil
}

type stalledFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
}

func (c stalledFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestRemoteBlockReaderFrozenBlocksCachesValue pins that the getter does not perform a
// live RPC on every call: receipt and capability handlers reach it while holding read
// transactions, so repeated calls within the TTL must be answered from the cache, and a
// stale one must be answered before the refresh it triggers.
func TestRemoteBlockReaderFrozenBlocksCachesValue(t *testing.T) {
	t.Parallel()

	client := &countingFrozenBlocksClient{}
	reader := NewRemoteBlockReader(client)

	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.EqualValues(t, 1, client.calls.Load())

	reader.frozenBlocks.SetTTL(0)
	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.Eventually(t, func() bool { return client.calls.Load() == 2 },
		time.Second, 10*time.Millisecond, "a stale value is refreshed behind the caller")
}

// TestRemoteBlockReaderFrozenBlocksStalledBackend pins that a connected but
// unresponsive backend cannot hold the caller forever: the internal context bounds the
// call and the getter falls back to the last known value (zero before any success).
func TestRemoteBlockReaderFrozenBlocksStalledBackend(t *testing.T) {
	t.Parallel()

	reader := NewRemoteBlockReader(stalledFrozenBlocksClient{})
	reader.frozenBlocksTimeout = 50 * time.Millisecond

	done := make(chan uint64, 1)
	go func() { done <- reader.FrozenBlocks() }()
	select {
	case v := <-done:
		require.Zero(t, v)
	case <-time.After(2 * time.Second):
		t.Fatal("FrozenBlocks did not return with a stalled backend")
	}
}

// recoveringFrozenBlocksClient fails its first call and answers every later one.
type recoveringFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	calls atomic.Int64
}

func (c *recoveringFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	if c.calls.Add(1) == 1 {
		return nil, errors.New("backend down")
	}
	return &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}, nil
}

// TestRemoteBlockReaderFrozenBlocksRetriesAfterFailure pins that a failed fetch is not
// remembered as an observation: it reports that nothing was observed, and once its
// attempt is no longer fresh the backend is asked again.
func TestRemoteBlockReaderFrozenBlocksRetriesAfterFailure(t *testing.T) {
	t.Parallel()

	reader := NewRemoteBlockReader(&recoveringFrozenBlocksClient{})
	reader.frozenBlocks.SetTTL(0)

	value, observed := reader.FrozenBlocksObserved()
	require.Zero(t, value)
	require.False(t, observed, "callers read zero as \"no snapshots\", which a failed fetch did not say")

	value, observed = reader.FrozenBlocksObserved()
	require.Equal(t, uint64(42), value)
	require.True(t, observed)
}

// TestRemoteBlockReaderFrozenBlocksSuppressesRepeatFetchesAfterFailure pins that a
// backend that is down costs one attempt per TTL rather than one per caller.
func TestRemoteBlockReaderFrozenBlocksSuppressesRepeatFetchesAfterFailure(t *testing.T) {
	t.Parallel()

	client := &countingFrozenBlocksClient{err: errors.New("backend down")}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocks.SetTTL(time.Hour)

	for range 8 {
		require.Zero(t, reader.FrozenBlocks())
	}
	require.EqualValues(t, 1, client.calls.Load(),
		"a recent failed attempt stands in for the ones that would follow it")
}

// blockingFrozenBlocksClient reports when a call arrives and holds it until released.
type blockingFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (c *blockingFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	c.calls.Add(1)
	c.entered <- struct{}{}
	select {
	case <-c.release:
		return &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// refreshingFrozenBlocksClient answers the first call at once and holds every later one,
// which is what a refresh behind an initialized value looks like.
type refreshingFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (c *refreshingFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	if c.calls.Add(1) == 1 {
		return &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}, nil
	}
	c.entered <- struct{}{}
	select {
	case <-c.release:
		return &remoteproto.FrozenBlocksReply{FrozenBlocks: 43}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestRemoteBlockReaderFrozenBlocksWaitsForTheFirstFetch pins that a caller arriving
// during the first fetch is given its result rather than the zero value. Before any
// answer there is no observation to serve, and zero is not a neutral stand-in: it reads
// as "no snapshots" and sends pre-Byzantium receipts down a re-execution that a node
// with pruned state history cannot perform.
func TestRemoteBlockReaderFrozenBlocksWaitsForTheFirstFetch(t *testing.T) {
	t.Parallel()

	const waiters = 8
	client := &blockingFrozenBlocksClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTimeout = 10 * time.Second

	fetching := make(chan uint64, 1)
	go func() { fetching <- reader.FrozenBlocks() }()
	<-client.entered

	waiting := make(chan uint64, waiters)
	for range waiters {
		go func() { waiting <- reader.FrozenBlocks() }()
	}
	select {
	case value := <-waiting:
		close(client.release)
		t.Fatalf("a caller was served %d before any fetch had answered", value)
	case <-time.After(100 * time.Millisecond):
	}
	close(client.release)

	require.Equal(t, uint64(42), <-fetching)
	for range waiters {
		require.Equal(t, uint64(42), <-waiting, "the first result answers every caller waiting for it")
	}
	require.EqualValues(t, 1, client.calls.Load(), "a waiting caller must not issue its own fetch")
}

// TestRemoteBlockReaderFrozenBlocksServesCacheWhileRefreshing pins that once a value has
// been observed no caller waits for the next one: a slow backend delays only the refresh
// running behind them. FrozenBlocks sits on every receipt request, so holding callers
// behind the refresh turns one slow backend into a stall of the whole handler pool.
func TestRemoteBlockReaderFrozenBlocksServesCacheWhileRefreshing(t *testing.T) {
	t.Parallel()

	client := &refreshingFrozenBlocksClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTimeout = 10 * time.Second
	require.Equal(t, uint64(42), reader.FrozenBlocks())

	reader.frozenBlocks.SetTTL(0)
	require.Equal(t, uint64(42), reader.FrozenBlocks(), "the caller that finds the value stale does not wait for the refresh")
	<-client.entered

	require.Equal(t, uint64(42), reader.FrozenBlocks(), "the observed value answers while the refresh runs")
	require.EqualValues(t, 2, client.calls.Load(), "the refresh in flight is not duplicated")

	close(client.release)
	require.Eventually(t, func() bool { return reader.FrozenBlocks() == 43 },
		2*time.Second, 10*time.Millisecond, "the refreshed value replaces the one served")
}

// failingFirstFrozenBlocksClient reports when a call arrives, holds it, and fails it.
type failingFirstFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	entered chan int64
	release chan struct{}
	calls   atomic.Int64
}

func (c *failingFirstFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	c.entered <- c.calls.Add(1)
	<-c.release
	return nil, errors.New("backend down")
}

// TestRemoteBlockReaderFrozenBlocksSharesTheFirstFailedFetch pins that callers arriving
// while the first fetch is in flight share it: when it fails they all report that nothing
// was observed, and none of them spends a timeout of its own on a backend that is down.
func TestRemoteBlockReaderFrozenBlocksSharesTheFirstFailedFetch(t *testing.T) {
	t.Parallel()

	const waiters = 8
	client := &failingFirstFrozenBlocksClient{
		entered: make(chan int64, waiters+1),
		release: make(chan struct{}),
	}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocks.SetTTL(time.Hour)

	results := make(chan uint64, waiters+1)
	go func() { results <- reader.FrozenBlocks() }()
	require.EqualValues(t, 1, <-client.entered)

	for range waiters {
		go func() { results <- reader.FrozenBlocks() }()
	}
	select {
	case value := <-results:
		close(client.release)
		t.Fatalf("a caller was served %d while the first fetch was still in flight", value)
	case <-time.After(100 * time.Millisecond):
	}
	close(client.release)

	for range waiters + 1 {
		require.Zero(t, <-results, "a failed first fetch leaves nothing to report")
	}
	require.EqualValues(t, 1, client.calls.Load(), "one attempt, not one per caller")
}

// stalledCountingFrozenBlocksClient never answers and reports when a call arrives.
type stalledCountingFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	entered chan struct{}
	calls   atomic.Int64
}

func (c *stalledCountingFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	c.calls.Add(1)
	c.entered <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestRemoteBlockReaderFrozenBlocksBoundsTheWaitToOneTimeout pins that a stalled backend
// costs each caller a single timeout: a caller reaches the backend at most once, so one
// that waited on an in-flight fetch cannot then spend a fresh timeout of its own. This
// getter takes no context and is reached with a read transaction open, so stacking a
// wait and a fetch of its own would double what a stalled backend costs the handler pool.
func TestRemoteBlockReaderFrozenBlocksBoundsTheWaitToOneTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 400 * time.Millisecond
	client := &stalledCountingFrozenBlocksClient{entered: make(chan struct{}, 4)}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTimeout = timeout

	first := make(chan uint64, 1)
	go func() { first <- reader.FrozenBlocks() }()
	<-client.entered

	waiting := make(chan uint64, 1)
	go func() { waiting <- reader.FrozenBlocks() }()

	require.Zero(t, <-waiting, "a stalled backend leaves nothing to report")
	require.LessOrEqual(t, client.calls.Load(), int64(2), "each caller reaches the backend once, so no caller spends more than its own timeout")
	require.Zero(t, <-first)
}
