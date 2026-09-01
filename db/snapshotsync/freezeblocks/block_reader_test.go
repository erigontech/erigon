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
		view, release := blockReader.view(rwTx)
		defer release()
		return len(view.Bodies())
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
	calls atomic.Int64
}

func (c *countingFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	c.calls.Add(1)
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
// transactions, so repeated calls within the TTL must be answered from the cache.
func TestRemoteBlockReaderFrozenBlocksCachesValue(t *testing.T) {
	t.Parallel()

	client := &countingFrozenBlocksClient{}
	reader := NewRemoteBlockReader(client)

	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.EqualValues(t, 1, client.calls.Load())

	reader.frozenBlocksTTL = 0
	require.Equal(t, uint64(42), reader.FrozenBlocks())
	require.EqualValues(t, 2, client.calls.Load())
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
// cached as if it had succeeded. Zero is not a neutral answer: it makes the receipt
// paths treat pre-Byzantium blocks as needing re-execution, which a node with pruned
// state history cannot serve.
func TestRemoteBlockReaderFrozenBlocksRetriesAfterFailure(t *testing.T) {
	t.Parallel()

	client := &recoveringFrozenBlocksClient{}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTTL = time.Hour

	require.Zero(t, reader.FrozenBlocks())
	require.Equal(t, uint64(42), reader.FrozenBlocks())
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
// been observed a slow backend delays only the goroutine refreshing it. FrozenBlocks
// sits on every receipt request, so holding the others behind the refresh turns one slow
// backend into a stall of the whole handler pool.
func TestRemoteBlockReaderFrozenBlocksServesCacheWhileRefreshing(t *testing.T) {
	t.Parallel()

	client := &refreshingFrozenBlocksClient{entered: make(chan struct{}, 1), release: make(chan struct{})}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTimeout = 10 * time.Second
	require.Equal(t, uint64(42), reader.FrozenBlocks())

	reader.frozenBlocksTTL = 0
	refreshing := make(chan uint64, 1)
	go func() { refreshing <- reader.FrozenBlocks() }()
	<-client.entered

	waiting := make(chan uint64, 1)
	go func() { waiting <- reader.FrozenBlocks() }()
	select {
	case value := <-waiting:
		require.Equal(t, uint64(42), value, "the observed value answers while the refresh runs")
	case <-time.After(time.Second):
		close(client.release)
		t.Fatal("FrozenBlocks held a caller behind an in-flight refresh")
	}
	require.EqualValues(t, 2, client.calls.Load(), "the waiting caller must not issue its own fetch")

	close(client.release)
	require.Equal(t, uint64(43), <-refreshing)
}

// failingFirstFrozenBlocksClient holds and fails its first call, then holds its second
// and answers it, so a test can watch the queue re-form behind the caller that took over.
type failingFirstFrozenBlocksClient struct {
	remoteproto.ETHBACKENDClient
	entered chan int64
	release chan struct{}
	answer  chan struct{}
	calls   atomic.Int64
}

func (c *failingFirstFrozenBlocksClient) FrozenBlocks(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	call := c.calls.Add(1)
	c.entered <- call
	if call == 1 {
		<-c.release
		return nil, errors.New("backend down")
	}
	<-c.answer
	return &remoteproto.FrozenBlocksReply{FrozenBlocks: 42}, nil
}

// TestRemoteBlockReaderFrozenBlocksAsksAgainWhenTheFirstFetchFails pins that waiting for
// the first fetch is not a substitute for asking: a caller that waited and found nothing
// observed has spent no attempt of its own, and zero is the answer this getter must not
// hand out while it can still be asked.
func TestRemoteBlockReaderFrozenBlocksAsksAgainWhenTheFirstFetchFails(t *testing.T) {
	t.Parallel()

	const waiters = 8
	client := &failingFirstFrozenBlocksClient{
		entered: make(chan int64, waiters+1),
		release: make(chan struct{}),
		answer:  make(chan struct{}),
	}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTTL = time.Hour

	failing := make(chan uint64, 1)
	go func() { failing <- reader.FrozenBlocks() }()
	require.EqualValues(t, 1, <-client.entered)

	waiting := make(chan uint64, waiters)
	for range waiters {
		go func() { waiting <- reader.FrozenBlocks() }()
	}
	select {
	case value := <-waiting:
		close(client.release)
		close(client.answer)
		t.Fatalf("a caller was served %d while the first fetch was still in flight", value)
	case <-time.After(100 * time.Millisecond):
	}
	close(client.release)
	require.Zero(t, <-failing, "the caller whose own fetch failed has no value to report")

	require.EqualValues(t, 2, <-client.entered, "one of the waiting callers takes the fetch over")
	select {
	case value := <-waiting:
		close(client.answer)
		t.Fatalf("a caller was served %d while the retry was still in flight", value)
	case call := <-client.entered:
		close(client.answer)
		t.Fatalf("a waiting caller ran a fetch of its own (call %d) instead of taking the retry", call)
	case <-time.After(100 * time.Millisecond):
	}
	close(client.answer)

	for range waiters {
		select {
		case value := <-waiting:
			require.Equal(t, uint64(42), value, "the retry answers the caller that ran it and the ones queued behind it")
		case <-time.After(5 * time.Second):
			t.Fatal("a waiting caller neither answered nor retried")
		}
	}
	require.EqualValues(t, 2, client.calls.Load(), "one retry, not one per waiting caller")
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

// TestRemoteBlockReaderFrozenBlocksBoundsTheWaitToOneTimeout pins that no caller is held
// for longer than a single fetch timeout. This getter takes no context and is reached
// with a read transaction open, so waiting for one fetch and then running another would
// double what a stalled backend costs the handler pool.
func TestRemoteBlockReaderFrozenBlocksBoundsTheWaitToOneTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 400 * time.Millisecond
	client := &stalledCountingFrozenBlocksClient{entered: make(chan struct{}, 4)}
	reader := NewRemoteBlockReader(client)
	reader.frozenBlocksTimeout = timeout

	started := time.Now()
	first := make(chan uint64, 1)
	go func() { first <- reader.FrozenBlocks() }()
	<-client.entered

	waiting := make(chan uint64, 1)
	go func() { waiting <- reader.FrozenBlocks() }()

	require.Zero(t, <-waiting, "a stalled backend leaves nothing to report")
	require.Less(t, time.Since(started), timeout+timeout/2, "the wait and a fetch of its own must not stack up")
	require.Zero(t, <-first)
}
