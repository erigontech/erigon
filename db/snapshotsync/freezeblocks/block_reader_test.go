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
	"path/filepath"
	"testing"

	uint256 "github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
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

// TestBlockReaderGenesisBlockWithSnapshots tests that the genesis block is always read from the database, even when snapshots exist
func TestBlockReaderGenesisBlockWithSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	logger := log.New()

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	require.NoError(t, err)
	assert.Equal(t, genesisHash, (common.Hash{})) // genesis hash should be empty

	// create minimal genesis block for testing
	tx.Rollback()
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	genesisHeader := &types.Header{}
	genesisHash = genesisHeader.Hash()
	err = rawdb.WriteHeader(rwTx, genesisHeader)
	require.NoError(t, err)
	err = rawdb.WriteCanonicalHash(rwTx, genesisHash, 0)
	require.NoError(t, err)
	err = rawdb.WriteHeadHeaderHash(rwTx, genesisHash)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	// create snapshots file for testing starting from block 1
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, tmpDir, logger)
	ver := version.V1_0
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Headers, tmpDir, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Bodies, tmpDir, ver, logger)
	createTestSegmentFile(t, 1, 1000, snaptype2.Enums.Transactions, tmpDir, ver, logger)

	err = snapshots.OpenFolder()
	require.NoError(t, err)
	defer snapshots.Close()

	blocksAvailable := snapshots.BlocksAvailable()
	assert.Greater(t, blocksAvailable, uint64(0))

	blockReader := NewBlockReader(snapshots, nil)

	// Try to read genesis block (block 0) when snapshots exist.This should read from database not snapshots
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
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	logger := log.New()

	// Write a canonical hash to the DB
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()
	header := &types.Header{Number: *uint256.NewInt(0)}
	expectedHash := header.Hash()
	require.NoError(t, rawdb.WriteCanonicalHash(rwTx, expectedHash, 0))
	require.NoError(t, rwTx.Commit())

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, t.TempDir(), logger)
	defer snapshots.Close()
	blockReader := NewBlockReader(snapshots, nil)

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
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	logger := log.New()

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, t.TempDir(), logger)
	defer snapshots.Close()
	blockReader := NewBlockReader(snapshots, nil)

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
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	logger := log.New()

	// Write multiple canonical hashes
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	hashes := make([]common.Hash, 5)
	for i := uint64(0); i < 5; i++ {
		header := &types.Header{Number: *uint256.NewInt(i)}
		hashes[i] = header.Hash()
		require.NoError(t, rawdb.WriteCanonicalHash(rwTx, hashes[i], i))
	}
	require.NoError(t, rwTx.Commit())

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, t.TempDir(), logger)
	defer snapshots.Close()
	blockReader := NewBlockReader(snapshots, nil)

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Read all blocks — results come from DB, not snapshots, so the cache stays empty.
	for i := uint64(0); i < 5; i++ {
		hash, ok, err := blockReader.CanonicalHash(context.Background(), tx, i)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, hashes[i], hash)
	}

	// DB results should NOT be cached (only snapshot data is immutable and cacheable)
	for i := uint64(0); i < 5; i++ {
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
	tmpDir := t.TempDir()
	logger := log.New()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)

	ver := version.V1_0

	// Build a header and RLP-encode it.
	// Snapshot word format: 1 prefix byte (skipped by the decoder) + RLP bytes.
	header := &types.Header{Number: *uint256.NewInt(blockNum)}
	rlpBytes, err := rlp.EncodeToBytes(header)
	require.NoError(t, err)
	word := append([]byte{0}, rlpBytes...)

	// Write the headers segment with a single valid entry.
	segPath := filepath.Join(tmpDir, snaptype.SegmentFileName(ver, from, to, snaptype2.Enums.Headers))
	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, tmpDir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	c.DisableFsync()
	require.NoError(t, c.AddWord(word))
	require.NoError(t, c.Compress())
	c.Close()

	// Build index with BaseDataID=from so OrdinalLookup(blockNum-from)=OrdinalLookup(0).
	idxPath := filepath.Join(tmpDir, snaptype.IdxFileName(ver, from, to, snaptype2.Enums.Headers.String()))
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     tmpDir,
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
	createTestSegmentFile(t, from, to, snaptype2.Enums.Bodies, tmpDir, ver, logger)
	createTestSegmentFile(t, from, to, snaptype2.Enums.Transactions, tmpDir, ver, logger)

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, tmpDir, logger)
	require.NoError(t, snapshots.OpenFolder())
	defer snapshots.Close()

	blockReader := NewBlockReader(snapshots, nil)

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
