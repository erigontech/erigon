package downloader

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"runtime"
	"time"
)

const changeSetBufSize = 128 * 1024 * 1024

func spawnAccountHistoryIndex(db ethdb.Database, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped account index generation for plain state")
		return nil
	}
	log.Info("Account history index generation started", "from", lastProcessedBlockNumber)
	var m runtime.MemStats
	changesets := make([]byte, changeSetBufSize) // 128 Mb buffer
	var offsets []int
	var blockNums []uint64
	var blockNum = lastProcessedBlockNumber
	var done = false
	batch := db.NewBatch()
	for !done {
		offset := 0
		offsets = offsets[:0]
		blockNums = blockNums[:0]
		startKey := dbutils.EncodeTimestamp(blockNum)
		done = true
		if err := db.Walk(dbutils.AccountChangeSetBucket, startKey, 0, func(k, v []byte) (bool, error) {
			blockNum, _ = dbutils.DecodeTimestamp(k)
			if offset+len(v) > changeSetBufSize { // Adding the current changeset would overflow the buffer
				done = false
				return false, nil
			}
			copy(changesets[offset:], v)
			offset += len(v)
			offsets = append(offsets, offset)
			blockNums = append(blockNums, blockNum)
			return true, nil
		}); err != nil {
			return err
		}
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			changeset := changeset.AccountChangeSetBytes(changesets[prevOffset:offset])
			if err := changeset.Walk(func(k, v []byte) error {
				currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
				indexBytes, err := batch.Get(dbutils.AccountChangeSetBucket, currentChunkKey)
				if err != nil && err != ethdb.ErrKeyNotFound {
					return fmt.Errorf("find chunk failed: %w", err)
				}
				var index dbutils.HistoryIndexBytes
				if len(indexBytes) == 0 {
					index = dbutils.NewHistoryIndex()
				} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
					// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
					index = dbutils.WrapHistoryIndex(indexBytes)
					indexKey, err := index.Key(k)
					if err != nil {
						return err
					}
					// Flush the old chunk
					if err := batch.Put(dbutils.AccountChangeSetBucket, indexKey, index); err != nil {
						return err
					}
					// Start a new chunk
					index = dbutils.NewHistoryIndex()
				} else {
					index = dbutils.WrapHistoryIndex(indexBytes)
				}
				index = index.Append(blockNr, len(v) == 0)

				if err := batch.Put(dbutils.AccountChangeSetBucket, currentChunkKey, index); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		if err := SaveStageProgress(batch, AccountHistoryIndex, blockNum); err != nil {
			return err
		}
		batchSize := batch.BatchSize()
		start := time.Now()
		if _, err := batch.Commit(); err != nil {
			return err
		}
		runtime.ReadMemStats(&m)
		log.Info("Commited account index batch", "in", time.Since(start), "up to block", blockNum, "batch size", batchSize,
			"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	}
	return nil
}

func spawnStorageHistoryIndex(db ethdb.Database, plainState bool) error {
	lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex)
	if err != nil {
		return err
	}
	if plainState {
		log.Info("Skipped storage index generation for plain state")
		return nil
	}
	log.Info("Storage history index generation started", "from", lastProcessedBlockNumber)
	var m runtime.MemStats
	changesets := make([]byte, changeSetBufSize) // 128 Mb buffer
	var offsets []int
	var blockNums []uint64
	var blockNum = lastProcessedBlockNumber
	var done = false
	batch := db.NewBatch()
	for !done {
		offset := 0
		offsets = offsets[:0]
		blockNums = blockNums[:0]
		startKey := dbutils.EncodeTimestamp(blockNum)
		done = true
		if err := db.Walk(dbutils.StorageChangeSetBucket, startKey, 0, func(k, v []byte) (bool, error) {
			blockNum, _ = dbutils.DecodeTimestamp(k)
			if offset+len(v) > changeSetBufSize { // Adding the current changeset would overflow the buffer
				done = false
				return false, nil
			}
			copy(changesets[offset:], v)
			offset += len(v)
			offsets = append(offsets, offset)
			blockNums = append(blockNums, blockNum)
			return true, nil
		}); err != nil {
			return err
		}
		prevOffset := 0
		for i, offset := range offsets {
			blockNr := blockNums[i]
			changeset := changeset.StorageChangeSetBytes(changesets[prevOffset:offset])
			if err := changeset.Walk(func(k, v []byte) error {
				currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
				indexBytes, err := batch.Get(dbutils.StorageChangeSetBucket, currentChunkKey)
				if err != nil && err != ethdb.ErrKeyNotFound {
					return fmt.Errorf("find chunk failed: %w", err)
				}
				var index dbutils.HistoryIndexBytes
				if len(indexBytes) == 0 {
					index = dbutils.NewHistoryIndex()
				} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
					// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
					index = dbutils.WrapHistoryIndex(indexBytes)
					indexKey, err := index.Key(k)
					if err != nil {
						return err
					}
					// Flush the old chunk
					if err := batch.Put(dbutils.StorageChangeSetBucket, indexKey, index); err != nil {
						return err
					}
					// Start a new chunk
					index = dbutils.NewHistoryIndex()
				} else {
					index = dbutils.WrapHistoryIndex(indexBytes)
				}
				index = index.Append(blockNr, len(v) == 0)

				if err := batch.Put(dbutils.StorageChangeSetBucket, currentChunkKey, index); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			prevOffset = offset
		}
		if err := SaveStageProgress(batch, StorageHistoryIndex, blockNum); err != nil {
			return err
		}
		batchSize := batch.BatchSize()
		start := time.Now()
		if _, err := batch.Commit(); err != nil {
			return err
		}
		runtime.ReadMemStats(&m)
		log.Info("Commited storage index batch", "in", time.Since(start), "up to block", blockNum, "batch size", batchSize,
			"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	}
	return nil
}

func unwindAccountHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, walkerFactory(dbutils.AccountChangeSetBucket, plainState))
}

func unwindStorageHistoryIndex(unwindPoint uint64, db ethdb.Database, plainState bool) error {
	ig := core.NewIndexGenerator(db)
	return ig.Truncate(unwindPoint, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, walkerFactory(dbutils.StorageChangeSetBucket, plainState))
}

func walkerFactory(csBucket []byte, plainState bool) func(bytes []byte) core.ChangesetWalker {
	switch {
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetPlainBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && !plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && plainState:
		return func(bytes []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetPlainBytes(bytes)
		}
	default:
		log.Error("incorrect bucket", "bucket", string(csBucket), "plainState", plainState)
		panic("incorrect bucket " + string(csBucket))
	}
}
