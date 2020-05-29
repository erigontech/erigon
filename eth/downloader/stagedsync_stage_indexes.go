package downloader

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	emptyValBit uint64 = 0x8000000000000000
)

func spawnAccountHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	if plainState {
		log.Info("Skipped account index generation for plain state")
		return nil
	}
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, AccountHistoryIndex); err == nil {
		if lastProcessedBlockNumber > 0 {
			blockNum = lastProcessedBlockNumber + 1
		}
	} else {
		return fmt.Errorf("reading account history process: %v", err)
	}

	bytes2walker := func(b []byte) changeset.Walker {
		return changeset.AccountChangeSetBytes(b)
	}

	err := etl.Transform(
		db,
		dbutils.AccountChangeSetBucket,
		dbutils.AccountsHistoryBucket,
		datadir,
		getExtractFunc(bytes2walker),
		loadFunc,
	)
	if err != nil {
		return err
	}

	if err := SaveStageProgress(db, AccountHistoryIndex, blockNum); err != nil {
		return err
	}
	return nil
}

func spawnStorageHistoryIndex(db ethdb.Database, datadir string, plainState bool) error {
	if plainState {
		log.Info("Skipped storage index generation for plain state")
		return nil
	}
	var blockNum uint64
	if lastProcessedBlockNumber, err := GetStageProgress(db, StorageHistoryIndex); err == nil {
		if lastProcessedBlockNumber > 0 {
			blockNum = lastProcessedBlockNumber + 1
		}
	} else {
		return fmt.Errorf("reading storage history process: %v", err)
	}

	bytes2walker := func(b []byte) changeset.Walker {
		return changeset.StorageChangeSetBytes(b)
	}

	err := etl.Transform(
		db,
		dbutils.StorageChangeSetBucket,
		dbutils.StorageHistoryBucket,
		datadir,
		getExtractFunc(bytes2walker),
		loadFunc)
	if err != nil {
		return err
	}

	if err := SaveStageProgress(db, StorageHistoryIndex, blockNum); err != nil {
		return err
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

func walkerFactory(csBucket []byte, plainState bool) func(bytes []byte) changeset.Walker {
	switch {
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && !plainState:
		return func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.AccountChangeSetBucket) && plainState:
		return func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetPlainBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && !plainState:
		return func(bytes []byte) changeset.Walker {
			return changeset.StorageChangeSetBytes(bytes)
		}
	case bytes.Equal(csBucket, dbutils.StorageChangeSetBucket) && plainState:
		return func(bytes []byte) changeset.Walker {
			return changeset.StorageChangeSetPlainBytes(bytes)
		}
	default:
		log.Error("incorrect bucket", "bucket", string(csBucket), "plainState", plainState)
		panic("incorrect bucket " + string(csBucket))
	}
}

func loadFunc(k []byte, valueDecoder etl.Decoder, state etl.State, next etl.LoadNextFunc) error {
	var blockNumbers []uint64
	err := valueDecoder.Decode(&blockNumbers)
	if err != nil {
		return err
	}
	for _, b := range blockNumbers {
		vzero := (b & emptyValBit) != 0
		blockNr := b &^ emptyValBit
		currentChunkKey := dbutils.IndexChunkKey(k, ^uint64(0))
		indexBytes, err1 := state.Get(currentChunkKey)
		if err1 != nil && err1 != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err1)
		}
		var index dbutils.HistoryIndexBytes
		if len(indexBytes) == 0 {
			index = dbutils.NewHistoryIndex()
		} else if dbutils.CheckNewIndexChunk(indexBytes, blockNr) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			index = dbutils.WrapHistoryIndex(indexBytes)
			indexKey, err3 := index.Key(k)
			if err3 != nil {
				return err3
			}
			// Flush the old chunk
			if err4 := next(indexKey, index); err4 != nil {
				return err4
			}
			// Start a new chunk
			index = dbutils.NewHistoryIndex()
		} else {
			index = dbutils.WrapHistoryIndex(indexBytes)
		}
		index = index.Append(blockNr, vzero)

		err = next(currentChunkKey, index)
		if err != nil {
			return err
		}
	}
	return nil
}

func getExtractFunc(bytes2walker func([]byte) changeset.Walker) etl.ExtractFunc {
	return func(dbKey, dbValue []byte, next etl.ExtractNextFunc) error {
		bufferMap := make(map[string][]uint64)
		blockNum, _ := dbutils.DecodeTimestamp(dbKey)
		err := bytes2walker(dbValue).Walk(func(changesetKey, changesetValue []byte) error {
			sKey := string(changesetKey)
			list := bufferMap[sKey]
			b := blockNum
			if len(changesetValue) == 0 {
				b |= emptyValBit
			}
			list = append(list, b)
			bufferMap[sKey] = list
			return nil
		})
		if err != nil {
			return err
		}

		for k, v := range bufferMap {
			err = next([]byte(k), v)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
