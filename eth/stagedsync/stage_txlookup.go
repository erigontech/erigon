package stagedsync

import (
	"encoding/binary"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"math/big"
	"runtime"
)

func spawnTxLookup(s *StageState, db ethdb.Database, dataDir string, quitCh chan struct{}) error {
	var blockNum uint64
	var startKey []byte

	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}
	var chunks [][]byte
	syncHeadNumber, err := s.ExecutionAt(db)
	if err == nil {
		chunks = calculateTxLookupChunks(lastProcessedBlockNumber, syncHeadNumber, runtime.NumCPU()/2+1)
	}

	startKey = dbutils.HeaderHashKey(blockNum)
	err = TxLookupTransform(db, startKey, dbutils.HeaderHashKey(syncHeadNumber), quitCh, dataDir, chunks)
	if err != nil {
		return err
	}

	return s.DoneAndUpdate(db, blockNum)
}

func TxLookupTransform(db ethdb.Database, startKey, endKey []byte, quitCh chan struct{}, datadir string, chunks [][]byte) error {
	return etl.Transform(db, dbutils.HeaderPrefix, dbutils.TxLookupPrefix, datadir, func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		if !dbutils.CheckCanonicalKey(k) {
			return nil
		}
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadBody(db, blockHash, blocknum)
		if body == nil {
			log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
			return nil
			//return fmt.Errorf("empty block %v", blocknum)
		}

		blockNumBytes := new(big.Int).SetUint64(blocknum).Bytes()
		for _, tx := range body.Transactions {
			err := next(k, tx.Hash().Bytes(), blockNumBytes)
			if err != nil {
				return err
			}
		}
		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: startKey,
		ExtractEndKey:   endKey,
		Chunks:          chunks,
	})
}

func unwindTxLookup(unwindPoint uint64, db ethdb.Database, quitCh chan struct{}) error {
	var blocksToRemove [][]byte
	err := db.Walk(dbutils.HeaderHashKey(unwindPoint), dbutils.HeaderHashKey(unwindPoint), 0, func(k, v []byte) (b bool, e error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}
		if !dbutils.CheckCanonicalKey(k) {
			return true, nil
		}
		blocknum := binary.BigEndian.Uint64(k)
		body := rawdb.ReadBody(db, common.BytesToHash(v), blocknum)
		if body == nil {
			log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
			return true, nil
		}
		for _, tx := range body.Transactions {
			blocksToRemove = append(blocksToRemove, tx.Hash().Bytes())
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	for _, v := range blocksToRemove {
		if err = db.Delete(dbutils.TxLookupPrefix, v); err != nil {
			return err
		}
	}
	return nil
}

func calculateTxLookupChunks(startBlock, endBlock uint64, numOfChunks int) [][]byte {
	if endBlock < startBlock+1000000 || numOfChunks < 2 {
		return nil
	}

	chunkSize := (endBlock - startBlock) / uint64(numOfChunks)
	var chunks = make([][]byte, numOfChunks-1)
	for i := uint64(1); i < uint64(numOfChunks); i++ {
		chunks[i-1] = dbutils.HeaderHashKey(i * chunkSize)
	}
	return chunks
}
