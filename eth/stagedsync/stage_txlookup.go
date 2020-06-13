package stagedsync

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
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

	return s.DoneAndUpdate(db, syncHeadNumber)
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
			return fmt.Errorf("tx lookup generation, empty block body %d, hash %x", blocknum, v)
		}

		blockNumBytes := new(big.Int).SetUint64(blocknum).Bytes()
		for _, tx := range body.Transactions {
			if err := next(k, tx.Hash().Bytes(), blockNumBytes); err != nil {
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

func unwindTxLookup(u *UnwindState, db ethdb.Database, quitCh chan struct{}) error {
	var txsToRemove [][]byte
	// Remove lookup entries for all blocks above unwindPoint
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(u.UnwindPoint+1), 0, func(k, v []byte) (b bool, e error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}
		data := v
		if debug.IsBlockCompressionEnabled() && len(data) > 0 {
			var err1 error
			data, err1 = snappy.Decode(nil, v)
			if err1 != nil {
				return false, fmt.Errorf("unwindTxLookup, snappy err: %w", err1)
			}
		}
		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
			return false, fmt.Errorf("unwindTxLookup, rlp decode err: %w", err)
		}
		for _, tx := range body.Transactions {
			txsToRemove = append(txsToRemove, tx.Hash().Bytes())
		}

		return true, nil
	}); err != nil {
		return err
	}
	// TODO: Do it in a batcn and update the progress
	for _, v := range txsToRemove {
		if err := db.Delete(dbutils.TxLookupPrefix, v); err != nil {
			return err
		}
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind TxLookup: %w", err)
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
