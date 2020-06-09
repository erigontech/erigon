package stagedsync

import (
	"encoding/binary"
	"errors"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"math/big"
	"os"
)

func spawnTxLookup(s *StageState, db ethdb.Database, dataDir string, quitCh chan struct{}) error {
	var blockNum uint64
	var startKey []byte

	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}

	startKey = dbutils.HeaderHashKey(blockNum)
	err:=etl.Transform(db,dbutils.HeaderPrefix,dbutils.TxLookupPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if !dbutils.CheckCanonicalKey(k) {
				return nil
			}
			blocknum:=binary.BigEndian.Uint64(k)
			body := rawdb.ReadBody(db, common.BytesToHash(v), blocknum)
			if body == nil {
				log.Error("empty body", "blocknum", blocknum, "hash", common.BytesToHash(v))
				return errors.New("empty block")
			}

			blockNumBytes:=new(big.Int).SetUint64(blockNum).Bytes()
			for _, tx := range body.Transactions {
				err:=next(k, tx.Hash().Bytes(), blockNumBytes)
				if err!=nil {
					return err
				}
			}
			return nil
		},  func(k []byte, value []byte, state etl.State, next etl.LoadNextFunc) error {
			return next(k,value)
		}, etl.TransformArgs{
			Quit:            quitCh,
			ExtractStartKey: startKey,
		})
		if err!=nil {
			return err
		}

	return s.DoneAndUpdate(db, blockNum)
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
		blocknum:=binary.BigEndian.Uint64(k)
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
	if err!=nil {
		return err
	}
	for _,v:=range blocksToRemove {
		if err=db.Delete(dbutils.TxLookupPrefix, v); err!=nil {
			return err
		}
	}
	return nil
}
