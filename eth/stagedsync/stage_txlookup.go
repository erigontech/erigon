package stagedsync

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func SpawnTxLookup(s *StageState, db ethdb.Database, dataDir string, quitCh <-chan struct{}) error {
	var blockNum uint64
	var startKey []byte

	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}
	syncHeadNumber, err := s.ExecutionAt(db)
	if err != nil {
		return err
	}

	startKey = dbutils.HeaderHashKey(blockNum)
	if err = TxLookupTransform(db, startKey, dbutils.EncodeBlockNumber(syncHeadNumber), quitCh, dataDir); err != nil {
		return err
	}

	return s.DoneAndUpdate(db, syncHeadNumber)
}

func TxLookupTransform(db ethdb.Database, startKey, endKey []byte, quitCh <-chan struct{}, datadir string) error {
	return etl.Transform(db, dbutils.HeaderPrefix, dbutils.TxLookupPrefix, datadir, func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		if !dbutils.CheckCanonicalKey(k) {
			return nil
		}
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v[:common.HashLength])
		body := rawdb.ReadBody(db, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("tx lookup generation, empty block body %d, hash %x", blocknum, v)
		}

		for _, tx := range body.Transactions {
			if err := next(k, tx.Hash().Bytes(), k); err != nil {
				return err
			}
		}
		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: startKey,
		ExtractEndKey:   endKey,
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}

func UnwindTxLookup(u *UnwindState, s *StageState, db ethdb.Database, datadir string, quitCh <-chan struct{}) error {
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	// Remove lookup entries for blocks between unwindPoint+1 and stage.BlockNumber
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(u.UnwindPoint+1), 0, func(k, v []byte) (b bool, e error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber > s.BlockNumber {
			return false, nil
		}

		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		bodyRlp, err := rawdb.DecompressBlockBody(v)
		if err != nil {
			return false, err
		}

		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(bodyRlp), body); err != nil {
			return false, fmt.Errorf("unwindTxLookup, rlp decode err: %w", err)
		}
		for _, tx := range body.Transactions {
			if err := collector.Collect(tx.Hash().Bytes(), nil); err != nil {
				return false, err
			}
		}

		return true, nil
	}); err != nil {
		return err
	}
	if err := collector.Load(db, dbutils.TxLookupPrefix, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quitCh}); err != nil {
		return err
	}
	return u.Done(db)
}
