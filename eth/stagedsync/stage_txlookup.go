package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type TxLookupCfg struct {
	db     ethdb.RwKV
	tmpdir string
}

func StageTxLookupCfg(
	db ethdb.RwKV,
	tmpdir string,
) TxLookupCfg {
	return TxLookupCfg{
		db:     db,
		tmpdir: tmpdir,
	}
}

func SpawnTxLookup(s *StageState, tx ethdb.RwTx, cfg TxLookupCfg, quitCh <-chan struct{}) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	var blockNum uint64
	var startKey []byte

	lastProcessedBlockNumber := s.BlockNumber
	if lastProcessedBlockNumber > 0 {
		blockNum = lastProcessedBlockNumber + 1
	}
	syncHeadNumber, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	logPrefix := s.state.LogPrefix()
	startKey = dbutils.EncodeBlockNumber(blockNum)
	if err = TxLookupTransform(logPrefix, tx, startKey, dbutils.EncodeBlockNumber(syncHeadNumber), quitCh, cfg); err != nil {
		return err
	}
	if err = s.DoneAndUpdate(tx, syncHeadNumber); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func TxLookupTransform(logPrefix string, tx ethdb.RwTx, startKey, endKey []byte, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	return etl.Transform(logPrefix, tx, dbutils.HeaderCanonicalBucket, dbutils.TxLookupPrefix, cfg.tmpdir, func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadBody(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("%s: tx lookup generation, empty block body %d, hash %x", logPrefix, blocknum, v)
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
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}

func UnwindTxLookup(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg TxLookupCfg, quitCh <-chan struct{}) error {
	if s.BlockNumber <= u.UnwindPoint {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := unwindTxLookup(u, s, tx, cfg, quitCh); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindTxLookup(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg TxLookupCfg, quitCh <-chan struct{}) error {
	collector := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	logPrefix := s.state.LogPrefix()
	c, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer c.Close()
	// Remove lookup entries for blocks between unwindPoint+1 and stage.BlockNumber
	if err := ethdb.Walk(c, dbutils.EncodeBlockNumber(u.UnwindPoint+1), 0, func(k, v []byte) (b bool, e error) {
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

		body := new(types.BodyForStorage)
		if err := rlp.Decode(bytes.NewReader(v), body); err != nil {
			return false, fmt.Errorf("%s, rlp decode err: %w", logPrefix, err)
		}

		txs, _ := rawdb.ReadTransactions(tx, body.BaseTxId, body.TxAmount)
		for _, txn := range txs {
			if err := collector.Collect(txn.Hash().Bytes(), nil); err != nil {
				return false, err
			}
		}

		return true, nil
	}); err != nil {
		return err
	}
	if err := collector.Load(logPrefix, tx, dbutils.TxLookupPrefix, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quitCh}); err != nil {
		return err
	}
	return nil
}
