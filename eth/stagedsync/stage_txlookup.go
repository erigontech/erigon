package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
)

type TxLookupCfg struct {
	db     ethdb.RwKV
	prune  ethdb.Prune
	tmpdir string
}

func StageTxLookupCfg(
	db ethdb.RwKV,
	prune ethdb.Prune,
	tmpdir string,
) TxLookupCfg {
	return TxLookupCfg{
		db:     db,
		prune:  prune,
		tmpdir: tmpdir,
	}
}

func SpawnTxLookup(s *StageState, tx ethdb.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
	quitCh := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := s.LogPrefix()
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	var from uint64
	if s.BlockNumber > 0 {
		from = s.BlockNumber + 1
	}

	pruneTo := cfg.prune.TxIndex.PruneTo(to)
	if from < pruneTo {
		from = pruneTo
	}
	startKey := dbutils.EncodeBlockNumber(from)
	if err = TxLookupTransform(logPrefix, tx, startKey, dbutils.EncodeBlockNumber(to), quitCh, cfg); err != nil {
		return err
	}
	if err = s.Update(tx, to); err != nil {
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
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, dbutils.HeaderCanonicalBucket, dbutils.TxLookupPrefix, cfg.tmpdir, func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadBody(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("%s: tx lookup generation, empty block body %d, hash %x", logPrefix, blocknum, v)
		}

		blockNumBytes := bigNum.SetUint64(blocknum).Bytes()
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

func UnwindTxLookup(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
	quitCh := ctx.Done()
	if s.BlockNumber <= u.UnwindPoint {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
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
	defer collector.Close("TxLookup")

	logPrefix := s.LogPrefix()
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

		txs, err := rawdb.ReadTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return false, err
		}
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

func PruneTxLookup(s *PruneState, tx ethdb.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if cfg.prune.TxIndex.Enabled() {
		if err = pruneTxLookup(tx, logPrefix, cfg.tmpdir, cfg.prune.TxIndex.PruneTo(s.CurrentBlockNumber), ctx); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func pruneTxLookup(tx ethdb.RwTx, logPrefix, tmpDir string, pruneTo uint64, ctx context.Context) error {
	collector := etl.NewCollector(tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close("TxLookup")

	c, err := tx.Cursor(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}
	defer c.Close()

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber >= pruneTo {
			break
		}

		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Prune collect", logPrefix), "table", dbutils.BlockBodyPrefix, "key", fmt.Sprintf("%x", k))
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}

		body := new(types.BodyForStorage)
		if err := rlp.Decode(bytes.NewReader(v), body); err != nil {
			return fmt.Errorf("%s, rlp decode err: %w", logPrefix, err)
		}

		txs, err := rawdb.ReadTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return err
		}
		for _, txn := range txs {
			if err := collector.Collect(txn.Hash().Bytes(), nil); err != nil {
				return err
			}
		}
	}
	if err := collector.Load(logPrefix, tx, dbutils.TxLookupPrefix, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}
