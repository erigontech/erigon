package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/rlp"
)

type TxLookupCfg struct {
	db     kv.RwDB
	prune  prune.Mode
	tmpdir string
}

func StageTxLookupCfg(
	db kv.RwDB,
	prune prune.Mode,
	tmpdir string,
) TxLookupCfg {
	return TxLookupCfg{
		db:     db,
		prune:  prune,
		tmpdir: tmpdir,
	}
}

func SpawnTxLookup(s *StageState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
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
	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	startBlock := s.BlockNumber
	pruneTo := cfg.prune.TxIndex.PruneTo(endBlock)
	if startBlock < pruneTo {
		startBlock = pruneTo
	}
	if startBlock > 0 {
		startBlock++
	}
	startKey := dbutils.EncodeBlockNumber(startBlock)
	if err = TxLookupTransform(logPrefix, tx, startKey, dbutils.EncodeBlockNumber(endBlock), quitCh, cfg); err != nil {
		return err
	}
	if err = s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func TxLookupTransform(logPrefix string, tx kv.RwTx, startKey, endKey []byte, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadBodyWithTransactions(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("empty block body %d, hash %x", blocknum, v)
		}

		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), bigNum.SetUint64(blocknum).Bytes()); err != nil {
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

func UnwindTxLookup(u *UnwindState, s *StageState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
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

func unwindTxLookup(u *UnwindState, s *StageState, tx kv.RwTx, cfg TxLookupCfg, quitCh <-chan struct{}) error {
	reader := bytes.NewReader(nil)
	logPrefix := s.LogPrefix()
	return etl.Transform(logPrefix, tx, kv.BlockBody, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		body := new(types.BodyForStorage)
		reader.Reset(v)
		if err := rlp.Decode(reader, body); err != nil {
			return fmt.Errorf("rlp decode err: %w", err)
		}

		txs, err := rawdb.ReadTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return err
		}
		for _, txn := range txs {
			if err = next(k, txn.Hash().Bytes(), nil); err != nil {
				return err
			}
		}
		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: dbutils.EncodeBlockNumber(u.UnwindPoint + 1),
		ExtractEndKey:   dbutils.EncodeBlockNumber(s.BlockNumber),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}

func PruneTxLookup(s *PruneState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
	if !cfg.prune.TxIndex.Enabled() {
		return nil
	}
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	to := cfg.prune.TxIndex.PruneTo(s.ForwardProgress)
	// Forward stage doesn't write anything before PruneTo point
	// TODO: maybe need do binary search of values in db in this case
	if s.PruneProgress != 0 {
		if err = pruneTxLookup(tx, logPrefix, cfg.tmpdir, s, to, ctx); err != nil {
			return err
		}
	}
	if err = s.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func pruneTxLookup(tx kv.RwTx, logPrefix, tmpDir string, s *PruneState, pruneTo uint64, ctx context.Context) error {
	reader := bytes.NewReader(nil)
	return etl.Transform(logPrefix, tx, kv.BlockBody, kv.TxLookup, tmpDir, func(k, v []byte, next etl.ExtractNextFunc) error {
		body := new(types.BodyForStorage)
		reader.Reset(v)
		if err := rlp.Decode(reader, body); err != nil {
			return fmt.Errorf("rlp decode: %w", err)
		}

		txs, err := rawdb.ReadTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return err
		}
		for _, txn := range txs {
			if err := next(k, txn.Hash().Bytes(), nil); err != nil {
				return err
			}
		}
		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            ctx.Done(),
		ExtractStartKey: dbutils.EncodeBlockNumber(s.ForwardProgress),
		ExtractEndKey:   dbutils.EncodeBlockNumber(pruneTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}
