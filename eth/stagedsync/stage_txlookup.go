package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type TxLookupCfg struct {
	db        kv.RwDB
	prune     prune.Mode
	tmpdir    string
	snapshots *snapshotsync.RoSnapshots
	isBor     bool
}

func StageTxLookupCfg(
	db kv.RwDB,
	prune prune.Mode,
	tmpdir string,
	snapshots *snapshotsync.RoSnapshots,
	isBor bool,
) TxLookupCfg {
	return TxLookupCfg{
		db:        db,
		prune:     prune,
		tmpdir:    tmpdir,
		snapshots: snapshots,
		isBor:     isBor,
	}
}

func SpawnTxLookup(s *StageState, tx kv.RwTx, toBlock uint64, cfg TxLookupCfg, ctx context.Context) (err error) {
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
	if toBlock > 0 {
		endBlock = min(endBlock, toBlock)
	}

	startBlock := s.BlockNumber
	if cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		if cfg.snapshots.BlocksAvailable() > startBlock {
			// Snapshot .idx files already have TxLookup index - then no reason iterate over them here
			startBlock = cfg.snapshots.BlocksAvailable()
			if err = s.UpdatePrune(tx, startBlock); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
				return err
			}
		}
	} else if cfg.prune.TxIndex.Enabled() {
		pruneTo := cfg.prune.TxIndex.PruneTo(endBlock)
		if startBlock < pruneTo {
			startBlock = pruneTo
			if err = s.UpdatePrune(tx, pruneTo); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
				return err
			}
		}
	}

	if startBlock > 0 {
		startBlock++
	}
	startKey := dbutils.EncodeBlockNumber(startBlock)
	if err = txnLookupTransform(logPrefix, tx, startKey, dbutils.EncodeBlockNumber(endBlock), quitCh, cfg); err != nil {
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

func txnLookupTransform(logPrefix string, tx kv.RwTx, startKey, endKey []byte, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("empty block body %d, hash %x", blocknum, v)
		}

		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), bigNum.SetUint64(blocknum).Bytes()); err != nil {
				return err
			}
		}

		if cfg.isBor {
			borPrefix := []byte("matic-bor-receipt-")
			if err := next(k, crypto.Keccak256(append(append(borPrefix, k...), v...)), bigNum.SetUint64(blocknum).Bytes()); err != nil {
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

	// end key needs to be s.BlockNumber + 1 and not s.BlockNumber, because
	// the keys in BlockBody table always have hash after the block number
	if err := deleteTxLookupRange(tx, s.LogPrefix(), u.UnwindPoint+1, s.BlockNumber+1, ctx, cfg); err != nil {
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

func PruneTxLookup(s *PruneState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	blockFrom := s.PruneProgress

	// Forward stage doesn't write anything before PruneTo point
	if cfg.prune.TxIndex.Enabled() {
		to := cfg.prune.TxIndex.PruneTo(s.ForwardProgress)
		if blockFrom < to {
			if err = deleteTxLookupRange(tx, logPrefix, blockFrom, to, ctx, cfg); err != nil {
				return err
			}
		}
		if err = s.DoneAt(tx, to); err != nil {
			return err
		}
	} else if cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		to := snapshotsync.CanDeleteTo(s.ForwardProgress, cfg.snapshots)
		if blockFrom < to {
			if err = deleteTxLookupRange(tx, logPrefix, blockFrom, to, ctx, cfg); err != nil {
				return err
			}
		}
		if err = s.DoneAt(tx, to); err != nil {
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

// deleteTxLookupRange [from,to)
func deleteTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg) error {
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum := binary.BigEndian.Uint64(k)
		blockHash := common.BytesToHash(v)
		body := rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("empty block body %d, hash %x", blocknum, v)
		}

		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), nil); err != nil {
				return err
			}
		}
		if cfg.isBor {
			borPrefix := []byte("matic-bor-receipt-")
			if err := next(k, crypto.Keccak256(append(append(borPrefix, k...), v...)), nil); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            ctx.Done(),
		ExtractStartKey: dbutils.EncodeBlockNumber(blockFrom),
		ExtractEndKey:   dbutils.EncodeBlockNumber(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}
