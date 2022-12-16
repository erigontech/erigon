package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type TxLookupCfg struct {
	db        kv.RwDB
	prune     prune.Mode
	tmpdir    string
	snapshots *snapshotsync.RoSnapshots
	borConfig *params.BorConfig
}

func StageTxLookupCfg(
	db kv.RwDB,
	prune prune.Mode,
	tmpdir string,
	snapshots *snapshotsync.RoSnapshots,
	borConfig *params.BorConfig,
) TxLookupCfg {
	return TxLookupCfg{
		db:        db,
		prune:     prune,
		tmpdir:    tmpdir,
		snapshots: snapshots,
		borConfig: borConfig,
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
		endBlock = cmp.Min(endBlock, toBlock)
	}

	startBlock := s.BlockNumber
	if cfg.prune.TxIndex.Enabled() {
		pruneTo := cfg.prune.TxIndex.PruneTo(endBlock)
		if startBlock < pruneTo {
			startBlock = pruneTo
			if err = s.UpdatePrune(tx, pruneTo); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
				return err
			}
		}
	}
	if cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		if cfg.snapshots.BlocksAvailable() > startBlock {
			// Snapshot .idx files already have TxLookup index - then no reason iterate over them here
			startBlock = cfg.snapshots.BlocksAvailable()
			if err = s.UpdatePrune(tx, startBlock); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
				return err
			}
		}
	}

	if startBlock > 0 {
		startBlock++
	}
	// etl.Transform uses ExtractEndKey as exclusive bound, therefore endBlock + 1
	if err = txnLookupTransform(logPrefix, tx, startBlock, endBlock+1, quitCh, cfg); err != nil {
		return fmt.Errorf("txnLookupTransform: %w", err)
	}

	if cfg.borConfig != nil {
		if err = borTxnLookupTransform(logPrefix, tx, startBlock, endBlock+1, quitCh, cfg); err != nil {
			return fmt.Errorf("borTxnLookupTransform: %w", err)
		}
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

// txnLookupTransform - [startKey, endKey)
func txnLookupTransform(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)
		body := rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, blocknum)
		if body == nil {
			return fmt.Errorf("transform: empty block body %d, hash %x", blocknum, v)
		}

		blockNumBytes := bigNum.SetUint64(blocknum).Bytes()
		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), blockNumBytes); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: dbutils.EncodeBlockNumber(blockFrom),
		ExtractEndKey:   dbutils.EncodeBlockNumber(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}

// txnLookupTransform - [startKey, endKey)
func borTxnLookupTransform(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.BorTxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)
		blockNumBytes := bigNum.SetUint64(blocknum).Bytes()

		// we add state sync transactions every bor Sprint amount of blocks
		if blocknum%cfg.borConfig.CalculateSprint(blocknum) == 0 && rawdb.HasBorReceipts(tx, blocknum) {
			txnHash := types.ComputeBorTxHash(blocknum, blockHash)
			if err := next(k, txnHash.Bytes(), blockNumBytes); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: dbutils.EncodeBlockNumber(blockFrom),
		ExtractEndKey:   dbutils.EncodeBlockNumber(blockTo),
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
	blockFrom, blockTo := u.UnwindPoint+1, s.BlockNumber+1
	if cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		smallestInDB := cfg.snapshots.BlocksAvailable()
		blockFrom, blockTo = cmp.Max(blockFrom, smallestInDB), cmp.Max(blockTo, smallestInDB)
	}
	// etl.Transform uses ExtractEndKey as exclusive bound, therefore blockTo + 1
	if err := deleteTxLookupRange(tx, s.LogPrefix(), blockFrom, blockTo+1, ctx, cfg); err != nil {
		return fmt.Errorf("unwind TxLookUp: %w", err)
	}
	if cfg.borConfig != nil {
		if err := deleteBorTxLookupRange(tx, s.LogPrefix(), blockFrom, blockTo+1, ctx, cfg); err != nil {
			return fmt.Errorf("unwind BorTxLookUp: %w", err)
		}
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

func PruneTxLookup(s *PruneState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	blockFrom, blockTo := s.PruneProgress, uint64(0)

	var pruneBor bool

	// Forward stage doesn't write anything before PruneTo point
	if cfg.prune.TxIndex.Enabled() {
		blockTo = cfg.prune.TxIndex.PruneTo(s.ForwardProgress)
		pruneBor = true
	} else if cfg.snapshots != nil && cfg.snapshots.Cfg().Enabled {
		blockTo = snapshotsync.CanDeleteTo(s.ForwardProgress, cfg.snapshots)
	}

	if !initialCycle { // limit time for pruning
		blockTo = cmp.Min(blockTo, blockFrom+100)
	}

	if blockFrom < blockTo {
		if err = deleteTxLookupRange(tx, logPrefix, blockFrom, blockTo, ctx, cfg); err != nil {
			return fmt.Errorf("prune TxLookUp: %w", err)
		}

		if cfg.borConfig != nil && pruneBor {
			if err = deleteBorTxLookupRange(tx, logPrefix, blockFrom, blockTo, ctx, cfg); err != nil {
				return fmt.Errorf("prune BorTxLookUp: %w", err)
			}
		}

		if err = s.DoneAt(tx, blockTo); err != nil {
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

// deleteTxLookupRange - [blockFrom, blockTo)
func deleteTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg) error {
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)
		body := rawdb.ReadCanonicalBodyWithTransactions(tx, blockHash, blocknum)
		if body == nil {
			log.Debug("TxLookup pruning, empty block body", "height", blocknum)
			return nil
		}

		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), nil); err != nil {
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

// deleteTxLookupRange - [blockFrom, blockTo)
func deleteBorTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg) error {
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.BorTxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)

		borTxHash := types.ComputeBorTxHash(blocknum, blockHash)
		if err := next(k, borTxHash.Bytes(), nil); err != nil {
			return err
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
