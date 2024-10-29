// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/turbo/services"
)

type TxLookupCfg struct {
	db          kv.RwDB
	prune       prune.Mode
	tmpdir      string
	borConfig   *borcfg.BorConfig
	blockReader services.FullBlockReader
}

func StageTxLookupCfg(
	db kv.RwDB,
	prune prune.Mode,
	tmpdir string,
	borConfigInterface chain.BorConfig,
	blockReader services.FullBlockReader,
) TxLookupCfg {
	var borConfig *borcfg.BorConfig
	if borConfigInterface != nil {
		borConfig = borConfigInterface.(*borcfg.BorConfig)
	}

	return TxLookupCfg{
		db:          db,
		prune:       prune,
		tmpdir:      tmpdir,
		borConfig:   borConfig,
		blockReader: blockReader,
	}
}

func SpawnTxLookup(s *StageState, tx kv.RwTx, toBlock uint64, cfg TxLookupCfg, ctx context.Context, logger log.Logger) (err error) {
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
	if s.BlockNumber > endBlock { // Erigon will self-heal (download missed blocks) eventually
		return nil
	}
	if toBlock > 0 {
		endBlock = min(endBlock, toBlock)
	}

	startBlock := s.BlockNumber
	if cfg.prune.History.Enabled() {
		pruneTo := cfg.prune.History.PruneTo(endBlock)
		if startBlock < pruneTo {
			startBlock = pruneTo
			if err = s.UpdatePrune(tx, pruneTo); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
				return err
			}
		}
	}

	if cfg.blockReader.FrozenBlocks() > startBlock {
		// Snapshot .idx files already have TxLookup index - then no reason iterate over them here
		startBlock = cfg.blockReader.FrozenBlocks()
		if err = s.UpdatePrune(tx, startBlock); err != nil { // prune func of this stage will use this value to prevent all ancient blocks traversal
			return err
		}
	}

	if startBlock > 0 {
		startBlock++
	}
	// etl.Transform uses ExtractEndKey as exclusive bound, therefore endBlock + 1
	if err = txnLookupTransform(logPrefix, tx, startBlock, endBlock+1, ctx, cfg, logger); err != nil {
		return fmt.Errorf("txnLookupTransform: %w", err)
	}

	if cfg.borConfig != nil {
		if err = borTxnLookupTransform(logPrefix, tx, startBlock, endBlock+1, ctx.Done(), cfg, logger); err != nil {
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
func txnLookupTransform(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) (err error) {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), libcommon.CastToHash(v)
		body, err := cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blocknum)
		if err != nil {
			return err
		}
		if body == nil { // tolerate such an error, because likely it's corner-case - and not critical one
			log.Warn(fmt.Sprintf("[%s] transform: empty block body %d, hash %x", logPrefix, blocknum, v))
			return nil
		}

		blockNumBytes := bigNum.SetUint64(blocknum).Bytes()
		for _, txn := range body.Transactions {
			if err := next(k, txn.Hash().Bytes(), blockNumBytes); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            ctx.Done(),
		ExtractStartKey: hexutility.EncodeTs(blockFrom),
		ExtractEndKey:   hexutility.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
}

// txnLookupTransform - [startKey, endKey)
func borTxnLookupTransform(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, quitCh <-chan struct{}, cfg TxLookupCfg, logger log.Logger) error {
	bigNum := new(big.Int)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.BorTxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), libcommon.CastToHash(v)
		blockNumBytes := bigNum.SetUint64(blocknum).Bytes()

		// we add state sync transactions every bor Sprint amount of blocks
		if cfg.borConfig.IsSprintStart(blocknum) && rawdb.HasBorReceipts(tx, blocknum) {
			txnHash := bortypes.ComputeBorTxHash(blocknum, blockHash)
			if err := next(k, txnHash.Bytes(), blockNumBytes); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: hexutility.EncodeTs(blockFrom),
		ExtractEndKey:   hexutility.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
}

func UnwindTxLookup(u *UnwindState, s *StageState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context, logger log.Logger) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files

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
	smallestInDB := cfg.blockReader.FrozenBlocks()
	blockFrom, blockTo = max(blockFrom, smallestInDB), max(blockTo, smallestInDB)

	// etl.Transform uses ExtractEndKey as exclusive bound, therefore blockTo + 1
	if _, err := deleteTxLookupRange(tx, s.LogPrefix(), blockFrom, blockTo+1, ctx, cfg, logger); err != nil {
		return fmt.Errorf("unwind TxLookUp: %w", err)
	}
	if cfg.borConfig != nil {
		if err := deleteBorTxLookupRange(tx, s.LogPrefix(), blockFrom, blockTo+1, ctx, cfg, logger); err != nil {
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

func PruneTxLookup(s *PruneState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context, logger log.Logger) (err error) {
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
	var blockTo uint64

	var pruneBor bool
	// Forward stage doesn't write anything before PruneTo point
	if cfg.prune.History.Enabled() {
		blockTo = cfg.prune.History.PruneTo(s.ForwardProgress)
		pruneBor = true
	} else {
		blockTo = cfg.blockReader.CanPruneTo(s.ForwardProgress)
	}
	// can't prune much here: because tx_lookup index has crypto-hashed-keys, and 1 block producing hundreds of deletes
	blockTo = min(blockTo, blockFrom+10)

	pruneTimeout := 250 * time.Millisecond
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = time.Hour
	}

	if blockFrom < blockTo {
		t := time.Now()
		deletedTotal := 0
		for bn := blockFrom; bn < blockTo; bn++ {
			deleted, err := deleteTxLookupRange(tx, logPrefix, bn, bn+1, ctx, cfg, logger)
			if err != nil {
				return fmt.Errorf("prune TxLookUp: %w", err)
			}
			deletedTotal += deleted

			if time.Since(t) > pruneTimeout {
				break
			}
		}
		log.Warn("[dbg] TxLookup", "pruned_txs", deletedTotal, "took", time.Since(t), "cfg.prune.History.Enabled()", cfg.prune.History.Enabled(), "cfg.prune.History.PruneTo(s.ForwardProgress)", cfg.prune.History.PruneTo(s.ForwardProgress), "cfg.blockReader.CanPruneTo(s.ForwardProgress)", cfg.blockReader.CanPruneTo(s.ForwardProgress))

		if cfg.borConfig != nil && pruneBor {
			if err = deleteBorTxLookupRange(tx, logPrefix, blockFrom, blockTo, ctx, cfg, logger); err != nil {
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
func deleteTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) (deleted int, err error) {
	err = etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), libcommon.CastToHash(v)
		body, err := cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blocknum)
		if err != nil {
			return err
		}
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
		ExtractStartKey: hexutility.EncodeTs(blockFrom),
		ExtractEndKey:   hexutility.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// deleteTxLookupRange - [blockFrom, blockTo)
func deleteBorTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) error {
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.BorTxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), libcommon.CastToHash(v)

		borTxHash := bortypes.ComputeBorTxHash(blocknum, blockHash)
		if err := next(k, borTxHash.Bytes(), nil); err != nil {
			return err
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            ctx.Done(),
		ExtractStartKey: hexutility.EncodeTs(blockFrom),
		ExtractEndKey:   hexutility.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
}
