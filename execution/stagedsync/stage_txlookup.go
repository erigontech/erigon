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
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
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

	if err = s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	if dbg.AssertEnabled {
		err = txnLookupIntegrity(logPrefix, tx, startBlock, endBlock, ctx, cfg, logger)
		if err != nil {
			return fmt.Errorf("txnLookupIntegrity: %w", err)
		}
	}
	return nil
}

// txnLookupTransform - [startKey, endKey)
func txnLookupTransform(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) (err error) {
	txNumReader := cfg.blockReader.TxnumReader(context.TODO())
	data := make([]byte, 16)
	return etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)
		body, err := cfg.blockReader.BodyWithTransactions(ctx, tx, blockHash, blocknum)
		if err != nil {
			return err
		}
		if body == nil { // tolerate such an error, because likely it's corner-case - and not critical one
			log.Warn(fmt.Sprintf("[%s] transform: empty block body %d, hash %x", logPrefix, blocknum, v))
			return nil
		}

		firstTxNumInBlock, err := txNumReader.Min(tx, blocknum)
		if err != nil {
			return err
		}

		binary.BigEndian.PutUint64(data[:8], blocknum)

		for i, txn := range body.Transactions {
			binary.BigEndian.PutUint64(data[8:], firstTxNumInBlock+uint64(i)+1)
			if err := next(k, txn.Hash().Bytes(), data); err != nil {
				return err
			}
		}

		return nil
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            ctx.Done(),
		ExtractStartKey: hexutil.EncodeTs(blockFrom),
		ExtractEndKey:   hexutil.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
}

// txnLookupTransform - [startKey, endKey)
func txnLookupIntegrity(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) (err error) {
	for i := blockFrom; i < blockTo; i++ {
		var blockHeader *types.Header
		blockHash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return err
		}
		emptyHash := common.Hash{}
		if blockHash != emptyHash {
			blockHeader = rawdb.ReadHeader(tx, blockHash, i)
			if blockHeader == nil {
				logger.Warn(fmt.Sprintf("[%s] txnLookup integrity: empty block %d", logPrefix, i))
				return fmt.Errorf("[%s] txnLookup integrity: header not found in db block: %d", logPrefix, i)
			}
		} else {
			return fmt.Errorf("[%s] txnLookup integrity: hash not found in db block: %d", logPrefix, i)
		}

		calcHash := blockHeader.CalcHash()
		if blockHash.Cmp(calcHash) != 0 {
			logger.Error(fmt.Sprintf("[%s] txnLookup calc hash mismatch: block %d, currentHash %x calcHash %x", logPrefix, i, blockHash, calcHash))
			return fmt.Errorf("[%s] txnLookup calc hash mismatch: block %d, currentHash %x calcHash %x", logPrefix, i, blockHash, calcHash)
		}
	}
	return nil
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
	if err := deleteTxLookupRange(tx, s.LogPrefix(), blockFrom, blockTo+1, ctx, cfg, logger); err != nil {
		return fmt.Errorf("unwind TxLookUp: %w", err)
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
	if blockFrom == 0 {
		firstNonGenesisHeader, err := rawdbv3.SecondKey(tx, kv.Headers)
		if err != nil {
			return err
		}
		if firstNonGenesisHeader != nil {
			blockFrom = binary.BigEndian.Uint64(firstNonGenesisHeader)
		} else {
			execProgress, err := stageProgress(tx, nil, stages.Senders)
			if err != nil {
				return err
			}
			blockFrom = execProgress
		}
	}
	var blockTo uint64

	// Forward stage doesn't write anything before PruneTo point
	if cfg.prune.History.Enabled() {
		blockTo = cfg.prune.History.PruneTo(s.ForwardProgress)
	} else {
		blockTo = cfg.blockReader.CanPruneTo(s.ForwardProgress)
	}

	pruneTimeout := time.Hour // aggressive pruning at non-chain-tip
	if !s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = 250 * time.Millisecond
		// can't prune much on non-chain-tip: because tx_lookup has crypto-hashed-keys. 1 block producing hundreds of random deletes: ~2pages updated per delete
		blockTo = min(blockTo, blockFrom+10)
	}

	if blockFrom < blockTo {
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

		t := time.Now()
		var pruneBlockNum = blockFrom
		for ; pruneBlockNum < blockTo; pruneBlockNum++ {
			select {
			case <-logEvery.C:
				logger.Info(fmt.Sprintf("[%s] progress", logPrefix), "blockNum", pruneBlockNum)
			default:
			}

			err = deleteTxLookupRange(tx, logPrefix, pruneBlockNum, pruneBlockNum+1, ctx, cfg, logger)
			if err != nil {
				return fmt.Errorf("prune TxLookUp: %w", err)
			}

			if time.Since(t) > pruneTimeout {
				break
			}
		}
		if err = s.DoneAt(tx, pruneBlockNum); err != nil {
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
func deleteTxLookupRange(tx kv.RwTx, logPrefix string, blockFrom, blockTo uint64, ctx context.Context, cfg TxLookupCfg, logger log.Logger) (err error) {
	err = etl.Transform(logPrefix, tx, kv.HeaderCanonical, kv.TxLookup, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		blocknum, blockHash := binary.BigEndian.Uint64(k), common.CastToHash(v)
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
		ExtractStartKey: hexutil.EncodeTs(blockFrom),
		ExtractEndKey:   hexutil.EncodeTs(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
	if err != nil {
		return err
	}
	return nil
}
