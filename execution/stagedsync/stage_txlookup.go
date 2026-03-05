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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

type TxLookupCfg struct {
	prune       prune.Mode
	tmpdir      string
	blockReader services.FullBlockReader
}

func StageTxLookupCfg(
	prune prune.Mode,
	tmpdir string,
	blockReader services.FullBlockReader,
) TxLookupCfg {
	return TxLookupCfg{
		prune:       prune,
		tmpdir:      tmpdir,
		blockReader: blockReader,
	}
}

func SpawnTxLookup(s *StageState, tx kv.RwTx, toBlock uint64, cfg TxLookupCfg, ctx context.Context, logger log.Logger) (err error) {
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
	txNumReader := cfg.blockReader.TxnumReader()
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

		firstTxNumInBlock, err := txNumReader.Min(ctx, tx, blocknum)
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
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []any) {
			return []any{"block", binary.BigEndian.Uint64(k)}
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
	return nil
}

func PruneTxLookup(s *PruneState, tx kv.RwTx, cfg TxLookupCfg, ctx context.Context, logger log.Logger) (err error) {
	logPrefix := s.LogPrefix()
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

	if blockFrom >= blockTo {
		return nil
	}

	txNumReader := cfg.blockReader.TxnumReader()
	txFrom, err := txNumReader.Min(ctx, tx, blockFrom)
	if err != nil {
		return fmt.Errorf("txNumReader.Min(%d): %w", blockFrom, err)
	}
	txTo, err := txNumReader.Max(ctx, tx, blockTo)
	if err != nil {
		return fmt.Errorf("txNumReader.Max(%d): %w", blockTo, err)
	}
	if txFrom >= txTo {
		return nil
	}

	prevStat, err := state.GetPruneValProgress(tx, []byte(kv.TxLookup))
	if err != nil {
		return err
	}
	if prevStat != nil && prevStat.TxFrom == txFrom && prevStat.TxTo == txTo && prevStat.ValueProgress == prune.Done {
		return nil
	}
	if prevStat == nil {
		prevStat = &prune.Stat{}
	}

	valsRwCursor, err := tx.RwCursor(kv.TxLookup)
	if err != nil {
		return fmt.Errorf("create TxLookup cursor: %w", err)
	}
	defer valsRwCursor.Close()
	var valsCursor kv.PseudoDupSortRwCursor
	switch c := valsRwCursor.(type) {
	case *mdbx2.MdbxCursor:
		valsCursor = &mdbx2.MdbxCursorPseudoDupSort{MdbxCursor: c}
	default:
		return fmt.Errorf("unexpected cursor type %T for table %s", valsRwCursor, kv.TxLookup)
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	pruneTimeout := 2 * time.Second
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = time.Hour
	}
	pruneCtx, pruneCancel := context.WithTimeout(ctx, pruneTimeout)
	defer pruneCancel()

	pruneStat, err := prune.TableScanningPrune(pruneCtx, logPrefix, "txlookup", txFrom, txTo, 0, 1,
		logEvery, logger, nil, valsCursor, false, prevStat, prune.ValueOffset8StorageMode)
	if err != nil {
		return fmt.Errorf("prune TxLookup: %w", err)
	}
	defer func() {
		pruneStat.TxFrom, pruneStat.TxTo = txFrom, txTo
		if e := state.SavePruneValProgress(tx, kv.TxLookup, pruneStat); e != nil {
			logger.Error("[snapshots] save prune val progress", "name", "txlookup", "err", e)
		}
	}()

	if pruneStat.ValueProgress == prune.Done {
		if err = s.DoneAt(tx, blockTo); err != nil {
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
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []any) {
			return []any{"block", binary.BigEndian.Uint64(k)}
		},
	}, logger)
	if err != nil {
		return err
	}
	return nil
}
