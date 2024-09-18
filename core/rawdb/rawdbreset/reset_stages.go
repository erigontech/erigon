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

package rawdbreset

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/backup"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
)

func ResetState(db kv.RwDB, ctx context.Context, chain string, tmpDir string, logger log.Logger) error {
	// don't reset senders here
	if err := db.Update(ctx, ResetTxLookup); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.CustomTrace); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.Finish); err != nil {
		return err
	}

	if err := ResetExec(ctx, db, chain, tmpDir, logger); err != nil {
		return err
	}
	return nil
}

func ResetBlocks(tx kv.RwTx, db kv.RoDB, agg *state.Aggregator, br services.FullBlockReader, bw *blockio.BlockWriter, dirs datadir.Dirs, cc chain.Config, logger log.Logger) error {
	// keep Genesis
	if err := rawdb.TruncateBlocks(context.Background(), tx, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Snapshots, 0); err != nil {
		return fmt.Errorf("saving Snapshots progress failed: %w", err)
	}

	// remove all canonical markers from this point
	if err := rawdb.TruncateCanonicalHash(tx, 1, false); err != nil {
		return err
	}
	if err := rawdb.TruncateTd(tx, 1); err != nil {
		return err
	}
	hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return err
	}
	if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}

	// ensure no garbage records left (it may happen if db is inconsistent)
	if err := bw.TruncateBodies(db, tx, 2); err != nil {
		return err
	}

	if br.FrozenBlocks() > 0 {
		logger.Info("filling db from snapshots", "blocks", br.FrozenBlocks())
		if err := stagedsync.FillDBFromSnapshots("filling_db_from_snapshots", context.Background(), tx, dirs, br, agg, logger); err != nil {
			return err
		}
		_ = stages.SaveStageProgress(tx, stages.Snapshots, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Headers, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Bodies, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Senders, br.FrozenBlocks())
	}

	return nil
}
func ResetBorHeimdall(ctx context.Context, tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.BorEventNums); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.BorEvents); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.BorSpans); err != nil {
		return err
	}
	return clearStageProgress(tx, stages.BorHeimdall)
}

func ResetPolygonSync(tx kv.RwTx, db kv.RoDB, agg *state.Aggregator, br services.FullBlockReader, bw *blockio.BlockWriter, dirs datadir.Dirs, cc chain.Config, logger log.Logger) error {
	tables := []string{
		kv.BorEventNums,
		kv.BorEvents,
		kv.BorSpans,
		kv.BorEventProcessedBlocks,
		kv.BorMilestones,
		kv.BorCheckpoints,
		kv.BorProducerSelections,
	}

	for _, table := range tables {
		if err := tx.ClearBucket(table); err != nil {
			return err
		}
	}

	if err := ResetBlocks(tx, db, agg, br, bw, dirs, cc, logger); err != nil {
		return err
	}

	return stages.SaveStageProgress(tx, stages.PolygonSync, 0)
}

func ResetSenders(ctx context.Context, db kv.RwDB, tx kv.RwTx) error {
	if err := backup.ClearTables(ctx, db, tx, kv.Senders); err != nil {
		return nil
	}
	return clearStageProgress(tx, stages.Senders)
}

func WarmupExec(ctx context.Context, db kv.RwDB) (err error) {
	for _, tbl := range stateBuckets {
		backup.WarmupTable(ctx, db, tbl, log.LvlInfo, backup.ReadAheadThreads)
	}
	for _, tbl := range stateHistoryV3Buckets {
		backup.WarmupTable(ctx, db, tbl, log.LvlInfo, backup.ReadAheadThreads)
	}
	return
}

func ResetExec(ctx context.Context, db kv.RwDB, chain string, tmpDir string, logger log.Logger) (err error) {
	cleanupList := make([]string, 0)
	cleanupList = append(cleanupList, stateBuckets...)
	cleanupList = append(cleanupList, stateHistoryBuckets...)
	cleanupList = append(cleanupList, stateHistoryV3Buckets...)
	cleanupList = append(cleanupList, stateV3Buckets...)

	return db.Update(ctx, func(tx kv.RwTx) error {
		if err := clearStageProgress(tx, stages.Execution); err != nil {
			return err
		}

		if err := backup.ClearTables(ctx, db, tx, cleanupList...); err != nil {
			return nil
		}
		// corner case: state files may be ahead of block files - so, can't use SharedDomains here. juts leave progress as 0.
		return nil
	})
}

func ResetTxLookup(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.TxLookup); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

var Tables = map[stages.SyncStage][]string{
	stages.CustomTrace: {
		kv.TblReceiptKeys, kv.TblReceiptVals, kv.TblReceiptHistoryKeys, kv.TblReceiptHistoryVals, kv.TblReceiptIdx,
	},
	stages.Finish: {},
}
var stateBuckets = []string{
	kv.Epoch, kv.PendingEpoch, kv.BorReceipts,
	kv.Code, kv.PlainContractCode, kv.ContractCode, kv.IncarnationMap,
}
var stateHistoryBuckets = []string{
	kv.Receipts,
}
var stateHistoryV3Buckets = []string{
	kv.TblAccountHistoryKeys, kv.TblAccountHistoryVals, kv.TblAccountIdx,
	kv.TblStorageHistoryKeys, kv.TblStorageHistoryVals, kv.TblStorageIdx,
	kv.TblCodeHistoryKeys, kv.TblCodeHistoryVals, kv.TblCodeIdx,
	kv.TblLogAddressKeys, kv.TblLogAddressIdx,
	kv.TblLogTopicsKeys, kv.TblLogTopicsIdx,
	kv.TblTracesFromKeys, kv.TblTracesFromIdx,
	kv.TblTracesToKeys, kv.TblTracesToIdx,
}
var stateV3Buckets = []string{
	kv.TblAccountKeys, kv.TblStorageKeys, kv.TblCodeKeys, kv.TblCommitmentKeys, kv.TblReceiptKeys,
	kv.TblAccountVals, kv.TblStorageVals, kv.TblCodeVals, kv.TblCommitmentVals, kv.TblReceiptVals,
	kv.TblCommitmentHistoryKeys, kv.TblCommitmentHistoryVals, kv.TblCommitmentIdx,
	kv.TblReceiptHistoryKeys, kv.TblReceiptHistoryVals, kv.TblReceiptIdx,
	kv.TblPruningProgress,
	kv.ChangeSets3,
}

func clearStageProgress(tx kv.RwTx, stagesList ...stages.SyncStage) error {
	for _, stage := range stagesList {
		if err := stages.SaveStageProgress(tx, stage, 0); err != nil {
			return err
		}
		if err := stages.SaveStagePruneProgress(tx, stage, 0); err != nil {
			return err
		}
	}
	return nil
}

func Reset(ctx context.Context, db kv.RwDB, stagesList ...stages.SyncStage) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		for _, st := range stagesList {
			if err := backup.ClearTables(ctx, db, tx, Tables[st]...); err != nil {
				return err
			}
			if err := clearStageProgress(tx, stagesList...); err != nil {
				return err
			}
		}
		return nil
	})
}

func ResetPruneAt(ctx context.Context, db kv.RwDB, stage stages.SyncStage) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		return stages.SaveStagePruneProgress(tx, stage, 0)
	})
}

func Warmup(ctx context.Context, db kv.RwDB, lvl log.Lvl, stList ...stages.SyncStage) error {
	for _, st := range stList {
		for _, tbl := range Tables[st] {
			backup.WarmupTable(ctx, db, tbl, lvl, backup.ReadAheadThreads)
		}
	}
	return nil
}
