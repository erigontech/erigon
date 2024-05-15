package rawdbreset

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/backup"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func ResetState(db kv.RwDB, ctx context.Context, chain string, tmpDir string, logger log.Logger) error {
	// don't reset senders here
	if err := Reset(ctx, db, stages.HashState); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.IntermediateHashes); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.AccountHistoryIndex, stages.StorageHistoryIndex); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.LogIndex); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.CallTraces); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetTxLookup); err != nil {
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

	if br.FreezingCfg().Enabled && br.FrozenBlocks() > 0 {
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
		if err := clearStageProgress(tx, stages.Execution, stages.HashState, stages.IntermediateHashes); err != nil {
			return err
		}

		if err := backup.ClearTables(ctx, db, tx, cleanupList...); err != nil {
			return nil
		}
		v3db := db.(*temporal.DB)
		agg := v3db.Agg()
		aggTx := agg.BeginFilesRo()
		defer aggTx.Close()
		doms, err := state.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer doms.Close()

		_ = stages.SaveStageProgress(tx, stages.Execution, doms.BlockNum())
		mxs := agg.EndTxNumMinimax() / agg.StepSize()
		if mxs > 0 {
			mxs--
		}
		log.Info("[reset] exec", "toBlock", doms.BlockNum(), "toTxNum", doms.TxNum(), "maxStepInFiles", mxs)

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
	stages.HashState:           {kv.HashedAccounts, kv.HashedStorage, kv.ContractCode},
	stages.IntermediateHashes:  {kv.TrieOfAccounts, kv.TrieOfStorage},
	stages.CallTraces:          {kv.CallFromIndex, kv.CallToIndex},
	stages.LogIndex:            {kv.LogAddressIndex, kv.LogTopicIndex},
	stages.AccountHistoryIndex: {kv.E2AccountsHistory},
	stages.StorageHistoryIndex: {kv.E2StorageHistory},
	stages.CustomTrace:         {},
	stages.Finish:              {},
}
var stateBuckets = []string{
	kv.PlainState, kv.HashedAccounts, kv.HashedStorage, kv.TrieOfAccounts, kv.TrieOfStorage,
	kv.Epoch, kv.PendingEpoch, kv.BorReceipts,
	kv.Code, kv.PlainContractCode, kv.ContractCode, kv.IncarnationMap,
}
var stateHistoryBuckets = []string{
	kv.AccountChangeSet,
	kv.StorageChangeSet,
	kv.Receipts,
	kv.Log,
	kv.CallTraceSet,
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
	kv.TblAccountKeys, kv.TblStorageKeys, kv.TblCodeKeys, kv.TblCommitmentKeys,
	kv.TblAccountVals, kv.TblStorageVals, kv.TblCodeVals, kv.TblCommitmentVals,
	kv.TblCommitmentHistoryKeys, kv.TblCommitmentHistoryVals, kv.TblCommitmentIdx,
	//kv.TblGasUsedHistoryKeys, kv.TblGasUsedHistoryVals, kv.TblGasUsedIdx,
	kv.TblPruningProgress,
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
