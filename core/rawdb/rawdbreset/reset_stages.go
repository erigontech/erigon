package rawdbreset

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/backup"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func ResetState(db kv.RwDB, ctx context.Context, chain string, tmpDir string) error {
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

	if err := ResetExec(ctx, db, chain, tmpDir); err != nil {
		return err
	}
	return nil
}

func ResetBlocks(tx kv.RwTx, db kv.RoDB, agg *state.AggregatorV3,
	br services.FullBlockReader, bw *blockio.BlockWriter, dirs datadir.Dirs, cc chain.Config, engine consensus.Engine, logger log.Logger) error {
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
		if err := stagedsync.FillDBFromSnapshots("fillind_db_from_snapshots", context.Background(), tx, dirs, br, agg, logger); err != nil {
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
	historyV3 := kvcfg.HistoryV3.FromDB(db)
	if historyV3 { //hist v2 is too big, if you have so much ram, just use `cat mdbx.dat > /dev/null` to warmup
		for _, tbl := range stateHistoryV3Buckets {
			backup.WarmupTable(ctx, db, tbl, log.LvlInfo, backup.ReadAheadThreads)
		}
	}
	return
}

func ResetExec(ctx context.Context, db kv.RwDB, chain string, tmpDir string) (err error) {
	historyV3 := kvcfg.HistoryV3.FromDB(db)
	if historyV3 {
		stateHistoryBuckets = append(stateHistoryBuckets, stateHistoryV3Buckets...)
		stateHistoryBuckets = append(stateHistoryBuckets, stateHistoryV4Buckets...)
	}

	return db.Update(ctx, func(tx kv.RwTx) error {
		if err := clearStageProgress(tx, stages.Execution, stages.HashState, stages.IntermediateHashes); err != nil {
			return err
		}

		if err := backup.ClearTables(ctx, db, tx, stateBuckets...); err != nil {
			return nil
		}
		for _, b := range stateBuckets {
			if err := tx.ClearBucket(b); err != nil {
				return err
			}
		}

		if err := backup.ClearTables(ctx, db, tx, stateHistoryBuckets...); err != nil {
			return nil
		}
		if !historyV3 {
			genesis := core.GenesisBlockByChainName(chain)
			if _, _, err := core.WriteGenesisState(genesis, tx, tmpDir); err != nil {
				return err
			}
		}

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
	kv.TblAccountHistoryKeys, kv.TblAccountIdx, kv.TblAccountHistoryVals,
	kv.TblStorageKeys, kv.TblStorageVals, kv.TblStorageHistoryKeys, kv.TblStorageHistoryVals, kv.TblStorageIdx,
	kv.TblCodeKeys, kv.TblCodeVals, kv.TblCodeHistoryKeys, kv.TblCodeHistoryVals, kv.TblCodeIdx,
	kv.TblAccountHistoryKeys, kv.TblAccountIdx, kv.TblAccountHistoryVals,
	kv.TblStorageHistoryKeys, kv.TblStorageIdx, kv.TblStorageHistoryVals,
	kv.TblCodeHistoryKeys, kv.TblCodeIdx, kv.TblCodeHistoryVals,
	kv.TblLogAddressKeys, kv.TblLogAddressIdx,
	kv.TblLogTopicsKeys, kv.TblLogTopicsIdx,
	kv.TblTracesFromKeys, kv.TblTracesFromIdx,
	kv.TblTracesToKeys, kv.TblTracesToIdx,
}
var stateHistoryV4Buckets = []string{
	kv.TblAccountKeys, kv.TblStorageKeys, kv.TblCodeKeys,
	kv.TblCommitmentKeys, kv.TblCommitmentVals, kv.TblCommitmentHistoryKeys, kv.TblCommitmentHistoryVals, kv.TblCommitmentIdx,
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
func Warmup(ctx context.Context, db kv.RwDB, lvl log.Lvl, stList ...stages.SyncStage) error {
	for _, st := range stList {
		for _, tbl := range Tables[st] {
			backup.WarmupTable(ctx, db, tbl, lvl, backup.ReadAheadThreads)
		}
	}
	return nil
}
