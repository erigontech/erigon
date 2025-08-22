package stages

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	eristate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/sequencer"
	"github.com/erigontech/erigon/zk/witness"
)

type WitnessDb interface {
}

type WitnessCfg struct {
	db              kv.RwDB
	zkCfg           *ethconfig.Zk
	chainConfig     *chain.Config
	engine          consensus.Engine
	blockReader     services.FullBlockReader
	agg             *eristate.Aggregator
	historyV3       bool
	dirs            datadir.Dirs
	forcedContracts []common.Address
	unwindLimit     uint64
}

func StageWitnessCfg(db kv.RwDB, zkCfg *ethconfig.Zk, chainConfig *chain.Config, engine consensus.Engine, blockReader services.FullBlockReader, agg *eristate.Aggregator, historyV3 bool, dirs datadir.Dirs, forcedContracts []common.Address, unwindLimit uint64) WitnessCfg {
	cfg := WitnessCfg{
		db:              db,
		zkCfg:           zkCfg,
		chainConfig:     chainConfig,
		engine:          engine,
		blockReader:     blockReader,
		agg:             agg,
		historyV3:       historyV3,
		dirs:            dirs,
		forcedContracts: forcedContracts,
		unwindLimit:     unwindLimit,
	}

	return cfg
}

// ///////////////////////////////////////////
// 1. Check to which block it should calculate witnesses
// 2. Unwind to that block
// 3. Calculate witnesses up to current executed block
// 4. Delete old block witnesses
// ////////////////////////////////////////////
func SpawnStageWitness(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg WitnessCfg,
) error {
	logPrefix := s.LogPrefix()
	if !cfg.zkCfg.WitnessCacheEnabled {
		return nil
	}
	log.Info(fmt.Sprintf("[%s] Starting witness cache stage", logPrefix))
	if sequencer.IsSequencer() {
		log.Info(fmt.Sprintf("[%s] skipping -- sequencer", logPrefix))
		return nil
	}
	if cfg.chainConfig.IsPmtEnabled(s.BlockNumber) {
		log.Info(fmt.Sprintf("[%s] skipping -- using PMT", logPrefix))
		return nil
	}
	defer log.Info(fmt.Sprintf("[%s] Finished witness cache stage", logPrefix))

	freshTx := false
	if tx == nil {
		freshTx = true
		log.Debug(fmt.Sprintf("[%s] no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw, %w", err)
		}
		defer tx.Rollback()
	}

	stageWitnessProgressBlockNo, err := stages.GetStageProgress(tx, stages.Witness)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	stageInterhashesProgressBlockNo, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	if stageInterhashesProgressBlockNo <= stageWitnessProgressBlockNo {
		log.Info(fmt.Sprintf("[%s] Skipping stage, no new blocks", logPrefix))
		return nil
	}

	reader := hermez_db.NewHermezDbReader(tx)

	var highestVerifiedBatchNo uint64
	highestVerifiedBatch, err := reader.GetLatestVerification()
	if err != nil {
		return fmt.Errorf("GetLatestVerification: %w", err)
	}
	if highestVerifiedBatch == nil {
		highestVerifiedBatchNo = 0
	} else {
		highestVerifiedBatchNo = highestVerifiedBatch.BatchNo
	}

	latestBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	latestBatchEndBlock, err := reader.GetLatestBatchEndBlock()
	if err != nil {
		return fmt.Errorf("GetLatestBatchEndBlock: %w", err)
	}

	if latestBlock > latestBatchEndBlock {
		latestBlock = latestBatchEndBlock
	}

	latestExecutedBatchNo, err := reader.GetBatchNoByL2Block(latestBlock)
	if err != nil {
		return fmt.Errorf("GetBatchNoByL2Block: %w", err)
	}

	if latestExecutedBatchNo == 0 {
		log.Warn(fmt.Sprintf("[%s] No executed batches found", logPrefix))
		return nil
	}

	latestCachedWitnessBatchNo, err := reader.GetLatestCachedWitnessBatchNo()
	if err != nil {
		return fmt.Errorf("GetLatestCachedWitnessBatchNo: %w", err)
	}

	startBatch, endBatch, truncateTo := witness.GetBatchesToCache(highestVerifiedBatchNo, latestExecutedBatchNo, latestCachedWitnessBatchNo, cfg.zkCfg.WitnessCacheBatchAheadOffset, cfg.zkCfg.WitnessCacheBatchBehindOffset)

	hermezDb := hermez_db.NewHermezDb(tx)
	g := witness.NewGenerator(cfg.dirs, cfg.historyV3, cfg.agg, cfg.blockReader, cfg.chainConfig, cfg.zkCfg, cfg.engine, cfg.forcedContracts, cfg.unwindLimit)

	for batchNo := startBatch; batchNo <= endBatch; batchNo++ {
		badBatch, err := reader.GetInvalidBatch(batchNo)
		if err != nil {
			return fmt.Errorf("GetInvalidBatch: %w", err)
		}

		if badBatch {
			log.Warn(fmt.Sprintf("[%s] Bad batch not collecting", logPrefix))
			continue
		}

		blockNumbers, err := reader.GetL2BlockNosByBatch(batchNo)
		if err != nil {
			return fmt.Errorf("GetL2BlockNosByBatch: %w", err)
		}
		if len(blockNumbers) == 0 {
			return fmt.Errorf("no blocks found for batch %d", batchNo)
		}
		var startBlock, endBlock uint64
		for _, blockNumber := range blockNumbers {
			if startBlock == 0 || blockNumber < startBlock {
				startBlock = blockNumber
			}
			if blockNumber > endBlock {
				endBlock = blockNumber
			}
		}

		w, err := g.GetWitnessByBlockRange(tx, ctx, startBlock, endBlock, false, false)
		if err != nil {
			return fmt.Errorf("GetWitnessByBlockRange: %w", err)
		}

		if err = hermezDb.WriteWitnessCache(batchNo, w); err != nil {
			return fmt.Errorf("WriteWitnessCache: %w", err)
		}

		log.Info(fmt.Sprintf("[%s] Witnesses collected", logPrefix))
	}

	log.Info(fmt.Sprintf("[%s] Deleting old witness caches", logPrefix))
	if err = hermezDb.TruncateWitnessCacheBelow(truncateTo); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	if err = stages.SaveStageProgress(tx, stages.Witness, stageInterhashesProgressBlockNo); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "lastBlockNumber", stageInterhashesProgressBlockNo)

	if freshTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}

	return nil
}

func UnwindWitnessStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg WitnessCfg, ctx context.Context) (err error) {
	logPrefix := u.LogPrefix()
	if !cfg.zkCfg.WitnessCacheEnabled {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		if tx, err = cfg.db.BeginRw(ctx); err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	if !cfg.zkCfg.WitnessCacheEnabled {
		return nil
	}
	fromBlock := u.UnwindPoint + 1
	toBlock := u.CurrentBlockNumber
	log.Info(fmt.Sprintf("[%s] Unwinding witness cache stage from block number", logPrefix), "fromBlock", fromBlock, "toBlock", toBlock)
	defer log.Info(fmt.Sprintf("[%s] Unwinding witness cache complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)
	if err := hermezDb.DeleteWitnessCaches(fromBlock, toBlock); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	if err := stages.SaveStageProgress(tx, stages.Witness, fromBlock); err != nil {
		return fmt.Errorf("SaveStageProgress: %w", err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("u.Done: %w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}

func PruneWitnessStage(s *stagedsync.PruneState, tx kv.RwTx, cfg WitnessCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()
	if !cfg.zkCfg.WitnessCacheEnabled {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	log.Info(fmt.Sprintf("[%s] Pruning witnes caches...", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Pruning witnes caches complete", logPrefix))

	hermezDb := hermez_db.NewHermezDb(tx)

	toBlock, err := stages.GetStageProgress(tx, stages.Witness)
	if err != nil {
		return fmt.Errorf("GetStageProgress: %w", err)
	}

	if err := hermezDb.DeleteWitnessCaches(0, toBlock); err != nil {
		return fmt.Errorf("DeleteWitnessCache: %w", err)
	}

	log.Info(fmt.Sprintf("[%s] Saving stage progress", logPrefix), "stageProgress", 0)
	if err := stages.SaveStageProgress(tx, stages.Witness, 0); err != nil {
		return fmt.Errorf("SaveStageProgress: %v", err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx.Commit: %w", err)
		}
	}
	return nil
}
