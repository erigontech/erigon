package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type DecodedStateCfg struct {
	db            kv.TemporalRwDB
	ExecArgs      *exec.ExecArgs
	DecodedConfig decodedstate.Config
	StartBlock    uint64
	ToBlock       uint64
}

func StageDecodedStateCfg(db kv.TemporalRwDB, dirs datadir.Dirs, br services.FullBlockReader,
	cc *chain.Config, engine rules.Engine, genesis *types.Genesis, syncCfg ethconfig.Sync,
	decodedConfig decodedstate.Config) DecodedStateCfg {
	execArgs := &exec.ExecArgs{
		ChainDB:     db,
		BlockReader: br,
		ChainConfig: cc,
		Dirs:        dirs,
		Engine:      engine,
		Genesis:     genesis,
		Workers:     syncCfg.ExecWorkerCount,
	}
	return DecodedStateCfg{
		db:            db,
		ExecArgs:      execArgs,
		DecodedConfig: decodedConfig,
	}
}

func SpawnDecodedState(cfg DecodedStateCfg, ctx context.Context, logger log.Logger) error {
	if !cfg.DecodedConfig.Enabled {
		return errors.New("decoded state replay requires decoded.enabled=true")
	}

	var execProgress uint64
	var startBlock uint64
	if err := cfg.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		var err error
		execProgress, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if execProgress == 0 {
			return errors.New("stage_exec progress is 0. run execution before rebuilding decoded state")
		}
		startBlock, err = decodedReplayStartBlock(tx)
		return err
	}); err != nil {
		return err
	}

	if startBlock == 0 {
		startBlock = 1
	}
	if cfg.StartBlock > 0 && startBlock < cfg.StartBlock {
		startBlock = cfg.StartBlock
	}
	if cfg.ToBlock > 0 && cfg.ToBlock < execProgress {
		execProgress = cfg.ToBlock
	}
	if startBlock > execProgress {
		logger.Info("[stage_decoded_state] already up to date", "startBlock", startBlock, "execProgress", execProgress)
		return nil
	}

	defer cfg.ExecArgs.BlockReader.Snapshots().(*freezeblocks.RoSnapshots).MadvNormal().DisableReadAhead()

	logger.Info("[stage_decoded_state] start",
		"startBlock", startBlock,
		"endBlock", execProgress,
		"fullMode", cfg.DecodedConfig.FullMode,
		"whitelist", len(cfg.DecodedConfig.Whitelist),
	)

	const batchSize = uint64(50_000)
	for startBlock <= execProgress {
		to := min(execProgress+1, startBlock+batchSize)
		if err := decodedStateBatchProduce(ctx, cfg, startBlock, to, "decoded_state", logger); err != nil {
			return err
		}
		startBlock = to
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	chkEvery := time.NewTicker(3 * time.Second)
	defer chkEvery.Stop()

Loop:
	for {
		select {
		case <-ctx.Done():
			logger.Warn("[stage_decoded_state] user has interrupted process, waiting for build & merge files")
		case <-cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).WaitForBuildAndMerge(context.Background()):
			break Loop
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			logger.Info("[stage_decoded_state] building files", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		case <-chkEvery.C:
		}
	}

	tx, err := cfg.db.BeginTemporalRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	latestBlock, err := decodedstate.LatestBlockTx(tx)
	if err != nil {
		return err
	}
	logger.Info("[stage_decoded_state] finish",
		"decoded", tx.Debug().DomainProgress(kv.DecodedStorageDomain),
		"latestBlock", latestBlock,
	)
	return nil
}

func decodedReplayStartBlock(tx kv.TemporalTx) (uint64, error) {
	latestBlock, err := decodedstate.LatestBlockTx(tx)
	if err != nil {
		return 0, err
	}
	if latestBlock == 0 {
		return 1, nil
	}
	return latestBlock + 1, nil
}

func decodedStateBatchProduce(ctx context.Context, cfg DecodedStateCfg, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	if err := cfg.db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		if _, err := tx.PruneSmallBatches(ctx, 10*time.Hour); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	lastProcessedBlock := toBlock - 1
	{
		tx, err := cfg.db.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		doms, err := execctx.NewSharedDomains(ctx, tx, logger)
		if err != nil {
			return err
		}
		defer doms.Close()

		if err := decodedStateBatch(ctx, cfg.DecodedConfig, cfg.ExecArgs, tx, doms, fromBlock, toBlock, logPrefix, logger); err != nil {
			return err
		}

		if err := doms.Flush(ctx, tx); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	var fromStep, toStep kv.Step
	if err := cfg.db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		fromStep = kv.Step(dbstate.AggTx(tx).DbgDomain(kv.DecodedStorageDomain).FirstStepNotInFiles())
		latestTxNum, err := decodedstate.LatestTxNumTx(tx)
		if err != nil {
			return err
		}
		if latestTxNum == 0 {
			latestTxNum = lastProcessedBlock // fallback
		}
		toStep = kv.Step(latestTxNum / agg.StepSize())
		return nil
	}); err != nil {
		return err
	}
	if toStep >= fromStep {
		if err := agg.BuildFiles2(ctx, fromStep, toStep, true); err != nil {
			return err
		}
	}
	if err := cfg.db.Update(ctx, func(tx kv.RwTx) error {
		if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, 10*time.Hour); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func decodedStateBatch(ctx context.Context, decodedCfg decodedstate.Config, cfg *exec.ExecArgs, tx kv.TemporalRwTx, doms *execctx.SharedDomains, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	const logPeriod = 5 * time.Second
	logEvery := time.NewTicker(logPeriod)
	defer logEvery.Stop()

	txNumsReader := cfg.BlockReader.TxnumReader()
	fromTxNum, err := txNumsReader.Min(ctx, tx, fromBlock)
	if err != nil {
		return err
	}
	prevTxNumLog := fromTxNum

	var currentBlock uint64
	var blockEntries []decodedstate.DecodedEntry
	var m runtime.MemStats

	taskSetup := func(task *exec.TxTask) {
		collector := decodedstate.NewCollector(decodedCfg)
		task.DecodedCollector = collector
		task.Hooks = decodedstate.NewTracingHooks(collector, task.Hooks)
	}

	return exec.CustomTraceMapReduceWithTaskSetup(ctx, fromBlock, toBlock, exec.TraceConsumerFunc(
		func(_ *exec.BlockResult, result *exec.TxResult, _ kv.TemporalTx) error {
			txTask := result.Task.(*exec.TxTask)
			if currentBlock == 0 {
				currentBlock = txTask.BlockNumber()
			}
			if txTask.BlockNumber() != currentBlock {
				return fmt.Errorf("decoded replay lost block boundary: current=%d next=%d", currentBlock, txTask.BlockNumber())
			}

			if txTask.DecodedCollector != nil {
				collector, ok := txTask.DecodedCollector.(decodedstate.Collector)
				if !ok {
					return fmt.Errorf("decoded collector has unexpected type %T", txTask.DecodedCollector)
				}
				blockEntries = append(blockEntries, collector.Entries()...)
			}

			select {
			case <-logEvery.C:
				if prevTxNumLog > 0 {
					dbg.ReadMemStats(&m)
					txsPerSec := (txTask.TxNum - prevTxNumLog) / uint64(logPeriod.Seconds())
					logger.Info(fmt.Sprintf("[%s] scanned", logPrefix),
						"block", fmt.Sprintf("%.3fm", float64(txTask.BlockNumber())/1_000_000),
						"tx/s", fmt.Sprintf("%.1fK", float64(txsPerSec)/1_000.0),
						"alloc", common.ByteCount(m.Alloc),
						"sys", common.ByteCount(m.Sys),
					)
				}
				prevTxNumLog = txTask.TxNum
			default:
			}

			if !result.IsBlockEnd() {
				return nil
			}

			if len(blockEntries) > 0 {
				blockEndTxNum := txTask.TxNum
				if err := decodedstate.WriteEntriesToFlatTx(tx, blockEndTxNum, blockEntries); err != nil {
					return err
				}
				if err := decodedstate.AdvanceLatestBlockTx(tx, currentBlock); err != nil {
					return err
				}
				if err := decodedstate.WriteEntriesToDomains(doms, tx, blockEndTxNum, blockEntries); err != nil {
					return err
				}
			} else if err := decodedstate.AdvanceLatestBlockTx(tx, currentBlock); err != nil {
				return err
			}

			blockEntries = blockEntries[:0]
			currentBlock = 0
			return nil
		}), tx, cfg, taskSetup, logger)
}

func StageDecodedStateReset(ctx context.Context, db kv.TemporalRwDB) error {
	return rawdbreset.ResetDecodedState(ctx, db)
}
