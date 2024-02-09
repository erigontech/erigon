package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"

	"math/big"

	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	rawdbZk "github.com/ledgerwatch/erigon/zk/rawdb"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func SpawnExecuteBlocksStageZk(s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, quiet bool) (err error) {
	if cfg.historyV3 {
		if err = ExecBlockV3(s, u, tx, toBlock, ctx, cfg, initialCycle); err != nil {
			return err
		}
		return nil
	}

	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	shouldShortCircuit, noProgressTo, err := utils.ShouldShortCircuitExecution(tx)
	if err != nil {
		return err
	}
	if shouldShortCircuit {
		return nil
	}

	prevStageProgress, errStart := stages.GetStageProgress(tx, stages.Senders)
	if errStart != nil {
		return errStart
	}
	nextStageProgress, err := stages.GetStageProgress(tx, stages.HashState)
	if err != nil {
		return err
	}
	nextStagesExpectData := nextStageProgress > 0 // Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet

	logPrefix := s.LogPrefix()
	var to = prevStageProgress
	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}
	if to <= s.BlockNumber {
		return nil
	}

	stateStream := !initialCycle && cfg.stateStream && to-s.BlockNumber < stateStreamLimit

	// changes are stored through memory buffer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTx, lastLogTx := uint64(0), uint64(0)
	logTime := time.Now()
	var gas uint64             // used for logs
	var currentStateGas uint64 // used for batch commits of state
	// Transform batch_size limit into Ggas
	gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	var stoppedErr error

	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return fmt.Errorf("failed to create hermezDb: %v", err)
	}

	var batch ethdb.DbWithPendingMutations
	// state is stored through ethdb batches
	batch = olddb.NewHashBatch(tx, quit, cfg.dirs.Tmp)
	// avoids stacking defers within the loop
	defer func() {
		batch.Rollback()
	}()

	if s.BlockNumber == 0 {
		to = noProgressTo
	}

	// limit execution to 100 blocks at a time for faster sync near tip
	// [TODO] remove it after Interhashes  incremental is optimized
	total := to - stageProgress
	if total > cfg.zk.RebuildTreeAfter && total < 100000 {
		to = stageProgress + cfg.zk.RebuildTreeAfter
		total = cfg.zk.RebuildTreeAfter
	}
	if !quiet && to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	initialBlock := stageProgress + 1
	eridb := erigon_db.NewErigonDb(tx)

	prevheaderHash, err := rawdb.ReadCanonicalHash(tx, stageProgress)
	if err != nil {
		return err
	}
	header, err := cfg.blockReader.Header(ctx, tx, prevheaderHash, stageProgress)
	if err != nil {
		return err
	}
	prevBlockHash := header.Root
Loop:
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		stageProgress = blockNum

		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
		}

		preExecuteHeaderHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}

		block, senders, err := cfg.blockReader.BlockWithSenders(ctx, tx, preExecuteHeaderHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			continue
		}

		header, err := cfg.blockReader.Header(ctx, tx, preExecuteHeaderHash, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			log.Error(fmt.Sprintf("[%s] Empty header", logPrefix), "blocknum", blockNum)
			continue
		}

		lastLogTx += uint64(block.Transactions().Len())

		// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
		writeChangeSets := nextStagesExpectData || blockNum > cfg.prune.History.PruneTo(to)
		writeReceipts := nextStagesExpectData || blockNum > cfg.prune.Receipts.PruneTo(to)
		writeCallTraces := nextStagesExpectData || blockNum > cfg.prune.CallTraces.PruneTo(to)
		if err = updateZkEVMBlockCfg(&cfg, hermezDb, logPrefix); err != nil {
			return err
		}
		if err = executeBlockZk(block, &prevBlockHash, header, tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, initialCycle, stateStream, hermezDb); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
				if cfg.hd != nil {
					cfg.hd.ReportBadHeaderPoS(preExecuteHeaderHash, block.ParentHash())
				}
				if cfg.badBlockHalt {
					return err
				}
			}
			u.UnwindTo(blockNum-1, block.Hash())
			break Loop
		}

		shouldUpdateProgress := batch.BatchSize() >= int(cfg.batchSize)
		if shouldUpdateProgress {
			log.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState)
			currentStateGas = 0
			if err = batch.Commit(); err != nil {
				return err
			}
			if err = s.Update(tx, stageProgress); err != nil {
				return err
			}
			if !useExternalTx {
				if err = tx.Commit(); err != nil {
					return err
				}
				tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return err
				}
				// TODO: This creates stacked up deferrals
				defer tx.Rollback()
				eridb = erigon_db.NewErigonDb(tx)
			}
			batch = olddb.NewHashBatch(tx, quit, cfg.dirs.Tmp)
			hermezDb, err = hermez_db.NewHermezDb(tx)
			if err != nil {
				return fmt.Errorf("failed to create hermezDb: %v", err)
			}
		}

		gasUsed := header.GasUsed
		gas = gas + gasUsed
		currentStateGas = currentStateGas + gasUsed

		// TODO: how can we store this data right first time?  Or mop up old data as we're currently duping storage
		/*
				        ,     \    /      ,
				       / \    )\__/(     / \
				      /   \  (_\  /_)   /   \
				 ____/_____\__\@  @/___/_____\____
				|             |\../|              |
				|              \VV/               |
				|       ZKEVM duping storage      |
				|_________________________________|
				 |    /\ /      \\       \ /\    |
				 |  /   V        ))       V   \  |
				 |/     `       //        '     \|
				 `              V                '

			 we need to write the header back to the db at this point as the gas
			 used wasn't available from the data stream, or receipt hash, or bloom, so we're relying on execution to
			 provide it.  We also need to update the canonical hash, so we can retrieve this newly updated header
			 later.
		*/
		headerHash := header.Hash()
		prevBlockHash = header.Root

		rawdb.WriteHeader(tx, header)
		err = rawdb.WriteCanonicalHash(tx, headerHash, blockNum)
		if err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}

		err = eridb.WriteBody(header.Number, headerHash, block.Transactions())
		if err != nil {
			return fmt.Errorf("failed to write body: %v", err)
		}

		// [zkevm] senders were saved in stage_senders for headerHashes based on incomplete headers
		// in stage execute we complete the headers and senders should be moved to the correct headerHash
		// also we should delete other ata based on the old hash, since it is unaccessable now
		if err := rawdb.WriteSenders(tx, headerHash, blockNum, senders); err != nil {
			return fmt.Errorf("failed to write senders: %v", err)
		}

		if err := rawdbZk.DeleteSenders(tx, preExecuteHeaderHash, blockNum); err != nil {
			return fmt.Errorf("failed to delete senders: %v", err)
		}

		if err := rawdbZk.DeleteHeader(tx, preExecuteHeaderHash, blockNum); err != nil {
			return fmt.Errorf("failed to delete header: %v", err)
		}

		// write the new block lookup entries
		rawdb.WriteTxLookupEntries(tx, block)

		select {
		default:
		case <-logEvery.C:
			logBlock, logTx, logTime = logProgress(logPrefix, total, initialBlock, logBlock, logTime, blockNum, logTx, lastLogTx, gas, float64(currentStateGas)/float64(gasState), batch)
			gas = 0
			tx.CollectMetrics()
			Metrics[stages.Execution].Set(blockNum)
		}
	}

	if err = s.Update(batch, stageProgress); err != nil {
		return err
	}
	if err = batch.Commit(); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}

	_, err = rawdb.IncrementStateVersion(tx)
	if err != nil {
		return fmt.Errorf("writing plain state version: %w", err)
	}

	if !useExternalTx {
		log.Info(fmt.Sprintf("[%s] Commiting DB transaction...", logPrefix), "block", stageProgress)

		if err = tx.Commit(); err != nil {
			return err
		}
	}

	if !quiet {
		log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	}
	return stoppedErr
}

func executeBlockZk(
	block *types.Block,
	prevBlockHash *common.Hash,
	header *types.Header,
	tx kv.RwTx,
	batch ethdb.Database,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	initialCycle bool,
	stateStream bool,
	roHermezDb state.ReadOnlyHermezDb,
) error {
	blockNum := block.NumberU64()

	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, initialCycle, stateStream)
	if err != nil {
		return err
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		// return logger.NewJSONFileLogger(&logger.LogConfig{}, txHash.String()), nil
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	var receipts types.Receipts
	var stateSyncReceipt *types.Receipt
	var execRs *core.EphemeralExecResult
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	execRs, err = core.ExecuteBlockEphemerallyZk(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, prevBlockHash, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer, tx, roHermezDb)

	if err != nil {
		return err
	}
	receipts = execRs.Receipts
	stateSyncReceipt = execRs.StateSyncReceipt

	// [zkevm] - add in the state root to the receipts.  As we only have one tx per block
	// for now just add the header root to the receipt
	for _, r := range receipts {
		r.PostState = header.Root.Bytes()
	}

	header.GasUsed = uint64(execRs.GasUsed)
	header.ReceiptHash = types.DeriveSha(receipts)
	header.Bloom = execRs.Bloom

	if writeReceipts {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.Hash(), block.NumberU64(), stateSyncReceipt); err != nil {
				return err
			}
		}
	}

	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	if writeCallTraces {
		return callTracer.WriteToDb(tx, block, *cfg.vmConfig)
	}
	return nil
}

func UnwindExecutionStageZk(u *UnwindState, s *StageState, tx kv.RwTx, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindExecutionStage(u, s, tx, ctx, cfg, initialCycle); err != nil {
		return err
	}
	if err = u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneExecutionStageZk(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if cfg.historyV3 {
		cfg.agg.SetTx(tx)
		if initialCycle {
			if err = cfg.agg.Prune(ctx, ethconfig.HistoryV3AggregationStep/10); err != nil { // prune part of retired data, before commit
				return err
			}
		} else {
			if err = cfg.agg.PruneWithTiemout(ctx, 1*time.Second); err != nil { // prune part of retired data, before commit
				return err
			}
		}
	} else {
		if cfg.prune.History.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
			if err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}

		if cfg.prune.Receipts.Enabled() {
			if err = rawdb.PruneTable(tx, kv.Receipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
			if err = rawdb.PruneTable(tx, kv.BorReceipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxUint32); err != nil {
				return err
			}
			// LogIndex.Prune will read everything what not pruned here
			if err = rawdb.PruneTable(tx, kv.Log, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
		}
		if cfg.prune.CallTraces.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func updateZkEVMBlockCfg(cfg *ExecuteBlockCfg, hermezDb *hermez_db.HermezDb, logPrefix string) error {
	update := func(forkId uint64, forkBlock **big.Int) error {
		if *forkBlock != nil && *forkBlock != big.NewInt(0) {
			return nil
		}
		blockNum, err := hermezDb.GetForkIdBlock(forkId)
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Error getting fork id %v from db: %v", logPrefix, forkId, err))
			return err
		}
		if blockNum != 0 {
			*forkBlock = big.NewInt(0).SetUint64(blockNum)
			log.Info(fmt.Sprintf("[%s] Set execute block cfg, fork id %v, block:%v, ", logPrefix, forkId, blockNum))
		}

		return nil
	}

	if err := update(chain.ForkID5Dragonfruit, &cfg.chainConfig.ForkID5DragonfruitBlock); err != nil {
		return err
	}
	if err := update(chain.ForkID6IncaBerry, &cfg.chainConfig.ForkID6IncaBerryBlock); err != nil {
		return err
	}
	if err := update(chain.ForkID7Etrog, &cfg.chainConfig.ForkID7EtrogBlock); err != nil {
		return err
	}
	return nil
}
