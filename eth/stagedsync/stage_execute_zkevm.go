package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/wrap"

	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/membatch"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"

	"os"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	rawdbZk "github.com/ledgerwatch/erigon/zk/rawdb"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func SpawnExecuteBlocksStageZk(s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	if cfg.historyV3 {
		if err = ExecBlockV3(s, u, wrap.TxContainer{Tx: tx}, toBlock, ctx, cfg, initialCycle, log.New()); err != nil {
			return err
		}
		return nil
	}

	///// DEBUG BISECT /////
	highestBlockExecuted := s.BlockNumber
	defer func() {
		if cfg.zk.DebugLimit > 0 {
			if err != nil {
				log.Error("Execution Failed", "err", err, "block", highestBlockExecuted)
				os.Exit(2)
			}
		}
	}()
	///// DEBUG BISECT /////

	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	nextStageProgress, err := stages.GetStageProgress(tx, stages.HashState)
	if err != nil {
		return err
	}
	nextStagesExpectData := nextStageProgress > 0 // Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet

	var currentStateGas uint64 // used for batch commits of state
	// Transform batch_size limit into Ggas
	gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	hermezDb := hermez_db.NewHermezDb(tx)

	var batch kv.PendingMutations
	// state is stored through ethdb batches
	batch = membatch.NewHashBatch(tx, quit, cfg.dirs.Tmp, log.New())
	// avoids stacking defers within the loop
	defer func() {
		batch.Close()
	}()

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, hermezDb, s.LogPrefix()); err != nil {
		return err
	}

	eridb := erigon_db.NewErigonDb(tx)

	prevBlockRoot, prevBlockHash, err := getBlockHashValues(cfg, ctx, tx, s.BlockNumber)
	if err != nil {
		return err
	}

	to, total, err := getExecRange(cfg, tx, s.BlockNumber, toBlock, s.LogPrefix())
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Blocks execution", s.LogPrefix()), "from", s.BlockNumber, "to", to)

	stateStream := !initialCycle && cfg.stateStream && to-s.BlockNumber < stateStreamLimit

	logger := utils.NewTxGasLogger(logInterval, s.BlockNumber, total, gasState, s.LogPrefix(), &batch, tx, stages.SyncMetrics[stages.Execution])
	logger.Start()
	defer logger.Stop()

	stageProgress := s.BlockNumber
	var stoppedErr error
Loop:
	for blockNum := s.BlockNumber + 1; blockNum <= to; blockNum++ {
		if cfg.zk.SyncLimit > 0 && blockNum > cfg.zk.SyncLimit {
			log.Info(fmt.Sprintf("[%s] Sync limit reached", s.LogPrefix()), "block", blockNum)
			break
		}

		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
		}

		//fetch values pre execute
		datastreamBlockHash, block, senders, err := getPreexecuteValues(cfg, ctx, tx, blockNum, prevBlockHash)
		if err != nil {
			stoppedErr = err
			break
		}

		// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
		writeChangeSets := nextStagesExpectData || blockNum > cfg.prune.History.PruneTo(to)
		writeReceipts := nextStagesExpectData || blockNum > cfg.prune.Receipts.PruneTo(to)
		writeCallTraces := nextStagesExpectData || blockNum > cfg.prune.CallTraces.PruneTo(to)

		execRs, err := executeBlockZk(block, &prevBlockRoot, tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, initialCycle, stateStream, hermezDb)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn(fmt.Sprintf("[%s] Execution failed", s.LogPrefix()), "block", blockNum, "hash", datastreamBlockHash.Hex(), "err", err)
				if cfg.hd != nil {
					cfg.hd.ReportBadHeaderPoS(datastreamBlockHash, block.ParentHash())
				}
				if cfg.badBlockHalt {
					return err
				}
			}
			u.UnwindTo(blockNum-1, UnwindReason{Block: &datastreamBlockHash})
			break Loop
		}

		if execRs.BlockInfoTree != nil {
			if err = hermezDb.WriteBlockInfoRoot(blockNum, *execRs.BlockInfoTree); err != nil {
				return err
			}
		}

		// exec loop variables
		header := block.HeaderNoCopy()
		header.GasUsed = uint64(execRs.GasUsed)
		header.ReceiptHash = types.DeriveSha(execRs.Receipts)
		header.Bloom = execRs.Bloom
		// don't move above header values setting - wrong hash will be calculated
		prevBlockHash = header.Hash()
		prevBlockRoot = header.Root
		stageProgress = blockNum
		currentStateGas = currentStateGas + header.GasUsed

		logger.AddBlock(uint64(block.Transactions().Len()), stageProgress, currentStateGas, blockNum)

		// should update progress
		if batch.BatchSize() >= int(cfg.batchSize) {
			log.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState)
			currentStateGas = 0
			if err = s.Update(batch, stageProgress); err != nil {
				return err
			}
			if err = batch.Flush(ctx, tx); err != nil {
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
				logger.SetTx(tx)
			}
			batch = membatch.NewHashBatch(tx, quit, cfg.dirs.Tmp, log.New())
			hermezDb = hermez_db.NewHermezDb(tx)
		}

		//commit values post execute
		if err := postExecuteCommitValues(s.LogPrefix(), cfg, tx, eridb, batch, datastreamBlockHash, block, senders); err != nil {
			return err
		}
	}

	if err = s.Update(batch, stageProgress); err != nil {
		return err
	}

	// we need to artificially update the headers stage here as well to ensure that notifications
	// can fire at the end of the stage loop and inform RPC subscriptions of new blocks for example
	if err = stages.SaveStageProgress(tx, stages.Headers, stageProgress); err != nil {
		return err
	}

	if err = batch.Flush(ctx, tx); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}

	_, err = rawdb.IncrementStateVersionByBlockNumberIfNeeded(tx, stageProgress) // stageProgress is latest processsed block number
	if err != nil {
		return fmt.Errorf("writing plain state version: %w", err)
	}

	if !useExternalTx {
		log.Info(fmt.Sprintf("[%s] Commiting DB transaction...", s.LogPrefix()), "block", stageProgress)

		if err = tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", s.LogPrefix()), "block", stageProgress)

	err = stoppedErr
	return err
}

// returns the block's blockHash and header stateroot
func getBlockHashValues(cfg ExecuteBlockCfg, ctx context.Context, tx kv.RwTx, number uint64) (common.Hash, common.Hash, error) {
	prevheaderHash, err := rawdb.ReadCanonicalHash(tx, number)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	header, err := cfg.blockReader.Header(ctx, tx, prevheaderHash, number)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}

	return header.Root, prevheaderHash, nil
}

// returns calculated "to" block number for execution and the total blocks to be executed
func getExecRange(cfg ExecuteBlockCfg, tx kv.RwTx, stageProgress, toBlock uint64, logPrefix string) (uint64, uint64, error) {
	if cfg.zk.DebugLimit > 0 {
		prevStageProgress, err := stages.GetStageProgress(tx, stages.Senders)
		if err != nil {
			return 0, 0, err
		}
		to := prevStageProgress
		if cfg.zk.DebugLimit < to {
			to = cfg.zk.DebugLimit
		}
		total := to - stageProgress
		return to, total, nil
	}

	shouldShortCircuit, noProgressTo, err := utils.ShouldShortCircuitExecution(tx, logPrefix)
	if err != nil {
		return 0, 0, err
	}
	prevStageProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return 0, 0, err
	}

	// skip if no progress
	if prevStageProgress == 0 && toBlock == 0 {
		return 0, 0, nil
	}

	to := prevStageProgress
	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}

	if shouldShortCircuit {
		to = noProgressTo
	}

	total := to - stageProgress

	return to, total, nil
}

// gets the pre-execute values for a block and sets the previous block hash
func getPreexecuteValues(cfg ExecuteBlockCfg, ctx context.Context, tx kv.RwTx, blockNum uint64, prevBlockHash common.Hash) (common.Hash, *types.Block, []common.Address, error) {
	preExecuteHeaderHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return common.Hash{}, nil, nil, err
	}

	block, senders, err := cfg.blockReader.BlockWithSenders(ctx, tx, preExecuteHeaderHash, blockNum)
	if err != nil {
		return common.Hash{}, nil, nil, err
	}

	if block == nil {
		return common.Hash{}, nil, nil, fmt.Errorf("empty block blocknum: %d", blockNum)
	}

	block.HeaderNoCopy().ParentHash = prevBlockHash

	if cfg.chainConfig.IsLondon(blockNum) {
		parentHeader, err := cfg.blockReader.Header(ctx, tx, prevBlockHash, blockNum-1)
		if err != nil {
			return common.Hash{}, nil, nil, err
		}
		block.HeaderNoCopy().BaseFee = misc.CalcBaseFeeZk(cfg.chainConfig, parentHeader)
	}

	return preExecuteHeaderHash, block, senders, nil
}

func postExecuteCommitValues(
	logPrefix string,
	cfg ExecuteBlockCfg,
	tx kv.RwTx,
	eridb *erigon_db.ErigonDb,
	batch kv.PendingMutations,
	datastreamBlockHash common.Hash,
	block *types.Block,
	senders []common.Address,
) error {
	header := block.Header()
	blockHash := header.Hash()
	blockNum := block.NumberU64()

	// if datastream hash was wrong, remove old data
	if blockHash != datastreamBlockHash {
		if cfg.chainConfig.IsForkId9Elderberry2(blockNum) {
			log.Warn(fmt.Sprintf("[%s] Blockhash mismatch", logPrefix), "blockNumber", blockNum, "datastreamBlockHash", datastreamBlockHash, "calculatedBlockHash", blockHash)
		}
		if err := rawdbZk.DeleteSenders(tx, datastreamBlockHash, blockNum); err != nil {
			return fmt.Errorf("failed to delete senders: %v", err)
		}
		if err := rawdbZk.DeleteHeader(tx, datastreamBlockHash, blockNum); err != nil {
			return fmt.Errorf("failed to delete header: %v", err)
		}

		bodyForStorage, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(blockNum, datastreamBlockHash))
		if err != nil {
			return err
		}

		if err := rawdb.DeleteBodyAndTransactions(tx, blockNum, datastreamBlockHash); err != nil {
			return err
		}
		if err := rawdb.WriteBodyAndTransactions(tx, blockHash, blockNum, block.Transactions(), bodyForStorage); err != nil {
			return err
		}

		// [zkevm] senders were saved in stage_senders for headerHashes based on incomplete headers
		// in stage execute we complete the headers and senders should be moved to the correct headerHash
		// also we should delete other data based on the old hash, since it is unaccessable now
		if err := rawdb.WriteSenders(tx, blockHash, blockNum, senders); err != nil {
			return fmt.Errorf("failed to write senders: %v", err)
		}
	}

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
	if err := rawdb.WriteHeader_zkEvm(tx, header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	if err := rawdb.WriteHeadHeaderHash(tx, blockHash); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	// if err := eridb.WriteBody(block.Number(), blockHash, block.Transactions()); err != nil {
	// 	return fmt.Errorf("failed to write body: %v", err)
	// }

	// write the new block lookup entries
	if err := rawdb.WriteTxLookupEntries_zkEvm(tx, block); err != nil {
		return fmt.Errorf("failed to write tx lookup entries: %v", err)
	}

	return nil
}

func executeBlockZk(
	block *types.Block,
	prevBlockRoot *common.Hash,
	tx kv.RwTx,
	batch kv.StatelessRwTx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	initialCycle bool,
	stateStream bool,
	roHermezDb state.ReadOnlyHermezDb,
) (*core.EphemeralExecResultZk, error) {
	blockNum := block.NumberU64()

	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, cfg.blockReader, stateStream)
	if err != nil {
		return nil, err
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

	getHashFn := core.GetHashFn(block.Header(), getHeader)
	execRs, err := core.ExecuteBlockEphemerallyZk(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, ChainReaderImpl{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, getTracer, roHermezDb, prevBlockRoot)
	if err != nil {
		return nil, err
	}

	if writeReceipts {
		if err := rawdb.AppendReceipts(tx, blockNum, execRs.Receipts); err != nil {
			return nil, err
		}

		stateSyncReceipt := execRs.StateSyncReceipt
		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.NumberU64(), stateSyncReceipt); err != nil {
				return nil, err
			}
		}
	}

	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	if writeCallTraces {
		if err := callTracer.WriteToDb(tx, block, *cfg.vmConfig); err != nil {
			return nil, err
		}
	}
	return execRs, nil
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
	log.Info(fmt.Sprintf("[%s] Unwind Execution", u.LogPrefix()), "from", s.BlockNumber, "to", u.UnwindPoint)

	logger := log.New()
	if err = unwindExecutionStage(u, s, wrap.TxContainer{Tx: tx}, ctx, cfg, initialCycle, logger); err != nil {
		return err
	}
	if err = UnwindExecutionStageDbWrites(ctx, u, s, tx); err != nil {
		return err
	}

	// update the headers stage as we mark progress there as part of execution
	if err = stages.SaveStageProgress(tx, stages.Headers, u.UnwindPoint); err != nil {
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

func UnwindExecutionStageErigon(u *UnwindState, s *StageState, tx kv.RwTx, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) error {
	return unwindExecutionStage(u, s, wrap.TxContainer{Tx: tx}, ctx, cfg, initialCycle, logger)
}

func PruneExecutionStageZk(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context, initialCycle bool) (err error) {
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
			if err = cfg.agg.Prune(ctx, config3.HistoryV3AggregationStep/10); err != nil { // prune part of retired data, before commit
				return err
			}
		} else {
			if err = cfg.agg.PruneWithTiemout(ctx, 1*time.Second); err != nil { // prune part of retired data, before commit
				return err
			}
		}
	} else {
		if cfg.prune.History.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, s.LogPrefix(), cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
			if err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, s.LogPrefix(), cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
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
			if err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, s.LogPrefix(), cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
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

func UnwindExecutionStageDbWrites(ctx context.Context, u *UnwindState, s *StageState, tx kv.RwTx) error {
	// backward values that by default handinged in stage_headers
	// TODO: check for other missing value like - WriteHeader_zkEvm, WriteHeadHeaderHash, WriteCanonicalHash, WriteBody, WriteSenders, WriteTxLookupEntries_zkEvm
	hash, err := rawdb.ReadCanonicalHash(tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	rawdb.WriteHeadHeaderHash(tx, hash)

	/*
		unwind EffectiveGasPricePercentage here although it is written in stage batches (RPC) or stage execute (Sequencer)
		EffectiveGasPricePercentage could not be unwound after TruncateBlocks
	*/
	eriDb := erigon_db.NewErigonDb(tx)
	hermezDb := hermez_db.NewHermezDb(tx)

	transactions, err := eriDb.GetBodyTransactions(u.UnwindPoint+1, s.BlockNumber)
	if err != nil {
		return fmt.Errorf("get body transactions error: %v", err)
	}
	transactionHashes := make([]common.Hash, 0, len(*transactions))
	for _, tx := range *transactions {
		transactionHashes = append(transactionHashes, tx.Hash())
	}
	if err := hermezDb.DeleteEffectiveGasPricePercentages(&transactionHashes); err != nil {
		return fmt.Errorf("delete effective gas price percentages error: %v", err)
	}

	if err = rawdbZk.TruncateSenders(tx, u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("delete senders: %w", err)
	}
	if err = rawdb.TruncateTxLookupEntries_zkEvm(tx, u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("delete tx lookup entires: %w", err)
	}
	if err = rawdb.TruncateBlocks(ctx, tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete blocks: %w", err)
	}
	if err = rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, true); err != nil {
		return fmt.Errorf("delete cannonical hash with headers: %w", err)
	}
	if err = rawdb.TruncateStateVersion(tx, u.UnwindPoint+1); err != nil {
		return err
	}

	if err = hermezDb.DeleteBlockInfoRoots(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("delete block info roots: %w", err)
	}

	return nil
}
