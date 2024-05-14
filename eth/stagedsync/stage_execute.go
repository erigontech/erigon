package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/membatch"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/consensuschain"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	tracelogger "github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
)

const (
	logInterval = 30 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type headerDownloader interface {
	ReportBadHeaderPoS(badHeader, lastValidAncestor common.Hash)
}

type ExecuteBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *chain.Config
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader
	hd            headerDownloader
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis
	agg       *libstate.Aggregator

	silkworm        *silkworm.Silkworm
	blockProduction bool
}

func StageExecuteBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.Aggregator,
	silkworm *silkworm.Silkworm,
) ExecuteBlockCfg {
	return ExecuteBlockCfg{
		db:            db,
		prune:         pm,
		batchSize:     batchSize,
		changeSetHook: changeSetHook,
		chainConfig:   chainConfig,
		engine:        engine,
		vmConfig:      vmConfig,
		dirs:          dirs,
		accumulator:   accumulator,
		stateStream:   stateStream,
		badBlockHalt:  badBlockHalt,
		blockReader:   blockReader,
		hd:            hd,
		genesis:       genesis,
		historyV3:     true,
		syncCfg:       syncCfg,
		agg:           agg,
		silkworm:      silkworm,
	}
}

func executeBlock(
	block *types.Block,
	tx kv.RwTx,
	batch kv.StatelessRwTx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	stateStream bool,
	logger log.Logger,
) (err error) {
	blockNum := block.NumberU64()
	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, cfg.blockReader, stateStream)
	if err != nil {
		return err
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		return tracelogger.NewStructLogger(&tracelogger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	var receipts types.Receipts
	var stateSyncReceipt *types.Receipt
	var execRs *core.EphemeralExecResult
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	execRs, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, consensuschain.NewReader(cfg.chainConfig, tx, cfg.blockReader, logger), getTracer, logger)
	if err != nil {
		return fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, err)
	}
	receipts = execRs.Receipts
	stateSyncReceipt = execRs.StateSyncReceipt

	// If writeReceipts is false here, append the not to be pruned receipts anyways
	if writeReceipts || gatherNoPruneReceipts(&receipts, cfg.chainConfig) {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.NumberU64(), stateSyncReceipt); err != nil {
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

// Filters out and keeps receipts of the contracts that may be needed by CL, namely of the deposit contract.
func gatherNoPruneReceipts(receipts *types.Receipts, chainCfg *chain.Config) bool {
	cr := types.Receipts{}
	for _, r := range *receipts {
		toStore := false
		if chainCfg.DepositContract != nil && *chainCfg.DepositContract == r.ContractAddress {
			toStore = true
		} else {
			for _, l := range r.Logs {
				if chainCfg.DepositContract != nil && *chainCfg.DepositContract == l.Address {
					toStore = true
					break
				}
			}
		}

		if toStore {
			cr = append(cr, r)
		}
	}
	receipts = &cr
	return receipts.Len() > 0
}

func newStateReaderWriter(
	batch kv.StatelessRwTx,
	tx kv.RwTx,
	block *types.Block,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	br services.FullBlockReader,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets, error) {
	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if stateStream {
		txs, err := br.RawTransactions(context.Background(), tx, block.NumberU64(), block.NumberU64())
		if err != nil {
			return nil, nil, err
		}
		accumulator.StartChange(block.NumberU64(), block.Hash(), txs, false)
	} else {
		accumulator = nil
	}
	if writeChangesets {
		stateWriter = state.NewPlainStateWriter(batch, tx, block.NumberU64()).SetAccumulator(accumulator)
	} else {
		stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(accumulator)
	}

	return stateReader, stateWriter, nil
}

// ================ Erigon3 ================

func ExecBlockV3(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) (err error) {
	workersCount := cfg.syncCfg.ExecWorkerCount
	if !initialCycle {
		workersCount = 1
	}

	//if initialCycle {
	//	reconstituteToBlock, found, err := reconstituteBlock(cfg.agg, cfg.db, txc.Tx)
	//	if err != nil {
	//		return err
	//	}
	//
	//	if found && reconstituteToBlock > s.BlockNumber+1 {
	//		reconWorkers := cfg.syncCfg.ReconWorkerCount
	//		if err := ReconstituteState(ctx, s, cfg.dirs, reconWorkers, cfg.batchSize, cfg.db, cfg.blockReader, log.New(), cfg.agg, cfg.engine, cfg.chainConfig, cfg.genesis); err != nil {
	//			return err
	//		}
	//		if dbg.StopAfterReconst() {
	//			os.Exit(1)
	//		}
	//	}
	//}

	prevStageProgress, err := senderStageProgress(txc.Tx, cfg.db)
	if err != nil {
		return err
	}

	logPrefix := s.LogPrefix()
	var to = prevStageProgress
	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}
	if to < s.BlockNumber {
		return nil
	}
	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	parallel := txc.Tx == nil
	if err := ExecV3(ctx, s, u, workersCount, cfg, txc, parallel, to, logger, initialCycle); err != nil {
		return fmt.Errorf("ExecV3: %w", err)
	}
	return nil
}

// reconstituteBlock - First block which is not covered by the history snapshot files
func reconstituteBlock(agg *libstate.Aggregator, db kv.RoDB, tx kv.Tx) (n uint64, ok bool, err error) {
	sendersProgress, err := senderStageProgress(tx, db)
	if err != nil {
		return 0, false, err
	}
	reconToBlock := cmp.Min(sendersProgress, agg.EndTxNumDomainsFrozen())
	if tx == nil {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			ok, n, err = rawdbv3.TxNums.FindBlockNum(tx, reconToBlock)
			return err
		}); err != nil {
			return
		}
	} else {
		ok, n, err = rawdbv3.TxNums.FindBlockNum(tx, reconToBlock)
	}
	return
}

var ErrTooDeepUnwind = fmt.Errorf("too deep unwind")

func unwindExec3(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	fmt.Printf("unwindv3: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	//txTo, err := rawdbv3.TxNums.Min(tx, u.UnwindPoint+1)
	//if err != nil {
	//      return err
	//}
	//bn, _, ok, err := domains.SeekCommitment2(tx, 0, txTo)
	//if ok && bn != u.UnwindPoint {
	//	return fmt.Errorf("commitment can unwind only to block: %d, requested: %d. UnwindTo was called with wrong value", bn, u.UnwindPoint)
	//}
	start := time.Now()

	unwindToLimit, err := txc.Tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).CanUnwindDomainsToBlockNum(txc.Tx)
	if err != nil {
		return err
	}
	if u.UnwindPoint < unwindToLimit {
		return fmt.Errorf("%w: %d < %d", ErrTooDeepUnwind, u.UnwindPoint, unwindToLimit)
	}

	var domains *libstate.SharedDomains
	if txc.Doms == nil {
		domains, err = libstate.NewSharedDomains(txc.Tx, logger)
		if err != nil {
			return err
		}
		defer domains.Close()
	} else {
		domains = txc.Doms
	}
	rs := state.NewStateV3(domains, logger)
	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := rawdbv3.TxNums.Min(txc.Tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}
	t := time.Now()
	if err := rs.Unwind(ctx, txc.Tx, u.UnwindPoint, txNum, accumulator); err != nil {
		return fmt.Errorf("StateV3.Unwind(%d->%d): %w, took %s", s.BlockNumber, u.UnwindPoint, err, time.Since(t))
	}
	if err := rawdb.TruncateReceipts(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate receipts: %w", err)
	}
	if err := rawdb.TruncateBorReceipts(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate bor receipts: %w", err)
	}
	if err := rawdb.DeleteNewerEpochs(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}
	fmt.Printf("unwindv3: %d -> %d done within %s\n", s.BlockNumber, u.UnwindPoint, time.Since(start))
	return nil
}

func senderStageProgress(tx kv.Tx, db kv.RoDB) (prevStageProgress uint64, err error) {
	if tx != nil {
		prevStageProgress, err = stages.GetStageProgress(tx, stages.Senders)
		if err != nil {
			return prevStageProgress, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			prevStageProgress, err = stages.GetStageProgress(tx, stages.Senders)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return prevStageProgress, err
		}
	}
	return prevStageProgress, nil
}

// ================ Erigon3 End ================

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) (err error) {
	if dbg.StagesOnlyBlocks {
		return nil
	}
	if cfg.historyV3 {
		if err = ExecBlockV3(s, u, txc, toBlock, ctx, cfg, initialCycle, logger); err != nil {
			return err
		}
		return nil
	}
	if config3.EnableHistoryV4InTest {
		panic("must use ExecBlockV3")
	}

	quit := ctx.Done()
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}

	prevStageProgress, errStart := stages.GetStageProgress(txc.Tx, stages.Senders)
	if errStart != nil {
		return errStart
	}
	nextStageProgress, err := stages.GetStageProgress(txc.Tx, stages.HashState)
	if err != nil {
		return err
	}
	nextStagesExpectData := nextStageProgress > 0 // Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet

	logPrefix := s.LogPrefix()
	var to = prevStageProgress

	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}

	if cfg.syncCfg.LoopBlockLimit > 0 {
		to = s.BlockNumber + uint64(cfg.syncCfg.LoopBlockLimit)
	}

	if to <= s.BlockNumber {
		return nil
	}

	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	stateStream := cfg.stateStream && to-s.BlockNumber < stateStreamLimit

	// changes are stored through memory buffer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	stageProgress := s.BlockNumber
	logBlock := stageProgress
	logTx, lastLogTx := uint64(0), uint64(0)
	logTime := time.Now()
	startTime := time.Now()
	var gas uint64             // used for logs
	var currentStateGas uint64 // used for batch commits of state
	var stoppedErr error
	// Transform batch_size limit into Ggas
	gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	//var batch kv.PendingMutations
	// state is stored through ethdb batches
	batch := membatch.NewHashBatch(txc.Tx, quit, cfg.dirs.Tmp, logger)
	// avoids stacking defers within the loop
	defer func() {
		batch.Close()
	}()

	var readAhead chan uint64
	if initialCycle && cfg.silkworm == nil { // block read-ahead is not compatible w/ Silkworm one-shot block execution
		// snapshots are often stored on cheaper drives. don't expect low-read-latency and manually read-ahead.
		// can't use OS-level ReadAhead - because Data >> RAM
		// it also warmsup state a bit - by touching senders/coninbase accounts and code
		var clean func()
		readAhead, clean = blocksReadAhead(ctx, &cfg, 4, cfg.engine, false)
		defer clean()
	}
	//fmt.Printf("exec blocks: %d -> %d\n", stageProgress+1, to)

Loop:
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			log.Warn("Execution interrupted", "err", stoppedErr)
			break
		}
		if initialCycle && cfg.silkworm == nil { // block read-ahead is not compatible w/ Silkworm one-shot block execution
			select {
			case readAhead <- blockNum:
			default:
			}
		}

		blockHash, err := cfg.blockReader.CanonicalHash(ctx, txc.Tx, blockNum)
		if err != nil {
			return err
		}
		block, _, err := cfg.blockReader.BlockWithSenders(ctx, txc.Tx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			logger.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}

		lastLogTx += uint64(block.Transactions().Len())

		// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
		writeChangeSets := nextStagesExpectData || blockNum > cfg.prune.History.PruneTo(to)
		writeReceipts := nextStagesExpectData || blockNum > cfg.prune.Receipts.PruneTo(to)
		writeCallTraces := nextStagesExpectData || blockNum > cfg.prune.CallTraces.PruneTo(to)

		metrics.UpdateBlockConsumerPreExecutionDelay(block.Time(), blockNum, logger)

		_, isMemoryMutation := txc.Tx.(*membatchwithdb.MemoryMutation)
		if cfg.silkworm != nil && !isMemoryMutation {
			if useExternalTx {
				blockNum, err = silkworm.ExecuteBlocksEphemeral(cfg.silkworm, txc.Tx, cfg.chainConfig.ChainID, blockNum, to, uint64(cfg.batchSize), writeChangeSets, writeReceipts, writeCallTraces)
			} else {
				// In case of internal tx we close it (no changes, commit not needed): Silkworm will use its own internal tx
				txc.Tx.Rollback()
				txc.Tx = nil

				log.Info("Using Silkworm to commit full range", "fromBlock", s.BlockNumber+1, "toBlock", to)
				blockNum, err = silkworm.ExecuteBlocksPerpetual(cfg.silkworm, cfg.db, cfg.chainConfig.ChainID, blockNum, to, uint64(cfg.batchSize), writeChangeSets, writeReceipts, writeCallTraces)

				var txErr error
				if txc.Tx, txErr = cfg.db.BeginRw(context.Background()); txErr != nil {
					return txErr
				}
				defer txc.Tx.Rollback()

				// Recreate memory batch because underlying tx has changed
				batch.Close()
				batch = membatch.NewHashBatch(txc.Tx, quit, cfg.dirs.Tmp, logger)
			}

			// In case of any error we need to increment to have the failed block number
			if err != nil {
				blockNum++
			}
		} else {
			err = executeBlock(block, txc.Tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, stateStream, logger)
		}

		if err != nil {
			if errors.Is(err, silkworm.ErrInterrupted) {
				logger.Warn(fmt.Sprintf("[%s] Execution interrupted", logPrefix), "block", blockNum, "err", err)
				// Remount the termination signal
				p, err := os.FindProcess(os.Getpid())
				if err != nil {
					return err
				}
				p.Signal(os.Interrupt)
				return nil
			}
			if !errors.Is(err, context.Canceled) {
				if cfg.silkworm != nil {
					logger.Warn(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "err", err)
				} else {
					logger.Warn(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", blockHash.String(), "err", err)
				}
				if cfg.hd != nil && errors.Is(err, consensus.ErrInvalidBlock) {
					cfg.hd.ReportBadHeaderPoS(blockHash, block.ParentHash() /* lastValidAncestor */)
				}
				if cfg.badBlockHalt {
					return err
				}
			}
			if errors.Is(err, consensus.ErrInvalidBlock) {
				if err := u.UnwindTo(blockNum-1, BadBlock(blockHash, err), txc.Tx); err != nil {
					return err
				}
			} else {
				if err := u.UnwindTo(blockNum-1, ExecUnwind, txc.Tx); err != nil {
					return err
				}
			}
			break Loop
		}
		stageProgress = blockNum

		metrics.UpdateBlockConsumerPostExecutionDelay(block.Time(), blockNum, logger)

		shouldUpdateProgress := batch.BatchSize() >= int(cfg.batchSize)
		if shouldUpdateProgress {
			commitTime := time.Now()
			if err = batch.Flush(ctx, txc.Tx); err != nil {
				return err
			}

			if err = s.Update(txc.Tx, stageProgress); err != nil {
				return err
			}
			if !useExternalTx {
				if err = txc.Tx.Commit(); err != nil {
					return err
				}
				txc.Tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return err
				}
				// TODO: This creates stacked up deferrals
				defer txc.Tx.Rollback()
			}
			logger.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState, "block", blockNum, "time", time.Since(commitTime), "committedToDb", !useExternalTx)
			currentStateGas = 0
			batch = membatch.NewHashBatch(txc.Tx, quit, cfg.dirs.Tmp, logger)
		}

		gas = gas + block.GasUsed()
		currentStateGas = currentStateGas + block.GasUsed()
		select {
		default:
		case <-logEvery.C:
			logBlock, logTx, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, logTx, lastLogTx, gas, float64(currentStateGas)/float64(gasState), batch, logger, s.BlockNumber, to, startTime)
			gas = 0
			txc.Tx.CollectMetrics()
			stages.SyncMetrics[stages.Execution].SetUint64(blockNum)
		}
	}

	if err = s.Update(txc.Tx, stageProgress); err != nil {
		return err
	}
	if err = batch.Flush(ctx, txc.Tx); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}
	_, err = rawdb.IncrementStateVersion(txc.Tx)
	if err != nil {
		return fmt.Errorf("writing plain state version: %w", err)
	}

	//dumpPlainStateDebug(tx, nil)

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}

	logger.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	return stoppedErr
}

func blocksReadAhead(ctx context.Context, cfg *ExecuteBlockCfg, workers int, engine consensus.Engine, histV3 bool) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 100
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for workerNum := 0; workerNum < workers; workerNum++ {
		g.Go(func() (err error) {
			var bn uint64
			var ok bool
			var tx kv.Tx
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()

			for i := 0; ; i++ {
				select {
				case bn, ok = <-readAhead:
					if !ok {
						return
					}
				case <-gCtx.Done():
					return gCtx.Err()
				}

				if i%100 == 0 {
					if tx != nil {
						tx.Rollback()
					}
					tx, err = cfg.db.BeginRo(ctx)
					if err != nil {
						return err
					}
				}

				if err := blocksReadAheadFunc(gCtx, tx, cfg, bn+readAheadBlocks, engine, histV3); err != nil {
					return err
				}
			}
		})
	}
	return readAhead, func() {
		close(readAhead)
		_ = g.Wait()
	}
}
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, cfg *ExecuteBlockCfg, blockNum uint64, engine consensus.Engine, histV3 bool) error {
	block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = cfg.engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache
	if histV3 {
		return nil
	}

	senders := block.Body().SendersFromTxs()     //TODO: BlockByNumber can return senders
	stateReader := state.NewPlainStateReader(tx) //TODO: can do on batch! if make batch thread-safe
	for _, sender := range senders {
		a, _ := stateReader.ReadAccountData(sender)
		if a == nil || a.Incarnation == 0 {
			continue
		}
		if code, _ := stateReader.ReadAccountCode(sender, a.Incarnation, a.CodeHash); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}

	for _, txn := range block.Transactions() {
		to := txn.GetTo()
		if to == nil {
			continue
		}
		a, _ := stateReader.ReadAccountData(*to)
		if a == nil || a.Incarnation == 0 {
			continue
		}
		if code, _ := stateReader.ReadAccountCode(*to, a.Incarnation, a.CodeHash); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}
	_, _ = stateReader.ReadAccountData(block.Coinbase())
	_, _ = block, senders
	return nil
}

func logProgress(logPrefix string, prevBlock uint64, prevTime time.Time, currentBlock uint64, prevTx, currentTx uint64, gas uint64,
	gasState float64, batch kv.PendingMutations, logger log.Logger, from uint64, to uint64, startTime time.Time) (uint64, uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(currentTx-prevTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(gas) / 1_000_000 / (float64(interval) / float64(time.Second))

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"blk/s", fmt.Sprintf("%.1f", speed),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"Mgas/s", fmt.Sprintf("%.1f", speedMgas),
		"gasState", fmt.Sprintf("%.2f", gasState),
	}

	batchSize := 0

	if batch != nil {
		batchSize = batch.BatchSize()
		logpairs = append(logpairs, "batch", common.ByteCount(uint64(batchSize)))
	}
	logpairs = append(logpairs, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	diagnostics.Send(diagnostics.BlockExecutionStatistics{
		From:        from,
		To:          to,
		BlockNumber: currentBlock,
		BlkPerSec:   speed,
		TxPerSec:    speedTx,
		MgasPerSec:  speedMgas,
		GasState:    gasState,
		Batch:       uint64(batchSize),
		Alloc:       m.Alloc,
		Sys:         m.Sys,
		TimeElapsed: time.Since(startTime).Round(time.Second).Seconds(),
	})

	logger.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix), logpairs...)

	return currentBlock, currentTx, currentTime
}

func UnwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) (err error) {
	//fmt.Printf("unwind: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	logger.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindExecutionStage(u, s, txc, ctx, cfg, initialCycle, logger); err != nil {
		return err
	}
	if err = u.Done(txc.Tx); err != nil {
		return err
	}
	//dumpPlainStateDebug(tx, nil)

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) error {
	var accumulator *shards.Accumulator
	if cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.accumulator

		hash, err := cfg.blockReader.CanonicalHash(ctx, txc.Tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		txs, err := cfg.blockReader.RawTransactions(ctx, txc.Tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(u.UnwindPoint, hash, txs, true)
	}

	//TODO: why we don't call accumulator.ChangeCode???
	return unwindExec3(u, s, txc, ctx, accumulator, logger)
}

func recoverCodeHashPlain(acc *accounts.Account, db kv.Tx, key []byte) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func PruneExecutionStage(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context, initialCycle bool) (err error) {
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

	pruneTimeout := 3 * time.Second
	if initialCycle {
		pruneTimeout = 12 * time.Hour
	}
	if _, err = tx.(*temporal.Tx).AggTx().(*libstate.AggregatorRoTx).PruneSmallBatches(ctx, pruneTimeout, tx); err != nil { // prune part of retired data, before commit
		return err
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
