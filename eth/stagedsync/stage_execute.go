package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/membatch"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	tracelogger "github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
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

type HermezDb interface {
	GetBlockGlobalExitRoot(l2BlockNo uint64) (common.Hash, error)
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
	zk        *ethconfig.Zk

	silkworm *silkworm.Silkworm
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

	historyV3 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.Aggregator,
	zk *ethconfig.Zk,
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
		historyV3:     historyV3,
		syncCfg:       syncCfg,
		agg:           agg,
		zk:            zk,
		silkworm:      silkworm,
	}
}

func executeBlock(
	block *types.Block,
	header *types.Header,
	tx kv.RwTx,
	batch kv.StatelessRwTx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	stateStream bool,
	roHermezDb state.ReadOnlyHermezDb,
	logger log.Logger,
) error {
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

	execRs, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger), getTracer, tx, roHermezDb, logger)
	if err != nil {
		return fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, err)
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

// Filters out and keeps receipts of contracts that may be needed by CL, such as deposit contrac,
// The list of contracts to filter is config-specified
func gatherNoPruneReceipts(receipts *types.Receipts, chainCfg *chain.Config) bool {
	cr := types.Receipts{}
	for _, r := range *receipts {
		toStore := false
		if chainCfg.NoPruneContracts != nil && chainCfg.NoPruneContracts[r.ContractAddress] {
			toStore = true
		} else {
			for _, l := range r.Logs {
				if chainCfg.NoPruneContracts != nil && chainCfg.NoPruneContracts[l.Address] {
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
	//workersCount := 2
	if !initialCycle {
		workersCount = 1
	}
	cfg.agg.SetWorkers(estimate.CompressSnapshot.WorkersQuarter())

	if initialCycle {
		reconstituteToBlock, found, err := reconstituteBlock(cfg.agg, cfg.db, txc.Tx)
		if err != nil {
			return err
		}

		if found && reconstituteToBlock > s.BlockNumber+1 {
			reconWorkers := cfg.syncCfg.ReconWorkerCount
			if err := ReconstituteState(ctx, s, cfg.dirs, reconWorkers, cfg.batchSize, cfg.db, cfg.blockReader, log.New(), cfg.agg, cfg.engine, cfg.chainConfig, cfg.genesis); err != nil {
				return err
			}
			if dbg.StopAfterReconst() {
				os.Exit(1)
			}
		}
	}

	prevStageProgress, err := senderStageProgress(txc.Tx, cfg.db)
	if err != nil {
		return err
	}

	logPrefix := s.LogPrefix()
	var to = prevStageProgress
	if toBlock > 0 {
		to = cmp.Min(prevStageProgress, toBlock)
	}
	if to <= s.BlockNumber {
		return nil
	}
	if to > s.BlockNumber+16 {
		logger.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}
	parallel := txc.Tx == nil
	if err := ExecV3(ctx, s, u, workersCount, cfg, txc, parallel, logPrefix,
		to, logger, initialCycle); err != nil {
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
	reconToBlock := cmp.Min(sendersProgress, agg.EndTxNumFrozenAndIndexed())
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

func unwindExec3(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	cfg.agg.SetLogPrefix(s.LogPrefix())
	rs := state.NewStateV3(cfg.dirs.Tmp, logger)
	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := rawdbv3.TxNums.Min(txc.Tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}
	if err := rs.Unwind(ctx, txc.Tx, u.UnwindPoint, txNum, cfg.agg, accumulator); err != nil {
		return fmt.Errorf("StateV3.Unwind: %w", err)
	}
	if err := rs.Flush(ctx, txc.Tx, s.LogPrefix(), time.NewTicker(30*time.Second)); err != nil {
		return fmt.Errorf("StateV3.Flush: %w", err)
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
	if cfg.historyV3 {
		if err = ExecBlockV3(s, u, txc, toBlock, ctx, cfg, initialCycle, logger); err != nil {
			return err
		}
		return nil
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
	// Transform batch_size limit into Ggas
	gasState := uint64(cfg.batchSize) * uint64(datasize.KB) * 2

	var stoppedErr error

	hermezDb := hermez_db.NewHermezDb(txc.Tx)

	// state is stored through ethdb batches
	batch := membatch.NewHashBatch(txc.Tx, quit, cfg.dirs.Tmp, logger)
	// avoids stacking defers within the loop
	defer func() {
		batch.Close()
	}()

	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	initialBlock := stageProgress + 1
	eridb := erigon_db.NewErigonDb(txc.Tx)
	total := to - initialBlock

Loop:
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		stageProgress = blockNum

		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
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
			continue
		}

		header, err := cfg.blockReader.Header(ctx, txc.Tx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			logger.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}

		if cfg.chainConfig.IsLondon(blockNum) {
			parentHeader, err := cfg.blockReader.Header(ctx, txc.Tx, header.ParentHash, blockNum-1)
			if err != nil {
				return err
			}
			header.BaseFee = misc.CalcBaseFeeZk(cfg.chainConfig, parentHeader)
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
			err = executeBlock(block, header, txc.Tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, stateStream, hermezDb, logger)
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
				u.UnwindTo(blockNum-1, BadBlock(blockHash, err))
			} else {
				u.UnwindTo(blockNum-1, ExecUnwind)
			}
			break Loop
		}

		metrics.UpdateBlockConsumerPostExecutionDelay(block.Time(), blockNum, logger)

		shouldUpdateProgress := batch.BatchSize() >= int(cfg.batchSize)
		if shouldUpdateProgress {
			log.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState)
			currentStateGas = 0
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
				eridb = erigon_db.NewErigonDb(txc.Tx)
			}
			logger.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState, "block", blockNum, "time", time.Since(commitTime), "committedToDb", !useExternalTx)
			batch = membatch.NewHashBatch(txc.Tx, quit, cfg.dirs.Tmp, log.New())
			hermezDb = hermez_db.NewHermezDb(txc.Tx)
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
		rawdb.WriteHeader(txc.Tx, header)
		// if header.Hash() != blockHash {
		// 	return fmt.Errorf("header hash mismatch: %s != %s", header.Hash().String(), blockHash.String())
		// }
		err = rawdb.WriteCanonicalHash(txc.Tx, header.Hash(), blockNum)
		if err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}

		err = eridb.WriteBody(header.Number, header.Hash(), block.Transactions())
		if err != nil {
			return fmt.Errorf("failed to write body: %v", err)
		}

		// write the new block lookup entries
		rawdb.WriteTxLookupEntries(txc.Tx, block)

		select {
		default:
		case <-logEvery.C:
			logBlock, logTx, logTime = logProgress(logPrefix, total, initialBlock, logBlock, logTime, blockNum, logTx, lastLogTx, gas, float64(currentStateGas)/float64(gasState), batch, logger, s.BlockNumber, to, startTime)
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

	if !useExternalTx {
		log.Info(fmt.Sprintf("[%s] Commiting DB transaction...", logPrefix), "block", stageProgress)

		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}

	logger.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	return stoppedErr
}

func blocksReadAhead(ctx context.Context, cfg *ExecuteBlockCfg, workers int) (chan uint64, context.CancelFunc) {
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

				if err := blocksReadAheadFunc(gCtx, tx, cfg, bn+readAheadBlocks); err != nil {
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
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, cfg *ExecuteBlockCfg, blockNum uint64) error {
	block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = cfg.engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache

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

func logProgress(logPrefix string, total, initialBlock, prevBlock uint64, prevTime time.Time, currentBlock uint64, prevTx, currentTx uint64, gas uint64, gasState float64, batch kv.PendingMutations, logger log.Logger, from uint64, to uint64, startTime time.Time) (uint64, uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(currentTx-prevTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(gas) / 1_000_000 / (float64(interval) / float64(time.Second))
	percent := float64(currentBlock-initialBlock) / float64(total) * 100

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"%", percent,
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
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(context.Background())
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

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) error {
	logPrefix := s.LogPrefix()
	stateBucket := kv.PlainState
	storageKeyLength := length.Addr + length.Incarnation + length.Hash

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

	if cfg.historyV3 {
		return unwindExec3(u, s, txc, ctx, cfg, accumulator, logger)
	}

	changes := etl.NewCollector(logPrefix, cfg.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)
	defer changes.Close()
	errRewind := changeset.RewindData(txc.Tx, s.BlockNumber, u.UnwindPoint, changes, ctx.Done())
	if errRewind != nil {
		return fmt.Errorf("getting rewind data: %w", errRewind)
	}

	if err := changes.Load(txc.Tx, stateBucket, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == 20 {
			if len(v) > 0 {
				var acc accounts.Account
				if err := acc.DecodeForStorage(v); err != nil {
					return err
				}

				// Fetch the code hash
				recoverCodeHashPlain(&acc, txc.Tx, k)
				var address common.Address
				copy(address[:], k)

				// cleanup contract code bucket
				original, err := state.NewPlainStateReader(txc.Tx).ReadAccountData(address)
				if err != nil {
					return fmt.Errorf("read account for %x: %w", address, err)
				}
				if original != nil {
					// clean up all the code incarnations original incarnation and the new one
					for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
						err = txc.Tx.Delete(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
						if err != nil {
							return fmt.Errorf("writeAccountPlain for %x: %w", address, err)
						}
					}
				}

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
				if err := next(k, k, newV); err != nil {
					return err
				}
			} else {
				if accumulator != nil {
					var address common.Address
					copy(address[:], k)
					accumulator.DeleteAccount(address)
				}
				if err := next(k, k, nil); err != nil {
					return err
				}
			}
			return nil
		}
		if accumulator != nil {
			var address common.Address
			var incarnation uint64
			var location common.Hash
			copy(address[:], k[:length.Addr])
			incarnation = binary.BigEndian.Uint64(k[length.Addr:])
			copy(location[:], k[length.Addr+length.Incarnation:])
			logger.Debug(fmt.Sprintf("un ch st: %x, %d, %x, %x\n", address, incarnation, location, common.Copy(v)))
			accumulator.ChangeStorage(address, incarnation, location, common.Copy(v))
		}
		if len(v) > 0 {
			if err := next(k, k[:storageKeyLength], v); err != nil {
				return err
			}
		} else {
			if err := next(k, k[:storageKeyLength], nil); err != nil {
				return err
			}
		}
		return nil

	}, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := historyv2.Truncate(txc.Tx, u.UnwindPoint+1); err != nil {
		return err
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

	// Truncate CallTraceSet
	keyStart := hexutility.EncodeTs(u.UnwindPoint + 1)
	c, err := txc.Tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		if err = txc.Tx.Delete(kv.CallTraceSet, k); err != nil {
			return err
		}
	}

	return nil
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
			// EDIT: Don't prune yet, let LogIndex stage take care of it
			// LogIndex.Prune will read everything what not pruned here
			// if err = rawdb.PruneTable(tx, kv.Log, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
			// 	return err
			// }
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
