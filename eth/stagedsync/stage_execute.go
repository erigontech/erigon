package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	commonold "github.com/ledgerwatch/erigon/common"
	ecom "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

const (
	logInterval = 20 * time.Second
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type WithSnapshots interface {
	Snapshots() *snapshotsync.RoSnapshots
}

type ExecuteBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *params.ChainConfig
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader
	hd            *headerdownload.HeaderDownload

	dirs         datadir.Dirs
	exec22       bool
	workersCount int
	genesis      *core.Genesis
	agg          *libstate.Aggregator22
	txNums       *exec22.TxNums
}

func StageExecuteBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *params.ChainConfig,
	engine consensus.Engine,
	vmConfig *vm.Config,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	exec22 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	genesis *core.Genesis,
	workersCount int,
	txNums *exec22.TxNums,
	agg *libstate.Aggregator22,
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
		exec22:        exec22,
		workersCount:  workersCount,
		txNums:        txNums,
		agg:           agg,
	}
}

func executeBlock(
	block *types.Block,
	tx kv.RwTx,
	batch ethdb.Database,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	initialCycle bool,
	effectiveEngine consensus.Engine,
) error {
	blockNum := block.NumberU64()
	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, initialCycle, cfg.stateStream)
	if err != nil {
		return err
	}

	// where the magic happens
	getHeader := func(hash commonold.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash ecom.Hash) (vm.Tracer, error) {
		return vm.NewStructLogger(&vm.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	var receipts types.Receipts
	var stateSyncReceipt *types.ReceiptForStorage
	var execRs *core.EphemeralExecResult
	_, isPoSa := cfg.engine.(consensus.PoSA)
	isBor := cfg.chainConfig.Bor != nil
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	if isPoSa {
		execRs, err = core.ExecuteBlockEphemerallyForBSC(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, epochReader{tx: tx}, chainReader{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, false, getTracer)
	} else if isBor {
		execRs, err = core.ExecuteBlockEphemerallyBor(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, epochReader{tx: tx}, chainReader{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, false, getTracer)
	} else {
		execRs, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, epochReader{tx: tx}, chainReader{config: cfg.chainConfig, tx: tx, blockReader: cfg.blockReader}, false, getTracer)
	}
	if err != nil {
		return err
	}
	receipts = execRs.Receipts
	stateSyncReceipt = execRs.ReceiptForStorage

	if writeReceipts {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}

		if stateSyncReceipt != nil {
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

func newStateReaderWriter(
	batch ethdb.Database,
	tx kv.RwTx,
	block *types.Block,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	initialCycle bool,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets, error) {

	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if !initialCycle && stateStream {
		txs, err := rawdb.RawTransactionsRange(tx, block.NumberU64(), block.NumberU64())
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

// ================ Erigon22 ================

func ExecBlock22(s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	execCtx, cancel := context.WithCancel(ctx)
	go func() {
		<-sigs
		cancel()
	}()
	ctx = context.Background()

	workersCount := cfg.workersCount
	//workersCount := 2
	if !initialCycle {
		workersCount = 1
	}

	fmt.Printf("recon to : %t, txn=%d\n", initialCycle, cfg.agg.EndTxNumMinimax())
	allSnapshots := cfg.blockReader.(WithSnapshots).Snapshots()
	if initialCycle {
		if found, reconstituteToBlock := cfg.txNums.Find(cfg.agg.EndTxNumMinimax()); found && reconstituteToBlock > s.BlockNumber {
			reconDbPath := path.Join(cfg.dirs.DataDir, "recondb")
			os.RemoveAll(reconDbPath)
			dir.MustExist(reconDbPath)
			limiterB := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
			reconDB, err := kv2.NewMDBX(log.New()).Path(reconDbPath).RoTxsLimiter(limiterB).WriteMap().WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).Open()
			if err != nil {
				return err
			}
			if err := Recon22(execCtx, s, cfg.dirs, workersCount, cfg.db, reconDB, cfg.blockReader, allSnapshots, cfg.txNums, log.New(), cfg.agg, cfg.engine, cfg.chainConfig, cfg.genesis); err != nil {
				return err
			}
			os.RemoveAll(reconDbPath)
		}
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	prevStageProgress, err := senderStageProgress(tx, cfg.db)
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
		log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}
	if workersCount > 1 {
		tx.Rollback()
		tx = nil
	}

	rs := state.NewState22()
	if err := Exec22(execCtx, s, workersCount, cfg.db, tx, rs,
		cfg.blockReader, allSnapshots, cfg.txNums, log.New(), cfg.agg, cfg.engine,
		to,
		cfg.chainConfig, cfg.genesis, initialCycle); err != nil {
		return err
	}
	if !useExternalTx && tx != nil {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExec22(u *UnwindState, s *StageState, tx kv.RwTx, ctx context.Context, cfg ExecuteBlockCfg) (err error) {
	cfg.agg.SetLogPrefix(s.LogPrefix())
	rs := state.NewState22()
	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	if err := rs.Unwind(ctx, tx, cfg.txNums.MaxOf(u.UnwindPoint), cfg.agg, cfg.accumulator); err != nil {
		return fmt.Errorf("State22.Unwind: %w", err)
	}
	if err := rs.Flush(tx); err != nil {
		return fmt.Errorf("State22.Flush: %w", err)
	}

	if err := rawdb.TruncateReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate receipts: %w", err)
	}
	if err := rawdb.TruncateBorReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate bor receipts: %w", err)
	}
	if err := rawdb.DeleteNewerEpochs(tx, u.UnwindPoint+1); err != nil {
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

// ================ Erigon22 End ================

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, tx kv.RwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
	if cfg.exec22 {
		return ExecBlock22(s, u, tx, toBlock, ctx, cfg, initialCycle)
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
	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber, "to", to)
	}

	startTime := time.Now()

	var batch ethdb.DbWithPendingMutations
	// state is stored through ethdb batches
	batch = olddb.NewHashBatch(tx, quit, cfg.dirs.Tmp)

	defer batch.Rollback()
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

	startGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, s.BlockNumber)
	if err != nil {
		return err
	}

	totalGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, to)
	if err != nil {
		return err
	}
	var stoppedErr error

	effectiveEngine := cfg.engine
	if asyncEngine, ok := effectiveEngine.(consensus.AsyncEngine); ok {
		asyncEngine = asyncEngine.WithExecutionContext(ctx)
		effectiveEngine = asyncEngine.(consensus.Engine)
	}
Loop:
	for blockNum := stageProgress + 1; blockNum <= to; blockNum++ {
		if stoppedErr = common.Stopped(quit); stoppedErr != nil {
			break
		}

		blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		block, _, err := cfg.blockReader.BlockWithSenders(ctx, tx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			log.Error(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", blockNum)
			break
		}

		lastLogTx += uint64(block.Transactions().Len())

		// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
		writeChangeSets := nextStagesExpectData || blockNum > cfg.prune.History.PruneTo(to)
		writeReceipts := nextStagesExpectData || blockNum > cfg.prune.Receipts.PruneTo(to)
		writeCallTraces := nextStagesExpectData || blockNum > cfg.prune.CallTraces.PruneTo(to)
		if err = executeBlock(block, tx, batch, cfg, *cfg.vmConfig, writeChangeSets, writeReceipts, writeCallTraces, initialCycle, effectiveEngine); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", block.Hash().String(), "err", err)
				if cfg.hd != nil {
					cfg.hd.ReportBadHeaderPoS(blockHash, block.ParentHash())
				}
				if cfg.badBlockHalt {
					return err
				}
			}
			u.UnwindTo(blockNum-1, block.Hash())
			break Loop
		}
		stageProgress = blockNum

		if currentStateGas >= gasState {
			log.Info("Committed State", "gas reached", currentStateGas, "gasTarget", gasState)
			currentStateGas = 0
			if err = batch.Commit(); err != nil {
				return err
			}
			if !useExternalTx {
				if err = s.Update(tx, stageProgress); err != nil {
					return err
				}
				if err = tx.Commit(); err != nil {
					return err
				}
				tx, err = cfg.db.BeginRw(context.Background())
				if err != nil {
					return err
				}
				// TODO: This creates stacked up deferrals
				defer tx.Rollback()
			}
			batch = olddb.NewHashBatch(tx, quit, cfg.dirs.Tmp)
			// TODO: This creates stacked up deferrals
			defer batch.Rollback()
		}

		gas = gas + block.GasUsed()
		currentStateGas = currentStateGas + block.GasUsed()
		select {
		default:
		case <-logEvery.C:
			cumulativeGas, err := rawdb.ReadCumulativeGasUsed(tx, blockNum)
			if err != nil {
				return err
			}
			totalGasTmp := new(big.Int).Set(totalGasUsed)
			elapsed := time.Since(startTime)
			estimateRatio := float64(cumulativeGas.Sub(cumulativeGas, startGasUsed).Uint64()) / float64(totalGasTmp.Sub(totalGasTmp, startGasUsed).Uint64())
			var estimatedTime commonold.PrettyDuration
			if estimateRatio != 0 {
				estimatedTime = commonold.PrettyDuration((elapsed.Seconds() / estimateRatio) * float64(time.Second))
			}
			logBlock, logTx, logTime = logProgress(logPrefix, logBlock, logTime, blockNum, logTx, lastLogTx, gas, float64(currentStateGas)/float64(gasState), estimatedTime, batch)
			gas = 0
			tx.CollectMetrics()
			syncMetrics[stages.Execution].Set(blockNum)
		}
	}

	if err = s.Update(batch, stageProgress); err != nil {
		return err
	}
	if err = batch.Commit(); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", stageProgress)
	return stoppedErr
}

func logProgress(logPrefix string, prevBlock uint64, prevTime time.Time, currentBlock uint64, prevTx, currentTx uint64, gas uint64, gasState float64, estimatedTime commonold.PrettyDuration, batch ethdb.DbWithPendingMutations) (uint64, uint64, time.Time) {
	currentTime := time.Now()
	interval := currentTime.Sub(prevTime)
	speed := float64(currentBlock-prevBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(currentTx-prevTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(gas) / 1_000_000 / (float64(interval) / float64(time.Second))

	var m runtime.MemStats
	common.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", currentBlock,
		"blk/s", fmt.Sprintf("%.1f", speed),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"Mgas/s", fmt.Sprintf("%.1f", speedMgas),
		"gasState", fmt.Sprintf("%.2f", gasState),
	}
	if estimatedTime > 0 {
		logpairs = append(logpairs, "estimated duration", estimatedTime)
	}
	if batch != nil {
		logpairs = append(logpairs, "batch", common.ByteCount(uint64(batch.BatchSize())))
	}
	logpairs = append(logpairs, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	log.Info(fmt.Sprintf("[%s] Executed blocks", logPrefix), logpairs...)

	return currentBlock, currentTx, currentTime
}

func UnwindExecutionStage(u *UnwindState, s *StageState, tx kv.RwTx, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) (err error) {
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

func unwindExecutionStage(u *UnwindState, s *StageState, tx kv.RwTx, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool) error {
	logPrefix := s.LogPrefix()
	stateBucket := kv.PlainState
	storageKeyLength := length.Addr + length.Incarnation + length.Hash

	var accumulator *shards.Accumulator
	if !initialCycle && cfg.stateStream {
		accumulator = cfg.accumulator

		hash, err := rawdb.ReadCanonicalHash(tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		txs, err := rawdb.RawTransactionsRange(tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(u.UnwindPoint, hash, txs, true)
	}

	if cfg.exec22 {
		return unwindExec22(u, s, tx, ctx, cfg)
	}

	changes := etl.NewCollector(logPrefix, cfg.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer changes.Close()
	errRewind := changeset.RewindData(tx, s.BlockNumber, u.UnwindPoint, changes, ctx.Done())
	if errRewind != nil {
		return fmt.Errorf("getting rewind data: %w", errRewind)
	}

	if err := changes.Load(tx, stateBucket, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == 20 {
			if len(v) > 0 {
				var acc accounts.Account
				if err := acc.DecodeForStorage(v); err != nil {
					return err
				}

				// Fetch the code hash
				recoverCodeHashPlain(&acc, tx, k)
				var address commonold.Address
				copy(address[:], k)

				// cleanup contract code bucket
				original, err := state.NewPlainStateReader(tx).ReadAccountData(address)
				if err != nil {
					return fmt.Errorf("read account for %x: %w", address, err)
				}
				if original != nil {
					// clean up all the code incarnations original incarnation and the new one
					for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
						err = tx.Delete(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
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
					var address commonold.Address
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
			var address commonold.Address
			var incarnation uint64
			var location commonold.Hash
			copy(address[:], k[:length.Addr])
			incarnation = binary.BigEndian.Uint64(k[length.Addr:])
			copy(location[:], k[length.Addr+length.Incarnation:])
			log.Debug(fmt.Sprintf("un ch st: %x, %d, %x, %x\n", address, incarnation, location, common.Copy(v)))
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

	if err := changeset.Truncate(tx, u.UnwindPoint+1); err != nil {
		return err
	}

	if err := rawdb.TruncateReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate receipts: %w", err)
	}
	if err := rawdb.TruncateBorReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate bor receipts: %w", err)
	}
	if err := rawdb.DeleteNewerEpochs(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}

	// Truncate CallTraceSet
	keyStart := dbutils.EncodeBlockNumber(u.UnwindPoint + 1)
	c, err := tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		err = c.DeleteCurrentDuplicates()
		if err != nil {
			return err
		}
	}

	return nil
}

func recoverCodeHashPlain(acc *accounts.Account, db kv.Tx, key []byte) {
	var address commonold.Address
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
