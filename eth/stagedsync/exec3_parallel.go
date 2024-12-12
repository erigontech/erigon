package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	chaos_monkey "github.com/erigontech/erigon/tests/chaos-monkey"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/turbo/shards"
	"golang.org/x/sync/errgroup"
)

/*
ExecV3 - parallel execution. Has many layers of abstractions - each layer does accumulate
state changes (updates) and can "atomically commit all changes to underlying layer of abstraction"

Layers from top to bottom:
- IntraBlockState - used to exec txs. It does store inside all updates of given txn.
Can understand if txn failed or OutOfGas - then revert all changes.
Each parallel-worker hav own IntraBlockState.
IntraBlockState does commit changes to lower-abstraction-level by method `ibs.MakeWriteSet()`

- StateWriterBufferedV3 - txs which executed by parallel workers can conflict with each-other.
This writer does accumulate updates and then send them to conflict-resolution.
Until conflict-resolution succeed - none of execution updates must pass to lower-abstraction-level.
Object TxTask it's just set of small buffers (readset + writeset) for each transaction.
Write to TxTask happens by code like `txTask.ReadLists = rw.stateReader.ReadSet()`.

- TxTask - objects coming from parallel-workers to conflict-resolution goroutine (ApplyLoop and method ReadsValid).
Flush of data to lower-level-of-abstraction is done by method `agg.ApplyState` (method agg.ApplyHistory exists
only for performance - to reduce time of RwLock on state, but by meaning `ApplyState+ApplyHistory` it's 1 method to
flush changes from TxTask to lower-level-of-abstraction).

- StateV3 - it's all updates which are stored in RAM - all parallel workers can see this updates.
Execution of txs always done on Valid version of state (no partial-updates of state).
Flush of updates to lower-level-of-abstractions done by method `StateV3.Flush`.
On this level-of-abstraction also exists ReaderV3.
IntraBlockState does call ReaderV3, and ReaderV3 call StateV3(in-mem-cache) or DB (RoTx).
WAL - also on this level-of-abstraction - agg.ApplyHistory does write updates from TxTask to WAL.
WAL it's like StateV3 just without reading api (can only write there). WAL flush to disk periodically (doesn't need much RAM).

- RoTx - see everything what committed to DB. Commit is done by rwLoop goroutine.
rwloop does:
  - stop all Workers
  - call StateV3.Flush()
  - commit
  - open new RoTx
  - set new RoTx to all Workers
  - start Worker start workers

When rwLoop has nothing to do - it does Prune, or flush of WAL to RwTx (agg.rotate+agg.Flush)
*/

type executor interface {
	execute(ctx context.Context, tasks []exec.Task) (bool, error)
	processEvents(ctx context.Context, commitThreshold uint64) error
	wait(ctx context.Context) error
	getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header)

	//these are reset by commit - so need to be read from the executor once its processing
	tx() kv.RwTx
	readState() *state.StateV3
	domains() *state2.SharedDomains
}

type blockResult struct {
	BlockNum  uint64
	lastTxNum uint64
	complete  bool
	TxIO      *state.VersionedIO
	Stats     *map[int]ExecutionStat
	Deps      *state.DAG
	AllDeps   map[int]map[int]bool
}

type execTask struct {
	exec.Task
	versionMap                 *state.VersionMap
	statedb                    *state.IntraBlockState // State database that stores the modified values after tx execution.
	finalStateDB               *state.IntraBlockState // The final statedb.
	result                     *evmtypes.ExecutionResult
	shouldDelayFeeCalc         bool
	shouldRerunWithoutFeeDelay bool

	// length of dependencies          -> 2 + k (k = a whole number)
	// first 2 element in dependencies -> transaction index, and flag representing if delay is allowed or not
	//                                       (0 -> delay is not allowed, 1 -> delay is allowed)
	// next k elements in dependencies -> transaction indexes on which transaction i is dependent on
	dependencies []int
}

func (task *execTask) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	_ exec.ApplyMessage,
	stateWriter *state.StateWriterV3,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) *exec.Result {
	return task.execute(0, evm, vmCfg, engine, genesis, gasPool, rs, ibs,
		stateWriter, stateReader, chainConfig, chainReader, dirs, isMining)
}

func (task *execTask) execute(
	incarnation int,
	evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	stateWriter *state.StateWriterV3,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) *exec.Result {

	var result *exec.Result

	task.statedb = state.NewWithVersionMap(stateReader, task.versionMap)
	task.statedb.SetVersion(incarnation)

	defer func() {
		if r := recover(); r != nil {
			// Recover from dependency panic and retry the execution.
			log.Debug("Recovered from EVM failure.", "Error:", r)

			result.Err = exec.ErrExecAbortError{Dependency: task.statedb.DepTxIndex()}
		}
	}()

	result = task.Task.Execute(evm, vmCfg, engine, genesis, gasPool, rs, ibs,
		task.applyMessage, stateWriter, stateReader, chainConfig, chainReader, dirs, isMining)
	result.Task = task

	if task.statedb.HadInvalidRead() || result.Err != nil {
		if err, ok := result.Err.(exec.ErrExecAbortError); !ok {
			result.Err = exec.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		}
	}

	//TODO this should already be handled ?
	//task.statedb.FinalizeTx(evm.ChainRules(), task.stateWriter)
	return result
}

func (task *execTask) applyMessage(evm *vm.EVM, msg core.Message, gp *core.GasPool, refunds bool, gasBailout bool) (*evmtypes.ExecutionResult, error) {
	// Apply the transaction to the current state (included in the env).
	if task.shouldDelayFeeCalc {
		result, err := core.ApplyMessageNoFeeBurnOrTip(evm, task.TxMessage(), gp, true, false)

		if result == nil || err != nil {
			return nil, exec.ErrExecAbortError{Dependency: task.statedb.DepTxIndex(), OriginError: err}
		}

		reads := task.statedb.VersionedReadMap()

		if _, ok := reads[state.VersionSubpathKey(evm.Context.Coinbase, state.BalancePath)]; ok {
			log.Debug("Coinbase is in MVReadMap", "address", evm.Context.Coinbase)

			task.shouldRerunWithoutFeeDelay = true
		}

		if _, ok := reads[state.VersionSubpathKey(result.BurntContractAddress, state.BalancePath)]; ok {
			log.Debug("BurntContractAddress is in MVReadMap", "address", task.result.BurntContractAddress)

			task.shouldRerunWithoutFeeDelay = true
		}

		return result, err
	}

	return core.ApplyMessage(evm, task.TxMessage(), gp, true, false)
}

func (task *execTask) settle(engine consensus.Engine, stateWriter state.StateWriter) error {
	task.finalStateDB.SetTxContext(task.Version().TxIndex)

	task.finalStateDB.ApplyVersionedWrites(task.statedb.VersionedWrites())

	txHash := task.TxHash()
	BlockNum := task.Version().BlockNum

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil
	}

	for _, l := range task.statedb.GetLogs(txTask.TxIndex, txHash, BlockNum, txTask.BlockHash) {
		task.finalStateDB.AddLog(l)
	}

	if task.shouldDelayFeeCalc {
		if txTask.Config.IsLondon(txTask.BlockNum) {
			task.finalStateDB.AddBalance(task.result.BurntContractAddress, task.result.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		task.finalStateDB.AddBalance(txTask.Coinbase, task.result.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if engine != nil {
			if postApplyMessageFunc := engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				result := *task.result
				coinbaseBalance, err := task.finalStateDB.GetBalance(txTask.Coinbase)

				if err != nil {
					return err
				}

				result.CoinbaseInitBalance = coinbaseBalance.Clone()

				postApplyMessageFunc(
					task.finalStateDB,
					txTask.TxAsMessage.From(),
					txTask.Coinbase,
					&result,
				)
			}
		}
	}

	// Update the state with pending changes.
	var root []byte

	if txTask.Config.IsByzantium(txTask.BlockNum) {
		task.finalStateDB.FinalizeTx(txTask.Rules, stateWriter)
	}

	txTask.UsedGas += task.result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: txTask.Tx.Type(), PostState: root, CumulativeGasUsed: txTask.UsedGas}
	if task.result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	receipt.TxHash = txHash
	receipt.GasUsed = task.result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if txTask.TxAsMessage.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(txTask.TxAsMessage.From(), txTask.Tx.GetNonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = task.finalStateDB.GetLogs(txTask.TxIndex, txHash, BlockNum, txTask.BlockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = txTask.BlockHash
	receipt.BlockNumber = new(big.Int).SetUint64(txTask.BlockNum)
	receipt.TransactionIndex = uint(txTask.TxIndex)

	txTask.BlockReceipts = append(txTask.BlockReceipts, receipt)
	txTask.Logs = append(txTask.Logs, receipt.Logs...)

	return nil
}

type taskVersion struct {
	*execTask
	version    state.Version
	versionMap *state.VersionMap
}

func (ev *taskVersion) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	_ exec.ApplyMessage,
	stateWriter *state.StateWriterV3,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) *exec.Result {

	result := ev.execTask.execute(ev.version.Incarnation, evm, vmCfg, engine, genesis, gasPool, rs, ibs,
		stateWriter, stateReader, chainConfig, chainReader, dirs, isMining)

	if result.Err != nil {
		return result
	}

	result.TxIn = ev.execTask.statedb.VersionedReads()
	result.TxOut = ev.execTask.statedb.VersionedWrites()
	result.TxAllOut = ev.execTask.statedb.VersionedWrites()

	return result
}

func (ev *taskVersion) Version() state.Version {
	return ev.version
}

type txExecutor struct {
	sync.RWMutex
	cfg            ExecuteBlockCfg
	execStage      *StageState
	agg            *state2.Aggregator
	rs             *state.StateV3
	doms           *state2.SharedDomains
	accumulator    *shards.Accumulator
	u              Unwinder
	isMining       bool
	inMemExec      bool
	applyTx        kv.RwTx
	applyWorker    *exec3.Worker
	outputTxNum    *atomic.Uint64
	outputBlockNum metrics.Gauge
	logger         log.Logger
}

func (te *txExecutor) tx() kv.RwTx {
	return te.applyTx
}

func (te *txExecutor) readState() *state.StateV3 {
	return te.rs
}

func (te *txExecutor) domains() *state2.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header) {
	var err error
	if te.applyTx != nil {
		h, err = te.cfg.blockReader.Header(ctx, te.applyTx, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}

	if err = te.cfg.db.View(ctx, func(tx kv.Tx) error {
		h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return h
}

type blockExecStatus struct {
	tasks []*execTask

	// For a task that runs only after all of its preceding tasks have finished and passed validation,
	// its result will be absolutely valid and therefore its validation could be skipped.
	// This map stores the boolean value indicating whether a task satisfy this condition (absolutely valid).
	skipCheck map[int]bool

	// Execution tasks stores the state of each execution task
	execTasks execStatusManager

	// Validate tasks stores the state of each validation task
	validateTasks execStatusManager

	// Multi-version map
	versionMap *state.VersionMap

	// Stores the inputs and outputs of the last incarnation of all transactions
	blockIO *state.VersionedIO

	// Tracks the incarnation number of each transaction
	txIncarnations []int

	// A map that stores the estimated dependency of a transaction if it is aborted without any known dependency
	estimateDeps map[int][]int

	// A map that records whether a transaction result has been speculatively validated
	preValidated map[int]bool

	// Time records when the parallel execution starts
	begin time.Time

	// Enable profiling
	profile bool

	// Stats for debugging purposes
	cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	diagExecSuccess, diagExecAbort []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	result *blockResult
}

type parallelExecutor struct {
	txExecutor
	rwLoopErrCh              chan error
	rwLoopG                  *errgroup.Group
	applyLoopWg              sync.WaitGroup
	execWorkers              []*exec3.Worker
	stopWorkers              func()
	waitWorkers              func()
	lastBlockNum             atomic.Uint64
	in                       *exec.QueueWithRetry
	rws                      *exec.ResultsQueue
	rwsConsumed              chan struct{}
	shouldGenerateChangesets bool
	workerCount              int
	pruneEvery               *time.Ticker
	logEvery                 *time.Ticker
	slowDownLimit            *time.Ticker
	progress                 *Progress

	blockStatus map[uint64]*blockExecStatus
}

func (pe *parallelExecutor) applyLoop(ctx context.Context, maxTxNum uint64, blockComplete *atomic.Bool, errCh chan error) {
	defer pe.applyLoopWg.Done()
	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn("[dbg] apply loop panic", "rec", rec)
		}
		pe.logger.Warn("[dbg] apply loop exit")
	}()

	err := func(ctx context.Context) error {
		tx, err := pe.cfg.db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		pe.applyWorker.ResetTx(tx)

		for pe.outputTxNum.Load() <= maxTxNum {
			if err := pe.rws.Drain(ctx); err != nil {
				return err
			}

			blockResult, err := pe.processResults(ctx, pe.rwsConsumed)
			if err != nil {
				return err
			}
			//TODO
			//mxExecRepeats.AddInt(conflicts)
			//mxExecTriggers.AddInt(triggers)

			if blockResult.complete {
				blockComplete.Store(blockResult.complete)
				pe.outputTxNum.Store(blockResult.lastTxNum)
				if blockStatus, ok := pe.blockStatus[blockResult.BlockNum+1]; ok {
					nextTx := blockStatus.execTasks.takeNextPending()

					if nextTx == -1 {
						return fmt.Errorf("block exec %d failed: no executable transactions: bad dependency", blockResult.BlockNum+1)
					}

					pe.lastBlockNum.Store(blockResult.BlockNum + 1)
					blockStatus.cntExec++
					execTask := blockStatus.tasks[nextTx]
					pe.in.Add(ctx,
						&taskVersion{
							execTask:   execTask,
							version:    execTask.Version(),
							versionMap: blockStatus.versionMap})
				}
			}
		}
		return nil
	}(ctx)

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}
}

////TODO: owner of `resultCh` is main goroutine, but owner of `retryQueue` is applyLoop.
// Now rwLoop closing both (because applyLoop we completely restart)
// Maybe need split channels? Maybe don't exit from ApplyLoop? Maybe current way is also ok?

func (pe *parallelExecutor) rwLoop(ctx context.Context, maxTxNum uint64, logger log.Logger) error {
	//fmt.Println("rwLoop started", maxTxNum)
	//defer fmt.Println("rwLoop done")

	tx := pe.applyTx
	if tx == nil {
		tx, err := pe.cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	pe.doms.SetTx(tx)

	pe.outputBlockNum.SetUint64(pe.doms.BlockNum())

	defer pe.applyLoopWg.Wait()
	applyCtx, cancelApplyCtx := context.WithCancel(ctx)
	defer cancelApplyCtx()
	pe.applyLoopWg.Add(1)

	blockComplete := atomic.Bool{}
	blockComplete.Store(true)

	go pe.applyLoop(applyCtx, maxTxNum, &blockComplete, pe.rwLoopErrCh)

	for pe.outputTxNum.Load() <= maxTxNum {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-pe.logEvery.C:
			stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
			pe.progress.Log("", pe.rs, pe.in, pe.rws, pe.rs.DoneCount(), 0 /* TODO logGas*/, pe.outputBlockNum.GetValueUint64(), pe.outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, pe.shouldGenerateChangesets, pe.inMemExec)
			if pe.agg.HasBackgroundFilesBuild() {
				logger.Info(fmt.Sprintf("[%s] Background files build", pe.execStage.LogPrefix()), "progress", pe.agg.BackgroundProgress())
			}
		case <-pe.pruneEvery.C:
			if pe.rs.SizeEstimate() < pe.cfg.batchSize.Bytes() {
				if pe.doms.BlockNum() != pe.outputBlockNum.GetValueUint64() {
					panic(fmt.Errorf("%d != %d", pe.doms.BlockNum(), pe.outputBlockNum.GetValueUint64()))
				}
				_, err := pe.doms.ComputeCommitment(ctx, true, pe.outputBlockNum.GetValueUint64(), pe.execStage.LogPrefix())
				if err != nil {
					return err
				}
				if !pe.inMemExec {
					err = tx.ApplyRw(func(tx kv.RwTx) error {
						ac := pe.agg.BeginFilesRo()
						if _, err = ac.PruneSmallBatches(ctx, 150*time.Millisecond, tx); err != nil { // prune part of retired data, before commit
							return err
						}
						ac.Close()

						return pe.doms.Flush(ctx, tx)
					})
					if err != nil {
						return err
					}
				}
				break
			}
			if pe.inMemExec {
				break
			}

			cancelApplyCtx()
			pe.applyLoopWg.Wait()

			var t0, t1, t2, t3, t4 time.Duration
			commitStart := time.Now()
			logger.Info("Committing (parallel)...", "blockComplete.Load()", blockComplete.Load())
			if err := func() error {
				//Drain results (and process) channel because read sets do not carry over
				for !blockComplete.Load() {
					pe.rws.DrainNonBlocking(ctx)
					pe.applyWorker.ResetTx(tx)

					blockResult, err := pe.processResults(ctx, nil)

					if err != nil {
						return err
					}

					//TODO
					//mxExecRepeats.AddInt(conflicts)
					//mxExecTriggers.AddInt(triggers)

					if blockResult.complete && blockResult.BlockNum > 0 {
						pe.outputBlockNum.SetUint64(blockResult.BlockNum)
						pe.outputTxNum.Store(blockResult.lastTxNum)
						blockComplete.Store(blockResult.complete)
					}
				}
				t0 = time.Since(commitStart)
				pe.Lock() // This is to prevent workers from starting work on any new txTask
				defer pe.Unlock()

				select {
				case pe.rwsConsumed <- struct{}{}:
				default:
				}

				// Drain results channel because read sets do not carry over
				pe.rws.DropResults(ctx, func(result *exec.Result) {
					pe.in.ReTry(result.Task)
				})

				//lastTxNumInDb, _ := txNumsReader.Max(tx, outputBlockNum.Get())
				//if lastTxNumInDb != outputTxNum.Load()-1 {
				//	panic(fmt.Sprintf("assert: %d != %d", lastTxNumInDb, outputTxNum.Load()))
				//}

				t1 = time.Since(commitStart)
				tt := time.Now()
				t2 = time.Since(tt)
				tt = time.Now()

				if err := pe.doms.Flush(ctx, tx); err != nil {
					return err
				}
				pe.doms.ClearRam(true)
				t3 = time.Since(tt)

				if err := pe.execStage.Update(tx, pe.outputBlockNum.GetValueUint64()); err != nil {
					return err
				}
				if _, err := rawdb.IncrementStateVersion(tx); err != nil {
					return fmt.Errorf("writing plain state version: %w", err)
				}

				tx.CollectMetrics()
				tt = time.Now()
				if err := tx.Commit(); err != nil {
					return err
				}
				t4 = time.Since(tt)
				for i := 0; i < len(pe.execWorkers); i++ {
					pe.execWorkers[i].ResetTx(nil)
				}

				return nil
			}(); err != nil {
				return err
			}
			var err error
			if tx, err = pe.cfg.db.BeginRw(ctx); err != nil {
				return err
			}
			defer tx.Rollback()
			pe.doms.SetTx(tx)

			applyCtx, cancelApplyCtx = context.WithCancel(ctx) //nolint:fatcontext
			defer cancelApplyCtx()
			pe.applyLoopWg.Add(1)
			go pe.applyLoop(applyCtx, maxTxNum, &blockComplete, pe.rwLoopErrCh)

			logger.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
		}
	}
	if err := pe.doms.Flush(ctx, tx); err != nil {
		return err
	}
	if err := pe.execStage.Update(tx, pe.outputBlockNum.GetValueUint64()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (pe *parallelExecutor) processResults(ctx context.Context, backPressure chan<- struct{}) (result blockResult, err error) {
	rwsIt := pe.rws.Iter()
	defer rwsIt.Close()
	//defer fmt.Println("PRQ", "Done")

	for rwsIt.HasNext() && !result.complete {
		txResult := rwsIt.PopNext()
		txTask := txResult.Task.(*taskVersion)
		//fmt.Println("PRQ", txTask.BlockNum, txTask.TxIndex, txTask.TxNum)
		if pe.cfg.syncCfg.ChaosMonkey {
			chaosErr := chaos_monkey.ThrowRandomConsensusError(pe.execStage.CurrentSyncCycle.IsInitialCycle, txTask.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
			if chaosErr != nil {
				log.Warn("Monkey in consensus")
				return result, chaosErr
			}
		}

		result, err = pe.nextResult(ctx, txTask.Version().BlockNum, txResult)

		if err != nil {
			return result, err
		}

		if result.complete {
			if txTask, ok := txTask.Task.(*exec.TxTask); ok {
				pe.rs.SetTxNum(result.lastTxNum, result.BlockNum)

				err := pe.rs.ApplyState4(ctx,
					txTask.BlockNum, txTask.TxNum, txTask.ReadLists, txTask.WriteLists,
					txTask.BalanceIncreaseSet, txTask.Logs, txResult.TraceFroms, txResult.TraceTos,
					txTask.Config, txTask.Rules, txTask.PruneNonEssentials, txTask.HistoryExecution)

				if err != nil {
					return result, fmt.Errorf("StateV3.Apply: %w", err)
				}
				txTask.ReadLists, txTask.WriteLists = nil, nil

				if result.BlockNum > pe.lastBlockNum.Load() {
					pe.doms.SetTxNum(result.lastTxNum)
					pe.doms.SetBlockNum(result.BlockNum)
					pe.outputBlockNum.SetUint64(result.BlockNum)
					pe.lastBlockNum.Store(result.BlockNum)
				}
			}
		}

		if backPressure != nil {
			select {
			case backPressure <- struct{}{}:
			default:
			}
		}

		if txTask, ok := txTask.Task.(*exec.TxTask); ok {
			if err := pe.rs.ApplyLogsAndTraces4(
				txTask.Logs, txResult.TraceFroms, txResult.TraceTos,
				pe.rs.Domains(), txTask.PruneNonEssentials, txTask.Config); err != nil {
				return result, fmt.Errorf("ApplyLogsAndTraces4: %w", err)
			}
		}
	}

	return result, nil
}

func (pe *parallelExecutor) nextResult(ctx context.Context, blockNum uint64, res *exec.Result) (result blockResult, err error) {
	tx := res.Version().TxIndex

	blockStatus, ok := pe.blockStatus[blockNum]

	if !ok {
		return result, fmt.Errorf("unknown block: %d", blockNum)
	}

	if abortErr, ok := res.Err.(exec.ErrExecAbortError); ok && abortErr.OriginError != nil && blockStatus.skipCheck[tx] {
		// If the transaction failed when we know it should not fail, this means the transaction itself is
		// bad (e.g. wrong nonce), and we should exit the execution immediately
		err = fmt.Errorf("could not apply tx %d [%v]: %w", tx, res.Task.TxHash(), abortErr.OriginError)
		return result, err
	}

	if execErr, ok := res.Err.(exec.ErrExecAbortError); ok {
		addedDependencies := false

		if execErr.Dependency >= 0 {
			l := len(blockStatus.estimateDeps[tx])
			for l > 0 && blockStatus.estimateDeps[tx][l-1] > execErr.Dependency {
				blockStatus.execTasks.removeDependency(blockStatus.estimateDeps[tx][l-1])
				blockStatus.estimateDeps[tx] = blockStatus.estimateDeps[tx][:l-1]
				l--
			}

			addedDependencies = blockStatus.execTasks.addDependencies(execErr.Dependency, tx)
		} else {
			estimate := 0

			if len(blockStatus.estimateDeps[tx]) > 0 {
				estimate = blockStatus.estimateDeps[tx][len(blockStatus.estimateDeps[tx])-1]
			}
			addedDependencies = blockStatus.execTasks.addDependencies(estimate, tx)
			newEstimate := estimate + (estimate+tx)/2
			if newEstimate >= tx {
				newEstimate = tx - 1
			}
			blockStatus.estimateDeps[tx] = append(blockStatus.estimateDeps[tx], newEstimate)
		}

		blockStatus.execTasks.clearInProgress(tx)

		if !addedDependencies {
			blockStatus.execTasks.pushPending(tx)
		}
		blockStatus.txIncarnations[tx]++
		blockStatus.diagExecAbort[tx]++
		blockStatus.cntAbort++
	} else {
		blockStatus.blockIO.RecordRead(tx, res.TxIn)

		if res.Version().Incarnation == 0 {
			blockStatus.blockIO.RecordWrite(tx, res.TxOut)
			blockStatus.blockIO.RecordAllWrites(tx, res.TxAllOut)
		} else {
			if res.TxAllOut.HasNewWrite(blockStatus.blockIO.AllWriteSet(tx)) {
				blockStatus.validateTasks.pushPendingSet(blockStatus.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := blockStatus.blockIO.AllWriteSet(tx)

			// Remove entries that were previously written but are no longer written

			cmpMap := make(map[state.VersionKey]bool)

			for _, w := range res.TxAllOut {
				cmpMap[w.Path] = true
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap[v.Path]; !ok {
					blockStatus.versionMap.Delete(v.Path, tx)
				}
			}

			blockStatus.blockIO.RecordWrite(tx, res.TxOut)
			blockStatus.blockIO.RecordAllWrites(tx, res.TxAllOut)
		}

		blockStatus.validateTasks.pushPending(tx)
		blockStatus.execTasks.markComplete(tx)
		blockStatus.diagExecSuccess[tx]++
		blockStatus.cntSuccess++

		blockStatus.execTasks.removeDependency(tx)
	}

	// pe.mvh.FlushMVWriteSet(res.txAllOut)

	// do validations ...
	maxComplete := blockStatus.execTasks.maxAllComplete()
	toValidate := make([]int, 0, 2)

	for blockStatus.validateTasks.minPending() <= maxComplete && blockStatus.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, blockStatus.validateTasks.takeNextPending())
	}

	for i := 0; i < len(toValidate); i++ {
		blockStatus.cntTotalValidations++

		tx := toValidate[i]

		if blockStatus.skipCheck[tx] || state.ValidateVersion(tx, blockStatus.blockIO, blockStatus.versionMap) {
			blockStatus.validateTasks.markComplete(tx)
		} else {
			blockStatus.cntValidationFail++
			blockStatus.diagExecAbort[tx]++
			for _, v := range blockStatus.blockIO.AllWriteSet(tx) {
				blockStatus.versionMap.MarkEstimate(v.Path, tx)
			}
			// 'create validation tasks for all transactions > tx ...'
			blockStatus.validateTasks.pushPendingSet(blockStatus.execTasks.getRevalidationRange(tx + 1))
			blockStatus.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes

			blockStatus.execTasks.clearComplete(tx)
			blockStatus.execTasks.pushPending(tx)

			blockStatus.preValidated[tx] = false
			blockStatus.txIncarnations[tx]++
		}
	}

	maxValidated := blockStatus.validateTasks.maxAllComplete()
	if blockStatus.validateTasks.countComplete() == len(blockStatus.tasks) && blockStatus.execTasks.countComplete() == len(blockStatus.tasks) {
		pe.logger.Debug("exec summary", "block", blockNum, "execs", blockStatus.cntExec, "success", blockStatus.cntSuccess, "aborts", blockStatus.cntAbort, "validations", blockStatus.cntTotalValidations, "failures", blockStatus.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(blockStatus.tasks))/float64(blockStatus.cntExec)*100))
		fmt.Println("exec summary", "block", blockNum, "execs", blockStatus.cntExec, "success", blockStatus.cntSuccess, "aborts", blockStatus.cntAbort, "validations", blockStatus.cntTotalValidations, "failures", blockStatus.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(blockStatus.tasks))/float64(blockStatus.cntExec)*100))

		var allDeps map[int]map[int]bool

		var deps state.DAG

		if blockStatus.profile {
			allDeps = state.GetDep(*blockStatus.blockIO)
			deps = state.BuildDAG(*blockStatus.blockIO, pe.logger)
		}

		return blockResult{blockNum, blockStatus.tasks[len(blockStatus.tasks)-1].Version().TxNum, true, blockStatus.blockIO, &blockStatus.stats, &deps, allDeps}, nil
	}

	// Send the next immediate pending transaction to be executed
	if blockStatus.execTasks.minPending() != -1 && blockStatus.execTasks.minPending() == maxValidated+1 {
		nextTx := blockStatus.execTasks.takeNextPending()
		if nextTx != -1 {
			blockStatus.cntExec++

			blockStatus.skipCheck[nextTx] = true

			execTask := blockStatus.tasks[nextTx]

			if incarnation := blockStatus.txIncarnations[nextTx]; incarnation == 0 {
				pe.in.Add(ctx, &taskVersion{
					execTask:   execTask,
					version:    execTask.Version(),
					versionMap: blockStatus.versionMap})
			} else {
				version := execTask.Version()
				version.Incarnation = incarnation
				pe.in.ReTry(&taskVersion{
					execTask:   execTask,
					version:    version,
					versionMap: blockStatus.versionMap})
			}
		}
	}

	// Send speculative tasks
	for blockStatus.execTasks.minPending() != -1 {
		nextTx := blockStatus.execTasks.takeNextPending()

		if nextTx != -1 {
			blockStatus.cntExec++
			execTask := blockStatus.tasks[nextTx]
			version := execTask.Version()
			version.Incarnation = blockStatus.txIncarnations[nextTx]
			pe.in.Add(ctx, &taskVersion{
				execTask:   execTask,
				version:    version,
				versionMap: blockStatus.versionMap})
		}
	}

	var lastTxNum uint64
	if maxValidated >= 0 {
		lastTxNum = blockStatus.tasks[maxValidated].Version().TxNum
	}

	return blockResult{blockNum, lastTxNum, false, blockStatus.blockIO, &blockStatus.stats, nil, nil}, nil
}

func (pe *parallelExecutor) run(ctx context.Context, maxTxNum uint64, logger log.Logger) context.CancelFunc {
	pe.slowDownLimit = time.NewTicker(time.Second)
	pe.rwsConsumed = make(chan struct{}, 1)
	pe.rwLoopErrCh = make(chan error)
	pe.in = exec.NewQueueWithRetry(100_000)

	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		pe.RWMutex.RLocker(), pe.accumulator, logger, ctx, true, pe.cfg.db, pe.rs, pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.workerCount+1, pe.cfg.dirs, pe.isMining)

	rwLoopCtx, rwLoopCtxCancel := context.WithCancel(ctx)
	pe.rwLoopG, rwLoopCtx = errgroup.WithContext(rwLoopCtx)
	pe.rwLoopG.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Close()
		defer pe.applyLoopWg.Wait()
		defer func() {
			logger.Warn("[dbg] rwloop exit")
		}()
		return pe.rwLoop(rwLoopCtx, maxTxNum, logger)
	})

	return func() {
		rwLoopCtxCancel()
		pe.slowDownLimit.Stop()
		pe.wait(ctx)
		pe.stopWorkers()
		close(pe.rwsConsumed)
		pe.in.Close()
	}
}

func (pe *parallelExecutor) processEvents(ctx context.Context, commitThreshold uint64) error {
	var applyChan mdbx.TxApplyChan

	if temporalTx, ok := pe.applyTx.(*temporal.RwTx); ok {
		if applySource, ok := temporalTx.RwTx.(mdbx.TxApplySource); ok {
			applyChan = applySource.ApplyChan()
		}
	}

	for len(applyChan) > 0 {
		select {
		case err := <-pe.rwLoopErrCh:
			if err != nil {
				return err
			}
		case request := <-applyChan:
			request.Apply()

		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	for pe.rws.Len() > pe.rws.Limit() || pe.rs.SizeEstimate() >= commitThreshold {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-pe.rwsConsumed:
			if !ok {
				return nil
			}
		case <-pe.slowDownLimit.C:
			//logger.Warn("skip", "rws.Len()", rws.Len(), "rws.Limit()", rws.Limit(), "rws.ResultChLen()", rws.ResultChLen())
			//if tt := rws.Dbg(); tt != nil {
			//	log.Warn("fst", "n", tt.TxNum, "in.len()", in.Len(), "out", outputTxNum.Load(), "in.NewTasksLen", in.NewTasksLen())
			//}
			return nil
		}
	}

	return nil
}

func (pe *parallelExecutor) wait(ctx context.Context) error {
	pe.applyLoopWg.Wait()

	if pe.rwLoopG != nil {
		err := pe.rwLoopG.Wait()

		if err != nil {
			return err
		}

		pe.waitWorkers()
	}

	return nil
}

func (pe *parallelExecutor) execute(ctx context.Context, tasks []exec.Task) (bool, error) {
	prevSenderTx := map[common.Address]int{}

	for i, txTask := range tasks {
		t := &execTask{
			Task:               txTask,
			finalStateDB:       state.New(state.NewReaderV3(pe.doms)),
			shouldDelayFeeCalc: true,
			dependencies:       nil,
		}

		var blockStatus *blockExecStatus

		blockNum := t.Version().BlockNum

		if pe.blockStatus == nil {
			blockStatus = &blockExecStatus{
				begin:        time.Now(),
				stats:        map[int]ExecutionStat{},
				skipCheck:    map[int]bool{},
				estimateDeps: map[int][]int{},
				blockIO:      &state.VersionedIO{},
			}
			pe.lastBlockNum.Store(blockNum)
			pe.blockStatus = map[uint64]*blockExecStatus{
				blockNum: blockStatus,
			}
		} else {
			var ok bool
			blockStatus, ok = pe.blockStatus[blockNum]

			if !ok {
				blockStatus = &blockExecStatus{
					begin:        time.Now(),
					stats:        map[int]ExecutionStat{},
					skipCheck:    map[int]bool{},
					estimateDeps: map[int][]int{},
					blockIO:      &state.VersionedIO{},
				}
				pe.blockStatus[t.Version().BlockNum] = blockStatus
			}
		}

		blockStatus.tasks = append(blockStatus.tasks, t)
		blockStatus.txIncarnations = append(blockStatus.txIncarnations, 0)
		blockStatus.diagExecSuccess = append(blockStatus.diagExecSuccess, 0)
		blockStatus.diagExecAbort = append(blockStatus.diagExecAbort, 0)

		blockStatus.skipCheck[len(blockStatus.tasks)-1] = false
		blockStatus.estimateDeps[len(blockStatus.tasks)-1] = []int{}

		blockStatus.execTasks.pushPending(i)
		blockStatus.validateTasks.pushPending(i)

		if len(t.dependencies) > 0 {
			for _, val := range t.dependencies {
				blockStatus.execTasks.addDependencies(val, i)
			}
			blockStatus.execTasks.clearPending(i)
		} else if t.TxSender() != nil {
			if tx, ok := prevSenderTx[*t.TxSender()]; ok {
				blockStatus.execTasks.addDependencies(tx, i)
				blockStatus.execTasks.clearPending(i)
			}

			prevSenderTx[*t.TxSender()] = i
		}

		if t.IsBlockEnd() && pe.lastBlockNum.Load() == blockNum {
			nextTx := blockStatus.execTasks.takeNextPending()

			if nextTx == -1 {
				return false, errors.New("no executable transactions: bad dependency")
			}

			blockStatus.cntExec++
			execTask := blockStatus.tasks[nextTx]
			pe.in.Add(ctx,
				&taskVersion{
					execTask:   execTask,
					version:    execTask.Version(),
					versionMap: blockStatus.versionMap})
		}
	}

	return false, nil
}
