package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/eth/consensuschain"
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
	execute(ctx context.Context, tasks []exec.Task, profile bool) (bool, error)
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
	BlockHash common.Hash
	StateRoot common.Hash
	lastTxNum uint64
	complete  bool
	TxIO      *state.VersionedIO
	Stats     map[int]ExecutionStat
	Deps      *state.DAG
	AllDeps   map[int]map[int]bool
}

type execTask struct {
	exec.Task
	index              int
	finalStateDB       *state.IntraBlockState // The final statedb.
	shouldDelayFeeCalc bool
}

func (t *execTask) ShouldDelayFeeCalc() bool {
	return t.shouldDelayFeeCalc
}

type execResult struct {
	*exec.Result
}

func (result *execResult) settle(ibs *state.IntraBlockState, engine consensus.Engine, stateWriter state.StateWriter) error {
	task, ok := result.Task.(*taskVersion)

	if !ok {
		return fmt.Errorf("unexpected task type: %T", result.Task)
	}

	txIndex := task.Version().TxIndex

	if txIndex < 0 {
		return nil
	}

	ibs.SetTxContext(txIndex)
	ibs.ApplyVersionedWrites(result.StateDb.VersionedWrites())

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil
	}

	txHash := task.TxHash()
	blockNum := txTask.BlockNum
	blockHash := txTask.BlockHash

	for _, l := range result.StateDb.GetLogs(txIndex, txHash, blockNum, blockHash) {
		ibs.AddLog(l)
	}

	if task.IsBlockEnd() {
		return nil
	}

	if task.ShouldDelayFeeCalc() {
		if txTask.Config.IsLondon(blockNum) {
			ibs.AddBalance(result.ExecutionResult.BurntContractAddress, result.ExecutionResult.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		ibs.AddBalance(txTask.Coinbase, result.ExecutionResult.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if engine != nil {
			if postApplyMessageFunc := engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				result := *result.ExecutionResult
				coinbaseBalance, err := ibs.GetBalance(txTask.Coinbase)

				if err != nil {
					return err
				}

				result.CoinbaseInitBalance = coinbaseBalance.Clone()

				postApplyMessageFunc(
					ibs,
					task.TxMessage().From(),
					txTask.Coinbase,
					&result,
				)
			}
		}
	}

	// Update the state with pending changes.
	var root []byte

	if txTask.Config.IsByzantium(blockNum) {
		ibs.FinalizeTx(txTask.Rules, stateWriter)
	}

	txTask.UsedGas += result.ExecutionResult.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: txTask.Tx.Type(), PostState: root, CumulativeGasUsed: txTask.UsedGas}
	if result.ExecutionResult.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	receipt.TxHash = txHash
	receipt.GasUsed = result.ExecutionResult.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if task.TxMessage().To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(task.TxMessage().From(), txTask.Tx.GetNonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = ibs.GetLogs(txTask.TxIndex, txHash, blockNum, blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = new(big.Int).SetUint64(blockNum)
	receipt.TransactionIndex = uint(txTask.TxIndex)

	txTask.BlockReceipts = append(txTask.BlockReceipts, receipt)
	txTask.Logs = append(txTask.Logs, receipt.Logs...)

	return nil
}

type taskVersion struct {
	*execTask
	version    state.Version
	versionMap *state.VersionMap
	profile    bool
	stats      map[int]ExecutionStat
	statsMutex *sync.Mutex
}

func (ev *taskVersion) Execute(evm *vm.EVM,
	vmCfg vm.Config,
	engine consensus.Engine,
	genesis *types.Genesis,
	gasPool *core.GasPool,
	rs *state.StateV3,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	isMining bool) (result *exec.Result) {

	defer func() {
		if r := recover(); r != nil {
			// Recover from dependency panic and retry the execution.
			log.Debug("Recovered from EVM failure.", "Error:", r)
			result = &exec.Result{Err: exec.ErrExecAbortError{Dependency: ibs.DepTxIndex()}}
		}
	}()

	var start time.Time
	if ev.profile {
		start = time.Now()
	}

	result = ev.execTask.Execute(evm, vmCfg, engine, genesis, gasPool, rs, ibs,
		stateWriter, stateReader, chainConfig, chainReader, dirs, isMining)

	if result.Err != nil {
		return result
	}

	result.TxIn = ev.VersionedReads(ibs)
	result.TxOut = ev.VersionedWrites(ibs)
	ev.versionMap.FlushVersionedWrites(result.TxOut)

	if ev.profile {
		ev.statsMutex.Lock()
		ev.stats[ev.version.TxIndex] = ExecutionStat{
			TxIdx:       ev.version.TxIndex,
			Incarnation: ev.version.Incarnation,
			Duration:    time.Since(start),
		}
		ev.statsMutex.Unlock()
	}

	if ibs.HadInvalidRead() || result.Err != nil {
		if err, ok := result.Err.(exec.ErrExecAbortError); !ok {
			result.Err = exec.ErrExecAbortError{Dependency: ibs.DepTxIndex(), OriginError: err}
		}
	}

	result.StateDb = ibs.Copy()

	return result
}

func (ev *taskVersion) Reset(ibs *state.IntraBlockState) {
	ev.execTask.Reset(ibs)
	ibs.SetVersionMap(ev.versionMap)
	ibs.SetVersion(ev.version.Incarnation)
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
	sync.Mutex
	tasks   []*execTask
	results []*execResult

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
	cntExec, cntSpecExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	diagExecSuccess, diagExecAbort []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	result *blockResult
}

func newBlockStatus(profile bool) *blockExecStatus {
	return &blockExecStatus{
		begin:        time.Now(),
		stats:        map[int]ExecutionStat{},
		skipCheck:    map[int]bool{},
		estimateDeps: map[int][]int{},
		preValidated: map[int]bool{},
		blockIO:      &state.VersionedIO{},
		versionMap:   &state.VersionMap{},
		profile:      profile,
	}
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

func (pe *parallelExecutor) applyLoop(ctx context.Context, maxTxNum uint64, blockResults chan *blockResult, errCh chan error) {
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

		for i := 0; i < len(pe.execWorkers); i++ {
			pe.execWorkers[i].ResetTx(tx)
		}

		var lastTxNum uint64

		for lastTxNum <= maxTxNum {
			if err := pe.rws.Drain(ctx); err != nil {
				return err
			}

			blockResult, err := pe.processResults(ctx)
			if err != nil {
				return err
			}

			if blockResult.complete {
				lastTxNum = blockResult.lastTxNum

				if blockStatus, ok := pe.blockStatus[blockResult.BlockNum]; ok {
					if blockResult.complete {
						ibs := state.NewWithVersionMap(state.NewReaderParallelV3(pe.rs.Domains()), blockStatus.versionMap)
						stateWriter := state.NewStateWriterV3(pe.rs, pe.accumulator)

						for _, result := range blockStatus.results {
							result.settle(ibs, pe.cfg.engine, stateWriter)

							if txTask, ok := result.Task.(*exec.TxTask); ok && txTask.IsBlockEnd() {
								syscall := func(contract common.Address, data []byte) ([]byte, error) {
									return core.SysCallContract(contract, data, pe.cfg.chainConfig, ibs, txTask.Header, pe.cfg.engine, false /* constCall */)
								}

								chainReader := consensuschain.NewReader(pe.cfg.chainConfig, pe.applyTx, pe.cfg.blockReader, pe.logger)

								if pe.isMining {
									_, txTask.Txs, txTask.BlockReceipts, _, err =
										pe.cfg.engine.FinalizeAndAssemble(
											pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts,
											txTask.Withdrawals, chainReader, syscall, nil, txTask.Logger)
								} else {
									_, _, _, err =
										pe.cfg.engine.Finalize(
											pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts,
											txTask.Withdrawals, chainReader, syscall, txTask.Logger)
								}

								if err != nil {
									return fmt.Errorf("can't finalize block: %w", err)
								}

								pe.rs.SetTxNum(blockResult.lastTxNum, blockResult.BlockNum)

								err = pe.rs.ApplyState4(ctx,
									txTask.BlockNum, txTask.TxNum, txTask.ReadLists, txTask.WriteLists,
									txTask.BalanceIncreaseSet, txTask.Logs, result.TraceFroms, result.TraceTos,
									txTask.Config, txTask.Rules, txTask.PruneNonEssentials, txTask.HistoryExecution)

								if err != nil {
									return fmt.Errorf("apply state failed: %w", err)
								}

								txTask.ReadLists, txTask.WriteLists = nil, nil
							}
						}
					}

					mxExecRepeats.AddInt(blockStatus.cntAbort)
					mxExecTriggers.AddInt(blockStatus.cntExec)

					blockResults <- blockResult
				}

				if blockStatus, ok := pe.blockStatus[blockResult.BlockNum+1]; ok {
					nextTx := blockStatus.execTasks.takeNextPending()

					if nextTx == -1 {
						return fmt.Errorf("block exec %d failed: no executable transactions: bad dependency", blockResult.BlockNum+1)
					}

					blockStatus.cntExec++
					/*execTask := blockStatus.tasks[nextTx]
					fmt.Println("Start Block", blockResult.BlockNum+1)
					pe.in.Add(ctx,
						&taskVersion{
							execTask:   execTask,
							version:    execTask.Version(),
							versionMap: blockStatus.versionMap,
							profile:    blockStatus.profile,
							stats:      blockStatus.stats,
							statsMutex: &blockStatus.Mutex})*/
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
		var err error
		tx, err = pe.cfg.db.BeginRw(ctx)
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

	blockComplete := false
	blockResults := make(chan *blockResult)

	go pe.applyLoop(applyCtx, maxTxNum, blockResults, pe.rwLoopErrCh)

	var logEvery <-chan time.Time
	var pruneEvery <-chan time.Time

	if pe.logEvery != nil {
		logEvery = pe.logEvery.C
	}

	if pe.pruneEvery != nil {
		pruneEvery = pe.pruneEvery.C
	}

	for pe.outputTxNum.Load() < maxTxNum {
		select {
		case blockResult := <-blockResults:
			blockComplete = blockResult.complete
			if blockResult.BlockNum > pe.lastBlockNum.Load() {
				pe.doms.SetTxNum(blockResult.lastTxNum)
				pe.doms.SetBlockNum(blockResult.BlockNum)
				pe.outputTxNum.Store(blockResult.lastTxNum)
				pe.outputBlockNum.SetUint64(blockResult.BlockNum)
				pe.lastBlockNum.Store(blockResult.BlockNum)
				fmt.Println("BC", pe.outputBlockNum.GetValueUint64()) //Temp
				rhash, err := pe.doms.ComputeCommitment(ctx, false, pe.outputBlockNum.GetValueUint64(), pe.execStage.LogPrefix())
				if err != nil {
					return err
				}
				if !bytes.Equal(rhash, blockResult.StateRoot.Bytes()) {
					logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x",
						pe.execStage.LogPrefix(), blockResult.BlockNum, rhash, blockResult.StateRoot.Bytes(), blockResult.BlockHash))
					return errors.New("wrong trie root")
				}
				fmt.Println("BC DONE", rhash, blockResult.StateRoot.Bytes()) //Temp Done
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery:
			stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
			pe.progress.Log("", pe.rs, pe.in, pe.rws, pe.rs.DoneCount(), 0 /* TODO logGas*/, pe.outputBlockNum.GetValueUint64(), pe.outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, pe.shouldGenerateChangesets, pe.inMemExec)
			if pe.agg.HasBackgroundFilesBuild() {
				logger.Info(fmt.Sprintf("[%s] Background files build", pe.execStage.LogPrefix()), "progress", pe.agg.BackgroundProgress())
			}
		case <-pruneEvery:
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
			logger.Info("Committing (parallel)...", "blockComplete", blockComplete)
			if err := func() error {
				//Drain results (and process) channel because read sets do not carry over
				for !blockComplete {
					pe.rws.DrainNonBlocking(ctx)
					for i := 0; i < len(pe.execWorkers); i++ {
						pe.execWorkers[i].ResetTx(tx)
					}

					blockResult, err := pe.processResults(ctx)

					if err != nil {
						return err
					}

					//TODO
					//mxExecRepeats.AddInt(conflicts)
					//mxExecTriggers.AddInt(triggers)

					if blockResult.complete && blockResult.BlockNum > 0 {
						pe.outputBlockNum.SetUint64(blockResult.BlockNum)
						pe.outputTxNum.Store(blockResult.lastTxNum)
						blockComplete = blockResult.complete
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
					pe.in.Add(ctx, result.Task)
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

				if pe.execStage != nil {
					if err := pe.execStage.Update(tx, pe.outputBlockNum.GetValueUint64()); err != nil {
						return err
					}
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
			go pe.applyLoop(applyCtx, maxTxNum, blockResults, pe.rwLoopErrCh)

			logger.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
		}
	}
	if err := pe.doms.Flush(ctx, tx); err != nil {
		return err
	}
	if pe.execStage != nil {
		if err := pe.execStage.Update(tx, pe.outputBlockNum.GetValueUint64()); err != nil {
			return err
		}
	}
	if tx != pe.applyTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (pe *parallelExecutor) processResults(ctx context.Context) (blockResult *blockResult, err error) {
	rwsIt := pe.rws.Iter()
	defer rwsIt.Close()
	//defer fmt.Println("PRQ", "Done")

	for rwsIt.HasNext() && (blockResult == nil || !blockResult.complete) {
		txResult := rwsIt.PopNext()
		txTask := txResult.Task.(*taskVersion)
		//fmt.Println("PRQ", txTask.BlockNum, txTask.TxIndex, txTask.TxNum)
		if pe.cfg.syncCfg.ChaosMonkey {
			chaosErr := chaos_monkey.ThrowRandomConsensusError(pe.execStage.CurrentSyncCycle.IsInitialCycle, txTask.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
			if chaosErr != nil {
				log.Warn("Monkey in consensus")
				return blockResult, chaosErr
			}
		}

		blockResult, err = pe.nextResult(ctx, txResult)

		if err != nil {
			return blockResult, err
		}

		if pe.rwsConsumed != nil {
			select {
			case pe.rwsConsumed <- struct{}{}:
			default:
			}
		}
	}

	return blockResult, nil
}

func (pe *parallelExecutor) nextResult(ctx context.Context, res *exec.Result) (result *blockResult, err error) {

	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	blockNum := res.Version().BlockNum
	tx := task.index

	blockStatus, ok := pe.blockStatus[blockNum]

	if !ok {
		return result, fmt.Errorf("unknown block: %d", blockNum)
	}

	blockStatus.results[tx] = &execResult{res}

	if abortErr, ok := res.Err.(exec.ErrExecAbortError); ok && abortErr.OriginError != nil && blockStatus.skipCheck[tx] {
		// If the transaction failed when we know it should not fail, this means the transaction itself is
		// bad (e.g. wrong nonce), and we should exit the execution immediately
		err = fmt.Errorf("could not apply tx %d [%v]: %w", tx, task.TxHash(), abortErr.OriginError)
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
		txIndex := res.Version().TxIndex

		blockStatus.blockIO.RecordReads(txIndex, res.TxIn)

		if res.Version().Incarnation == 0 {
			blockStatus.blockIO.RecordWrites(txIndex, res.TxOut)
			blockStatus.blockIO.RecordAllWrites(txIndex, res.TxOut)
		} else {
			if res.TxOut.HasNewWrite(blockStatus.blockIO.AllWriteSet(txIndex)) {
				blockStatus.validateTasks.pushPendingSet(blockStatus.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := blockStatus.blockIO.AllWriteSet(txIndex)

			// Remove entries that were previously written but are no longer written

			cmpMap := make(map[state.VersionKey]bool)

			for _, w := range res.TxOut {
				cmpMap[w.Path] = true
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap[v.Path]; !ok {
					blockStatus.versionMap.Delete(v.Path, txIndex)
				}
			}

			blockStatus.blockIO.RecordWrites(txIndex, res.TxOut)
			blockStatus.blockIO.RecordAllWrites(txIndex, res.TxOut)
		}

		blockStatus.validateTasks.pushPending(tx)
		blockStatus.execTasks.markComplete(tx)
		blockStatus.diagExecSuccess[tx]++
		blockStatus.cntSuccess++

		blockStatus.execTasks.removeDependency(tx)
	}

	// do validations ...
	maxComplete := blockStatus.execTasks.maxAllComplete()
	toValidate := make([]int, 0, 2)

	for blockStatus.validateTasks.minPending() <= maxComplete && blockStatus.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, blockStatus.validateTasks.takeNextPending())
	}

	for i := 0; i < len(toValidate); i++ {
		blockStatus.cntTotalValidations++

		tx := toValidate[i]
		txTask := blockStatus.tasks[tx].Task.(*exec.TxTask)
		txIndex := txTask.TxIndex

		if blockStatus.skipCheck[tx] || state.ValidateVersion(txIndex, blockStatus.blockIO, blockStatus.versionMap) {
			blockStatus.validateTasks.markComplete(tx)
			fmt.Printf("Validated Writes: %d:%d len=%d\n", blockNum, txIndex, len(blockStatus.blockIO.WriteSet(txIndex)))
			for i, v := range blockStatus.blockIO.WriteSet(txIndex) {
				fmt.Printf("%2d: %s\n", i, v.Path)
			}
			//stateWriter := state.NewStateWriterV3(pe.rs, pe.accumulator)
			state.ApplyVersionedWrites(txTask.Rules, blockStatus.blockIO.WriteSet(txIndex), state.NewNoopWriter(true))
		} else {
			blockStatus.cntValidationFail++
			blockStatus.diagExecAbort[tx]++
			for _, v := range blockStatus.blockIO.AllWriteSet(txIndex) {
				blockStatus.versionMap.MarkEstimate(v.Path, txIndex)
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
		pe.logger.Debug("exec summary", "block", blockNum, "tasks", len(blockStatus.tasks), "execs", blockStatus.cntExec, "speculative", blockStatus.cntSpecExec, "success", blockStatus.cntSuccess, "aborts", blockStatus.cntAbort, "validations", blockStatus.cntTotalValidations, "failures", blockStatus.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(blockStatus.tasks))/float64(blockStatus.cntExec)*100))
		//fmt.Println("exec summary", "block", blockNum, "execs", blockStatus.cntExec, "success", blockStatus.cntSuccess, "aborts", blockStatus.cntAbort, "validations", blockStatus.cntTotalValidations, "failures", blockStatus.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(blockStatus.tasks))/float64(blockStatus.cntExec)*100))

		var allDeps map[int]map[int]bool

		var deps state.DAG

		if blockStatus.profile {
			allDeps = state.GetDep(blockStatus.blockIO)
			deps = state.BuildDAG(blockStatus.blockIO, pe.logger)
		}

		txTask := blockStatus.tasks[len(blockStatus.tasks)-1].Task.(*exec.TxTask)
		blockStatus.result = &blockResult{blockNum, txTask.BlockHash, txTask.Header.Root, txTask.TxNum, true, blockStatus.blockIO, blockStatus.stats, &deps, allDeps}
		return blockStatus.result, nil
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
					versionMap: blockStatus.versionMap,
					profile:    blockStatus.profile,
					stats:      blockStatus.stats,
					statsMutex: &blockStatus.Mutex})
			} else {
				version := execTask.Version()
				version.Incarnation = incarnation
				pe.in.ReTry(&taskVersion{
					execTask:   execTask,
					version:    version,
					versionMap: blockStatus.versionMap,
					profile:    blockStatus.profile,
					stats:      blockStatus.stats,
					statsMutex: &blockStatus.Mutex})
			}
		}
	}

	// Send speculative tasks
	for nextTx := blockStatus.execTasks.minPending(); nextTx != -1; nextTx = blockStatus.execTasks.minPending() {
		execTask := blockStatus.tasks[nextTx]
		version := execTask.Version()
		version.Incarnation = blockStatus.txIncarnations[nextTx]

		if version.Incarnation == 0 {
			blockStatus.execTasks.takeNextPending()
			blockStatus.cntExec++
			blockStatus.cntSpecExec++
			pe.in.ReTry(&taskVersion{
				execTask:   execTask,
				version:    version,
				versionMap: blockStatus.versionMap,
				profile:    blockStatus.profile,
				stats:      blockStatus.stats,
				statsMutex: &blockStatus.Mutex})
		} else {
			break
		}
	}

	var lastTxNum uint64
	if maxValidated >= 0 {
		lastTxTask := blockStatus.tasks[maxValidated].Task.(*exec.TxTask)
		lastTxNum = lastTxTask.TxNum
	}

	txTask := blockStatus.tasks[0].Task.(*exec.TxTask)
	return &blockResult{blockNum, txTask.BlockHash, txTask.Header.Root, lastTxNum, false, blockStatus.blockIO, blockStatus.stats, nil, nil}, nil
}

func (pe *parallelExecutor) run(ctx context.Context, maxTxNum uint64) context.CancelFunc {
	pe.slowDownLimit = time.NewTicker(time.Second)
	pe.rwsConsumed = make(chan struct{}, 1)
	pe.rwLoopErrCh = make(chan error)
	pe.in = exec.NewQueueWithRetry(100_000)

	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		pe.RWMutex.RLocker(), pe.accumulator, pe.logger, ctx, true, pe.cfg.db, pe.rs, state.NewNoopWriter(true), pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.workerCount+1, pe.cfg.dirs, pe.isMining)

	rwLoopCtx, rwLoopCtxCancel := context.WithCancel(ctx)
	pe.rwLoopG, rwLoopCtx = errgroup.WithContext(rwLoopCtx)
	pe.rwLoopG.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Close()
		defer pe.applyLoopWg.Wait()
		defer func() {
			pe.logger.Warn("[dbg] rwloop exit")
		}()
		return pe.rwLoop(rwLoopCtx, maxTxNum, pe.logger)
	})

	return func() {
		if pe.logEvery != nil {
			pe.logEvery.Stop()
		}
		if pe.pruneEvery != nil {
			pe.pruneEvery.Stop()
		}

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

func (pe *parallelExecutor) execute(ctx context.Context, tasks []exec.Task, profile bool) (bool, error) {
	prevSenderTx := map[common.Address]int{}
	var initialTask exec.Task
	var blockStatus *blockExecStatus

	for i, txTask := range tasks {
		t := &execTask{
			Task:               txTask,
			index:              i,
			finalStateDB:       state.New(state.NewReaderV3(pe.doms)),
			shouldDelayFeeCalc: true,
		}

		blockNum := t.Version().BlockNum

		if blockStatus == nil {
			var ok bool
			blockStatus, ok = pe.blockStatus[blockNum]

			if !ok {
				blockStatus = newBlockStatus(profile)
			}
		}

		blockStatus.tasks = append(blockStatus.tasks, t)
		blockStatus.results = append(blockStatus.results, nil)
		blockStatus.txIncarnations = append(blockStatus.txIncarnations, 0)
		blockStatus.diagExecSuccess = append(blockStatus.diagExecSuccess, 0)
		blockStatus.diagExecAbort = append(blockStatus.diagExecAbort, 0)

		blockStatus.skipCheck[len(blockStatus.tasks)-1] = false
		blockStatus.estimateDeps[len(blockStatus.tasks)-1] = []int{}

		blockStatus.execTasks.pushPending(i)
		blockStatus.validateTasks.pushPending(i)

		if len(t.Dependencies()) > 0 {
			for _, val := range t.Dependencies() {
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

		if t.IsBlockEnd() {
			if pe.blockStatus == nil {
				pe.blockStatus = map[uint64]*blockExecStatus{
					blockNum: blockStatus,
				}

				nextTx := blockStatus.execTasks.takeNextPending()

				if nextTx == -1 {
					return false, errors.New("no executable transactions: bad dependency")
				}

				blockStatus.cntExec++
				execTask := blockStatus.tasks[nextTx]
				initialTask = &taskVersion{
					execTask:   execTask,
					version:    execTask.Version(),
					versionMap: blockStatus.versionMap,
					profile:    blockStatus.profile,
					stats:      blockStatus.stats,
					statsMutex: &blockStatus.Mutex}
			} else {
				pe.blockStatus[t.Version().BlockNum] = blockStatus
			}

			blockStatus = nil
		}
	}

	if initialTask != nil {
		pe.in.Add(ctx, initialTask)
	}

	return false, nil
}
