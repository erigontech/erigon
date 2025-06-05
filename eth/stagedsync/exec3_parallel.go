package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/eth/consensuschain"
	chaos_monkey "github.com/erigontech/erigon/tests/chaos-monkey"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
)

/*
ExecV3 - parallel execution. Has many layers of abstractions - each layer does accumulate
state changes (updates) and can "atomically commit all changes to underlying layer of abstraction"

Layers from top to bottom:
- IntraBlockState - used to exec txs. It does store inside all updates of given txn.
Can understand if txn failed or OutOfGas - then revert all changes.
Each parallel-worker have own IntraBlockState.
IntraBlockState does commit changes to lower-abstraction-level by method `ibs.MakeWriteSet()`

- StateWriterBuffered - txs which executed by parallel workers can conflict with each-other.
This writer does accumulate updates and then send them to conflict-resolution.
Until conflict-resolution succeed - none of execution updates must pass to lower-abstraction-level.
Object TxTask it's just set of small buffers (readset + writeset) for each transaction.
Write to TxTask happens by code like `txTask.ReadLists = rw.stateReader.ReadSet()`.

- TxTask - objects coming from parallel-workers to conflict-resolution goroutine (ApplyLoop and method ReadsValid).
Flush of data to lower-level-of-abstraction is done by method `agg.ApplyState` (method agg.ApplyHistory exists
only for performance - to reduce time of RwLock on state, but by meaning `ApplyState+ApplyHistory` it's 1 method to
flush changes from TxTask to lower-level-of-abstraction).

- ParallelExecutionState - it's all updates which are stored in RAM - all parallel workers can see this updates.
Execution of txs always done on Valid version of state (no partial-updates of state).
Flush of updates to lower-level-of-abstractions done by method `ParallelExecutionState.Flush`.
On this level-of-abstraction also exists ReaderV3.
IntraBlockState does call ReaderV3, and ReaderV3 call ParallelExecutionState(in-mem-cache) or DB (RoTx).
WAL - also on this level-of-abstraction - agg.ApplyHistory does write updates from TxTask to WAL.
WAL it's like ParallelExecutionState just without reading api (can only write there). WAL flush to disk periodically (doesn't need much RAM).

- RoTx - see everything what committed to DB. Commit is done by rwLoop goroutine.
rwloop does:
  - stop all Workers
  - call ParallelExecutionState.Flush()
  - commit
  - open new RoTx
  - set new RoTx to all Workers
  - start Worker start workers

When rwLoop has nothing to do - it does Prune, or flush of WAL to RwTx (agg.rotate+agg.Flush)
*/

type executor interface {
	executeBlocks(ctx context.Context, tx kv.Tx, blockNum uint64, maxBlockNum uint64, readAhead chan uint64, applyResults chan applyResult) error

	wait(ctx context.Context) error
	getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error)

	//these are reset by commit - so need to be read from the executor once its processing
	readState() *state.StateV3Buffered
	domains() *libstate.SharedDomains

	commit(ctx context.Context, execStage *StageState, tx kv.RwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.RwTx, time.Duration, error)
	resetWorkers(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.Tx) error

	LogExecuted(tx kv.Tx)
	LogCommitted(tx kv.Tx, commitStart time.Time)
	LogComplete(tx kv.Tx)
}

type applyResult interface {
}

type blockResult struct {
	BlockNum    uint64
	BlockTime   uint64
	BlockHash   common.Hash
	StateRoot   common.Hash
	Err         error
	GasUsed     uint64
	BlobGasUsed uint64
	lastTxNum   uint64
	complete    bool
	isPartial   bool
	TxIO        *state.VersionedIO
	Receipts    types.Receipts
	Stats       map[int]ExecutionStat
	Deps        *state.DAG
	AllDeps     map[int]map[int]bool
}

type txResult struct {
	blockNum   uint64
	blockTime  uint64
	txNum      uint64
	gasUsed    int64
	receipts   []*types.Receipt
	logs       []*types.Log
	traceFroms map[common.Address]struct{}
	traceTos   map[common.Address]struct{}
	writeSet   map[string]*libstate.KvList
}

type execTask struct {
	exec.Task
	index              int
	shouldDelayFeeCalc bool
}

type execResult struct {
	*exec.TxResult
}

func (result *execResult) finalize(prevReceipt *types.Receipt, engine consensus.Engine, vm *state.VersionMap, stateReader state.StateReader, stateWriter state.StateWriter) (*types.Receipt, error) {
	task, ok := result.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", result.Task)
	}

	txIndex := task.Version().TxIndex

	if txIndex < 0 {
		return nil, nil
	}

	txIncarnation := task.Version().Incarnation

	// we want to force a re-read of the conbiase & burnt contract address
	// if thay where referenced by the tx
	delete(result.TxIn, result.Coinbase)
	delete(result.TxIn, result.ExecutionResult.BurntContractAddress)

	ibs := state.New(state.NewVersionedStateReader(txIndex, result.TxIn, vm, stateReader))
	ibs.SetTrace(task.execTask.Task.(*exec.TxTask).Trace)
	ibs.SetTxContext(task.Version().BlockNum, txIndex)
	ibs.SetVersion(txIncarnation)
	ibs.ApplyVersionedWrites(result.TxOut)
	ibs.SetVersionMap(&state.VersionMap{})

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil, nil
	}

	if task.IsBlockEnd() {
		return nil, nil
	}

	blockNum := txTask.BlockNumber()
	for _, l := range result.Logs {
		ibs.AddLog(l)
	}

	if task.shouldDelayFeeCalc {
		if txTask.Config.IsLondon(blockNum) {
			ibs.AddBalance(result.ExecutionResult.BurntContractAddress, result.ExecutionResult.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		coinbaseBalance, err := ibs.GetBalance(result.Coinbase)

		if err != nil {
			return nil, err
		}

		if traceTx(blockNum, txIndex) {
			nonce, _ := ibs.GetNonce(result.Coinbase)
			fmt.Println(blockNum, fmt.Sprintf("(%d.%d)", txIndex, task.Version().Incarnation), "CB", fmt.Sprintf("%x", result.Coinbase), fmt.Sprintf("%d", &coinbaseBalance), "nonce", nonce)
		}

		ibs.AddBalance(result.Coinbase, result.ExecutionResult.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if engine != nil {
			if postApplyMessageFunc := engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				execResult := *result.ExecutionResult
				execResult.CoinbaseInitBalance = coinbaseBalance

				message, err := task.TxMessage()
				if err != nil {
					return nil, err
				}

				postApplyMessageFunc(
					ibs,
					message.From(),
					result.Coinbase,
					&execResult,
				)

				// capture postApplyMessageFunc side affects
				result.Logs = append([]*types.Log{}, ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), blockNum, txTask.BlockHash())...)
			}
		}
	}

	var tracePrefix string
	if dbg.TraceTransactionIO && traceTx(blockNum, txIndex) {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation)
		vm.SetTrace(true)
		fmt.Println(tracePrefix, ibs.VersionedWrites(true))
	}
	// we need to flush the finalized writes to the version map so
	// they are taken into account by subsequent transactions
	vm.FlushVersionedWrites(ibs.VersionedWrites(true), true, tracePrefix)
	vm.SetTrace(false)

	if txTask.Config.IsByzantium(blockNum) {
		ibs.FinalizeTx(txTask.Config.Rules(txTask.BlockNumber(), txTask.BlockTime()), stateWriter)
	}

	receipt, err := result.CreateReceipt(prevReceipt)

	if err != nil {
		return nil, err
	}

	if hooks := result.TracingHooks(); hooks != nil && hooks.OnTxEnd != nil {
		hooks.OnTxEnd(receipt, result.Err)
	}

	return receipt, nil
}

type taskVersion struct {
	*execTask
	version    state.Version
	versionMap *state.VersionMap
	profile    bool
	stats      map[int]ExecutionStat
	statsMutex *sync.Mutex
}

func (ev *taskVersion) Trace() bool {
	return ev.Task.(*exec.TxTask).Trace
}

func (ev *taskVersion) Execute(evm *vm.EVM,
	engine consensus.Engine,
	genesis *types.Genesis,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) (result *exec.TxResult) {

	var start time.Time
	if ev.profile {
		start = time.Now()
	}

	// Don't run post apply message during the state transition it is handled in finalize
	postApplyMessage := evm.Context.PostApplyMessage
	evm.Context.PostApplyMessage = nil
	defer func() { evm.Context.PostApplyMessage = postApplyMessage }()

	result = ev.execTask.Execute(evm, engine, genesis, ibs, stateWriter,
		chainConfig, chainReader, dirs, !ev.shouldDelayFeeCalc)

	if ibs.HadInvalidRead() || result.Err != nil {
		if err, ok := result.Err.(core.ErrExecAbortError); !ok {
			result.Err = core.ErrExecAbortError{DependencyTxIndex: ibs.DepTxIndex(), OriginError: err}
		}
	}

	if result.Err != nil {
		return result
	}

	if ev.profile {
		ev.statsMutex.Lock()
		ev.stats[ev.version.TxIndex] = ExecutionStat{
			TxIdx:       ev.version.TxIndex,
			Incarnation: ev.version.Incarnation,
			Duration:    time.Since(start),
		}
		ev.statsMutex.Unlock()
	}

	return result
}

func (ev *taskVersion) Reset(evm *vm.EVM, ibs *state.IntraBlockState, callTracer *calltracer.CallTracer) error {
	if err := ev.execTask.Reset(evm, ibs, callTracer); err != nil {
		return err
	}
	ibs.SetVersionMap(ev.versionMap)
	ibs.SetVersion(ev.version.Incarnation)
	return nil
}

func (ev *taskVersion) Version() state.Version {
	return ev.version
}

type blockExecMetrics struct {
	BlockCount atomic.Int64
	Duration   blockDuration
}

func newBlockExecMetrics() *blockExecMetrics {
	return &blockExecMetrics{
		Duration: blockDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
	}
}

type blockDuration struct {
	atomic.Int64
	Ema *metrics.EMA[time.Duration]
}

func (d *blockDuration) Add(i time.Duration) {
	d.Int64.Add(int64(i))
	d.Ema.Update(i)
}

type txExecutor struct {
	sync.RWMutex
	cfg                      ExecuteBlockCfg
	agg                      *libstate.Aggregator
	rs                       *state.StateV3Buffered
	doms                     *libstate.SharedDomains
	u                        Unwinder
	isMining                 bool
	inMemExec                bool
	applyTx                  kv.Tx
	logger                   log.Logger
	logPrefix                string
	progress                 *Progress
	taskExecMetrics          *exec3.WorkerMetrics
	blockExecMetrics         *blockExecMetrics
	hooks                    *tracing.Hooks
	shouldGenerateChangesets bool

	lastExecutedBlockNum  atomic.Int64
	lastExecutedTxNum     atomic.Int64
	executedGas           atomic.Int64
	lastCommittedBlockNum uint64
	lastCommittedTxNum    uint64
	committedGas          int64

	execLoopGroup *errgroup.Group

	execRequests chan *execRequest
	execCount    atomic.Int64
	abortCount   atomic.Int64
	invalidCount atomic.Int64
	readCount    atomic.Int64
	writeCount   atomic.Int64

	enableChaosMonkey bool
}

func (te *txExecutor) readState() *state.StateV3Buffered {
	return te.rs
}

func (te *txExecutor) domains() *libstate.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error) {
	if te.applyTx != nil {
		err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, te.applyTx, hash, number)
			return err
		})

		if err != nil {
			return nil, err
		}
	} else {
		if err := te.cfg.db.View(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
			return err
		}); err != nil {
			return nil, err
		}
	}

	return h, nil
}

func (te *txExecutor) onBlockStart(ctx context.Context, blockNum uint64, blockHash common.Hash) {
	defer func() {
		if rec := recover(); rec != nil {
			te.logger.Warn("hook paniced: %s", rec, "stack", dbg.Stack())
		}
	}()

	if te.hooks == nil {
		return
	}

	if blockHash == (common.Hash{}) {
		te.logger.Warn("hooks ignored: zero block hash")
		return
	}

	if blockNum == 0 {
		if te.hooks.OnGenesisBlock != nil {
			var b *types.Block
			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				return err
			}); err != nil {
				te.logger.Warn("hook: OnGenesisBlock: abandoned", "err", err)
			}
			te.hooks.OnGenesisBlock(b, te.cfg.genesis.Alloc)
		}
	} else {
		if te.hooks.OnBlockStart != nil {
			var b *types.Block
			var td *big.Int
			var finalized *types.Header
			var safe *types.Header

			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				if err != nil {
					return err
				}
				chainReader := NewChainReaderImpl(te.cfg.chainConfig, te.applyTx, te.cfg.blockReader, te.logger)
				td = chainReader.GetTd(b.ParentHash(), b.NumberU64()-1)
				finalized = chainReader.CurrentFinalizedHeader()
				safe = chainReader.CurrentSafeHeader()
				return nil
			}); err != nil {
				te.logger.Warn("hook: OnBlockStart: abandoned", "err", err)
			}

			te.hooks.OnBlockStart(tracing.BlockEvent{
				Block:     b,
				TD:        td,
				Finalized: finalized,
				Safe:      safe,
			})
		}
	}
}

func (te *txExecutor) executeBlocks(ctx context.Context, tx kv.Tx, blockNum uint64, maxBlockNum uint64, readAhead chan uint64, applyResults chan applyResult) error {
	inputTxNum, _, offsetFromBlockBeginning, err := restoreTxNum(ctx, &te.cfg, tx, te.doms, maxBlockNum)

	if err != nil {
		return err
	}

	if te.execLoopGroup == nil {
		return fmt.Errorf("no exec group")
	}

	te.execLoopGroup.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("exec blocks panic: %s", rec)
			} else if err != nil && !errors.Is(err, context.Canceled) {
				err = fmt.Errorf("exec blocks error: %w", err)
			} else {
				te.logger.Debug("[" + te.logPrefix + "] exec blocks exit")
			}
		}()

		for ; blockNum <= maxBlockNum; blockNum++ {
			changeset := &libstate.StateChangeSet{}
			if te.shouldGenerateChangesets && blockNum > 0 {
				te.doms.SetChangesetAccumulator(changeset)
			}

			select {
			case readAhead <- blockNum:
			default:
			}

			var b *types.Block
			err := tx.Apply(ctx, func(tx kv.Tx) error {
				b, err = exec3.BlockWithSenders(ctx, te.cfg.db, tx, te.cfg.blockReader, blockNum)
				return err
			})
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("nil block %d", blockNum)
			}

			txs := b.Transactions()
			header := b.HeaderNoCopy()
			skipAnalysis := core.SkipAnalysis(te.cfg.chainConfig, blockNum)
			getHashFnMutex := sync.Mutex{}

			blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, func(hash common.Hash, number uint64) (h *types.Header, err error) {
				getHashFnMutex.Lock()
				defer getHashFnMutex.Unlock()
				err = tx.Apply(ctx, func(tx kv.Tx) (err error) {
					h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
					return err
				})

				if err != nil {
					return nil, err
				}

				return h, err
			}), te.cfg.engine, te.cfg.author, te.cfg.chainConfig)

			var txTasks []exec.Task

			for txIndex := -1; txIndex <= len(txs); txIndex++ {
				if inputTxNum > 0 && inputTxNum <= te.progress.initialTxNum {
					inputTxNum++
					continue
				}

				// Do not oversend, wait for the result heap to go under certain size
				txTask := &exec.TxTask{
					TxNum:           inputTxNum,
					TxIndex:         txIndex,
					Header:          header,
					Uncles:          b.Uncles(),
					Txs:             txs,
					SkipAnalysis:    skipAnalysis,
					EvmBlockContext: blockContext,
					Withdrawals:     b.Withdrawals(),
					// use history reader instead of state reader to catch up to the tx where we left off
					HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),
					Config:           te.cfg.chainConfig,
					Engine:           te.cfg.engine,
					Trace:            traceTx(blockNum, txIndex),
					Hooks:            te.hooks,
				}

				txTasks = append(txTasks, txTask)
				inputTxNum++
			}

			te.execRequests <- &execRequest{
				b.Number().Uint64(), b.Hash(),
				core.NewGasPool(b.GasLimit(), te.cfg.chainConfig.GetMaxBlobGasPerBlock(b.Time())),
				txTasks, applyResults, false,
			}

			mxExecBlocks.Add(1)

			if offsetFromBlockBeginning > 0 {
				// after history execution no offset will be required
				offsetFromBlockBeginning = 0
			}
		}

		return nil
	})

	return nil
}

func (te *txExecutor) commit(ctx context.Context, execStage *StageState, tx kv.RwTx, useExternalTx bool, resetWorkers func(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.Tx) error) (kv.RwTx, time.Duration, error) {
	err := execStage.Update(tx, te.lastCommittedBlockNum)

	if err != nil {
		return nil, 0, err
	}

	_, err = rawdb.IncrementStateVersion(tx)

	if err != nil {
		return nil, 0, fmt.Errorf("writing plain state version: %w", err)
	}

	tx.CollectMetrics()

	var t2 time.Duration

	if !useExternalTx {
		tt := time.Now()
		err = tx.Commit()

		if err != nil {
			return nil, 0, err
		}

		t2 = time.Since(tt)
		tx, err = te.cfg.db.BeginRw(ctx)

		if err != nil {
			return nil, t2, err
		}
	}

	temporalTx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil, t2, errors.New("tx is not a temporal tx")
	}

	doms, err := libstate.NewSharedDomains(temporalTx, te.logger)

	if err != nil {
		tx.Rollback()
		return nil, t2, err
	}

	doms.SetTxNum(te.lastCommittedTxNum)
	rs := te.rs.WithDomains(doms)

	err = resetWorkers(ctx, rs, tx)

	if err != nil {
		if !useExternalTx {
			tx.Rollback()
		}

		return nil, t2, err
	}

	if !useExternalTx {
		te.agg.BuildFilesInBackground(te.lastCommittedTxNum)
	}

	te.doms.ClearRam(true)
	te.doms.Close()

	te.rs = rs
	te.doms = doms

	return tx, t2, nil
}

type execRequest struct {
	blockNum     uint64
	blockHash    common.Hash
	gasPool      *core.GasPool
	tasks        []exec.Task
	applyResults chan applyResult
	profile      bool
}

type blockExecutor struct {
	sync.Mutex
	blockNum  uint64
	blockHash common.Hash

	tasks   []*execTask
	results []*execResult

	// For a task that runs only after all of its preceding tasks have finished and passed validation,
	// its result will be absolutely valid and therefore its validation could be skipped.
	// This map stores the boolean value indicating whether a task satisfy this condition (absolutely valid).
	skipCheck map[int]bool

	// Execution tasks stores the state of each execution task
	execTasks execStatusList

	// Validate tasks stores the state of each validation task
	validateTasks execStatusList

	// Finalize tasks stores the state of each finalization task
	finalizeTasks execStatusList

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
	cntExec, cntSpecExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail, cntFinalized int

	// cummulative gas for this block
	gasUsed     uint64
	blobGasUsed uint64
	gasPool     *core.GasPool

	execFailed, execAborted []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	applyResults chan applyResult

	execStarted time.Time
	result      *blockResult
}

func newBlockExec(blockNum uint64, blockHash common.Hash, gasPool *core.GasPool, applyResults chan applyResult, profile bool) *blockExecutor {
	return &blockExecutor{
		blockNum:     blockNum,
		blockHash:    blockHash,
		begin:        time.Now(),
		stats:        map[int]ExecutionStat{},
		skipCheck:    map[int]bool{},
		estimateDeps: map[int][]int{},
		preValidated: map[int]bool{},
		blockIO:      &state.VersionedIO{},
		versionMap:   state.NewVersionMap(),
		profile:      profile,
		applyResults: applyResults,
		gasPool:      gasPool,
	}
}

func (be *blockExecutor) nextResult(ctx context.Context, pe *parallelExecutor, res *exec.TxResult, applyTx kv.Tx) (result *blockResult, err error) {

	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	tx := task.index
	be.results[tx] = &execResult{res}
	if res.Err != nil {
		if execErr, ok := res.Err.(core.ErrExecAbortError); ok {
			if execErr.OriginError != nil && be.skipCheck[tx] {
				// If the transaction failed when we know it should not fail, this means the transaction itself is
				// bad (e.g. wrong nonce), and we should exit the execution immediately
				return nil, fmt.Errorf("could not apply tx %d:%d [%v]: %w", be.blockNum, res.Version().TxIndex, task.TxHash(), execErr.OriginError)
			}

			if res.Version().Incarnation > len(be.tasks) {
				if execErr.OriginError != nil {
					return nil, fmt.Errorf("could not apply tx %d:%d [%v]: %w: too many incarnations: %d", be.blockNum, res.Version().TxIndex, task.TxHash(), execErr.OriginError, res.Version().Incarnation)
				} else {
					return nil, fmt.Errorf("could not apply tx %d:%d [%v]: too many incarnations: %d", be.blockNum, res.Version().TxIndex, task.TxHash(), res.Version().Incarnation)
				}
			}

			be.blockIO.RecordReads(res.Version().TxIndex, res.TxIn)

			addedDependencies := false

			if execErr.DependencyTxIndex >= 0 {
				dependency := execErr.DependencyTxIndex + 1

				l := len(be.estimateDeps[tx])
				for l > 0 && be.estimateDeps[tx][l-1] > dependency {
					be.execTasks.removeDependency(be.estimateDeps[tx][l-1])
					be.estimateDeps[tx] = be.estimateDeps[tx][:l-1]
					l--
				}

				addedDependencies = be.execTasks.addDependency(dependency, tx)
				be.execAborted[tx]++

				if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
					fmt.Println(be.blockNum, "ABORT", tx, be.txIncarnations[tx], be.execFailed[tx], be.execAborted[tx], "dep", dependency, "err", execErr.OriginError)
				}
			} else {
				estimate := 0

				if len(be.estimateDeps[tx]) > 0 {
					estimate = be.estimateDeps[tx][len(be.estimateDeps[tx])-1]
				}
				addedDependencies = be.execTasks.addDependency(estimate, tx)
				newEstimate := estimate + (estimate+tx)/2
				if newEstimate >= tx {
					newEstimate = tx - 1
				}
				be.estimateDeps[tx] = append(be.estimateDeps[tx], newEstimate)
				be.execAborted[tx]++

				if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
					fmt.Println(be.blockNum, "ABORT", tx, be.txIncarnations[tx], be.execFailed[tx], be.execAborted[tx], "est dep", estimate, "err", execErr.OriginError)
				}
			}

			be.execTasks.clearInProgress(tx)

			if !addedDependencies {
				be.execTasks.pushPending(tx)
			}
			be.txIncarnations[tx]++
			be.cntAbort++
		} else {
			return nil, fmt.Errorf("unexptected exec error: %w", err)
		}
	} else {
		txIndex := res.Version().TxIndex

		be.blockIO.RecordReads(txIndex, res.TxIn)

		if res.Version().Incarnation == 0 {
			be.blockIO.RecordWrites(txIndex, res.TxOut)
			be.blockIO.RecordAllWrites(txIndex, res.TxOut)
		} else {
			if res.TxOut.HasNewWrite(be.blockIO.AllWriteSet(txIndex)) {
				be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := be.blockIO.AllWriteSet(txIndex)

			// Remove entries that were previously written but are no longer written

			cmpMap := map[common.Address]map[state.AccountKey]struct{}{}

			for _, w := range res.TxOut {
				keys, ok := cmpMap[w.Address]
				if !ok {
					keys = map[state.AccountKey]struct{}{}
					cmpMap[w.Address] = keys
				}
				keys[state.AccountKey{Path: w.Path, Key: w.Key}] = struct{}{}
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap[v.Address][state.AccountKey{Path: v.Path, Key: v.Key}]; !ok {
					be.versionMap.Delete(v.Address, v.Path, v.Key, txIndex, true)
				}
			}

			be.blockIO.RecordWrites(txIndex, res.TxOut)
			be.blockIO.RecordAllWrites(txIndex, res.TxOut)
		}

		be.validateTasks.pushPending(tx)
		be.execTasks.markComplete(tx)
		be.cntSuccess++

		be.execTasks.removeDependency(tx)
	}

	// do validations ...
	maxComplete := be.execTasks.maxComplete()
	toValidate := make(sort.IntSlice, 0, 2)

	for be.validateTasks.minPending() <= maxComplete && be.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, be.validateTasks.takeNextPending())
	}

	cntInvalid := 0

	for i := 0; i < len(toValidate); i++ {
		be.cntTotalValidations++

		tx := toValidate[i]
		txVersion := be.tasks[tx].Task.Version()
		txIncarnation := be.txIncarnations[tx]
		trace := false
		tracePrefix := ""

		if trace = dbg.TraceTransactionIO && traceTx(be.blockNum, txVersion.TxIndex); trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txIncarnation)
			fmt.Println(tracePrefix, "RD", be.blockIO.ReadSet(txVersion.TxIndex).Len(), "WRT", len(be.blockIO.WriteSet(txVersion.TxIndex)))
			be.blockIO.ReadSet(txVersion.TxIndex).Scan(func(vr *state.VersionedRead) bool {
				fmt.Println(tracePrefix, "RD", vr.String())
				return true
			})
			for _, vw := range be.blockIO.WriteSet(txVersion.TxIndex) {
				fmt.Println(tracePrefix, "WRT", vw.String())
			}
		}

		if be.skipCheck[tx] ||
			state.ValidateVersion(txVersion.TxIndex, be.blockIO, be.versionMap,
				func(readsource state.ReadSource, readVersion, writtenVersion state.Version) bool {
					return readsource == state.MapRead && readVersion == writtenVersion
				}) {

			if cntInvalid == 0 {
				be.validateTasks.markComplete(tx)
				// note this assumes that tasks are pushed in order as finalization needs to happen in block order
				be.finalizeTasks.pushPending(tx)
			}
		} else {
			cntInvalid++

			be.cntValidationFail++
			be.execFailed[tx]++

			if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
				fmt.Println(be.blockNum, "FAILED", tx, be.txIncarnations[tx], "failed", be.execFailed[tx], "aborted", be.execAborted[tx])
			}

			// 'create validation tasks for all transactions > tx ...'
			be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			be.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes
			be.execTasks.clearComplete(tx)
			be.execTasks.pushPending(tx)
			be.preValidated[tx] = false
			be.txIncarnations[tx]++
		}

		be.versionMap.SetTrace(trace)
		be.versionMap.FlushVersionedWrites(be.blockIO.WriteSet(txVersion.TxIndex), cntInvalid == 0, tracePrefix)
		be.versionMap.SetTrace(false)
	}

	maxValidated := be.validateTasks.maxComplete()

	var applyResult txResult
	var stateWriter *state.StateWriterBuffered

	if be.finalizeTasks.minPending() != -1 {
		stateWriter = state.NewStateWriterBuffered(pe.rs, nil)
		stateReader := state.NewBufferedReader(pe.rs, state.NewReaderV3(pe.rs.Domains().AsGetter(applyTx)))

		applyResult = txResult{
			blockNum:   be.blockNum,
			traceFroms: map[common.Address]struct{}{},
			traceTos:   map[common.Address]struct{}{},
		}

		toFinalize := make(sort.IntSlice, 0, 2)

		for be.finalizeTasks.minPending() <= maxValidated && be.finalizeTasks.minPending() >= 0 {
			toFinalize = append(toFinalize, be.finalizeTasks.takeNextPending())
		}

		for i := 0; i < len(toFinalize); i++ {
			tx := toFinalize[i]
			txTask := be.tasks[tx].Task
			txIndex := txTask.Version().TxIndex

			var prevReceipt *types.Receipt
			if txIndex > 0 && tx > 0 {
				prevReceipt = be.results[tx-1].Receipt
			}

			txResult := be.results[tx]

			if err := be.gasPool.SubGas(txResult.ExecutionResult.GasUsed); err != nil {
				return nil, err
			}

			_, err = txResult.finalize(prevReceipt, pe.cfg.engine, be.versionMap, stateReader, stateWriter)

			if err != nil {
				return nil, err
			}

			applyResult.txNum = txTask.Version().TxNum
			if txResult.Receipt != nil {
				applyResult.gasUsed += int64(txResult.Receipt.GasUsed)
				be.gasUsed += txResult.Receipt.GasUsed
				applyResult.receipts = append(applyResult.receipts, txResult.Receipt)
			}
			applyResult.blockTime = txTask.BlockTime()
			applyResult.logs = append(applyResult.logs, txResult.Logs...)
			maps.Copy(applyResult.traceFroms, txResult.TraceFroms)
			maps.Copy(applyResult.traceTos, txResult.TraceTos)
			be.cntFinalized++
			be.finalizeTasks.markComplete(tx)
		}
	}

	// this needs to be called after finalization to make sure
	// that coinbase updates are considered by subsequent
	// transactions
	be.scheduleExecution(ctx, pe)

	if applyResult.txNum > 0 {
		pe.executedGas.Add(int64(applyResult.gasUsed))
		pe.lastExecutedTxNum.Store(int64(applyResult.txNum))
		applyResult.writeSet = stateWriter.WriteSet()
		be.applyResults <- &applyResult
	}

	if be.finalizeTasks.countComplete() == len(be.tasks) && be.execTasks.countComplete() == len(be.tasks) {
		pe.logger.Debug("exec summary", "block", be.blockNum, "tasks", len(be.tasks), "execs", be.cntExec,
			"speculative", be.cntSpecExec, "success", be.cntSuccess, "aborts", be.cntAbort, "validations", be.cntTotalValidations, "failures", be.cntValidationFail,
			"retries", fmt.Sprintf("%.2f%%", float64(be.cntAbort+be.cntValidationFail)/float64(be.cntExec)*100),
			"execs", fmt.Sprintf("%.2f%%", float64(be.cntExec)/float64(len(be.tasks))*100))

		var allDeps map[int]map[int]bool

		var deps state.DAG

		if be.profile {
			allDeps = state.GetDep(be.blockIO)
			deps = state.BuildDAG(be.blockIO, pe.logger)
		}

		isPartial := len(be.tasks) > 0 && be.tasks[0].Version().TxIndex != -1

		txTask := be.tasks[len(be.tasks)-1].Task

		var receipts types.Receipts

		for _, txResult := range be.results {
			if receipt := txResult.Receipt; receipt != nil {
				receipts = append(receipts, receipt)
			}
		}

		be.result = &blockResult{
			be.blockNum,
			txTask.BlockTime(),
			txTask.BlockHash(),
			txTask.BlockRoot(),
			nil,
			be.gasUsed,
			be.blobGasUsed,
			txTask.Version().TxNum,
			true,
			isPartial,
			be.blockIO,
			receipts,
			be.stats,
			&deps,
			allDeps}
		return be.result, nil
	}

	var lastTxNum uint64
	if maxValidated >= 0 {
		lastTxTask := be.tasks[maxValidated].Task
		lastTxNum = lastTxTask.Version().TxNum
	}

	txTask := be.tasks[0].Task

	return &blockResult{
		be.blockNum,
		txTask.BlockTime(),
		txTask.BlockHash(),
		txTask.BlockRoot(),
		nil,
		be.gasUsed,
		be.blobGasUsed,
		lastTxNum,
		false,
		len(be.tasks) > 0 && be.tasks[0].Version().TxIndex != -1,
		be.blockIO,
		nil,
		be.stats,
		nil,
		nil}, nil
}

func (be *blockExecutor) scheduleExecution(ctx context.Context, pe *parallelExecutor) {
	toExecute := make(sort.IntSlice, 0, 2)

	for be.execTasks.minPending() >= 0 {
		toExecute = append(toExecute, be.execTasks.takeNextPending())
	}

	gasPool := core.NewGasPool(be.gasPool.Gas(), be.gasPool.BlobGas())

	maxValidated := be.validateTasks.maxComplete()
	for i := 0; i < len(toExecute); i++ {
		nextTx := toExecute[i]
		execTask := be.tasks[nextTx]
		if nextTx == maxValidated+1 {
			be.skipCheck[nextTx] = true
		} else {
			txIndex := execTask.Version().TxIndex
			if be.txIncarnations[nextTx] > 0 &&
				(be.execAborted[nextTx] > 0 || be.execFailed[nextTx] > 0 || !be.blockIO.HasReads(txIndex) ||
					!state.ValidateVersion(txIndex, be.blockIO, be.versionMap,
						func(_ state.ReadSource, _, writtenVersion state.Version) bool {
							res := writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[writtenVersion.TxIndex+1]
							return res
						})) {
				be.execTasks.pushPending(nextTx)
				continue
			}
			be.cntSpecExec++
		}

		if dbg.TraceTransactionIO && be.txIncarnations[nextTx] > 1 {
			fmt.Println(be.blockNum, "EXEC", nextTx, be.txIncarnations[nextTx], "maxValidated", maxValidated, be.blockIO.HasReads(nextTx), "failed", be.execFailed[nextTx], "aborted", be.execAborted[nextTx])
		}

		be.cntExec++

		execTask.ResetGasPool(gasPool)

		if incarnation := be.txIncarnations[nextTx]; incarnation == 0 {
			pe.in.Add(ctx, &taskVersion{
				execTask:   execTask,
				version:    execTask.Version(),
				versionMap: be.versionMap,
				profile:    be.profile,
				stats:      be.stats,
				statsMutex: &be.Mutex})
		} else {
			version := execTask.Version()
			version.Incarnation = incarnation
			pe.in.ReTry(&taskVersion{
				execTask:   execTask,
				version:    version,
				versionMap: be.versionMap,
				profile:    be.profile,
				stats:      be.stats,
				statsMutex: &be.Mutex})
		}
	}
}

type parallelExecutor struct {
	txExecutor
	execWorkers    []*exec3.Worker
	stopWorkers    func()
	waitWorkers    func()
	in             *exec.QueueWithRetry
	rws            *exec.ResultsQueue
	workerCount    int
	blockExecutors map[uint64]*blockExecutor
}

func (pe *parallelExecutor) LogExecuted(tx kv.Tx) {
	pe.progress.LogExecuted(tx, pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) LogCommitted(tx kv.Tx, commitStart time.Time) {
	pe.progress.LogCommitted(tx, commitStart, pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) LogComplete(tx kv.Tx) {
	pe.progress.LogComplete(tx, pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) commit(ctx context.Context, execStage *StageState, tx kv.RwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.RwTx, time.Duration, error) {
	pe.pause()
	defer pe.resume()

	for {
		waiter, paused := pe.paused()
		if paused {
			break
		}
		select {
		case request := <-asyncTxChan:
			request.Apply()
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-waiter:
		}
	}

	return pe.txExecutor.commit(ctx, execStage, tx, useExternalTx, pe.resetWorkers)
}

func (pe *parallelExecutor) pause() {
	for _, worker := range pe.execWorkers {
		worker.Pause()
	}
}

func (pe *parallelExecutor) paused() (chan any, bool) {
	for _, worker := range pe.execWorkers {
		if waiter, paused := worker.Paused(); !paused {
			return waiter, false
		}
	}

	return nil, true
}

func (pe *parallelExecutor) resume() {
	for _, worker := range pe.execWorkers {
		worker.Resume()
	}
}

func (pe *parallelExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, applyTx kv.Tx) error {
	pe.Lock()
	defer pe.Unlock()

	for _, worker := range pe.execWorkers {
		worker.ResetState(rs, nil, nil, state.NewNoopWriter(), nil)
	}

	return nil
}

func (pe *parallelExecutor) execLoop(ctx context.Context) (err error) {
	defer func() {
		pe.Lock()
		applyTx := pe.applyTx
		pe.applyTx = nil
		pe.Unlock()

		if applyTx != nil {
			applyTx.Rollback()
		}
	}()

	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn("["+pe.logPrefix+"] exec loop panic", "rec", rec, "stack", dbg.Stack())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			pe.logger.Warn("["+pe.logPrefix+"] exec loop error", "err", err)
		} else {
			pe.logger.Debug("[" + pe.logPrefix + "] exec loop exit")
		}
	}()

	pe.RLock()
	applyTx := pe.applyTx
	pe.RUnlock()

	for {
		err := func() error {
			pe.Lock()
			defer pe.Unlock()
			if applyTx != pe.applyTx {
				if applyTx != nil {
					applyTx.Rollback()
				}
			}

			if pe.applyTx == nil {
				pe.applyTx, err = pe.cfg.db.BeginRo(ctx)

				if err != nil {
					return err
				}

				applyTx = pe.applyTx
			}
			return nil
		}()

		if err != nil {
			return err
		}

		select {
		case exec := <-pe.execRequests:
			if err := pe.processRequest(ctx, exec); err != nil {
				return err
			}
			continue
		case <-ctx.Done():
			return ctx.Err()
		case nextResult, ok := <-pe.rws.ResultCh():
			if !ok {
				return nil
			}
			closed, err := pe.rws.Drain(ctx, nextResult)
			if err != nil {
				return err
			}
			if closed {
				return nil
			}
		}

		blockResult, err := pe.processResults(ctx, applyTx)

		if err != nil {
			return err
		}

		if blockResult.complete {
			if blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum]; ok {
				pe.lastExecutedBlockNum.Store(int64(blockResult.BlockNum))
				pe.execCount.Add(int64(blockExecutor.cntExec))
				pe.abortCount.Add(int64(blockExecutor.cntAbort))
				pe.invalidCount.Add(int64(blockExecutor.cntValidationFail))
				pe.readCount.Add(blockExecutor.blockIO.ReadCount())
				pe.writeCount.Add(blockExecutor.blockIO.WriteCount())

				blockReceipts := make([]*types.Receipt, 0, len(blockExecutor.results))
				for _, result := range blockExecutor.results {
					if result.Receipt != nil {
						blockReceipts = append(blockReceipts, result.Receipt)
					}
				}

				result := blockExecutor.results[len(blockExecutor.results)-1]

				writeSet, err := func() (map[string]*libstate.KvList, error) {
					pe.RLock()
					defer pe.RUnlock()

					ibs := state.New(state.NewBufferedReader(pe.rs, state.NewReaderV3(pe.rs.Domains().AsGetter(applyTx))))
					ibs.SetTxContext(result.Version().BlockNum, result.Version().TxIndex)
					ibs.SetVersion(result.Version().Incarnation)

					txTask := result.Task.(*taskVersion).Task.(*exec.TxTask)

					syscall := func(contract common.Address, data []byte) ([]byte, error) {
						ret, err := core.SysCallContract(contract, data, pe.cfg.chainConfig, ibs, txTask.Header, pe.cfg.engine, false, pe.hooks, vm.Config{})
						if err != nil {
							return nil, err
						}
						result.Logs = append(result.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
						return ret, err
					}

					chainReader := consensuschain.NewReader(pe.cfg.chainConfig, applyTx, pe.cfg.blockReader, pe.logger)
					if pe.isMining {
						_, txTask.Txs, blockReceipts, _, err =
							pe.cfg.engine.FinalizeAndAssemble(
								pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
								txTask.Withdrawals, chainReader, syscall, nil, pe.logger)
					} else {
						_, _, _, err =
							pe.cfg.engine.Finalize(
								pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
								txTask.Withdrawals, chainReader, syscall, false, pe.logger)
					}

					if err != nil {
						return nil, fmt.Errorf("can't finalize block: %w", err)
					}

					stateWriter := state.NewStateWriterBuffered(pe.rs, nil)

					if err = ibs.MakeWriteSet(pe.cfg.chainConfig.Rules(result.BlockNumber(), result.BlockTime()), stateWriter); err != nil {
						return nil, err
					}

					return stateWriter.WriteSet(), nil
				}()

				if err != nil {
					return err
				}

				blockExecutor.applyResults <- &txResult{
					blockNum:   blockResult.BlockNum,
					txNum:      blockResult.lastTxNum,
					blockTime:  blockResult.BlockTime,
					writeSet:   writeSet,
					logs:       result.Logs,
					traceFroms: result.TraceFroms,
					traceTos:   result.TraceTos,
				}

				if !blockExecutor.execStarted.IsZero() {
					pe.blockExecMetrics.Duration.Add(time.Since(blockExecutor.execStarted))
					pe.blockExecMetrics.BlockCount.Add(1)
				}
				blockExecutor.applyResults <- blockResult
				delete(pe.blockExecutors, blockResult.BlockNum)
			}

			if blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum+1]; ok {
				pe.onBlockStart(ctx, blockExecutor.blockNum, blockExecutor.blockHash)
				blockExecutor.execStarted = time.Now()
				blockExecutor.scheduleExecution(ctx, pe)
			}
		}
	}
}

func (pe *parallelExecutor) processRequest(ctx context.Context, execRequest *execRequest) (err error) {
	prevSenderTx := map[common.Address]int{}
	var scheduleable *blockExecutor
	var executor *blockExecutor

	for i, txTask := range execRequest.tasks {
		t := &execTask{
			Task:               txTask,
			index:              i,
			shouldDelayFeeCalc: true,
		}

		blockNum := t.Version().BlockNum

		if executor == nil {
			var ok bool
			executor, ok = pe.blockExecutors[blockNum]

			if !ok {
				executor = newBlockExec(blockNum, execRequest.blockHash, execRequest.gasPool, execRequest.applyResults, execRequest.profile)
			}
		}

		executor.tasks = append(executor.tasks, t)
		executor.results = append(executor.results, nil)
		executor.txIncarnations = append(executor.txIncarnations, 0)
		executor.execFailed = append(executor.execFailed, 0)
		executor.execAborted = append(executor.execAborted, 0)

		executor.skipCheck[len(executor.tasks)-1] = false
		executor.estimateDeps[len(executor.tasks)-1] = []int{}

		executor.execTasks.pushPending(i)
		executor.validateTasks.pushPending(i)

		if len(t.Dependencies()) > 0 {
			for _, depTxIndex := range t.Dependencies() {
				executor.execTasks.addDependency(depTxIndex+1, i)
			}
			executor.execTasks.clearPending(i)
		} else {
			sender, err := t.TxSender()
			if err != nil {
				return err
			}
			if sender != nil {
				if tx, ok := prevSenderTx[*sender]; ok {
					executor.execTasks.addDependency(tx, i)
					executor.execTasks.clearPending(i)
				}

				prevSenderTx[*sender] = i
			}
		}

		if t.IsBlockEnd() {
			if len(pe.blockExecutors) == 0 {
				pe.blockExecutors = map[uint64]*blockExecutor{
					blockNum: executor,
				}
				scheduleable = executor
			} else {
				pe.blockExecutors[t.Version().BlockNum] = executor
			}

			executor = nil
		}
	}

	if scheduleable != nil {
		pe.blockExecMetrics.BlockCount.Add(1)
		scheduleable.execStarted = time.Now()
		scheduleable.scheduleExecution(ctx, pe)
	}

	return nil
}

func (pe *parallelExecutor) processResults(ctx context.Context, applyTx kv.Tx) (blockResult *blockResult, err error) {
	rwsIt := pe.rws.Iter()

	for rwsIt.HasNext() && (blockResult == nil || !blockResult.complete) {
		txResult := rwsIt.PopNext()

		if pe.cfg.syncCfg.ChaosMonkey && pe.enableChaosMonkey {
			chaosErr := chaos_monkey.ThrowRandomConsensusError(false, txResult.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
			if chaosErr != nil {
				log.Warn("Monkey in consensus")
				return blockResult, chaosErr
			}
		}

		blockExecutor, ok := pe.blockExecutors[txResult.Version().BlockNum]

		if !ok {
			return nil, fmt.Errorf("unknown block: %d", txResult.Version().BlockNum)
		}

		blockResult, err = blockExecutor.nextResult(ctx, pe, txResult, applyTx)

		if err != nil {
			return blockResult, err
		}
	}

	return blockResult, nil
}

func (pe *parallelExecutor) run(ctx context.Context) (context.Context, context.CancelFunc) {
	pe.execRequests = make(chan *execRequest, 100_000)
	pe.in = exec.NewQueueWithRetry(100_000)

	pe.taskExecMetrics = exec3.NewWorkerMetrics()
	pe.blockExecMetrics = newBlockExecMetrics()

	execLoopCtx, execLoopCtxCancel := context.WithCancel(ctx)
	pe.execLoopGroup, execLoopCtx = errgroup.WithContext(execLoopCtx)

	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		execLoopCtx, nil, true, pe.cfg.db, nil, nil, nil, pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine,
		pe.workerCount+1, pe.taskExecMetrics, pe.cfg.dirs, pe.isMining, pe.logger)

	pe.execLoopGroup.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Close()
		pe.resetWorkers(execLoopCtx, pe.rs, nil)
		return pe.execLoop(execLoopCtx)
	})

	return execLoopCtx, func() {
		execLoopCtxCancel()
		pe.wait(ctx)
		pe.stopWorkers()
		pe.in.Close()
	}
}

func (pe *parallelExecutor) wait(ctx context.Context) error {
	doneCh := make(chan error)

	go func() {
		if pe.execLoopGroup != nil {
			err := pe.execLoopGroup.Wait()
			if err != nil {
				doneCh <- err
				return
			}
			pe.waitWorkers()
		}
		doneCh <- nil
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-doneCh:
			return err
		}
	}
}
