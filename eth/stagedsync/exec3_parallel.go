package stagedsync

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/eth/consensuschain"
	chaos_monkey "github.com/erigontech/erigon/tests/chaos-monkey"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/rawdb"
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
Each parallel-worker have own IntraBlockState.
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
	processEvents(ctx context.Context, wait bool) *blockResult
	wait(ctx context.Context) error
	getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error)

	//these are reset by commit - so need to be read from the executor once its processing
	tx() kv.RwTx
	readState() *state.StateV3Buffered
	domains() *libstate.SharedDomains

	LogExecuted()
	LogCommitted(commitStart time.Time)
	LogComplete()
}

type applyResult interface {
}

type blockResult struct {
	BlockNum  uint64
	BlockTime uint64
	BlockHash common.Hash
	StateRoot common.Hash
	Err       error
	GasUsed   uint64
	lastTxNum uint64
	complete  bool
	TxIO      *state.VersionedIO
	Stats     map[int]ExecutionStat
	Deps      *state.DAG
	AllDeps   map[int]map[int]bool
}

type txResult struct {
	blockNum   uint64
	blockTime  uint64
	txNum      uint64
	gasUsed    uint64
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
	*exec.Result
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

	//fmt.Printf("(%d.%d) Finalize\n", txIndex, task.version.Incarnation)

	versionedReader := state.NewVersionedStateReader(txIndex, result.TxIn, vm)
	ibs := state.New(versionedReader)
	ibs.SetTxContext(task.Version().BlockNum, txIndex)
	ibs.SetVersion(task.version.Incarnation)
	ibs.ApplyVersionedWrites(result.TxOut)
	versionedReader.SetStateReader(stateReader)

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil, nil
	}

	if task.IsBlockEnd() {
		return nil, nil
	}

	txHash := task.TxHash()
	blockNum := txTask.BlockNumber()
	blockHash := txTask.BlockHash()

	for _, l := range result.Logs {
		ibs.AddLog(l)
	}

	if task.shouldDelayFeeCalc {
		if txTask.Config.IsLondon(blockNum) {
			ibs.AddBalance(result.ExecutionResult.BurntContractAddress, result.ExecutionResult.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		ibs.AddBalance(result.Coinbase, result.ExecutionResult.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if engine != nil {
			if postApplyMessageFunc := engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				coinbaseBalance, err := ibs.GetBalance(result.Coinbase)

				if err != nil {
					return nil, err
				}

				execResult := *result.ExecutionResult
				execResult.CoinbaseInitBalance = coinbaseBalance.Clone()

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
			}
		}
	}

	if txTask.Config.IsByzantium(blockNum) {
		ibs.FinalizeTx(txTask.Config.Rules(txTask.BlockNumber(), txTask.BlockTime()), stateWriter)
	}

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx.
	result.Receipt = &types.Receipt{
		Type:              txTask.TxType(),
		PostState:         nil,
		CumulativeGasUsed: result.ExecutionResult.UsedGas,
		GasUsed:           result.ExecutionResult.UsedGas,
		TxHash:            txHash,
		// Set the receipt logs and create the bloom filter.
		Logs:             ibs.GetLogs(txTask.TxIndex, txHash, blockNum, blockHash),
		BlockHash:        blockHash,
		BlockNumber:      new(big.Int).SetUint64(blockNum),
		TransactionIndex: uint(txTask.TxIndex),
	}

	if result.ExecutionResult.Failed() {
		result.Receipt.Status = types.ReceiptStatusFailed
	} else {
		result.Receipt.Status = types.ReceiptStatusSuccessful
	}

	if prevReceipt != nil {
		result.Receipt.CumulativeGasUsed += prevReceipt.CumulativeGasUsed
	}

	// If the transaction created a contract, store the creation address in the receipt.
	message, err := task.TxMessage()

	if err != nil {
		return nil, err
	}

	if message.To() == nil {
		result.Receipt.ContractAddress = crypto.CreateAddress(message.From(), txTask.Tx().GetNonce())
	}

	result.Receipt.Bloom = types.CreateBloom(types.Receipts{result.Receipt})

	return result.Receipt, nil
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
	rs *state.StateV3Buffered,
	ibs *state.IntraBlockState,
	stateWriter state.StateWriter,
	stateReader state.ResettableStateReader,
	chainConfig *chain.Config,
	chainReader consensus.ChainReader,
	dirs datadir.Dirs,
	calcFees bool) (result *exec.Result) {

	defer func() {
		if r := recover(); r != nil {
			// Recover from dependency panic and retry the execution.
			if r != state.ErrDependency {
				log.Debug("Recovered from EVM failure.", "Error:", r, "stack", dbg.Stack())
			}
			var err error
			if ibs.DepTxIndex() < 0 {
				err = fmt.Errorf("EVM failure: %s at: %s", r, dbg.Stack())
			}
			result = &exec.Result{Err: exec.ErrExecAbortError{Dependency: ibs.DepTxIndex(), OriginError: err}}
		}
	}()

	var start time.Time
	if ev.profile {
		start = time.Now()
	}

	result = ev.execTask.Execute(evm, vmCfg, engine, genesis, gasPool, rs, ibs,
		stateWriter, stateReader, chainConfig, chainReader, dirs, !ev.shouldDelayFeeCalc)

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

	if ibs.HadInvalidRead() || result.Err != nil {
		if err, ok := result.Err.(exec.ErrExecAbortError); !ok {
			result.Err = exec.ErrExecAbortError{Dependency: ibs.DepTxIndex(), OriginError: err}
		}
	}

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
	cfg                      ExecuteBlockCfg
	execStage                *StageState
	agg                      *libstate.Aggregator
	rs                       *state.StateV3Buffered
	doms                     *libstate.SharedDomains
	accumulator              *shards.Accumulator
	u                        Unwinder
	isMining                 bool
	inMemExec                bool
	applyTx                  kv.RwTx
	logger                   log.Logger
	progress                 *Progress
	shouldGenerateChangesets bool

	lastExecutedBlockNum  uint64
	lastExecutedTxNum     uint64
	executedGas           uint64
	lastCommittedBlockNum uint64
	lastCommittedTxNum    uint64
	committedGas          uint64

	execCount    atomic.Int64
	abortCount   atomic.Int64
	invalidCount atomic.Int64
	readCount    atomic.Int64
	writeCount   atomic.Int64
}

func (te *txExecutor) tx() kv.RwTx {
	return te.applyTx
}

func (te *txExecutor) readState() *state.StateV3Buffered {
	return te.rs
}

func (te *txExecutor) domains() *libstate.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error) {

	if te.applyTx != nil {
		err := te.applyTx.Apply(func(tx kv.Tx) (err error) {
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

type execRequest struct {
	tasks   []exec.Task
	profile bool
}

type blockExecutor struct {
	sync.Mutex
	blockNum uint64

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
	gasUsed uint64

	execFailed, execAborted []int

	// Stores the execution statistics for the last incarnation of each task
	stats map[int]ExecutionStat

	result *blockResult
}

func newBlockExec(blockNum uint64, profile bool) *blockExecutor {
	return &blockExecutor{
		blockNum:     blockNum,
		begin:        time.Now(),
		stats:        map[int]ExecutionStat{},
		skipCheck:    map[int]bool{},
		estimateDeps: map[int][]int{},
		preValidated: map[int]bool{},
		blockIO:      &state.VersionedIO{},
		versionMap:   state.NewVersionMap(),
		profile:      profile,
	}
}

func (be *blockExecutor) nextResult(ctx context.Context, res *exec.Result, cfg ExecuteBlockCfg, rs *state.StateV3Buffered,
	accumulator *shards.Accumulator, in *exec.QueueWithRetry, applyTx kv.Tx, applyResults chan applyResult, logger log.Logger) (result *blockResult, err error) {

	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	tx := task.index

	fmt.Println("res", res.Version().TxIndex+1, res.Version().Incarnation, res.Err)

	be.results[tx] = &execResult{res}

	if res.Err != nil {
		if execErr, ok := res.Err.(exec.ErrExecAbortError); ok {
			if execErr.OriginError != nil && be.skipCheck[tx] {
				// If the transaction failed when we know it should not fail, this means the transaction itself is
				// bad (e.g. wrong nonce), and we should exit the execution immediately
				return nil, fmt.Errorf("could not apply tx %d:%d [%v]: %w", be.blockNum, res.Version().TxIndex, task.TxHash(), execErr.OriginError)
			}

			if res.Version().Incarnation > len(be.tasks) {
				return nil, fmt.Errorf("could not apply tx %d [%v]: %w: too many incarnations: %d", tx, task.TxHash(), execErr.OriginError, res.Version().Incarnation)
			}

			addedDependencies := false

			if execErr.Dependency >= 0 {
				l := len(be.estimateDeps[tx])
				for l > 0 && be.estimateDeps[tx][l-1] > execErr.Dependency {
					be.execTasks.removeDependency(be.estimateDeps[tx][l-1])
					be.estimateDeps[tx] = be.estimateDeps[tx][:l-1]
					l--
				}

				addedDependencies = be.execTasks.addDependencies(execErr.Dependency, tx)
				be.execFailed[tx]++
			} else {
				estimate := 0

				if len(be.estimateDeps[tx]) > 0 {
					estimate = be.estimateDeps[tx][len(be.estimateDeps[tx])-1]
				}
				addedDependencies = be.execTasks.addDependencies(estimate, tx)
				newEstimate := estimate + (estimate+tx)/2
				if newEstimate >= tx {
					newEstimate = tx - 1
				}
				be.estimateDeps[tx] = append(be.estimateDeps[tx], newEstimate)
				be.execAborted[tx]++
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

			cmpMap := btree.NewBTreeGOptions(func(a, b state.VersionKey) bool { return state.VersionKeyLess(&a, &b) }, btree.Options{NoLocks: true})

			for _, w := range res.TxOut {
				cmpMap.Set(w.Path)
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap.Get(v.Path); !ok {
					be.versionMap.Delete(v.Path, txIndex, true)
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
	fmt.Println("Max Exec", maxComplete)
	fmt.Println("Exec Complete", be.execTasks.complete)

	toValidate := make(sort.IntSlice, 0, 2)

	for be.validateTasks.minPending() <= maxComplete && be.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, be.validateTasks.takeNextPending())
	}

	fmt.Println("To Validate", toValidate)

	cntInvalid := 0

	for i := 0; i < len(toValidate); i++ {
		be.cntTotalValidations++

		tx := toValidate[i]
		txVersion := be.tasks[tx].Task.Version()

		if be.skipCheck[tx] ||
			state.ValidateVersion(txVersion.TxIndex, be.blockIO, be.versionMap,
				func(readsource state.ReadSource, readVersion, writtenVersion state.Version) bool {
					return readsource == state.MapRead && readVersion == writtenVersion
				}) {
			if cntInvalid == 0 {
				fmt.Println("valid", tx)
				be.versionMap.FlushVersionedWrites(be.blockIO.WriteSet(txVersion.TxIndex), true)
				be.validateTasks.markComplete(tx)
				// note this assumes that tasks are pushed in order as finalization needs to happen in block order
				be.finalizeTasks.pushPending(tx)
			}
		} else {
			cntInvalid++

			be.cntValidationFail++
			be.execFailed[tx]++
			be.versionMap.FlushVersionedWrites(be.blockIO.WriteSet(txVersion.TxIndex), false)
			// 'create validation tasks for all transactions > tx ...'
			be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			be.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes

			be.execTasks.clearComplete(tx)
			be.execTasks.pushPending(tx)

			be.preValidated[tx] = false
			be.txIncarnations[tx]++
			fmt.Println("invalid", tx)
		}
	}

	maxValidated := be.validateTasks.maxComplete()
	fmt.Println("Max Validated", maxValidated)
	fmt.Println("Validation Complete", be.validateTasks.complete)
	fmt.Println("Exec Pending", be.execTasks.pending)
	be.scheduleExecution(ctx, in)

	if be.finalizeTasks.minPending() != -1 {
		stateWriter := state.NewStateWriterBufferedV3(rs, accumulator)
		stateReader := state.NewBufferedReader(rs, state.NewReaderParallelV3(rs.Domains(), applyTx))

		applyResult := txResult{
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
			_, err = txResult.finalize(prevReceipt, cfg.engine, be.versionMap, stateReader, stateWriter)

			if err != nil {
				return nil, err
			}

			applyResult.txNum = txTask.Version().TxNum
			if txResult.Receipt != nil {
				applyResult.gasUsed = txResult.Receipt.GasUsed
			}
			applyResult.blockTime = txTask.BlockTime()
			applyResult.logs = append(applyResult.logs, txResult.Logs...)
			maps.Copy(applyResult.traceFroms, txResult.TraceFroms)
			maps.Copy(applyResult.traceTos, txResult.TraceTos)
			be.cntFinalized++
			be.finalizeTasks.markComplete(tx)
			be.gasUsed += applyResult.gasUsed
		}

		if applyResult.txNum > 0 {
			applyResult.writeSet = stateWriter.WriteSet()
			applyResults <- &applyResult
		}
	}

	if be.finalizeTasks.countComplete() == len(be.tasks) && be.execTasks.countComplete() == len(be.tasks) {
		/*pe.logger.Debug*/ fmt.Println("exec summary", "block", be.blockNum, "tasks", len(be.tasks), "execs", be.cntExec,
			"speculative", be.cntSpecExec, "success", be.cntSuccess, "aborts", be.cntAbort, "validations", be.cntTotalValidations, "failures", be.cntValidationFail,
			"retries", fmt.Sprintf("%.2f%%", float64(be.cntAbort+be.cntValidationFail)/float64(be.cntExec)*100),
			"execs", fmt.Sprintf("%.2f%%", float64(be.cntExec)/float64(len(be.tasks))*100))

		var allDeps map[int]map[int]bool

		var deps state.DAG

		if be.profile {
			allDeps = state.GetDep(be.blockIO)
			deps = state.BuildDAG(be.blockIO, logger)
		}

		txTask := be.tasks[len(be.tasks)-1].Task
		be.result = &blockResult{
			be.blockNum,
			txTask.BlockTime(),
			txTask.BlockHash(),
			txTask.BlockRoot(),
			nil,
			be.gasUsed,
			txTask.Version().TxNum,
			true,
			be.blockIO,
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
		lastTxNum,
		false,
		be.blockIO,
		be.stats,
		nil,
		nil}, nil
}

func (be *blockExecutor) scheduleExecution(ctx context.Context, in *exec.QueueWithRetry) {
	toExecute := make(sort.IntSlice, 0, 2)

	for be.execTasks.minPending() >= 0 {
		toExecute = append(toExecute, be.execTasks.takeNextPending())
	}

	maxValidated := be.validateTasks.maxComplete()
	for i := 0; i < len(toExecute); i++ {
		nextTx := toExecute[i]

		execTask := be.tasks[nextTx]

		if nextTx == maxValidated+1 {
			be.skipCheck[nextTx] = true
		} else {
			txIndex := execTask.Version().TxIndex
			if be.txIncarnations[nextTx] > 0 &&
				(be.execAborted[nextTx] > 0 ||
					!be.blockIO.HasReads(txIndex) ||
					!state.ValidateVersion(txIndex, be.blockIO, be.versionMap,
						func(_ state.ReadSource, _, writtenVersion state.Version) bool {
							fmt.Println("Val Spec", maxValidated, be.txIncarnations[writtenVersion.TxIndex+1], writtenVersion, writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[writtenVersion.TxIndex+1])
							return writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[writtenVersion.TxIndex+1]
						})) {
				be.execTasks.pushPending(nextTx)
				continue
			}
			be.cntSpecExec++
		}

		be.cntExec++

		if incarnation := be.txIncarnations[nextTx]; incarnation == 0 {
			fmt.Println("exec", nextTx, incarnation)
			in.Add(ctx, &taskVersion{
				execTask:   execTask,
				version:    execTask.Version(),
				versionMap: be.versionMap,
				profile:    be.profile,
				stats:      be.stats,
				statsMutex: &be.Mutex})
		} else {
			fmt.Println("re exec", nextTx, incarnation)
			version := execTask.Version()
			version.Incarnation = incarnation
			in.ReTry(&taskVersion{
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
	blockResults chan *blockResult
	rwLoopG      *errgroup.Group
	applyLoopWg  sync.WaitGroup
	execWorkers  []*exec3.Worker
	stopWorkers  func()
	waitWorkers  func()
	in           *exec.QueueWithRetry
	rws          *exec.ResultsQueue
	workerCount  int
	pruneEvery   *time.Ticker
	logEvery     *time.Ticker

	blockExecutors map[uint64]*blockExecutor
	execRequests   chan *execRequest
}

func (pe *parallelExecutor) LogExecuted() {
	pe.progress.LogExecuted(pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) LogCommitted(commitStart time.Time) {
	pe.progress.LogCommitted(commitStart, pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) LogComplete() {
	pe.progress.LogComplete(pe.rs.StateV3, pe)
}

func (pe *parallelExecutor) applyLoop(ctx context.Context, applyResults chan applyResult) {
	defer pe.applyLoopWg.Done()

	tx, err := pe.cfg.db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()

	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn(pe.execStage.LogPrefix()+" apply loop panic", "rec", rec, "stack", dbg.Stack())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			pe.logger.Warn(pe.execStage.LogPrefix()+" apply loop error", "err", err)
		} else {
			pe.logger.Debug(pe.execStage.LogPrefix() + " apply loop exit")
		}
	}()

	err = func(ctx context.Context) error {
		for {
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

			blockResult, err := pe.processResults(ctx, tx, applyResults)

			if err != nil {
				return err
			}

			if blockResult.complete {
				fmt.Println("Block Complete", blockResult.BlockNum)
				//panic(blockResult.BlockNum)

				if blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum]; ok {
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

					ibs := state.New(state.NewBufferedReader(pe.rs, state.NewReaderParallelV3(pe.rs.Domains(), tx)))
					ibs.SetTxContext(result.Version().BlockNum, result.Version().TxIndex)
					ibs.SetVersion(result.Version().Incarnation)

					if txTask, ok := result.Task.(*exec.TxTask); ok {
						syscall := func(contract common.Address, data []byte) ([]byte, error) {
							return core.SysCallContract(contract, data, pe.cfg.chainConfig, ibs, txTask.Header, pe.cfg.engine, false)
						}

						chainReader := consensuschain.NewReader(pe.cfg.chainConfig, pe.applyTx, pe.cfg.blockReader, pe.logger)
						if pe.isMining {
							_, txTask.Txs, blockReceipts, _, err =
								pe.cfg.engine.FinalizeAndAssemble(
									pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
									txTask.Withdrawals, chainReader, syscall, nil, pe.logger)
						} else {
							_, _, _, err =
								pe.cfg.engine.Finalize(
									pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
									txTask.Withdrawals, chainReader, syscall, pe.logger)
						}

						if err != nil {
							return fmt.Errorf("can't finalize block: %w", err)
						}
					}

					stateWriter := state.NewStateWriterBufferedV3(pe.rs, pe.accumulator)

					if err = ibs.MakeWriteSet(pe.cfg.chainConfig.Rules(result.BlockNumber(), result.BlockTime()), stateWriter); err != nil {
						panic(err)
					}

					applyResults <- &txResult{
						blockNum:   blockResult.BlockNum,
						txNum:      blockResult.lastTxNum,
						blockTime:  blockResult.BlockTime,
						writeSet:   stateWriter.WriteSet(),
						logs:       result.Logs,
						traceFroms: result.TraceFroms,
						traceTos:   result.TraceTos,
					}

					applyResults <- blockResult
					delete(pe.blockExecutors, blockResult.BlockNum)
				}

				if blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum+1]; ok {
					blockExecutor.scheduleExecution(ctx, pe.in)
				}
			}
		}
	}(ctx)

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			//fmt.Println(err)
			pe.blockResults <- &blockResult{Err: err}
		}
	}
}

////TODO: owner of `resultCh` is main goroutine, but owner of `retryQueue` is applyLoop.
// Now rwLoop closing both (because applyLoop we completely restart)
// Maybe need split channels? Maybe don't exit from ApplyLoop? Maybe current way is also ok?

func (pe *parallelExecutor) rwLoop(ctx context.Context, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn(pe.execStage.LogPrefix()+"rw loop panic", "rec", rec, "stack", dbg.Stack())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			pe.logger.Warn(pe.execStage.LogPrefix()+" rw loop exit", "err", err)
		} else {
			pe.logger.Debug(pe.execStage.LogPrefix() + " rw loop loop exit")
		}
	}()

	tx := pe.applyTx
	if tx == nil {
		var err error
		tx, err = pe.cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	defer pe.applyLoopWg.Wait()
	applyCtx, cancelApplyCtx := context.WithCancel(ctx)
	defer cancelApplyCtx()
	pe.applyLoopWg.Add(1)

	blockComplete := false
	applyResults := make(chan applyResult, 10_000)

	go pe.applyLoop(applyCtx, applyResults)

	var logEvery <-chan time.Time
	var pruneEvery <-chan time.Time

	if pe.logEvery != nil {
		logEvery = pe.logEvery.C
	}

	if pe.pruneEvery != nil {
		pruneEvery = pe.pruneEvery.C
	}

	var lastBlockResult blockResult
	var uncommittedGas uint64

	err = func() error {
		for {
			select {
			case applyResult := <-applyResults:
				switch applyResult := applyResult.(type) {
				case *txResult:
					blockComplete = false
					pe.executedGas += applyResult.gasUsed
					pe.lastExecutedTxNum = applyResult.txNum

					pe.rs.SetTxNum(applyResult.txNum, applyResult.blockNum)

					if err := pe.rs.ApplyState4(ctx, tx,
						applyResult.blockNum, applyResult.txNum, applyResult.writeSet,
						nil, applyResult.logs, applyResult.traceFroms, applyResult.traceTos,
						pe.cfg.chainConfig, pe.cfg.chainConfig.Rules(applyResult.blockNum, applyResult.blockTime), false); err != nil {
						return err
					}

				case *blockResult:
					if applyResult.BlockNum > lastBlockResult.BlockNum {
						pe.doms.SetTxNum(applyResult.lastTxNum)
						pe.doms.SetBlockNum(applyResult.BlockNum)
						lastBlockResult = *applyResult
						pe.lastExecutedBlockNum = applyResult.BlockNum
						uncommittedGas += applyResult.GasUsed
					}

					if false {
						rh, err := pe.doms.ComputeCommitment(ctx, tx, true, applyResult.BlockNum, pe.execStage.LogPrefix())
						if err != nil {
							return err
						}
						if !bytes.Equal(rh, applyResult.StateRoot.Bytes()) {
							logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", pe.execStage.LogPrefix(), applyResult.BlockNum, rh, applyResult.StateRoot.Bytes(), applyResult.BlockHash))
							return errors.New("wrong trie root")
						}
					}

					if (applyResult.complete || applyResult.Err != nil) && pe.blockResults != nil {
						pe.blockResults <- applyResult
					}

				}
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery:
				pe.LogExecuted()
				if pe.agg.HasBackgroundFilesBuild() {
					logger.Info(fmt.Sprintf("[%s] Background files build", pe.execStage.LogPrefix()), "progress", pe.agg.BackgroundProgress())
				}
			case <-pruneEvery:
				if false {
					if pe.rs.SizeEstimate() < pe.cfg.batchSize.Bytes() && lastBlockResult.BlockNum > pe.lastCommittedBlockNum {
						rhash, err := pe.doms.ComputeCommitment(ctx, tx, true, lastBlockResult.BlockNum, pe.execStage.LogPrefix())

						if err != nil {
							return err
						}

						if !bytes.Equal(rhash, lastBlockResult.StateRoot.Bytes()) {
							logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x",
								pe.execStage.LogPrefix(), lastBlockResult.BlockNum, rhash, lastBlockResult.StateRoot.Bytes(), lastBlockResult.BlockHash))
							return errors.New("wrong trie root")
						}

						fmt.Println("BC DONE", lastBlockResult.BlockNum, hex.EncodeToString(rhash), hex.EncodeToString(lastBlockResult.StateRoot.Bytes())) //Temp Done

						pe.lastCommittedBlockNum = lastBlockResult.BlockNum
						pe.lastCommittedTxNum = lastBlockResult.lastTxNum
						pe.committedGas += uncommittedGas
						uncommittedGas = 0

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
					}
				}
				if pe.inMemExec || tx == pe.applyTx {
					break
				}

				// TODO call this on block complete event
				cancelApplyCtx()
				pe.applyLoopWg.Wait()

				var t0, t1, t2, t3, t4 time.Duration
				commitStart := time.Now()
				logger.Info("Committing (parallel)...", "blockComplete", blockComplete)
				if err := func() error {
					t0 = time.Since(commitStart)
					pe.Lock() // This is to prevent workers from starting work on any new txTask
					defer pe.Unlock()

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
						if err := pe.execStage.Update(tx, lastBlockResult.BlockNum); err != nil {
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

				applyCtx, cancelApplyCtx = context.WithCancel(ctx) //nolint:fatcontext
				defer cancelApplyCtx()
				pe.applyLoopWg.Add(1)
				go pe.applyLoop(applyCtx, applyResults)

				logger.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
			}
		}
	}()

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	if err := pe.doms.Flush(ctx, tx); err != nil {
		return err
	}
	if pe.execStage != nil {
		if err := pe.execStage.Update(tx, lastBlockResult.BlockNum); err != nil {
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
				executor = newBlockExec(blockNum, execRequest.profile)
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
			for _, val := range t.Dependencies() {
				executor.execTasks.addDependencies(val, i)
			}
			executor.execTasks.clearPending(i)
		} else {
			sender, err := t.TxSender()
			if err != nil {
				return err
			}
			if sender != nil {
				if tx, ok := prevSenderTx[*sender]; ok {
					executor.execTasks.addDependencies(tx, i)
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
		scheduleable.scheduleExecution(ctx, pe.in)
	}

	return nil
}

func (pe *parallelExecutor) processResults(ctx context.Context, applyTx kv.Tx, applyResults chan applyResult) (blockResult *blockResult, err error) {
	rwsIt := pe.rws.Iter()

	for rwsIt.HasNext() && (blockResult == nil || !blockResult.complete) {
		txResult := rwsIt.PopNext()

		//fmt.Println("PRQ", txTask.BlockNum, txTask.TxIndex, txTask.TxNum)
		if pe.cfg.syncCfg.ChaosMonkey {
			chaosErr := chaos_monkey.ThrowRandomConsensusError(pe.execStage.CurrentSyncCycle.IsInitialCycle, txResult.Version().TxIndex, pe.cfg.badBlockHalt, txResult.Err)
			if chaosErr != nil {
				log.Warn("Monkey in consensus")
				return blockResult, chaosErr
			}
		}

		blockExecutor, ok := pe.blockExecutors[txResult.Version().BlockNum]

		if !ok {
			return nil, fmt.Errorf("unknown block: %d", txResult.Version().BlockNum)
		}

		blockResult, err = blockExecutor.nextResult(ctx, txResult, pe.cfg, pe.rs, pe.accumulator, pe.in, applyTx, applyResults, pe.logger)

		if err != nil {
			return blockResult, err
		}
	}

	return blockResult, nil
}

func (pe *parallelExecutor) run(ctx context.Context) context.CancelFunc {
	pe.blockResults = make(chan *blockResult, 1000)
	pe.execRequests = make(chan *execRequest, 10_000)
	pe.in = exec.NewQueueWithRetry(10_000)

	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		pe.RWMutex.RLocker(), pe.accumulator, pe.logger, ctx, true, pe.cfg.db, pe.rs, nil, state.NewNoopWriter(), pe.in,
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
		return pe.rwLoop(rwLoopCtx, pe.logger)
	})

	return func() {
		if pe.logEvery != nil {
			pe.logEvery.Stop()
		}
		if pe.pruneEvery != nil {
			pe.pruneEvery.Stop()
		}

		rwLoopCtxCancel()
		pe.wait(ctx)
		pe.stopWorkers()
		close(pe.blockResults)
		pe.in.Close()
	}
}

func (pe *parallelExecutor) processEvents(ctx context.Context, wait bool) *blockResult {
	var applyChan mdbx.TxApplyChan

	if temporalTx, ok := pe.applyTx.(*temporal.RwTx); ok {
		if applySource, ok := temporalTx.RwTx.(mdbx.TxApplySource); ok {
			applyChan = applySource.ApplyChan()
		}
	}

	for wait || len(applyChan) > 0 {
		select {
		case result, ok := <-pe.blockResults:
			if !ok {
				return nil
			}
			return result
		case request := <-applyChan:
			request.Apply()
		case <-ctx.Done():
			return &blockResult{Err: ctx.Err()}
		}
	}

	return nil
}

func (pe *parallelExecutor) wait(ctx context.Context) error {
	doneCh := make(chan error)
	var applyChan mdbx.TxApplyChan

	if temporalTx, ok := pe.applyTx.(*temporal.RwTx); ok {
		if applySource, ok := temporalTx.RwTx.(mdbx.TxApplySource); ok {
			applyChan = applySource.ApplyChan()
		}
	}

	go func() {
		pe.applyLoopWg.Wait()
		if pe.rwLoopG != nil {
			err := pe.rwLoopG.Wait()
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
		case blockResult := <-pe.blockResults:
			if blockResult.Err != nil {
				return blockResult.Err
			}
		case request := <-applyChan:
			request.Apply()
		case <-ctx.Done():
			return ctx.Err()
		case err := <-doneCh:
			return err
		}
	}

}

func (pe *parallelExecutor) execute(ctx context.Context, tasks []exec.Task, profile bool) (bool, error) {
	pe.execRequests <- &execRequest{tasks, profile}
	return false, nil
}
