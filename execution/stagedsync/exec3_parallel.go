package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/turbo/shards"
)

/*
ExecV3 - parallel execution. Has many layers of abstractions - each layer does accumulate
state changes (updates) and can "atomically commit all changes to underlying layer of abstraction"

Layers from top to bottom:
- IntraBlockState - used to exec txs. It does store inside all updates of given txn.
Can understand if txn failed or OutOfGas - then revert all changes.
Each parallel-worker have own IntraBlockState.
IntraBlockState does commit changes to lower-abstraction-level by method `ibs.MakeWriteSet()`

- BufferedWriter - txs which executed by parallel workers can conflict with each-other.
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

func (pe *parallelExecutor) exec(ctx context.Context, execStage *StageState, u Unwinder,
	startBlockNum uint64, offsetFromBlockBeginning uint64, maxBlockNum uint64, blockLimit uint64,
	initialTxNum uint64, inputTxNum uint64, useExternalTx bool, initialCycle bool, rwTx kv.TemporalRwTx,
	accumulator *shards.Accumulator, readAhead chan uint64, logEvery *time.Ticker, flushEvery *time.Ticker) (*types.Header, kv.TemporalRwTx, error) {

	var asyncTxChan mdbx.TxApplyChan
	var asyncTx kv.Tx

	switch applyTx := rwTx.(type) {
	case *temporal.RwTx:
		temporalTx := applyTx.AsyncClone(mdbx.NewAsyncRwTx(applyTx.RwTx, 1000))
		asyncTxChan = temporalTx.ApplyChan()
		asyncTx = temporalTx
	default:
		return nil, rwTx, fmt.Errorf("expected *temporal.RwTx: got %T", rwTx)
	}

	applyResults := make(chan applyResult, 100_000)

	maxExecBlockNum := maxBlockNum
	if blockLimit > 0 && startBlockNum+uint64(blockLimit) < maxBlockNum {
		maxExecBlockNum = startBlockNum + blockLimit - 1
	}

	if blockLimit > 0 && min(startBlockNum+blockLimit, maxBlockNum) > startBlockNum+16 || maxBlockNum > startBlockNum+16 {
		log.Info(fmt.Sprintf("[%s] %s starting", execStage.LogPrefix(), "parallel"),
			"from", startBlockNum, "to", min(startBlockNum+blockLimit, maxBlockNum), "initialTxNum", initialTxNum,
			"initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx,
			"inMem", pe.inMemExec)
	}

	executorContext, executorCancel := pe.run(ctx)

	defer executorCancel()

	pe.resetWorkers(ctx, pe.rs, rwTx)

	if err := pe.executeBlocks(executorContext, asyncTx, startBlockNum, maxExecBlockNum, initialTxNum, readAhead, applyResults); err != nil {
		return nil, rwTx, err
	}

	var lastExecutedLog time.Time
	var lastCommitedLog time.Time
	var lastBlockResult blockResult
	var lastHeader *types.Header
	var uncommittedBlocks int64
	var uncommittedTransactions uint64
	var uncommittedGas int64
	var flushPending bool

	execErr := func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				pe.logger.Warn("["+execStage.LogPrefix()+"] rw panic", "rec", rec, "stack", dbg.Stack())
			} else if err != nil && !errors.Is(err, context.Canceled) {
				pe.logger.Warn("["+execStage.LogPrefix()+"] rw exit", "err", err)
			} else {
				pe.logger.Debug("[" + execStage.LogPrefix() + "] rw exit")
			}
		}()

		shouldGenerateChangesets := shouldGenerateChangeSets(pe.cfg, startBlockNum, maxBlockNum, initialCycle)
		changeSet := &changeset.StateChangeSet{}
		if shouldGenerateChangesets && startBlockNum > 0 {
			pe.domains().SetChangesetAccumulator(changeSet)
		}

		blockUpdateCount := 0
		blockApplyCount := 0

		for {
			select {
			case request := <-asyncTxChan:
				request.Apply()
			case applyResult := <-applyResults:
				switch applyResult := applyResult.(type) {
				case *txResult:
					uncommittedGas += applyResult.gasUsed
					uncommittedTransactions++
					pe.rs.SetTxNum(applyResult.blockNum, applyResult.txNum)
					if dbg.TraceApply && dbg.TraceBlock(applyResult.blockNum) {
						pe.rs.SetTrace(true)
						fmt.Println(applyResult.blockNum, "apply", applyResult.txNum, applyResult.stateUpdates.UpdateCount())
					}
					blockUpdateCount += applyResult.stateUpdates.UpdateCount()
					err := pe.rs.ApplyTxState(ctx, rwTx, applyResult.blockNum, applyResult.txNum, applyResult.stateUpdates,
						nil, applyResult.receipt, applyResult.logs, applyResult.traceFroms, applyResult.traceTos,
						pe.cfg.chainConfig, applyResult.rules, false)
					blockApplyCount += applyResult.stateUpdates.UpdateCount()
					pe.rs.SetTrace(false)
					if err != nil {
						return err
					}
				case *blockResult:
					if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
						checkReceipts := !pe.cfg.vmConfig.StatelessExec &&
							pe.cfg.chainConfig.IsByzantium(applyResult.BlockNum) &&
							!pe.cfg.vmConfig.NoReceipts && !pe.isMining

						b, err := pe.cfg.blockReader.BlockByHash(ctx, rwTx, applyResult.BlockHash)

						if err != nil {
							return fmt.Errorf("can't retrieve block %d: for post validation: %w", applyResult.BlockNum, err)
						}

						lastHeader = b.HeaderNoCopy()

						if lastHeader.Number.Uint64() != applyResult.BlockNum {
							return fmt.Errorf("block numbers don't match expected: %d: got: %d for hash %x", applyResult.BlockNum, lastHeader.Number.Uint64(), applyResult.BlockHash)
						}

						if blockUpdateCount != applyResult.ApplyCount {
							return fmt.Errorf("block %d: applyCount mismatch: got: %d expected %d", applyResult.BlockNum, blockUpdateCount, applyResult.ApplyCount)
						}

						if err := core.BlockPostValidation(applyResult.GasUsed, applyResult.BlobGasUsed, checkReceipts, applyResult.Receipts,
							lastHeader, pe.isMining, b.Transactions(), pe.cfg.chainConfig, pe.logger); err != nil {
							dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
							return fmt.Errorf("%w, block=%d, %v", consensus.ErrInvalidBlock, applyResult.BlockNum, err) //same as in stage_exec.go
						}

						if !pe.isMining && !applyResult.isPartial && !execStage.CurrentSyncCycle.IsInitialCycle {
							pe.cfg.notifications.RecentLogs.Add(applyResult.Receipts)
						}
					}

					if applyResult.BlockNum > lastBlockResult.BlockNum {
						uncommittedBlocks++
						pe.doms.SetTxNum(applyResult.lastTxNum)
						pe.doms.SetBlockNum(applyResult.BlockNum)
						lastBlockResult = *applyResult
					}

					flushPending = pe.rs.SizeEstimate() > pe.cfg.batchSize.Bytes()

					if !dbg.DiscardCommitment() {
						if !dbg.BatchCommitments || shouldGenerateChangesets || lastBlockResult.BlockNum == maxExecBlockNum ||
							(flushPending && lastBlockResult.BlockNum > pe.lastCommittedBlockNum) {

							resetExecGauges(ctx)

							if dbg.TraceApply && dbg.TraceBlock(applyResult.BlockNum) {
								fmt.Println(applyResult.BlockNum, "applied count", blockApplyCount, "last tx", applyResult.lastTxNum)
							}

							var trace bool
							if dbg.TraceBlock(applyResult.BlockNum) {
								fmt.Println(applyResult.BlockNum, "Commitment")
								trace = true
							}
							pe.doms.SetTrace(trace, !dbg.BatchCommitments)

							commitProgress := make(chan *commitment.CommitProgress, 100)

							go func() {
								logEvery := time.NewTicker(20 * time.Second)
								commitStart := time.Now()

								defer logEvery.Stop()
								var lastProgress commitment.CommitProgress

								var prevCommitedBlocks uint64
								var prevCommittedTransactions uint64
								var prevCommitedGas uint64

								logCommitted := func(commitProgress commitment.CommitProgress) {
									// this is an approximation of blcok prgress - it assumnes an
									// even distribution of keys to blocks
									if commitProgress.KeyIndex > 0 {
										progress := float64(commitProgress.KeyIndex) / float64(commitProgress.UpdateCount)
										committedGas := uint64(float64(uncommittedGas) * progress)
										committedTransactions := uint64(float64(uncommittedTransactions) * progress)
										commitedBlocks := uint64(float64(uncommittedBlocks) * progress)

										if committedTransactions-prevCommittedTransactions > 0 {
											pe.LogCommitted(commitStart,
												commitedBlocks-prevCommitedBlocks,
												committedTransactions-prevCommittedTransactions,
												committedGas-prevCommitedGas, rawdbhelpers.IdxStepsCountV3(rwTx), commitProgress)
										}

										lastCommitedLog = time.Now()
										prevCommitedBlocks = commitedBlocks
										prevCommittedTransactions = committedTransactions
										prevCommitedGas = committedGas
									}

									if pe.agg.HasBackgroundFilesBuild() {
										pe.logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
									}
								}

								for {
									select {
									case <-ctx.Done():
										return
									case progress, ok := <-commitProgress:
										if !ok {
											if time.Since(lastCommitedLog) > logInterval/20 {
												logCommitted(lastProgress)
											}
											return
										}
										lastProgress = *progress
									case <-logEvery.C:
										if time.Since(lastCommitedLog) > logInterval-(logInterval/90) {
											logCommitted(lastProgress)
										}
									}
								}
							}()

							if time.Since(lastExecutedLog) > logInterval/50 {
								pe.LogExecuted()
								lastExecutedLog = time.Now()
							}

							rh, err := pe.doms.ComputeCommitment(ctx, rwTx, true, applyResult.BlockNum, applyResult.lastTxNum, pe.logPrefix, commitProgress)
							close(commitProgress)
							captured := pe.doms.SetTrace(false, false)
							if err != nil {
								return err
							}
							resetCommitmentGauges(ctx)

							pe.domains().SavePastChangesetAccumulator(applyResult.BlockHash, applyResult.BlockNum, changeSet)
							if !pe.inMemExec {
								if err := changeset.WriteDiffSet(rwTx, applyResult.BlockNum, applyResult.BlockHash, changeSet); err != nil {
									return err
								}
							}
							pe.domains().SetChangesetAccumulator(nil)

							if !bytes.Equal(rh, applyResult.StateRoot.Bytes()) {
								pe.logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", pe.logPrefix, applyResult.BlockNum, rh, applyResult.StateRoot.Bytes(), applyResult.BlockHash))
								if !dbg.BatchCommitments {
									for _, line := range captured {
										fmt.Println(line)
									}

									dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
								}

								return handleIncorrectRootHashError(
									applyResult.BlockNum, applyResult.BlockHash, applyResult.ParentHash,
									rwTx, pe.cfg, execStage, maxBlockNum, pe.logger, u)
							}
							// fix these here - they will contain estimates after commit logging
							pe.txExecutor.lastCommittedBlockNum = lastBlockResult.BlockNum
							pe.txExecutor.lastCommittedTxNum = lastBlockResult.lastTxNum
							uncommittedBlocks = 0
							uncommittedGas = 0
							uncommittedGas = 0
						}
					}

					blockUpdateCount = 0
					blockApplyCount = 0

					if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
						return fmt.Errorf("stopping: block %d complete", applyResult.BlockNum)
					}

					if applyResult.BlockNum == maxExecBlockNum {
						switch {
						case applyResult.BlockNum == maxBlockNum:
							return nil
						case blockLimit > 0:
							return &ErrLoopExhausted{From: startBlockNum, To: applyResult.BlockNum, Reason: "block limit reached"}
						default:
							return nil
						}
					}

					if shouldGenerateChangesets && applyResult.BlockNum > 0 {
						changeSet = &changeset.StateChangeSet{}
						pe.domains().SetChangesetAccumulator(changeSet)
					}
				}
			case <-executorContext.Done():
				err = pe.wait(ctx)
				return fmt.Errorf("executor context failed: %w", err)
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				if time.Since(lastExecutedLog) > logInterval-(logInterval/90) {
					lastExecutedLog = time.Now()
					pe.LogExecuted()
					if pe.agg.HasBackgroundFilesBuild() {
						pe.logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
					}
				}
			case <-flushEvery.C:
				if flushPending {
					if !initialCycle {
						return &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult.BlockNum, Reason: "block batch is full"}
					}

					if rwTx, err = pe.flushAndCommit(ctx, execStage, rwTx, asyncTxChan, useExternalTx); err != nil {
						return fmt.Errorf("flush failed: %w", err)
					}

					flushPending = false
				}
			}
		}
	}()

	executorCancel()

	if execErr != nil {
		if !(errors.Is(execErr, context.Canceled) || errors.Is(execErr, &ErrLoopExhausted{})) {
			return nil, rwTx, execErr
		}
	}

	var err error
	if rwTx, err = pe.flushAndCommit(ctx, execStage, rwTx, asyncTxChan, useExternalTx); err != nil {
		return nil, rwTx, fmt.Errorf("flush failed: %w", err)
	}

	return lastHeader, rwTx, pe.wait(ctx)
}

func (pe *parallelExecutor) LogExecuted() {
	pe.progress.LogExecuted(pe.rs.StateV3, pe)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domain reads", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

func (pe *parallelExecutor) LogCommitted(commitStart time.Time, committedBlocks uint64, committedTransactions uint64, committedGas uint64, stepsInDb float64, lastProgress commitment.CommitProgress) {
	pe.committedGas += int64(committedGas)
	pe.txExecutor.lastCommittedBlockNum += committedBlocks
	pe.txExecutor.lastCommittedTxNum += committedTransactions
	pe.progress.LogCommitted(pe.rs.StateV3, pe, commitStart, stepsInDb, lastProgress)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domain reads", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

func (pe *parallelExecutor) LogComplete(stepsInDb float64) {
	pe.progress.LogComplete(pe.rs.StateV3, pe, stepsInDb)
	if domainMetrics := pe.domains().LogMetrics(); len(domainMetrics) > 0 {
		pe.logger.Info(fmt.Sprintf("[%s] domains", pe.logPrefix), domainMetrics...)
	}
	for domain, domainMetrics := range pe.domains().DomainLogMetrics() {
		pe.logger.Debug(fmt.Sprintf("[%s] %s", pe.logPrefix, domain), domainMetrics...)
	}
}

func (pe *parallelExecutor) flushAndCommit(ctx context.Context, execStage *StageState, applyTx kv.TemporalRwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.TemporalRwTx, error) {
	flushStart := time.Now()
	var flushTime time.Duration

	if !pe.inMemExec {
		if err := pe.doms.Flush(ctx, applyTx); err != nil {
			return applyTx, err
		}
		flushTime = time.Since(flushStart)
	}

	commitStart := time.Now()
	var t2 time.Duration
	var err error
	if applyTx, t2, err = pe.commit(ctx, execStage, applyTx, asyncTxChan, useExternalTx); err != nil {
		return applyTx, err
	}

	pe.logger.Info("["+pe.logPrefix+"] flushed", "block", pe.doms.BlockNum(), "time", time.Since(flushStart), "flush", flushTime, "commit", time.Since(commitStart), "db", t2, "externaltx", useExternalTx)
	return applyTx, nil
}

func (pe *parallelExecutor) commit(ctx context.Context, execStage *StageState, tx kv.TemporalRwTx, asyncTxChan mdbx.TxApplyChan, useExternalTx bool) (kv.TemporalRwTx, time.Duration, error) {
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

func (pe *parallelExecutor) resetWorkers(ctx context.Context, rs *state.StateV3Buffered, _ kv.TemporalTx) error {
	pe.Lock()
	defer pe.Unlock()

	for _, worker := range pe.execWorkers {
		// parallel workers hold thier own tx don't pass in an externals tx
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
				temporalDb, ok := pe.cfg.db.(kv.TemporalRwDB)
				if !ok {
					return errors.New("pe.cfg.db is not a temporal db")
				}
				pe.applyTx, err = temporalDb.BeginTemporalRo(ctx) //nolint

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
			pe.RLock()
			blockExecutor, ok := pe.blockExecutors[blockResult.BlockNum]
			pe.RUnlock()

			if ok {
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

				if blockResult.BlockNum > 0 {
					result := blockExecutor.results[len(blockExecutor.results)-1]

					stateUpdates, err := func() (state.StateUpdates, error) {
						pe.RLock()
						defer pe.RUnlock()

						ibs := state.New(state.NewBufferedReader(pe.rs, state.NewReaderV3(pe.rs.Domains().AsGetter(applyTx))))
						ibs.SetTxContext(result.Version().BlockNum, result.Version().TxIndex)
						ibs.SetVersion(result.Version().Incarnation)

						txTask, ok := result.Task.(*taskVersion).Task.(*exec.TxTask)

						if !ok {
							return state.StateUpdates{}, nil
						}

						syscall := func(contract common.Address, data []byte) ([]byte, error) {
							ret, err := core.SysCallContract(contract, data, pe.cfg.chainConfig, ibs, txTask.Header, pe.cfg.engine, false, *pe.cfg.vmConfig)
							if err != nil {
								return nil, err
							}
							result.Logs = append(result.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
							return ret, err
						}

						chainReader := consensuschain.NewReader(pe.cfg.chainConfig, applyTx, pe.cfg.blockReader, pe.logger)
						if pe.isMining {
							_, _, err =
								pe.cfg.engine.FinalizeAndAssemble(
									pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
									txTask.Withdrawals, chainReader, syscall, nil, pe.logger)
						} else {
							_, err =
								pe.cfg.engine.Finalize(
									pe.cfg.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, blockReceipts,
									txTask.Withdrawals, chainReader, syscall, false, pe.logger)
						}

						if err != nil {
							return state.StateUpdates{}, fmt.Errorf("can't finalize block: %w", err)
						}

						stateWriter := state.NewBufferedWriter(pe.rs, nil)

						if err = ibs.MakeWriteSet(txTask.EvmBlockContext.Rules(txTask.Config), stateWriter); err != nil {
							return state.StateUpdates{}, err
						}

						return stateWriter.WriteSet(), nil
					}()

					if err != nil {
						return err
					}

					blockResult.ApplyCount += stateUpdates.UpdateCount()
					if dbg.TraceApply && dbg.TraceBlock(blockResult.BlockNum) {
						stateUpdates.TraceBlockUpdates(blockResult.BlockNum, true)
						fmt.Println(blockResult.BlockNum, "apply count", blockResult.ApplyCount)
					}

					blockExecutor.applyResults <- &txResult{
						blockNum:     blockResult.BlockNum,
						txNum:        blockResult.lastTxNum,
						rules:        result.Rules(),
						stateUpdates: stateUpdates,
						logs:         result.Logs,
						traceFroms:   result.TraceFroms,
						traceTos:     result.TraceTos,
					}
				}

				if !blockExecutor.execStarted.IsZero() {
					pe.blockExecMetrics.Duration.Add(time.Since(blockExecutor.execStarted))
					pe.blockExecMetrics.BlockCount.Add(1)
				}
				blockExecutor.applyResults <- blockResult
				pe.Lock()
				delete(pe.blockExecutors, blockResult.BlockNum)
				pe.Unlock()
			}

			pe.RLock()
			blockExecutor, ok = pe.blockExecutors[blockResult.BlockNum+1]
			pe.RUnlock()

			if ok {
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
			pe.Lock()
			if len(pe.blockExecutors) == 0 {
				pe.blockExecutors = map[uint64]*blockExecutor{
					blockNum: executor,
				}
				scheduleable = executor
			} else {
				pe.blockExecutors[t.Version().BlockNum] = executor
			}
			pe.Unlock()

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

func (pe *parallelExecutor) processResults(ctx context.Context, applyTx kv.TemporalTx) (blockResult *blockResult, err error) {
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

		pe.RLock()
		blockExecutor, ok := pe.blockExecutors[txResult.Version().BlockNum]
		pe.RUnlock()

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
	doneCh := make(chan error, 1)

	go func() {
		if pe.execLoopGroup != nil {
			err := pe.execLoopGroup.Wait()
			if err != nil && !errors.Is(err, context.Canceled) {
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
			return nil
		case err := <-doneCh:
			return err
		}
	}
}

type applyResult interface {
}

type blockResult struct {
	BlockNum    uint64
	BlockTime   uint64
	BlockHash   common.Hash
	ParentHash  common.Hash
	StateRoot   common.Hash
	Err         error
	GasUsed     uint64
	BlobGasUsed uint64
	lastTxNum   uint64
	complete    bool
	isPartial   bool
	ApplyCount  int
	TxIO        *state.VersionedIO
	Receipts    types.Receipts
	Stats       map[int]ExecutionStat
	Deps        *state.DAG
	AllDeps     map[int]map[int]bool
}

type txResult struct {
	blockNum     uint64
	txNum        uint64
	gasUsed      int64
	receipt      *types.Receipt
	logs         []*types.Log
	traceFroms   map[common.Address]struct{}
	traceTos     map[common.Address]struct{}
	stateUpdates state.StateUpdates
	rules        *chain.Rules
}

type execTask struct {
	exec.Task
	index              int
	shouldDelayFeeCalc bool
}

type execResult struct {
	*exec.TxResult
	stateUpdates *state.StateUpdates
}

func (result *execResult) finalize(prevReceipt *types.Receipt, engine consensus.Engine, vm *state.VersionMap, stateReader state.StateReader, stateWriter state.StateWriter) (*types.Receipt, error) {
	task, ok := result.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", result.Task)
	}

	blockNum := task.Version().BlockNum
	txIndex := task.Version().TxIndex
	txIncarnation := task.Version().Incarnation

	txTrace := dbg.TraceTransactionIO &&
		(dbg.TraceTx(blockNum, txIndex) || dbg.TraceAccount(result.Coinbase) || dbg.TraceAccount(result.ExecutionResult.BurntContractAddress))

	var tracePrefix string
	if txTrace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation)
		fmt.Println(tracePrefix, "finalize")
		defer fmt.Println(tracePrefix, "done finalize")
	}

	// we want to force a re-read of the conbiase & burnt contract address
	// if thay where referenced by the tx
	delete(result.TxIn, result.Coinbase)
	delete(result.TxIn, result.ExecutionResult.BurntContractAddress)

	txTask, ok := task.Task.(*exec.TxTask)

	if !ok {
		return nil, nil
	}

	ibs := state.New(state.NewVersionedStateReader(txIndex, result.TxIn, vm, stateReader))
	ibs.SetTxContext(blockNum, txIndex)
	ibs.SetVersion(txIncarnation)
	if err := ibs.ApplyVersionedWrites(result.TxOut); err != nil {
		return nil, err
	}
	ibs.SetVersionMap(&state.VersionMap{})
	ibs.SetTrace(txTask.Trace)

	if task.IsBlockEnd() || txIndex < 0 {
		if blockNum == 0 || txTask.Config.IsByzantium(blockNum) {
			ibs.FinalizeTx(txTask.EvmBlockContext.Rules(txTask.Config), stateWriter)
		}
		return nil, nil
	}

	if task.shouldDelayFeeCalc {
		if txTask.Config.IsLondon(blockNum) {
			ibs.AddBalance(result.ExecutionResult.BurntContractAddress, result.ExecutionResult.FeeBurnt, tracing.BalanceDecreaseGasBuy)
		}

		ibs.AddBalance(result.Coinbase, result.ExecutionResult.FeeTipped, tracing.BalanceIncreaseRewardTransactionFee)

		if engine != nil {
			if postApplyMessageFunc := engine.GetPostApplyMessageFunc(); postApplyMessageFunc != nil {
				execResult := result.ExecutionResult
				coinbase, err := stateReader.ReadAccountData(result.Coinbase) // to generate logs we want the initial balance

				if err != nil {
					return nil, err
				}

				if coinbase != nil {
					if txTrace {
						fmt.Println(blockNum, fmt.Sprintf("(%d.%d)", txIndex, txIncarnation), "CB", fmt.Sprintf("%x", result.Coinbase), fmt.Sprintf("%d", &coinbase.Balance), "nonce", coinbase.Nonce)
					}
					execResult.CoinbaseInitBalance = coinbase.Balance
				}

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
				result.Logs = append(result.Logs, ibs.GetLogs(txTask.TxIndex, txTask.TxHash(), blockNum, txTask.BlockHash())...)
			}
		}
	}

	if txTrace {
		vm.SetTrace(true)
		fmt.Println(tracePrefix, ibs.VersionedWrites(true))
	}

	// we need to flush the finalized writes to the version map so
	// they are taken into account by subsequent transactions
	vm.FlushVersionedWrites(ibs.VersionedWrites(true), true, tracePrefix)
	vm.SetTrace(false)

	if txTask.Config.IsByzantium(blockNum) {
		ibs.FinalizeTx(txTask.EvmBlockContext.Rules(txTask.Config), stateWriter)
	}

	receipt, err := result.CreateNextReceipt(prevReceipt)

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

	// Publish tasks stores the state tasks ready for publication
	publishTasks execStatusList

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
	applyCount  int
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

func (be *blockExecutor) nextResult(ctx context.Context, pe *parallelExecutor, res *exec.TxResult, applyTx kv.TemporalTx) (result *blockResult, err error) {
	task, ok := res.Task.(*taskVersion)

	if !ok {
		return nil, fmt.Errorf("unexpected task type: %T", res.Task)
	}

	tx := task.index
	be.results[tx] = &execResult{res, nil}
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
			if dbg.TraceTransactionIO && be.txIncarnations[tx] > 1 {
				fmt.Println(be.blockNum, "err", execErr)
			}
			be.blockIO.RecordReads(res.Version(), res.TxIn)
			var addedDependencies bool
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
			return nil, fmt.Errorf("unexptected exec error: %w", res.Err)
		}
	} else {
		txVersion := res.Version()

		be.blockIO.RecordReads(txVersion, res.TxIn)

		if res.Version().Incarnation == 0 {
			be.blockIO.RecordWrites(txVersion, res.TxOut)
		} else {
			prevWrites := be.blockIO.WriteSet(txVersion.TxIndex)
			hasWriteChange := res.TxOut.HasNewWrite(prevWrites)

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

			for _, v := range prevWrites {
				if _, ok := cmpMap[v.Address][state.AccountKey{Path: v.Path, Key: v.Key}]; !ok {
					hasWriteChange = true
					be.versionMap.Delete(v.Address, v.Path, v.Key, txVersion.TxIndex, true)
				}
			}

			be.blockIO.RecordWrites(txVersion, res.TxOut)

			if hasWriteChange {
				be.validateTasks.pushPendingSet(be.execTasks.getRevalidationRange(tx + 1))
			}
		}

		tracePrefix := fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)

		var trace bool
		if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
			fmt.Println(tracePrefix, "RD", be.blockIO.ReadSet(txVersion.TxIndex).Len(), "WRT", len(be.blockIO.WriteSet(txVersion.TxIndex)))
			be.blockIO.ReadSet(txVersion.TxIndex).Scan(func(vr *state.VersionedRead) bool {
				fmt.Println(tracePrefix, "RD", vr.String())
				return true
			})
			for _, vw := range be.blockIO.WriteSet(txVersion.TxIndex) {
				fmt.Println(tracePrefix, "WRT", vw.String())
			}
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
	var stateReader state.StateReader

	for i := 0; i < len(toValidate); i++ {

		be.cntTotalValidations++

		tx := toValidate[i]
		txVersion := be.tasks[tx].Task.Version()

		var trace bool
		var tracePrefix string

		if trace = dbg.TraceTransactionIO && dbg.TraceTx(be.blockNum, txVersion.TxIndex); trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", be.blockNum, txVersion.TxIndex, txVersion.Incarnation)
		}

		validity := be.versionMap.ValidateVersion(txVersion.TxIndex, be.blockIO,
			func(readVersion, writtenVersion state.Version) state.VersionValidity {
				vv := state.VersionValid

				if readVersion != writtenVersion {
					vv = state.VersionInvalid
				} else if writtenVersion.TxIndex == state.UnknownDep && tx-1 > be.validateTasks.maxComplete() {
					vv = state.VersionTooEarly
				}

				return vv
			}, trace, tracePrefix)
		be.versionMap.SetTrace(false)

		if validity == state.VersionTooEarly {
			cntInvalid++
			continue
		}

		valid := be.skipCheck[tx] || validity == state.VersionValid

		be.versionMap.SetTrace(trace)
		be.versionMap.FlushVersionedWrites(be.blockIO.WriteSet(txVersion.TxIndex), cntInvalid == 0, tracePrefix)
		be.versionMap.SetTrace(false)

		if valid {
			if cntInvalid == 0 {
				be.validateTasks.markComplete(tx)
				var prevReceipt *types.Receipt
				if txVersion.TxIndex > 0 && tx > 0 {
					prevReceipt = be.results[tx-1].Receipt
				}

				txResult := be.results[tx]

				if err := be.gasPool.SubGas(txResult.ExecutionResult.GasUsed); err != nil {
					return nil, err
				}

				txTask := be.tasks[tx].Task

				if txTask.Tx() != nil {
					blobGasUsed := txTask.Tx().GetBlobGas()
					if err := be.gasPool.SubBlobGas(blobGasUsed); err != nil {
						return nil, err
					}
					be.blobGasUsed += blobGasUsed
				}

				if stateReader == nil {
					stateReader = state.NewBufferedReader(pe.rs, state.NewReaderV3(pe.rs.Domains().AsGetter(applyTx)))
				}

				stateWriter := state.NewBufferedWriter(pe.rs, nil)

				_, err = txResult.finalize(prevReceipt, pe.cfg.engine, be.versionMap, stateReader, stateWriter)

				if err != nil {
					return nil, err
				}

				stateUpdates := stateWriter.WriteSet()
				txResult.stateUpdates = &stateUpdates

				be.publishTasks.pushPending(tx)
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
	}

	maxValidated := be.validateTasks.maxComplete()
	be.scheduleExecution(ctx, pe)

	if be.publishTasks.minPending() != -1 {
		toPublish := make(sort.IntSlice, 0, 2)

		for be.publishTasks.minPending() <= maxValidated && be.publishTasks.minPending() >= 0 {
			toPublish = append(toPublish, be.publishTasks.takeNextPending())
		}

		for i := 0; i < len(toPublish); i++ {
			tx := toPublish[i]
			task := be.tasks[tx].Task
			result := be.results[tx]

			applyResult := txResult{
				blockNum:   be.blockNum,
				traceFroms: map[common.Address]struct{}{},
				traceTos:   map[common.Address]struct{}{},
				txNum:      task.Version().TxNum,
				rules:      task.Rules(),
			}

			if result.Receipt != nil {
				applyResult.gasUsed += int64(result.Receipt.GasUsed)
				be.gasUsed += result.Receipt.GasUsed
				applyResult.receipt = result.Receipt
			}

			applyResult.logs = append(applyResult.logs, result.Logs...)
			maps.Copy(applyResult.traceFroms, result.TraceFroms)
			maps.Copy(applyResult.traceTos, result.TraceTos)
			be.cntFinalized++
			be.publishTasks.markComplete(tx)

			pe.executedGas.Add(int64(applyResult.gasUsed))
			pe.lastExecutedTxNum.Store(int64(applyResult.txNum))
			if result.stateUpdates != nil {
				applyResult.stateUpdates = *result.stateUpdates
				if applyResult.stateUpdates.BTreeG != nil {
					be.applyCount += applyResult.stateUpdates.UpdateCount()
					if dbg.TraceApply {
						applyResult.stateUpdates.TraceBlockUpdates(applyResult.blockNum, dbg.TraceBlock(applyResult.blockNum))
					}
				}
			}

			be.applyResults <- &applyResult
		}
	}

	if be.publishTasks.countComplete() == len(be.tasks) && be.execTasks.countComplete() == len(be.tasks) {
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
			txTask.ParentHash(),
			txTask.BlockRoot(),
			nil,
			be.gasUsed,
			be.blobGasUsed,
			txTask.Version().TxNum,
			true,
			isPartial,
			be.applyCount,
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
		txTask.ParentHash(),
		txTask.BlockRoot(),
		nil,
		be.gasUsed,
		be.blobGasUsed,
		lastTxNum,
		false,
		len(be.tasks) > 0 && be.tasks[0].Version().TxIndex != -1,
		be.applyCount,
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
					be.versionMap.ValidateVersion(txIndex, be.blockIO,
						func(_, writtenVersion state.Version) state.VersionValidity {
							if writtenVersion.TxIndex < maxValidated &&
								writtenVersion.Incarnation == be.txIncarnations[writtenVersion.TxIndex+1] {
								return state.VersionValid
							}
							return state.VersionInvalid
						}, false, "") != state.VersionValid) {
				be.execTasks.pushPending(nextTx)
				continue
			}
			be.cntSpecExec++
		}

		if dbg.TraceTransactionIO && be.txIncarnations[nextTx] > 1 {
			fmt.Println(be.blockNum, "EXEC", nextTx, be.txIncarnations[nextTx], "maxValidated", maxValidated, be.blockIO.HasReads(nextTx), "failed", be.execFailed[nextTx], "aborted", be.execAborted[nextTx])
		}

		be.cntExec++

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
