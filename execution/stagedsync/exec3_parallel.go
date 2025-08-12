package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/types"
	chaos_monkey "github.com/erigontech/erigon/tests/chaos-monkey"
	"github.com/erigontech/erigon/turbo/shards"
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
	execute(ctx context.Context, tasks []*state.TxTask, gp *core.GasPool) (bool, error)
	status(ctx context.Context, commitThreshold uint64) error
	wait() error
	getHeader(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error)

	//these are reset by commit - so need to be read from the executor once its processing
	tx() kv.RwTx
	readState() *state.ParallelExecutionState
	domains() *dbstate.SharedDomains
}

type txExecutor struct {
	sync.RWMutex
	cfg            ExecuteBlockCfg
	execStage      *StageState
	agg            *dbstate.Aggregator
	rs             *state.ParallelExecutionState
	doms           *dbstate.SharedDomains
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

func (te *txExecutor) readState() *state.ParallelExecutionState {
	return te.rs
}

func (te *txExecutor) domains() *dbstate.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	if te.applyTx != nil {
		return te.cfg.blockReader.Header(ctx, te.applyTx, hash, number)
	}

	var h *types.Header
	var err error

	err = te.cfg.db.View(ctx, func(tx kv.Tx) error {
		h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
		if err != nil {
			return err
		}
		return nil
	})
	return h, err
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
	in                       *state.QueueWithRetry
	rws                      *state.ResultsQueue
	rwsConsumed              chan struct{}
	shouldGenerateChangesets bool
	workerCount              int
	pruneEvery               *time.Ticker
	logEvery                 *time.Ticker
	slowDownLimit            *time.Ticker
	progress                 *Progress
}

func (pe *parallelExecutor) applyLoop(ctx context.Context, maxTxNum uint64, blockComplete *atomic.Bool, errCh chan error) {
	defer pe.applyLoopWg.Done()
	defer func() {
		if rec := recover(); rec != nil {
			pe.logger.Warn("[dbg] apply loop panic", "rec", rec)
		}
		pe.logger.Warn("[dbg] apply loop exit")
	}()
	//fmt.Println("applyLoop started")
	//defer fmt.Println("applyLoop done")

	applyLoopInner := func(ctx context.Context) error {
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

			processedTxNum, conflicts, triggers, _ /*processedBlockNum*/, stoppedAtBlockEnd, err :=
				pe.processResultQueue(ctx, pe.outputTxNum.Load(), pe.rwsConsumed, true, false)
			if err != nil {
				return err
			}
			//fmt.Println("QR", processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err)
			mxExecRepeats.AddInt(conflicts)
			mxExecTriggers.AddInt(triggers)

			if processedTxNum > 0 {
				pe.outputTxNum.Store(processedTxNum)
				blockComplete.Store(stoppedAtBlockEnd)
				// TODO update logGas here
			}

		}
		return nil
	}

	if err := applyLoopInner(ctx); err != nil {
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
			pe.progress.Log("", pe.rs, pe.in, pe.rws, pe.rs.DoneCount(), 0 /* TODO logGas*/, pe.lastBlockNum.Load(), pe.outputBlockNum.GetValueUint64(), pe.outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, pe.shouldGenerateChangesets || pe.cfg.syncCfg.KeepExecutionProofs, pe.inMemExec)
			if pe.agg.HasBackgroundFilesBuild() {
				logger.Info(fmt.Sprintf("[%s] Background files build", pe.execStage.LogPrefix()), "progress", pe.agg.BackgroundProgress())
			}
		case <-pe.pruneEvery.C:
			if pe.rs.SizeEstimate() < pe.cfg.batchSize.Bytes() {
				if pe.doms.BlockNum() != pe.outputBlockNum.GetValueUint64() {
					panic(fmt.Errorf("%d != %d", pe.doms.BlockNum(), pe.outputBlockNum.GetValueUint64()))
				}
				_, err := pe.doms.ComputeCommitment(ctx, true, pe.outputBlockNum.GetValueUint64(), pe.outputTxNum.Load(), pe.execStage.LogPrefix())
				if err != nil {
					return err
				}
				if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, 10*time.Hour); err != nil {
					return err
				}
				if !pe.inMemExec {
					if err = pe.doms.Flush(ctx, tx); err != nil {
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

					processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err :=
						pe.processResultQueue(ctx, pe.outputTxNum.Load(), nil, false, true)
					if err != nil {
						return err
					}

					mxExecRepeats.AddInt(conflicts)
					mxExecTriggers.AddInt(triggers)
					if processedBlockNum > 0 {
						pe.outputBlockNum.SetUint64(processedBlockNum)
					}
					if processedTxNum > 0 {
						pe.outputTxNum.Store(processedTxNum)
						blockComplete.Store(stoppedAtBlockEnd)
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
				pe.rws.DropResults(ctx, func(txTask *state.TxTask) {
					pe.rs.ReTry(txTask, pe.in)
				})

				//lastTxNumInDb, _ := txNumsReader.Max(tx, outputBlockNum.Get())
				//if lastTxNumInDb != outputTxNum.Load()-1 {
				//	panic(fmt.Sprintf("assert: %d != %d", lastTxNumInDb, outputTxNum.Load()))
				//}

				t1 = time.Since(commitStart)
				tt := time.Now()
				t2 = time.Since(tt)
				tt = time.Now()
				_, err := pe.doms.ComputeCommitment(ctx, true, pe.doms.BlockNum(), pe.doms.TxNum(), "flush-commitment")
				if err != nil {
					return err
				}

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

func (pe *parallelExecutor) processResultQueue(ctx context.Context, inputTxNum uint64, backPressure chan<- struct{}, canRetry, forceStopAtBlockEnd bool) (outputTxNum uint64, conflicts, triggers int, processedBlockNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := pe.rws.Iter()
	defer rwsIt.Close()
	//defer fmt.Println("PRQ", "Done")

	var i int
	outputTxNum = inputTxNum
	for rwsIt.HasNext(outputTxNum) {
		txTask := rwsIt.PopNext()
		//fmt.Println("PRQ", txTask.BlockNum, txTask.TxIndex, txTask.TxNum)
		if txTask.Error != nil || !pe.rs.ReadsValid(txTask.ReadLists) {
			conflicts++
			//fmt.Println(txTask.TxNum, txTask.Error)
			if errors.Is(txTask.Error, vm.ErrIntraBlockStateFailed) ||
				errors.Is(txTask.Error, core.ErrStateTransitionFailed) {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, txTask.Error)
			}
			if i > 0 && canRetry {
				//send to re-exex
				pe.rs.ReTry(txTask, pe.in)
				continue
			}

			// resolve first conflict right here: it's faster and conflict-free
			pe.applyWorker.RunTxTaskNoLock(txTask.Reset(), pe.isMining, false)
			if txTask.Error != nil {
				//fmt.Println("RETRY", txTask.TxNum, txTask.Error)
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, txTask.Error)
			}
			if pe.cfg.syncCfg.ChaosMonkey {
				chaosErr := chaos_monkey.ThrowRandomConsensusError(pe.execStage.CurrentSyncCycle.IsInitialCycle, txTask.TxIndex, pe.cfg.badBlockHalt, txTask.Error)
				if chaosErr != nil {
					log.Warn("Monkey in a consensus")
					return outputTxNum, conflicts, triggers, processedBlockNum, false, chaosErr
				}
			}
			// TODO: post-validation of gasUsed and blobGasUsed
			i++
		}

		if txTask.Final {
			pe.rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			err := pe.rs.ApplyState(ctx, txTask)
			if err != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("ParallelExecutionState.Apply: %w", err)
			}

			if processedBlockNum > pe.lastBlockNum.Load() {
				pe.outputBlockNum.SetUint64(processedBlockNum)
				pe.lastBlockNum.Store(processedBlockNum)
			}
			//if !bytes.Equal(rh, txTask.BlockRoot[:]) {
			//	log.Error("block hash mismatch", "rh", hex.EncodeToString(rh), "blockRoot", hex.EncodeToString(txTask.BlockRoot[:]), "bn", txTask.BlockNum, "txn", txTask.TxNum)
			//	return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("block hashk mismatch: %x != %x bn =%d, txn= %d", rh, txTask.BlockRoot[:], txTask.BlockNum, txTask.TxNum)
			//}
		}
		triggers += pe.rs.CommitTxNum(txTask.Sender(), txTask.TxNum, pe.in)
		outputTxNum++
		if backPressure != nil {
			select {
			case backPressure <- struct{}{}:
			default:
			}
		}
		if err := pe.rs.ApplyLogsAndTraces(txTask, pe.rs.Domains()); err != nil {
			return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("ParallelExecutionState.Apply: %w", err)
		}
		processedBlockNum = txTask.BlockNum
		if !stopedAtBlockEnd {
			stopedAtBlockEnd = txTask.Final
		}
		if forceStopAtBlockEnd && txTask.Final {
			break
		}
	}
	return
}

func (pe *parallelExecutor) run(ctx context.Context, maxTxNum uint64, logger log.Logger) context.CancelFunc {
	pe.slowDownLimit = time.NewTicker(time.Second)
	pe.rwsConsumed = make(chan struct{}, 1)
	pe.rwLoopErrCh = make(chan error)
	pe.in = state.NewQueueWithRetry(100_000)

	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		pe.RWMutex.RLocker(), pe.accumulator, logger, nil, ctx, true, pe.cfg.db, pe.rs, pe.in,
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
		pe.wait()
		pe.stopWorkers()
		close(pe.rwsConsumed)
		pe.in.Close()
	}
}

func (pe *parallelExecutor) status(ctx context.Context, commitThreshold uint64) error {
	select {
	case err := <-pe.rwLoopErrCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	default:
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

func (pe *parallelExecutor) wait() error {
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

func (pe *parallelExecutor) execute(ctx context.Context, tasks []*state.TxTask, gp *core.GasPool) (bool, error) {
	for _, txTask := range tasks {
		if txTask.Sender() != nil {
			if ok := pe.rs.RegisterSender(txTask); ok {
				pe.rs.AddWork(ctx, txTask, pe.in)
			}
		} else {
			pe.rs.AddWork(ctx, txTask, pe.in)
		}
	}

	return false, nil
}
