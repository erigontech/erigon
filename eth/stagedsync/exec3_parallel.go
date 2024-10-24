package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
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
	execute(ctx context.Context, tasks []*state.TxTask) (bool, error)
	wait() error
}

type parallelExecutor struct {
	sync.RWMutex
	rwLoopErrCh              chan error
	rwLoopG                  *errgroup.Group
	applyLoopWg              sync.WaitGroup
	chainDb                  kv.RwDB
	applyTx                  kv.RwTx
	applyWorker              *exec3.Worker
	execWorkers              []*exec3.Worker
	stopWorkers              func()
	waitWorkers              func()
	execStage                *StageState
	cfg                      ExecuteBlockCfg
	lastBlockNum             atomic.Uint64
	outputTxNum              *atomic.Uint64
	in                       *state.QueueWithRetry
	rws                      *state.ResultsQueue
	rs                       *state.StateV3
	doms                     *state2.SharedDomains
	agg                      *state2.Aggregator
	rwsConsumed              chan struct{}
	isMining                 bool
	inMemExec                bool
	shouldGenerateChangesets bool
	accumulator              *shards.Accumulator
	workerCount              int
	pruneEvery               *time.Ticker
	logEvery                 *time.Ticker
	progress                 *Progress
}

func (pe *parallelExecutor) applyLoop(ctx context.Context, maxTxNum uint64, blockComplete *atomic.Bool, errCh chan error) {
	defer pe.applyLoopWg.Done()
	defer func() {
		if rec := recover(); rec != nil {
			log.Warn("[dbg] apply loop panic", "rec", rec)
		}
		log.Warn("[dbg] apply loop exit")
	}()

	outputBlockNum := stages.SyncMetrics[stages.Execution]

	applyLoopInner := func(ctx context.Context) error {
		tx, err := pe.chainDb.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		pe.applyWorker.ResetTx(tx)

		for pe.outputTxNum.Load() <= maxTxNum {
			if err := pe.rws.Drain(ctx); err != nil {
				return err
			}

			processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err :=
				pe.processResultQueue(ctx, pe.outputTxNum.Load(), tx, pe.rwsConsumed, true, false)
			if err != nil {
				return err
			}

			mxExecRepeats.AddInt(conflicts)
			mxExecTriggers.AddInt(triggers)
			if processedBlockNum > pe.lastBlockNum.Load() {
				outputBlockNum.SetUint64(processedBlockNum)
				pe.lastBlockNum.Store(processedBlockNum)
			}
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
	tx, err := pe.chainDb.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	pe.doms.SetTx(tx)

	defer pe.applyLoopWg.Wait()
	applyCtx, cancelApplyCtx := context.WithCancel(ctx)
	defer cancelApplyCtx()
	pe.applyLoopWg.Add(1)

	blockComplete := atomic.Bool{}
	blockComplete.Store(true)

	go pe.applyLoop(applyCtx, maxTxNum, &blockComplete, pe.rwLoopErrCh)

	outputBlockNum := stages.SyncMetrics[stages.Execution]

	for pe.outputTxNum.Load() <= maxTxNum {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-pe.logEvery.C:
			stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
			pe.progress.Log("", pe.rs, pe.in, pe.rws, pe.rs.DoneCount(), 0 /* TODO logGas*/, pe.lastBlockNum.Load(), outputBlockNum.GetValueUint64(), pe.outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, pe.shouldGenerateChangesets)
			if pe.agg.HasBackgroundFilesBuild() {
				logger.Info(fmt.Sprintf("[%s] Background files build", pe.execStage.LogPrefix()), "progress", pe.agg.BackgroundProgress())
			}
		case <-pe.pruneEvery.C:
			if pe.rs.SizeEstimate() < pe.cfg.batchSize.Bytes() {
				if pe.doms.BlockNum() != outputBlockNum.GetValueUint64() {
					panic(fmt.Errorf("%d != %d", pe.doms.BlockNum(), outputBlockNum.GetValueUint64()))
				}
				_, err := pe.doms.ComputeCommitment(ctx, true, outputBlockNum.GetValueUint64(), pe.execStage.LogPrefix())
				if err != nil {
					return err
				}
				ac := pe.agg.BeginFilesRo()
				if _, err = ac.PruneSmallBatches(ctx, 10*time.Second, tx); err != nil { // prune part of retired data, before commit
					return err
				}
				ac.Close()
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
						pe.processResultQueue(ctx, pe.outputTxNum.Load(), tx, nil, false, true)
					if err != nil {
						return err
					}

					mxExecRepeats.AddInt(conflicts)
					mxExecTriggers.AddInt(triggers)
					if processedBlockNum > 0 {
						outputBlockNum.SetUint64(processedBlockNum)
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

				if err := pe.doms.Flush(ctx, tx); err != nil {
					return err
				}
				pe.doms.ClearRam(true)
				t3 = time.Since(tt)

				if err = pe.execStage.Update(tx, outputBlockNum.GetValueUint64()); err != nil {
					return err
				}
				if _, err = rawdb.IncrementStateVersion(pe.applyTx); err != nil {
					return fmt.Errorf("writing plain state version: %w", err)
				}

				tx.CollectMetrics()
				tt = time.Now()
				if err = tx.Commit(); err != nil {
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
			if tx, err = pe.chainDb.BeginRw(ctx); err != nil {
				return err
			}
			defer tx.Rollback()
			pe.doms.SetTx(tx)

			applyCtx, cancelApplyCtx = context.WithCancel(ctx)
			defer cancelApplyCtx()
			pe.applyLoopWg.Add(1)
			go pe.applyLoop(applyCtx, maxTxNum, &blockComplete, pe.rwLoopErrCh)

			logger.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
		}
	}
	if err = pe.doms.Flush(ctx, tx); err != nil {
		return err
	}
	if err = pe.execStage.Update(tx, outputBlockNum.GetValueUint64()); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (pe *parallelExecutor) processResultQueue(ctx context.Context, inputTxNum uint64, applyTx kv.Tx, backPressure chan<- struct{}, canRetry, forceStopAtBlockEnd bool) (outputTxNum uint64, conflicts, triggers int, processedBlockNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := pe.rws.Iter()
	defer rwsIt.Close()

	var i int
	outputTxNum = inputTxNum
	for rwsIt.HasNext(outputTxNum) {
		txTask := rwsIt.PopNext()
		if txTask.Error != nil || !pe.rs.ReadsValid(txTask.ReadLists) {
			conflicts++

			if i > 0 && canRetry {
				//send to re-exex
				pe.rs.ReTry(txTask, pe.in)
				continue
			}

			// resolve first conflict right here: it's faster and conflict-free
			pe.applyWorker.RunTxTask(txTask, pe.isMining)
			if txTask.Error != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, txTask.Error)
			}
			// TODO: post-validation of gasUsed and blobGasUsed
			i++
		}

		if txTask.Final {
			pe.rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			err := pe.rs.ApplyState4(ctx, txTask)
			if err != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("StateV3.Apply: %w", err)
			}
			//if !bytes.Equal(rh, txTask.BlockRoot[:]) {
			//	log.Error("block hash mismatch", "rh", hex.EncodeToString(rh), "blockRoot", hex.EncodeToString(txTask.BlockRoot[:]), "bn", txTask.BlockNum, "txn", txTask.TxNum)
			//	return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("block hashk mismatch: %x != %x bn =%d, txn= %d", rh, txTask.BlockRoot[:], txTask.BlockNum, txTask.TxNum)
			//}
		}
		triggers += pe.rs.CommitTxNum(txTask.Sender, txTask.TxNum, pe.in)
		outputTxNum++
		if backPressure != nil {
			select {
			case backPressure <- struct{}{}:
			default:
			}
		}
		if err := pe.rs.ApplyLogsAndTraces4(txTask, pe.rs.Domains()); err != nil {
			return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("StateV3.Apply: %w", err)
		}
		processedBlockNum = txTask.BlockNum
		stopedAtBlockEnd = txTask.Final
		if forceStopAtBlockEnd && txTask.Final {
			break
		}
	}
	return
}

func (pe *parallelExecutor) run(ctx context.Context, maxTxNum uint64, logger log.Logger) context.CancelFunc {
	pe.execWorkers, _, pe.rws, pe.stopWorkers, pe.waitWorkers = exec3.NewWorkersPool(
		pe.RWMutex.RLocker(), pe.accumulator, logger, ctx, true, pe.chainDb, pe.rs, pe.in,
		pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.workerCount+1, pe.cfg.dirs, pe.isMining)

	rwLoopCtx, rwLoopCtxCancel := context.WithCancel(ctx)
	pe.rwLoopG, rwLoopCtx = errgroup.WithContext(rwLoopCtx)
	pe.rwLoopG.Go(func() error {
		defer pe.rws.Close()
		defer pe.in.Close()
		defer pe.applyLoopWg.Wait()
		defer func() {
			log.Warn("[dbg] rwloop exit")
		}()
		return pe.rwLoop(rwLoopCtx, maxTxNum, logger)
	})

	return rwLoopCtxCancel
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

func (pe *parallelExecutor) execute(ctx context.Context, tasks []*state.TxTask) (bool, error) {
	for _, txTask := range tasks {
		if txTask.Sender != nil {
			if ok := pe.rs.RegisterSender(txTask); ok {
				pe.rs.AddWork(ctx, txTask, pe.in)
			}
		} else {
			pe.rs.AddWork(ctx, txTask, pe.in)
		}
	}

	return false, nil
}
