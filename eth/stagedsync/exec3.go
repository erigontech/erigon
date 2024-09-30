// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/mdbx-go/mdbx"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	metrics2 "github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	kv2 "github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var (
	mxExecStepsInDB    = metrics.NewGauge(`exec_steps_in_db`) //nolint
	mxExecRepeats      = metrics.NewCounter(`exec_repeats`)   //nolint
	mxExecTriggers     = metrics.NewCounter(`exec_triggers`)  //nolint
	mxExecTransactions = metrics.NewCounter(`exec_txns`)
	mxExecGas          = metrics.NewCounter(`exec_gas`)
	mxExecBlocks       = metrics.NewGauge("exec_blocks")

	mxMgas = metrics.NewGauge(`exec_mgas`)
)

const (
	changesetSafeRange = 32 // Safety net for long-sync, keep last 32 changesets
)

func NewProgress(prevOutputBlockNum, commitThreshold uint64, workersCount int, updateMetrics bool, logPrefix string, logger log.Logger) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum, commitThreshold: commitThreshold, workersCount: workersCount, logPrefix: logPrefix, logger: logger}
}

type Progress struct {
	prevTime           time.Time
	prevTxCount        uint64
	prevGasUsed        uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
	commitThreshold    uint64

	workersCount int
	logPrefix    string
	logger       log.Logger
}

func (p *Progress) Log(suffix string, rs *state.StateV3, in *state.QueueWithRetry, rws *state.ResultsQueue, txCount uint64, gas uint64, inputBlockNum uint64, outputBlockNum uint64, outTxNum uint64, repeatCount uint64, idxStepsAmountInDB float64, shouldGenerateChangesets bool) {
	mxExecStepsInDB.Set(idxStepsAmountInDB * 100)
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	//var repeatRatio float64
	//if doneCount > p.prevCount {
	//	repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(doneCount-p.prevCount)
	//}

	if len(suffix) > 0 {
		suffix = " " + suffix
	}

	if shouldGenerateChangesets {
		suffix += " Commit every block"
	}

	gasSec := uint64(float64(gas-p.prevGasUsed) / interval.Seconds())
	txSec := uint64(float64(txCount-p.prevTxCount) / interval.Seconds())
	diffBlocks := max(int(outputBlockNum)-int(p.prevOutputBlockNum)+1, 0)

	p.logger.Info(fmt.Sprintf("[%s]"+suffix, p.logPrefix),
		"blk", outputBlockNum,
		"blks", diffBlocks,
		"blk/s", fmt.Sprintf("%.1f", float64(diffBlocks)/interval.Seconds()),
		"txs", txCount-p.prevTxCount,
		"tx/s", common.PrettyCounter(txSec),
		"gas/s", common.PrettyCounter(gasSec),
		//"pipe", fmt.Sprintf("(%d+%d)->%d/%d->%d/%d", in.NewTasksLen(), in.RetriesLen(), rws.ResultChLen(), rws.ResultChCap(), rws.Len(), rws.Limit()),
		//"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		//"workers", p.workersCount,
		"buf", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"stepsInDB", fmt.Sprintf("%.2f", idxStepsAmountInDB),
		"step", fmt.Sprintf("%.1f", float64(outTxNum)/float64(config3.HistoryV3AggregationStep)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)

	p.prevTime = currentTime
	p.prevTxCount = txCount
	p.prevGasUsed = gas
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

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
func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, workerCount int, cfg ExecuteBlockCfg, txc wrap.TxContainer,
	parallel bool, //nolint
	maxBlockNum uint64,
	logger log.Logger,
	initialCycle bool,
	isMining bool,
) error {
	// TODO: e35 doesn't support parallel-exec yet
	parallel = false //nolint

	batchSize := cfg.batchSize
	chainDb := cfg.db
	blockReader := cfg.blockReader
	engine := cfg.engine
	chainConfig, genesis := cfg.chainConfig, cfg.genesis
	totalGasUsed := uint64(0)
	start := time.Now()
	defer func() {
		fmt.Println((float64(totalGasUsed) / 1e6) / time.Since(start).Seconds())
		if totalGasUsed > 0 {
			mxMgas.Set((float64(totalGasUsed) / 1e6) / time.Since(start).Seconds())
		}
	}()

	applyTx := txc.Tx
	useExternalTx := applyTx != nil
	if !useExternalTx {
		if !parallel {
			var err error
			applyTx, err = chainDb.BeginRw(ctx) //nolint
			if err != nil {
				return err
			}
			defer func() { // need callback - because tx may be committed
				applyTx.Rollback()
			}()
		}
	}
	agg := cfg.db.(state2.HasAgg).Agg().(*state2.Aggregator)
	if initialCycle {
		agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	} else {
		agg.SetCompressWorkers(1)
		agg.SetCollateAndBuildWorkers(1)
	}

	pruneNonEssentials := cfg.prune.History.Enabled() && cfg.prune.History.PruneTo(execStage.BlockNumber) == execStage.BlockNumber

	var err error
	inMemExec := txc.Doms != nil
	var doms *state2.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		var err error
		doms, err = state2.NewSharedDomains(applyTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, state2.ErrBehindCommitment) {
			return nil
		}
		if err != nil {
			return err
		}
		defer doms.Close()
	}
	txNumInDB := doms.TxNum()

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))

	var (
		inputTxNum    = doms.TxNum()
		stageProgress = execStage.BlockNumber
		outputTxNum   = atomic.Uint64{}
		blockComplete = atomic.Bool{}

		offsetFromBlockBeginning uint64
		blockNum, maxTxNum       uint64
	)
	blockComplete.Store(true)

	nothingToExec := func(applyTx kv.Tx) (bool, error) {
		_, lastTxNum, err := txNumsReader.Last(applyTx)
		if err != nil {
			return false, err
		}
		return lastTxNum == inputTxNum, nil
	}
	// Cases:
	//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
	//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
	restoreTxNum := func(applyTx kv.Tx) error {
		var err error
		maxTxNum, err = txNumsReader.Max(applyTx, maxBlockNum)
		if err != nil {
			return err
		}
		ok, _blockNum, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
		if err != nil {
			return err
		}
		if !ok {
			_lb, _lt, _ := txNumsReader.Last(applyTx)
			_fb, _ft, _ := txNumsReader.First(applyTx)
			return fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, _fb, _lb, _ft, _lt)
		}
		{
			_max, _ := txNumsReader.Max(applyTx, _blockNum)
			if doms.TxNum() == _max {
				_blockNum++
			}
		}

		_min, err := txNumsReader.Min(applyTx, _blockNum)
		if err != nil {
			return err
		}

		if doms.TxNum() > _min {
			// if stopped in the middle of the block: start from beginning of block.
			// first part will be executed in HistoryExecution mode
			offsetFromBlockBeginning = doms.TxNum() - _min
		}

		inputTxNum = _min
		outputTxNum.Store(inputTxNum)

		//_max, _ := txNumsReader.Max(applyTx, blockNum)
		//fmt.Printf("[commitment] found domain.txn %d, inputTxn %d, offset %d. DB found block %d {%d, %d}\n", doms.TxNum(), inputTxNum, offsetFromBlockBeginning, blockNum, _min, _max)
		doms.SetBlockNum(_blockNum)
		doms.SetTxNum(inputTxNum)
		return nil
	}
	if applyTx != nil {
		if _nothing, err := nothingToExec(applyTx); err != nil {
			return err
		} else if _nothing {
			return nil
		}

		if err := restoreTxNum(applyTx); err != nil {
			return err
		}
	} else {
		var _nothing bool
		if err := chainDb.View(ctx, func(tx kv.Tx) (err error) {
			if _nothing, err = nothingToExec(applyTx); err != nil {
				return err
			} else if _nothing {
				return nil
			}

			return restoreTxNum(applyTx)
		}); err != nil {
			return err
		}
		if _nothing {
			return nil
		}
	}

	ts := time.Duration(0)
	blockNum = doms.BlockNum()
	outputTxNum.Store(doms.TxNum())

	if maxBlockNum < blockNum {
		return nil
	}

	shouldGenerateChangesets := maxBlockNum-blockNum <= changesetSafeRange || cfg.keepAllChangesets
	if blockNum < cfg.blockReader.FrozenBlocks() {
		shouldGenerateChangesets = false
	}

	if maxBlockNum > blockNum+16 {
		log.Info(fmt.Sprintf("[%s] starting", execStage.LogPrefix()),
			"from", blockNum, "to", maxBlockNum, "fromTxNum", doms.TxNum(), "offsetFromBlockBeginning", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx)
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	var outputBlockNum = stages.SyncMetrics[stages.Execution]
	inputBlockNum := &atomic.Uint64{}
	var count uint64
	var lock sync.RWMutex

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3(doms, logger)

	////TODO: owner of `resultCh` is main goroutine, but owner of `retryQueue` is applyLoop.
	// Now rwLoop closing both (because applyLoop we completely restart)
	// Maybe need split channels? Maybe don't exit from ApplyLoop? Maybe current way is also ok?

	// input queue
	in := state.NewQueueWithRetry(100_000)
	defer in.Close()

	rwsConsumed := make(chan struct{}, 1)
	defer close(rwsConsumed)

	execWorkers, _, rws, stopWorkers, waitWorkers := exec3.NewWorkersPool(lock.RLocker(), accumulator, logger, ctx, parallel, chainDb, rs, in, blockReader, chainConfig, genesis, engine, workerCount+1, cfg.dirs, isMining)
	defer stopWorkers()
	applyWorker := cfg.applyWorker
	if isMining {
		applyWorker = cfg.applyWorkerMining
	}
	applyWorker.ResetState(rs, accumulator)
	defer applyWorker.LogLRUStats()

	commitThreshold := batchSize.Bytes()
	progress := NewProgress(blockNum, commitThreshold, workerCount, false, execStage.LogPrefix(), logger)
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneEvery := time.NewTicker(2 * time.Second)
	defer pruneEvery.Stop()

	var logGas uint64
	var txCount uint64
	var stepsInDB float64

	processed := NewProgress(blockNum, commitThreshold, workerCount, true, execStage.LogPrefix(), logger)
	defer func() {
		processed.Log("Done", rs, in, rws, txCount, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets)
	}()

	applyLoopWg := sync.WaitGroup{} // to wait for finishing of applyLoop after applyCtx cancel
	defer applyLoopWg.Wait()

	applyLoopInner := func(ctx context.Context) error {
		tx, err := chainDb.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		applyWorker.ResetTx(tx)

		var lastBlockNum uint64

		for outputTxNum.Load() <= maxTxNum {
			if err := rws.Drain(ctx); err != nil {
				return err
			}

			processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err := processResultQueue(ctx, in, rws, outputTxNum.Load(), rs, agg, tx, rwsConsumed, applyWorker, true, false, isMining)
			if err != nil {
				return err
			}

			mxExecRepeats.AddInt(conflicts)
			mxExecTriggers.AddInt(triggers)
			if processedBlockNum > lastBlockNum {
				outputBlockNum.SetUint64(processedBlockNum)
				lastBlockNum = processedBlockNum
			}
			if processedTxNum > 0 {
				outputTxNum.Store(processedTxNum)
				blockComplete.Store(stoppedAtBlockEnd)
			}

		}
		return nil
	}
	applyLoop := func(ctx context.Context, errCh chan error) {
		defer applyLoopWg.Done()
		defer func() {
			if rec := recover(); rec != nil {
				log.Warn("[dbg] apply loop panic", "rec", rec)
			}
			log.Warn("[dbg] apply loop exit")
		}()
		if err := applyLoopInner(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}
	}

	var rwLoopErrCh chan error

	var rwLoopG *errgroup.Group
	if parallel {
		// `rwLoop` lives longer than `applyLoop`
		rwLoop := func(ctx context.Context) error {
			tx, err := chainDb.BeginRw(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			doms.SetTx(tx)

			defer applyLoopWg.Wait()
			applyCtx, cancelApplyCtx := context.WithCancel(ctx)
			defer cancelApplyCtx()
			applyLoopWg.Add(1)
			go applyLoop(applyCtx, rwLoopErrCh)
			for outputTxNum.Load() <= maxTxNum {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case <-logEvery.C:
					stepsInDB = rawdbhelpers.IdxStepsCountV3(tx)
					progress.Log("", rs, in, rws, rs.DoneCount(), logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets)
					if agg.HasBackgroundFilesBuild() {
						logger.Info(fmt.Sprintf("[%s] Background files build", execStage.LogPrefix()), "progress", agg.BackgroundProgress())
					}
				case <-pruneEvery.C:
					if rs.SizeEstimate() < commitThreshold {
						if doms.BlockNum() != outputBlockNum.GetValueUint64() {
							panic(fmt.Errorf("%d != %d", doms.BlockNum(), outputBlockNum.GetValueUint64()))
						}
						_, err := doms.ComputeCommitment(ctx, true, outputBlockNum.GetValueUint64(), execStage.LogPrefix())
						if err != nil {
							return err
						}
						ac := agg.BeginFilesRo()
						if _, err = ac.PruneSmallBatches(ctx, 10*time.Second, tx); err != nil { // prune part of retired data, before commit
							return err
						}
						ac.Close()
						if !inMemExec {
							if err = doms.Flush(ctx, tx); err != nil {
								return err
							}
						}
						break
					}
					if inMemExec {
						break
					}

					cancelApplyCtx()
					applyLoopWg.Wait()

					var t0, t1, t2, t3, t4 time.Duration
					commitStart := time.Now()
					logger.Info("Committing (parallel)...", "blockComplete.Load()", blockComplete.Load())
					if err := func() error {
						//Drain results (and process) channel because read sets do not carry over
						for !blockComplete.Load() {
							rws.DrainNonBlocking(ctx)
							applyWorker.ResetTx(tx)

							processedTxNum, conflicts, triggers, processedBlockNum, stoppedAtBlockEnd, err := processResultQueue(ctx, in, rws, outputTxNum.Load(), rs, agg, tx, nil, applyWorker, false, true, isMining)
							if err != nil {
								return err
							}

							mxExecRepeats.AddInt(conflicts)
							mxExecTriggers.AddInt(triggers)
							if processedBlockNum > 0 {
								outputBlockNum.SetUint64(processedBlockNum)
							}
							if processedTxNum > 0 {
								outputTxNum.Store(processedTxNum)
								blockComplete.Store(stoppedAtBlockEnd)
							}
						}
						t0 = time.Since(commitStart)
						lock.Lock() // This is to prevent workers from starting work on any new txTask
						defer lock.Unlock()

						select {
						case rwsConsumed <- struct{}{}:
						default:
						}

						// Drain results channel because read sets do not carry over
						rws.DropResults(ctx, func(txTask *state.TxTask) {
							rs.ReTry(txTask, in)
						})

						//lastTxNumInDb, _ := txNumsReader.Max(tx, outputBlockNum.Get())
						//if lastTxNumInDb != outputTxNum.Load()-1 {
						//	panic(fmt.Sprintf("assert: %d != %d", lastTxNumInDb, outputTxNum.Load()))
						//}

						t1 = time.Since(commitStart)
						tt := time.Now()
						t2 = time.Since(tt)
						tt = time.Now()

						if err := doms.Flush(ctx, tx); err != nil {
							return err
						}
						doms.ClearRam(true)
						t3 = time.Since(tt)

						if err = execStage.Update(tx, outputBlockNum.GetValueUint64()); err != nil {
							return err
						}
						if _, err = rawdb.IncrementStateVersion(applyTx); err != nil {
							return fmt.Errorf("writing plain state version: %w", err)
						}

						tx.CollectMetrics()
						tt = time.Now()
						if err = tx.Commit(); err != nil {
							return err
						}
						t4 = time.Since(tt)
						for i := 0; i < len(execWorkers); i++ {
							execWorkers[i].ResetTx(nil)
						}

						return nil
					}(); err != nil {
						return err
					}
					if tx, err = chainDb.BeginRw(ctx); err != nil {
						return err
					}
					defer tx.Rollback()
					doms.SetTx(tx)

					applyCtx, cancelApplyCtx = context.WithCancel(ctx)
					defer cancelApplyCtx()
					applyLoopWg.Add(1)
					go applyLoop(applyCtx, rwLoopErrCh)

					logger.Info("Committed", "time", time.Since(commitStart), "drain", t0, "drain_and_lock", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
				}
			}
			if err = doms.Flush(ctx, tx); err != nil {
				return err
			}
			if err = execStage.Update(tx, outputBlockNum.GetValueUint64()); err != nil {
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			return nil
		}

		rwLoopCtx, rwLoopCtxCancel := context.WithCancel(ctx)
		defer rwLoopCtxCancel()
		rwLoopG, rwLoopCtx = errgroup.WithContext(rwLoopCtx)
		defer rwLoopG.Wait()
		rwLoopG.Go(func() error {
			defer rws.Close()
			defer in.Close()
			defer applyLoopWg.Wait()
			defer func() {
				log.Warn("[dbg] rwloop exit")
			}()
			return rwLoop(rwLoopCtx)
		})
	}

	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header) {
		var err error
		if parallel {
			if err = chainDb.View(ctx, func(tx kv.Tx) error {
				h, err = blockReader.Header(ctx, tx, hash, number)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				panic(err)
			}
			return h
		} else {
			h, err = blockReader.Header(ctx, applyTx, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}
	}
	if !parallel {
		applyWorker.ResetTx(applyTx)
		doms.SetTx(applyTx)
	}

	slowDownLimit := time.NewTicker(time.Second)
	defer slowDownLimit.Stop()

	var readAhead chan uint64
	if !parallel {
		// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
		// can't use OS-level ReadAhead - because Data >> RAM
		// it also warmsup state a bit - by touching senders/coninbase accounts and code
		if !execStage.CurrentSyncCycle.IsInitialCycle {
			var clean func()

			readAhead, clean = blocksReadAhead(ctx, &cfg, 4, true)
			defer clean()
		}
	}

	var b *types.Block

	// Only needed by bor chains
	shouldGenerateChangesetsForLastBlocks := cfg.chainConfig.Bor != nil

Loop:
	for ; blockNum <= maxBlockNum; blockNum++ {
		// set shouldGenerateChangesets=true if we are at last n blocks from maxBlockNum. this is as a safety net in chains
		// where during initial sync we can expect bogus blocks to be imported.
		if !shouldGenerateChangesets && shouldGenerateChangesetsForLastBlocks && blockNum > cfg.blockReader.FrozenBlocks() && blockNum+changesetSafeRange >= maxBlockNum {
			aggTx := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
			aggTx.RestrictSubsetFileDeletions(true)
			start := time.Now()
			doms.SetChangesetAccumulator(nil) // Make sure we don't have an active changeset accumulator
			// First compute and commit the progress done so far
			if _, err := doms.ComputeCommitment(ctx, true, blockNum, execStage.LogPrefix()); err != nil {
				return err
			}
			ts += time.Since(start)
			aggTx.RestrictSubsetFileDeletions(false)
			shouldGenerateChangesets = true // now we can generate changesets for the safety net
		}
		changeset := &state2.StateChangeSet{}
		if shouldGenerateChangesets && blockNum > 0 {
			doms.SetChangesetAccumulator(changeset)
		}
		if !parallel {
			select {
			case readAhead <- blockNum:
			default:
			}
		}
		inputBlockNum.Store(blockNum)
		doms.SetBlockNum(blockNum)

		b, err = blockWithSenders(ctx, chainDb, applyTx, blockReader, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		metrics2.UpdateBlockConsumerPreExecutionDelay(b.Time(), blockNum, logger)
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum, header.Time)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		totalGasUsed += b.GasUsed()
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, cfg.author /* author */, chainConfig)
		// print type of engine
		if parallel {
			select {
			case err := <-rwLoopErrCh:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			func() {
				for rws.Len() > rws.Limit() || rs.SizeEstimate() >= commitThreshold {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-rwsConsumed:
						if !ok {
							return
						}
					case <-slowDownLimit.C:
						//logger.Warn("skip", "rws.Len()", rws.Len(), "rws.Limit()", rws.Limit(), "rws.ResultChLen()", rws.ResultChLen())
						//if tt := rws.Dbg(); tt != nil {
						//	log.Warn("fst", "n", tt.TxNum, "in.len()", in.Len(), "out", outputTxNum.Load(), "in.NewTasksLen", in.NewTasksLen())
						//}
						return
					}
				}
			}()
		} else if shouldReportToTxPool {
			txs, err := blockReader.RawTransactions(context.Background(), applyTx, b.NumberU64(), b.NumberU64())
			if err != nil {
				return err
			}
			accumulator.StartChange(b.NumberU64(), b.Hash(), txs, false)
		}

		rules := chainConfig.Rules(blockNum, b.Time())
		blockReceipts := make(types.Receipts, len(txs))
		// During the first block execution, we may have half-block data in the snapshots.
		// Thus, we need to skip the first txs in the block, however, this causes the GasUsed to be incorrect.
		// So we skip that check for the first block, if we find half-executed data.
		skipPostEvaluation := false
		var usedGas, blobGasUsed uint64

		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:           blockNum,
				Header:             header,
				Coinbase:           b.Coinbase(),
				Uncles:             b.Uncles(),
				Rules:              rules,
				Txs:                txs,
				TxNum:              inputTxNum,
				TxIndex:            txIndex,
				BlockHash:          b.Hash(),
				SkipAnalysis:       skipAnalysis,
				Final:              txIndex == len(txs),
				GetHashFn:          getHashFn,
				EvmBlockContext:    blockContext,
				Withdrawals:        b.Withdrawals(),
				Requests:           b.Requests(),
				PruneNonEssentials: pruneNonEssentials,

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),

				BlockReceipts: blockReceipts,

				Config: chainConfig,
			}
			if txTask.HistoryExecution && usedGas == 0 {
				usedGas, blobGasUsed, _, err = rawtemporaldb.ReceiptAsOf(applyTx.(kv.TemporalTx), txTask.TxNum)
				if err != nil {
					return err
				}
			}

			if cfg.genesis != nil {
				txTask.Config = cfg.genesis.Config
			}

			if txTask.TxNum <= txNumInDB && txTask.TxNum > 0 {
				inputTxNum++
				skipPostEvaluation = true
				continue
			}
			doms.SetTxNum(txTask.TxNum)
			doms.SetBlockNum(txTask.BlockNum)

			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}

				if sender, ok := txs[txIndex].GetSender(); ok {
					txTask.Sender = &sender
				} else {
					sender, err := signer.Sender(txTask.Tx)
					if err != nil {
						return err
					}
					txTask.Sender = &sender
					logger.Warn("[Execution] expensive lazy sender recovery", "blockNum", txTask.BlockNum, "txIdx", txTask.TxIndex)
				}
			}

			if parallel {
				if txTask.TxIndex >= 0 && txTask.TxIndex < len(txs) {
					if ok := rs.RegisterSender(txTask); ok {
						rs.AddWork(ctx, txTask, in)
					}
				} else {
					rs.AddWork(ctx, txTask, in)
				}
				stageProgress = blockNum
				inputTxNum++
				continue
			}

			count++
			if txTask.Error != nil {
				break Loop
			}
			applyWorker.RunTxTaskNoLock(txTask, isMining)
			if err := func() error {
				if errors.Is(txTask.Error, context.Canceled) {
					return err
				}
				if txTask.Error != nil {
					return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, txTask.Error) //same as in stage_exec.go
				}

				txCount++
				usedGas += txTask.UsedGas
				logGas += txTask.UsedGas
				mxExecGas.Add(float64(txTask.UsedGas))
				mxExecTransactions.Add(1)

				if txTask.Tx != nil {
					blobGasUsed += txTask.Tx.GetBlobGas()
				}

				txTask.CreateReceipt(applyTx)

				if txTask.Final {
					if !isMining && !inMemExec && !execStage.CurrentSyncCycle.IsInitialCycle {
						cfg.notifications.RecentLogs.Add(blockReceipts)
					}
					checkReceipts := !cfg.vmConfig.StatelessExec && chainConfig.IsByzantium(txTask.BlockNum) && !cfg.vmConfig.NoReceipts && !isMining
					if txTask.BlockNum > 0 && !skipPostEvaluation { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
						if err := core.BlockPostValidation(usedGas, blobGasUsed, checkReceipts, txTask.BlockReceipts, txTask.Header, isMining); err != nil {
							return fmt.Errorf("%w, txnIdx=%d, %v", consensus.ErrInvalidBlock, txTask.TxIndex, err) //same as in stage_exec.go
						}
					}
					usedGas, blobGasUsed = 0, 0
				}
				return nil
			}(); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				logger.Warn(fmt.Sprintf("[%s] Execution failed", execStage.LogPrefix()), "block", blockNum, "txNum", txTask.TxNum, "hash", header.Hash().String(), "err", err)
				if cfg.hd != nil && errors.Is(err, consensus.ErrInvalidBlock) {
					cfg.hd.ReportBadHeaderPoS(header.Hash(), header.ParentHash)
				}
				if cfg.badBlockHalt {
					return err
				}
				if errors.Is(err, consensus.ErrInvalidBlock) {
					if u != nil {
						if err := u.UnwindTo(blockNum-1, BadBlock(header.Hash(), err), applyTx); err != nil {
							return err
						}
					}
				} else {
					if u != nil {
						if err := u.UnwindTo(blockNum-1, ExecUnwind, applyTx); err != nil {
							return err
						}
					}
				}
				break Loop
			}

			if !txTask.Final {
				var receipt *types.Receipt
				if txTask.TxIndex >= 0 && !txTask.Final {
					receipt = txTask.BlockReceipts[txTask.TxIndex]
				}
				if err := rawtemporaldb.AppendReceipt(doms, receipt, blobGasUsed); err != nil {
					return err
				}
			}

			// MA applystate
			if err := rs.ApplyState4(ctx, txTask); err != nil {
				return err
			}

			stageProgress = blockNum
			outputTxNum.Add(1)
			inputTxNum++
		}
		mxExecBlocks.Add(1)

		if shouldGenerateChangesets {
			aggTx := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
			aggTx.RestrictSubsetFileDeletions(true)
			start := time.Now()
			if _, err := doms.ComputeCommitment(ctx, true, blockNum, execStage.LogPrefix()); err != nil {
				return err
			}
			ts += time.Since(start)
			aggTx.RestrictSubsetFileDeletions(false)
			doms.SavePastChangesetAccumulator(b.Hash(), blockNum, changeset)
			if !inMemExec {
				if err := state2.WriteDiffSet(applyTx, blockNum, b.Hash(), changeset); err != nil {
					return err
				}
			}
			doms.SetChangesetAccumulator(nil)
		}

		if offsetFromBlockBeginning > 0 {
			// after history execution no offset will be required
			offsetFromBlockBeginning = 0
		}

		// MA commitTx
		if !parallel {
			if !inMemExec && !isMining {
				metrics2.UpdateBlockConsumerPostExecutionDelay(b.Time(), blockNum, logger)
			}

			outputBlockNum.SetUint64(blockNum)

			select {
			case <-logEvery.C:
				if inMemExec || isMining {
					break
				}

				stepsInDB := rawdbhelpers.IdxStepsCountV3(applyTx)
				progress.Log("", rs, in, rws, count, logGas, inputBlockNum.Load(), outputBlockNum.GetValueUint64(), outputTxNum.Load(), mxExecRepeats.GetValueUint64(), stepsInDB, shouldGenerateChangesets)

				//TODO: https://github.com/erigontech/erigon/issues/10724
				//if applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).CanPrune(applyTx, outputTxNum.Load()) {
				//	//small prune cause MDBX_TXN_FULL
				//	if _, err := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, applyTx); err != nil {
				//		return err
				//	}
				//}

				aggregatorRo := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)

				needCalcRoot := rs.SizeEstimate() >= commitThreshold ||
					skipPostEvaluation || // If we skip post evaluation, then we should compute root hash ASAP for fail-fast
					aggregatorRo.CanPrune(applyTx, outputTxNum.Load()) // if have something to prune - better prune ASAP to keep chaindata smaller
				if !needCalcRoot {
					break
				}

				var (
					commitStart = time.Now()
					tt          = time.Now()

					t1, t2, t3 time.Duration
				)

				if ok, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, doms, cfg, execStage, stageProgress, parallel, logger, u, inMemExec); err != nil {
					return err
				} else if !ok {
					break Loop
				}

				t1 = time.Since(tt) + ts

				tt = time.Now()
				if _, err := aggregatorRo.PruneSmallBatches(ctx, 10*time.Hour, applyTx); err != nil {
					return err
				}
				t3 = time.Since(tt)

				if err := func() error {
					doms.Close()
					if err = execStage.Update(applyTx, outputBlockNum.GetValueUint64()); err != nil {
						return err
					}

					tt = time.Now()
					applyTx.CollectMetrics()

					if !useExternalTx {
						tt = time.Now()
						if err = applyTx.Commit(); err != nil {
							return err
						}

						t2 = time.Since(tt)
						agg.BuildFilesInBackground(outputTxNum.Load())

						applyTx, err = cfg.db.BeginRw(context.Background()) //nolint
						if err != nil {
							return err
						}
					}
					doms, err = state2.NewSharedDomains(applyTx, logger)
					if err != nil {
						return err
					}
					doms.SetTxNum(inputTxNum)
					rs = state.NewStateV3(doms, logger)

					applyWorker.ResetTx(applyTx)
					applyWorker.ResetState(rs, accumulator)

					return nil
				}(); err != nil {
					return err
				}

				// on chain-tip: if batch is full then stop execution - to allow stages commit
				if !execStage.CurrentSyncCycle.IsInitialCycle {
					break Loop
				}
				logger.Info("Committed", "time", time.Since(commitStart),
					"block", doms.BlockNum(), "txNum", doms.TxNum(),
					"step", fmt.Sprintf("%.1f", float64(doms.TxNum())/float64(agg.StepSize())),
					"flush+commitment", t1, "tx.commit", t2, "prune", t3)
			default:
			}
		}

		if parallel { // sequential exec - does aggregate right after commit
			agg.BuildFilesInBackground(outputTxNum.Load())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	//log.Info("Executed", "blocks", inputBlockNum.Load(), "txs", outputTxNum.Load(), "repeats", mxExecRepeats.GetValueUint64())

	if parallel {
		logger.Warn("[dbg] all txs sent")
		if err := rwLoopG.Wait(); err != nil {
			return err
		}
		waitWorkers()
	}

	if u != nil && !u.HasUnwindPoint() {
		if b != nil {
			_, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, doms, cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
			if err != nil {
				return err
			}
		} else {
			fmt.Printf("[dbg] mmmm... do we need action here????\n")
		}
	}

	//dumpPlainStateDebug(applyTx, doms)

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	return nil
}

// nolint
func dumpPlainStateDebug(tx kv.RwTx, doms *state2.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}
	{
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).DomainRangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
		if err != nil {
			panic(err)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			a := accounts.NewAccount()
			accounts.DeserialiseV3(&a, v)
			fmt.Printf("%x, %d, %d, %d, %x\n", k, &a.Balance, a.Nonce, a.Incarnation, a.CodeHash)
		}
	}
	{
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).DomainRangeLatest(tx, kv.StorageDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
		}
	}
	{
		it, err := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).DomainRangeLatest(tx, kv.CommitmentDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
			if bytes.Equal(k, []byte("state")) {
				fmt.Printf("state: t=%d b=%d\n", binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]))
			}
		}
	}
}

// flushAndCheckCommitmentV3 - does write state to db and then check commitment
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.RwTx, doms *state2.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, parallel bool, logger log.Logger, u Unwinder, inMemExec bool) (bool, error) {

	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if !parallel {
		if err := e.Update(applyTx, maxBlockNum); err != nil {
			return false, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	if header == nil {
		return false, errors.New("header is nil")
	}

	if dbg.DiscardCommitment() {
		return true, nil
	}
	if doms.BlockNum() != header.Number.Uint64() {
		panic(fmt.Errorf("%d != %d", doms.BlockNum(), header.Number.Uint64()))
	}

	rh, err := doms.ComputeCommitment(ctx, true, header.Number.Uint64(), e.LogPrefix())
	if err != nil {
		return false, fmt.Errorf("StateV3.Apply: %w", err)
	}
	if cfg.blockProduction {
		header.Root = common.BytesToHash(rh)
		return true, nil
	}
	if bytes.Equal(rh, header.Root.Bytes()) {
		if !inMemExec {
			if err := doms.Flush(ctx, applyTx); err != nil {
				return false, err
			}
			if err = applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).PruneCommitHistory(ctx, applyTx, nil); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
	if cfg.badBlockHalt {
		return false, errors.New("wrong trie root")
	}
	if cfg.hd != nil {
		cfg.hd.ReportBadHeaderPoS(header.Hash(), header.ParentHash)
	}
	minBlockNum := e.BlockNumber
	if maxBlockNum <= minBlockNum {
		return false, nil
	}

	aggTx := applyTx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
	unwindToLimit, err := aggTx.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return false, err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, 1000, (maxBlockNum-minBlockNum)/2)
	unwindTo := maxBlockNum - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := aggTx.CanUnwindBeforeBlockNum(unwindTo, applyTx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("%w: requested=%d, minAllowed=%d", ErrTooDeepUnwind, unwindTo, allowedUnwindTo)
	}
	logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
	if u != nil {
		if err := u.UnwindTo(allowedUnwindTo, BadBlock(header.Hash(), ErrInvalidStateRootHash), applyTx); err != nil {
			return false, err
		}
	}
	return false, nil
}

func blockWithSenders(ctx context.Context, db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	if tx == nil {
		tx, err = db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	b, err = blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	for _, txn := range b.Transactions() {
		_ = txn.Hash()
	}
	return b, err
}

func processResultQueue(ctx context.Context, in *state.QueueWithRetry, rws *state.ResultsQueue, outputTxNumIn uint64, rs *state.StateV3, agg *state2.Aggregator, applyTx kv.Tx, backPressure chan struct{}, applyWorker *exec3.Worker, canRetry, forceStopAtBlockEnd bool, isMining bool) (outputTxNum uint64, conflicts, triggers int, processedBlockNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := rws.Iter()
	defer rwsIt.Close()

	var i int
	outputTxNum = outputTxNumIn
	for rwsIt.HasNext(outputTxNum) {
		txTask := rwsIt.PopNext()
		if txTask.Error != nil || !rs.ReadsValid(txTask.ReadLists) {
			conflicts++

			if i > 0 && canRetry {
				//send to re-exex
				rs.ReTry(txTask, in)
				continue
			}

			// resolve first conflict right here: it's faster and conflict-free
			applyWorker.RunTxTask(txTask, isMining)
			if txTask.Error != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, txTask.Error)
			}
			// TODO: post-validation of gasUsed and blobGasUsed
			i++
		}

		if txTask.Final {
			rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			err := rs.ApplyState4(ctx, txTask)
			if err != nil {
				return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("StateV3.Apply: %w", err)
			}
			//if !bytes.Equal(rh, txTask.BlockRoot[:]) {
			//	log.Error("block hash mismatch", "rh", hex.EncodeToString(rh), "blockRoot", hex.EncodeToString(txTask.BlockRoot[:]), "bn", txTask.BlockNum, "txn", txTask.TxNum)
			//	return outputTxNum, conflicts, triggers, processedBlockNum, false, fmt.Errorf("block hashk mismatch: %x != %x bn =%d, txn= %d", rh, txTask.BlockRoot[:], txTask.BlockNum, txTask.TxNum)
			//}
		}
		triggers += rs.CommitTxNum(txTask.Sender, txTask.TxNum, in)
		outputTxNum++
		if backPressure != nil {
			select {
			case backPressure <- struct{}{}:
			default:
			}
		}
		if err := rs.ApplyLogsAndTraces4(txTask, rs.Domains()); err != nil {
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

func reconstituteStep(last bool,
	workerCount int, ctx context.Context, db kv.RwDB, txNum uint64, dirs datadir.Dirs,
	as *state2.AggregatorStep, chainDb kv.RwDB, blockReader services.FullBlockReader,
	chainConfig *chain.Config, logger log.Logger, genesis *types.Genesis, engine consensus.Engine,
	batchSize datasize.ByteSize, s *StageState, blockNum uint64, total uint64,
) error {
	var startOk, endOk bool
	startTxNum, endTxNum := as.TxNumRange()

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader))
	var startBlockNum, endBlockNum uint64 // First block which is not covered by the history snapshot files
	if err := chainDb.View(ctx, func(tx kv.Tx) (err error) {
		startOk, startBlockNum, err = txNumsReader.FindBlockNum(tx, startTxNum)
		if err != nil {
			return err
		}
		if startBlockNum > 0 {
			startBlockNum--
			startTxNum, err = txNumsReader.Min(tx, startBlockNum)
			if err != nil {
				return err
			}
		}
		endOk, endBlockNum, err = txNumsReader.FindBlockNum(tx, endTxNum)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !startOk {
		return fmt.Errorf("step startTxNum not found in snapshot blocks: %d", startTxNum)
	}
	if !endOk {
		return fmt.Errorf("step endTxNum not found in snapshot blocks: %d", endTxNum)
	}
	if last {
		endBlockNum = blockNum
	}

	logger.Info(fmt.Sprintf("[%s] Reconstitution", s.LogPrefix()), "startTxNum", startTxNum, "endTxNum", endTxNum, "startBlockNum", startBlockNum, "endBlockNum", endBlockNum)

	var maxTxNum = startTxNum

	scanWorker := exec3.NewScanWorker(txNum, as)

	t := time.Now()
	if err := scanWorker.BitmapAccounts(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Scan accounts history", s.LogPrefix()), "took", time.Since(t))
	}

	t = time.Now()
	if err := scanWorker.BitmapStorage(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Scan storage history", s.LogPrefix()), "took", time.Since(t))
	}

	t = time.Now()
	if err := scanWorker.BitmapCode(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Scan code history", s.LogPrefix()), "took", time.Since(t))
	}
	bitmap := scanWorker.Bitmap()

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	logger.Info(fmt.Sprintf("[%s] Ready to replay", s.LogPrefix()), "transactions", bitmap.GetCardinality(), "out of", txNum)
	var lock sync.RWMutex
	reconWorkers := make([]*exec3.ReconWorker, workerCount)
	roTxs := make([]kv.Tx, workerCount)
	chainTxs := make([]kv.Tx, workerCount)
	defer func() {
		for i := 0; i < workerCount; i++ {
			if roTxs[i] != nil {
				roTxs[i].Rollback()
			}
			if chainTxs[i] != nil {
				chainTxs[i].Rollback()
			}
		}
	}()
	for i := 0; i < workerCount; i++ {
		var err error
		if roTxs[i], err = db.BeginRo(ctx); err != nil {
			return err
		}
		if chainTxs[i], err = chainDb.BeginRo(ctx); err != nil {
			return err
		}
	}
	g, reconstWorkersCtx := errgroup.WithContext(ctx)
	defer g.Wait()
	workCh := make(chan *state.TxTask, workerCount*4)
	defer func() {
		fmt.Printf("close1\n")
		safeCloseTxTaskCh(workCh)
	}()

	rs := state.NewReconState(workCh)
	prevCount := rs.DoneCount()
	for i := 0; i < workerCount; i++ {
		var localAs *state2.AggregatorStep
		if i == 0 {
			localAs = as
		} else {
			localAs = as.Clone()
		}
		reconWorkers[i] = exec3.NewReconWorker(lock.RLocker(), reconstWorkersCtx, rs, localAs, blockReader, chainConfig, logger, genesis, engine, chainTxs[i])
		reconWorkers[i].SetTx(roTxs[i])
		reconWorkers[i].SetChainTx(chainTxs[i])
		reconWorkers[i].SetDirs(dirs)
	}

	rollbackCount := uint64(0)

	for i := 0; i < workerCount; i++ {
		i := i
		g.Go(func() error { return reconWorkers[i].Run() })
	}
	commitThreshold := batchSize.Bytes()
	prevRollbackCount := uint64(0)
	prevTime := time.Now()
	reconDone := make(chan struct{}, 1)

	defer close(reconDone)

	commit := func(ctx context.Context) error {
		t := time.Now()
		lock.Lock()
		defer lock.Unlock()
		for i := 0; i < workerCount; i++ {
			roTxs[i].Rollback()
		}
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			if err := rs.Flush(tx); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		for i := 0; i < workerCount; i++ {
			var err error
			if roTxs[i], err = db.BeginRo(ctx); err != nil {
				return err
			}
			reconWorkers[i].SetTx(roTxs[i])
		}
		logger.Info(fmt.Sprintf("[%s] State reconstitution, commit", s.LogPrefix()), "took", time.Since(t))
		return nil
	}
	g.Go(func() error {
		for {
			select {
			case <-reconDone: // success finish path
				return nil
			case <-reconstWorkersCtx.Done(): // force-stop path
				return reconstWorkersCtx.Err()
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				sizeEstimate := rs.SizeEstimate()
				maxTxNum = rs.MaxTxNum()
				count := rs.DoneCount()
				rollbackCount = rs.RollbackCount()
				currentTime := time.Now()
				interval := currentTime.Sub(prevTime)
				speedTx := float64(count-prevCount) / (float64(interval) / float64(time.Second))
				progress := 100.0 * float64(maxTxNum) / float64(total)
				stepProgress := 100.0 * float64(maxTxNum-startTxNum) / float64(endTxNum-startTxNum)
				var repeatRatio float64
				if count > prevCount {
					repeatRatio = 100.0 * float64(rollbackCount-prevRollbackCount) / float64(count-prevCount)
				}
				prevTime = currentTime
				prevCount = count
				prevRollbackCount = rollbackCount
				logger.Info(fmt.Sprintf("[%s] State reconstitution", s.LogPrefix()), "overall progress", fmt.Sprintf("%.2f%%", progress),
					"step progress", fmt.Sprintf("%.2f%%", stepProgress),
					"tx/s", fmt.Sprintf("%.1f", speedTx), "workCh", fmt.Sprintf("%d/%d", len(workCh), cap(workCh)),
					"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "queue.len", rs.QueueLen(), "blk", stages.SyncMetrics[stages.Execution].GetValueUint64(),
					"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(commitThreshold)),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				if sizeEstimate >= commitThreshold {
					if err := commit(reconstWorkersCtx); err != nil {
						return err
					}
				}
			}
		}
	})

	var inputTxNum = startTxNum
	var b *types.Block
	var txKey [8]byte
	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header) {
		var err error
		if err = chainDb.View(ctx, func(tx kv.Tx) error {
			h, err = blockReader.Header(ctx, tx, hash, number)
			if err != nil {
				return err
			}
			return nil

		}); err != nil {
			panic(err)
		}
		return h
	}

	if err := func() (err error) {
		defer func() {
			close(workCh)
			reconDone <- struct{}{} // Complete logging and committing go-routine
			if waitErr := g.Wait(); waitErr != nil {
				if err == nil {
					err = waitErr
				}
				return
			}
		}()

		for bn := startBlockNum; bn <= endBlockNum; bn++ {
			t = time.Now()
			b, err = blockWithSenders(ctx, chainDb, nil, blockReader, bn)
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("could not find block %d", bn)
			}
			txs := b.Transactions()
			header := b.HeaderNoCopy()
			skipAnalysis := core.SkipAnalysis(chainConfig, bn)
			signer := *types.MakeSigner(chainConfig, bn, header.Time)

			f := core.GetHashFn(header, getHeaderFunc)
			getHashFnMute := &sync.Mutex{}
			getHashFn := func(n uint64) common.Hash {
				getHashFnMute.Lock()
				defer getHashFnMute.Unlock()
				return f(n)
			}
			blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */, chainConfig)
			rules := chainConfig.Rules(bn, b.Time())

			for txIndex := -1; txIndex <= len(txs); txIndex++ {
				if bitmap.Contains(inputTxNum) {
					binary.BigEndian.PutUint64(txKey[:], inputTxNum)
					txTask := &state.TxTask{
						BlockNum:        bn,
						Header:          header,
						Coinbase:        b.Coinbase(),
						Uncles:          b.Uncles(),
						Rules:           rules,
						TxNum:           inputTxNum,
						Txs:             txs,
						TxIndex:         txIndex,
						BlockHash:       b.Hash(),
						SkipAnalysis:    skipAnalysis,
						Final:           txIndex == len(txs),
						GetHashFn:       getHashFn,
						EvmBlockContext: blockContext,
						Withdrawals:     b.Withdrawals(),
						Requests:        b.Requests(),
					}
					if txIndex >= 0 && txIndex < len(txs) {
						txTask.Tx = txs[txIndex]
						txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
						if err != nil {
							return err
						}
						if sender, ok := txs[txIndex].GetSender(); ok {
							txTask.Sender = &sender
						}
					} else {
						txTask.Txs = txs
					}

					select {
					case workCh <- txTask:
					case <-reconstWorkersCtx.Done():
						// if ctx canceled, then maybe it's because of error in errgroup
						//
						// errgroup doesn't play with pattern where some 1 goroutine-producer is outside of errgroup
						// but RwTx doesn't allow move between goroutines
						return g.Wait()
					}
				}
				inputTxNum++
			}

			stages.SyncMetrics[stages.Execution].SetUint64(bn)
		}
		return err
	}(); err != nil {
		return err
	}

	for i := 0; i < workerCount; i++ {
		roTxs[i].Rollback()
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := rs.Flush(tx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	plainStateCollector := etl.NewCollector(s.LogPrefix()+" recon plainState", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector(s.LogPrefix()+" recon code", dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector(s.LogPrefix()+" recon plainContract", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer plainContractCollector.Close()
	var transposedKey []byte

	if err := db.View(ctx, func(roTx kv.Tx) error {
		clear := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.PlainStateR, nil, math.MaxUint32)
		defer clear()
		if err := roTx.ForEach(kv.PlainStateR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return plainStateCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		clear2 := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.PlainStateD, nil, math.MaxUint32)
		defer clear2()
		if err := roTx.ForEach(kv.PlainStateD, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], v...)
			transposedKey = append(transposedKey, k...)
			return plainStateCollector.Collect(transposedKey, nil)
		}); err != nil {
			return err
		}
		clear3 := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.CodeR, nil, math.MaxUint32)
		defer clear3()
		if err := roTx.ForEach(kv.CodeR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return codeCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		clear4 := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.CodeD, nil, math.MaxUint32)
		defer clear4()
		if err := roTx.ForEach(kv.CodeD, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], v...)
			transposedKey = append(transposedKey, k...)
			return codeCollector.Collect(transposedKey, nil)
		}); err != nil {
			return err
		}
		clear5 := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.PlainContractR, nil, math.MaxUint32)
		defer clear5()
		if err := roTx.ForEach(kv.PlainContractR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return plainContractCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		clear6 := kv.ReadAhead(ctx, db, &atomic.Bool{}, kv.PlainContractD, nil, math.MaxUint32)
		defer clear6()
		if err := roTx.ForEach(kv.PlainContractD, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], v...)
			transposedKey = append(transposedKey, k...)
			return plainContractCollector.Collect(transposedKey, nil)
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := tx.ClearBucket(kv.PlainStateR); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.PlainStateD); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.CodeR); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.CodeD); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.PlainContractR); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.PlainContractD); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := chainDb.Update(ctx, func(tx kv.RwTx) error {
		var lastKey []byte
		var lastVal []byte
		if err := plainStateCollector.Load(tx, kv.PlainState, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			if v == nil { // `nil` value means delete, `empty value []byte{}` means empty value
				lastVal = nil
			} else {
				lastVal = append(lastVal[:0], v...)
			}
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		plainStateCollector.Close()
		if lastKey != nil {
			if len(lastVal) > 0 {
				if e := tx.Put(kv.PlainState, lastKey, lastVal); e != nil {
					return e
				}
			} else {
				if e := tx.Delete(kv.PlainState, lastKey); e != nil {
					return e
				}
			}
		}
		lastKey = nil
		lastVal = nil
		if err := codeCollector.Load(tx, kv.Code, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			if v == nil {
				lastVal = nil
			} else {
				lastVal = append(lastVal[:0], v...)
			}
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		codeCollector.Close()
		if lastKey != nil {
			if len(lastVal) > 0 {
				if e := tx.Put(kv.Code, lastKey, lastVal); e != nil {
					return e
				}
			} else {
				if e := tx.Delete(kv.Code, lastKey); e != nil {
					return e
				}
			}
		}
		lastKey = nil
		lastVal = nil
		if err := plainContractCollector.Load(tx, kv.PlainContractCode, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			if v == nil {
				lastVal = nil
			} else {
				lastVal = append(lastVal[:0], v...)
			}
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		plainContractCollector.Close()
		if lastKey != nil {
			if len(lastVal) > 0 {
				if e := tx.Put(kv.PlainContractCode, lastKey, lastVal); e != nil {
					return e
				}
			} else {
				if e := tx.Delete(kv.PlainContractCode, lastKey); e != nil {
					return e
				}
			}
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func safeCloseTxTaskCh(ch chan *state.TxTask) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// Channel was already closed
	default:
		close(ch)
	}
}

func ReconstituteState(ctx context.Context, s *StageState, dirs datadir.Dirs, workerCount int, batchSize datasize.ByteSize, chainDb kv.RwDB,
	blockReader services.FullBlockReader,
	logger log.Logger, agg *state2.Aggregator, engine consensus.Engine,
	chainConfig *chain.Config, genesis *types.Genesis) (err error) {
	startTime := time.Now()

	// force merge snapshots before reconstitution, to allign domains progress
	// un-finished merge can happen at "kill -9" during merge
	if err := agg.MergeLoop(ctx); err != nil {
		return err
	}

	// Incremental reconstitution, step by step (snapshot range by snapshot range)
	aggSteps, err := agg.MakeSteps()
	if err != nil {
		return err
	}
	if len(aggSteps) == 0 {
		return nil
	}
	lastStep := aggSteps[len(aggSteps)-1]
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader))
	var ok bool
	var blockNum uint64 // First block which is not covered by the history snapshot files
	var txNum uint64
	if err := chainDb.View(ctx, func(tx kv.Tx) error {
		_, toTxNum := lastStep.TxNumRange()
		ok, blockNum, err = txNumsReader.FindBlockNum(tx, toTxNum)
		if err != nil {
			return err
		}
		if !ok {
			lastBn, lastTn, _ := txNumsReader.Last(tx)
			return fmt.Errorf("blockNum for mininmaxTxNum=%d not found. See lastBlockNum=%d,lastTxNum=%d", toTxNum, lastBn, lastTn)
		}
		if blockNum == 0 {
			return errors.New("not enough transactions in the history data")
		}
		blockNum--
		txNum, err = txNumsReader.Max(tx, blockNum)
		if err != nil {
			return err
		}
		txNum++
		return nil
	}); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("[%s] Blocks execution, reconstitution", s.LogPrefix()), "fromBlock", s.BlockNumber, "toBlock", blockNum, "toTxNum", txNum)

	reconDbPath := filepath.Join(dirs.DataDir, "recondb")
	dir.Recreate(reconDbPath)
	db, err := kv2.NewMDBX(log.New()).Path(reconDbPath).
		Flags(func(u uint) uint {
			return mdbx.UtterlyNoSync | mdbx.NoMetaSync | mdbx.NoMemInit | mdbx.LifoReclaim | mdbx.WriteMap
		}).
		PageSize(uint64(8 * datasize.KB)).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).
		Open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	defer os.RemoveAll(reconDbPath)

	for step, as := range aggSteps {
		logger.Info("Step of incremental reconstitution", "step", step+1, "out of", len(aggSteps), "workers", workerCount)
		if err := reconstituteStep(step+1 == len(aggSteps), workerCount, ctx, db,
			txNum, dirs, as, chainDb, blockReader, chainConfig, logger, genesis,
			engine, batchSize, s, blockNum, txNum,
		); err != nil {
			return err
		}
	}
	db.Close()
	plainStateCollector := etl.NewCollector(s.LogPrefix()+" recon plainState", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector(s.LogPrefix()+" recon code", dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector(s.LogPrefix()+" recon plainContract", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer plainContractCollector.Close()

	fillWorker := exec3.NewFillWorker(txNum, aggSteps[len(aggSteps)-1])
	t := time.Now()
	if err := fillWorker.FillAccounts(plainStateCollector); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Filled accounts", s.LogPrefix()), "took", time.Since(t))
	}
	t = time.Now()
	if err := fillWorker.FillStorage(plainStateCollector); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Filled storage", s.LogPrefix()), "took", time.Since(t))
	}
	t = time.Now()
	if err := fillWorker.FillCode(codeCollector, plainContractCollector); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		logger.Info(fmt.Sprintf("[%s] Filled code", s.LogPrefix()), "took", time.Since(t))
	}

	// Load all collections into the main collector
	if err = chainDb.Update(ctx, func(tx kv.RwTx) error {
		if err = plainStateCollector.Load(tx, kv.PlainState, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return err
		}
		plainStateCollector.Close()
		if err = codeCollector.Load(tx, kv.Code, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return err
		}
		codeCollector.Close()
		if err = plainContractCollector.Load(tx, kv.PlainContractCode, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return err
		}
		plainContractCollector.Close()
		if err := s.Update(tx, blockNum); err != nil {
			return err
		}
		s.BlockNumber = blockNum
		return nil
	}); err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("[%s] Reconstitution done", s.LogPrefix()), "in", time.Since(startTime))
	return nil
}
