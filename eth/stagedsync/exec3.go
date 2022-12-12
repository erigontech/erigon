package stagedsync

import (
	"bytes"
	"container/heap"
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

	"github.com/VictoriaMetrics/metrics"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"github.com/torquem-ch/mdbx-go/mdbx"
	atomic2 "go.uber.org/atomic"
)

var ExecStepsInDB = metrics.NewCounter(`exec_steps_in_db`) //nolint

func NewProgress(prevOutputBlockNum, commitThreshold uint64, workersCount int, logPrefix string) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum, commitThreshold: commitThreshold, workersCount: workersCount, logPrefix: logPrefix}
}

type Progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
	commitThreshold    uint64

	workersCount int
	logPrefix    string
}

func (p *Progress) Log(rs *state.State22, rwsLen int, queueSize, count, inputBlockNum, outputBlockNum, outTxNum, repeatCount uint64, resultsSize uint64, resultCh chan *exec22.TxTask, idxStepsAmountInDB float64) {
	ExecStepsInDB.Set(uint64(idxStepsAmountInDB * 100))
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	speedTx := float64(count-p.prevCount) / (float64(interval) / float64(time.Second))
	//speedBlock := float64(outputBlockNum-p.prevOutputBlockNum) / (float64(interval) / float64(time.Second))
	var repeatRatio float64
	if count > p.prevCount {
		repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(count-p.prevCount)
	}
	log.Info(fmt.Sprintf("[%s] Transaction replay", p.logPrefix),
		//"workers", workerCount,
		"blk", outputBlockNum, "step", fmt.Sprintf("%.1f", float64(outTxNum)/float64(ethconfig.HistoryV3AggregationStep)),
		"inBlk", atomic.LoadUint64(&inputBlockNum),
		//"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"resultCh", fmt.Sprintf("%d/%d", len(resultCh), cap(resultCh)),
		"resultQueue", fmt.Sprintf("%d/%d", rwsLen, queueSize),
		"resultsSize", common.ByteCount(resultsSize),
		"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		"workers", p.workersCount,
		"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"idxStepsInDB", fmt.Sprintf("%.2f", idxStepsAmountInDB),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	//var txNums []string
	//for _, t := range rws {
	//	txNums = append(txNums, fmt.Sprintf("%d", t.TxNum))
	//}
	//s := strings.Join(txNums, ",")
	//log.Info(fmt.Sprintf("[%s] Transaction replay queue", logPrefix), "txNums", s)

	p.prevTime = currentTime
	p.prevCount = count
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, workerCount int, cfg ExecuteBlockCfg, applyTx kv.RwTx,
	parallel bool, rs *state.State22, logPrefix string,
	logger log.Logger,
	maxBlockNum uint64,
) (err error) {
	batchSize, chainDb := cfg.batchSize, cfg.db
	blockReader := cfg.blockReader
	agg, engine := cfg.agg, cfg.engine
	chainConfig, genesis := cfg.chainConfig, cfg.genesis
	blockSnapshots := blockReader.(WithSnapshots).Snapshots()

	useExternalTx := applyTx != nil
	if !useExternalTx && !parallel {
		applyTx, err = chainDb.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer applyTx.Rollback()
	} else {
		if blockSnapshots.Cfg().Enabled {
			defer blockSnapshots.EnableMadvNormal().DisableReadAhead()
		}
	}

	var block, stageProgress uint64
	var outputTxNum, maxTxNum = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var inputTxNum uint64
	if execStage.BlockNumber > 0 {
		stageProgress = execStage.BlockNumber
		block = execStage.BlockNumber + 1
	}
	if applyTx != nil {
		agg.SetTx(applyTx)
		if dbg.DiscardHistory() {
			defer agg.DiscardHistory().FinishWrites()
		} else {
			defer agg.StartWrites().FinishWrites()
		}

		_maxTxNum, err := rawdb.TxNums.Max(applyTx, maxBlockNum)
		if err != nil {
			return err
		}
		maxTxNum.Store(_maxTxNum)
		if block > 0 {
			_outputTxNum, err := rawdb.TxNums.Max(applyTx, execStage.BlockNumber)
			if err != nil {
				return err
			}
			outputTxNum.Store(_outputTxNum)
			outputTxNum.Inc()
			inputTxNum = outputTxNum.Load()
		}
	} else {
		if err := chainDb.View(ctx, func(tx kv.Tx) error {
			_maxTxNum, err := rawdb.TxNums.Max(tx, maxBlockNum)
			if err != nil {
				return err
			}
			maxTxNum.Store(_maxTxNum)
			if block > 0 {
				_outputTxNum, err := rawdb.TxNums.Max(tx, execStage.BlockNumber)
				if err != nil {
					return err
				}
				outputTxNum.Store(_outputTxNum)
				outputTxNum.Inc()
				inputTxNum = outputTxNum.Load()
			}
			return nil
		}); err != nil {
			return err
		}
	}
	agg.SetTxNum(inputTxNum)

	var inputBlockNum, outputBlockNum = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var count uint64
	var repeatCount, triggerCount = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var resultsSize = atomic2.NewInt64(0)
	var lock sync.RWMutex

	rws := &exec22.TxTaskQueue{}
	heap.Init(rws)
	var rwsLock sync.RWMutex
	rwsReceiveCond := sync.NewCond(&rwsLock)

	queueSize := workerCount * 4
	execWorkers, resultCh, stopWorkers := exec3.NewWorkersPool(lock.RLocker(), ctx, parallel, chainDb, rs, blockReader, chainConfig, logger, genesis, engine, workerCount+1)
	defer stopWorkers()

	commitThreshold := batchSize.Bytes()
	resultsThreshold := int64(batchSize.Bytes())
	progress := NewProgress(block, commitThreshold, workerCount, execStage.LogPrefix())
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneEvery := time.NewTicker(2 * time.Second)
	defer pruneEvery.Stop()

	applyLoopWg := sync.WaitGroup{} // to wait for finishing of applyLoop after applyCtx cancel
	defer applyLoopWg.Wait()

	applyLoopInner := func(ctx context.Context) error {
		tx, err := chainDb.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		notifyReceived := func() { rwsReceiveCond.Signal() }
		for outputTxNum.Load() < maxTxNum.Load() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case txTask := <-resultCh:
				if err := func() error {
					rwsLock.Lock()
					defer rwsLock.Unlock()
					resultsSize.Add(txTask.ResultsSize)
					heap.Push(rws, txTask)
					if err := processResultQueue(rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize, notifyReceived); err != nil {
						return err
					}
					syncMetrics[stages.Execution].Set(outputBlockNum.Load())
					return nil
				}(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	applyLoop := func(ctx context.Context, errCh chan error) {
		defer applyLoopWg.Done()
		if err := applyLoopInner(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}
	}

	var errCh chan error

	if parallel {
		errCh = make(chan error, 1)
		defer close(errCh)

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		// Go-routine gathering results from the workers
		rwLoop := func(ctx context.Context) error {
			tx, err := chainDb.BeginRw(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			agg.SetTx(tx)
			if dbg.DiscardHistory() {
				defer agg.DiscardHistory().FinishWrites()
			} else {
				defer agg.StartWrites().FinishWrites()
			}

			applyCtx, cancelApplyCtx := context.WithCancel(ctx)
			defer cancelApplyCtx()
			applyLoopWg.Add(1)
			go applyLoop(applyCtx, errCh)

			for outputTxNum.Load() < maxTxNum.Load() {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case <-logEvery.C:
					rwsLock.RLock()
					rwsLen := rws.Len()
					rwsLock.RUnlock()

					stepsInDB := idxStepsInDB(tx)
					progress.Log(rs, rwsLen, uint64(queueSize), rs.DoneCount(), inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh, stepsInDB)
				case <-pruneEvery.C:
					if rs.SizeEstimate() < commitThreshold {
						if err = agg.Flush(tx); err != nil {
							return err
						}
						if err = agg.PruneWithTiemout(ctx, 1*time.Second); err != nil {
							return err
						}
						break
					}

					cancelApplyCtx()
					applyLoopWg.Wait()

					var t1, t2, t3, t4 time.Duration
					commitStart := time.Now()
					log.Info("Committing...")
					if err := func() error {
						rwsLock.Lock()
						defer rwsLock.Unlock()
						// Drain results (and process) channel because read sets do not carry over
						for {
							var drained bool
							for !drained {
								select {
								case txTask := <-resultCh:
									resultsSize.Add(txTask.ResultsSize)
									heap.Push(rws, txTask)
								default:
									drained = true
								}
							}
							if err := processResultQueue(rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize, func() {}); err != nil {
								return err
							}
							syncMetrics[stages.Execution].Set(outputBlockNum.Load())
							if rws.Len() == 0 {
								break
							}
						}
						rwsReceiveCond.Signal()
						lock.Lock() // This is to prevent workers from starting work on any new txTask
						defer lock.Unlock()
						// Drain results channel because read sets do not carry over
						var drained bool
						for !drained {
							select {
							case txTask := <-resultCh:
								rs.AddWork(txTask)
							default:
								drained = true
							}
						}

						// Drain results queue as well
						for rws.Len() > 0 {
							txTask := heap.Pop(rws).(*exec22.TxTask)
							resultsSize.Add(-txTask.ResultsSize)
							rs.AddWork(txTask)
						}
						t1 = time.Since(commitStart)
						tt := time.Now()
						if err := rs.Flush(tx); err != nil {
							return err
						}
						t2 = time.Since(tt)

						tt = time.Now()
						if err := agg.Flush(tx); err != nil {
							return err
						}
						t3 = time.Since(tt)

						if err = execStage.Update(tx, outputBlockNum.Load()); err != nil {
							return err
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
					agg.SetTx(tx)

					applyCtx, cancelApplyCtx = context.WithCancel(ctx)
					defer cancelApplyCtx()
					applyLoopWg.Add(1)
					go applyLoop(applyCtx, errCh)

					log.Info("Committed", "time", time.Since(commitStart), "drain", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
				}
			}
			if err = rs.Flush(tx); err != nil {
				return err
			}
			if err = agg.Flush(tx); err != nil {
				return err
			}
			if err = execStage.Update(tx, outputBlockNum.Load()); err != nil {
				return err
			}
			//if err = execStage.Update(tx, stageProgress); err != nil {
			//	panic(err)
			//}
			if err = tx.Commit(); err != nil {
				return err
			}
			return nil
		}
		go func() {
			defer wg.Done()
			defer rwsReceiveCond.Broadcast() // unlock listners in case of cancelation
			defer rs.Finish()

			if err := rwLoop(ctx); err != nil {
				errCh <- err
			}
		}()
	}

	if !parallel {
		execWorkers[0].ResetTx(applyTx)
	}

	if block < blockSnapshots.BlocksAvailable() {
		agg.KeepInDB(0)
		defer agg.KeepInDB(ethconfig.HistoryV3AggregationStep)
	}

	getHeaderFunc := func(hash common2.Hash, number uint64) (h *types.Header) {
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

	var b *types.Block
	var blockNum uint64
Loop:
	for blockNum = block; blockNum <= maxBlockNum; blockNum++ {
		t := time.Now()

		inputBlockNum.Store(blockNum)
		b, err = blockWithSenders(chainDb, applyTx, blockReader, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall prodcess deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common2.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */)

		if parallel {
			select {
			case err := <-errCh:
				if err != nil {
					return err
				}
			default:
			}

			func() {
				rwsLock.RLock()
				needWait := rws.Len() > queueSize || resultsSize.Load() >= resultsThreshold || rs.SizeEstimate() >= commitThreshold
				rwsLock.RUnlock()
				if !needWait {
					return
				}
				rwsLock.Lock()
				defer rwsLock.Unlock()
				for rws.Len() > queueSize || resultsSize.Load() >= resultsThreshold || rs.SizeEstimate() >= commitThreshold {
					select {
					case <-ctx.Done():
						return
					default:
					}
					rwsReceiveCond.Wait()
				}
			}()
		}
		rules := chainConfig.Rules(blockNum, b.Time())
		var gasUsed uint64
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Do not oversend, wait for the result heap to go under certain size
			txTask := &exec22.TxTask{
				BlockNum:        blockNum,
				Header:          header,
				Coinbase:        b.Coinbase(),
				Uncles:          b.Uncles(),
				Rules:           rules,
				Txs:             txs,
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				BlockHash:       b.Hash(),
				SkipAnalysis:    skipAnalysis,
				Final:           txIndex == len(txs),
				GetHashFn:       getHashFn,
				EvmBlockContext: blockContext,
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					panic(err)
				}

				if sender, ok := txs[txIndex].GetSender(); ok {
					txTask.Sender = &sender
				}
				if parallel {
					if ok := rs.RegisterSender(txTask); ok {
						rs.AddWork(txTask)
					}
				}
			} else if parallel {
				rs.AddWork(txTask)
			}
			if !parallel {
				count++
				execWorkers[0].RunTxTask(txTask)
				if err := func() error {
					if txTask.Final {
						gasUsed += txTask.UsedGas
						if gasUsed != txTask.Header.GasUsed {
							return fmt.Errorf("gas used by execution: %d, in header: %d", gasUsed, txTask.Header.GasUsed)
						}
						gasUsed = 0
					} else {
						gasUsed += txTask.UsedGas
					}
					return nil
				}(); err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Warn(fmt.Sprintf("[%s] Execution failed", logPrefix), "block", blockNum, "hash", header.Hash().String(), "err", err)
						if cfg.hd != nil {
							cfg.hd.ReportBadHeaderPoS(header.Hash(), header.ParentHash)
						}
						if cfg.badBlockHalt {
							return err
						}
					}
					u.UnwindTo(blockNum-1, header.Hash())
					break Loop
				}

				if err := rs.ApplyState(applyTx, txTask, agg); err != nil {
					panic(fmt.Errorf("State22.Apply: %w", err))
				}
				triggerCount.Add(rs.CommitTxNum(txTask.Sender, txTask.TxNum))
				outputTxNum.Inc()
				outputBlockNum.Store(txTask.BlockNum)
				if err := rs.ApplyHistory(txTask, agg); err != nil {
					panic(fmt.Errorf("State22.Apply: %w", err))
				}
			}
			stageProgress = blockNum
			inputTxNum++
		}

		core.BlockExecutionTimer.UpdateDuration(t)
		if !parallel {
			syncMetrics[stages.Execution].Set(blockNum)

			select {
			case <-logEvery.C:
				stepsInDB := idxStepsInDB(applyTx)
				progress.Log(rs, rws.Len(), uint64(queueSize), count, inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh, stepsInDB)
				if rs.SizeEstimate() < commitThreshold {
					break
				}

				var t1, t2, t3, t4 time.Duration
				commitStart := time.Now()
				if err := func() error {
					t1 = time.Since(commitStart)
					tt := time.Now()
					if err := rs.Flush(applyTx); err != nil {
						return err
					}
					t2 = time.Since(tt)

					tt = time.Now()
					if err := agg.Flush(applyTx); err != nil {
						return err
					}
					t3 = time.Since(tt)

					if err = execStage.Update(applyTx, outputBlockNum.Load()); err != nil {
						return err
					}

					applyTx.CollectMetrics()

					return nil
				}(); err != nil {
					return err
				}
				log.Info("Committed", "time", time.Since(commitStart), "drain", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
			default:
			}
		}

		if blockSnapshots.Cfg().Produce {
			if err := agg.BuildFilesInBackground(chainDb); err != nil {
				return err
			}
		}
	}

	if parallel {
		if err := <-errCh; err != nil {
			return err
		}
	}

	if !parallel {
		stopWorkers()
	} else {
		if err = rs.Flush(applyTx); err != nil {
			return err
		}
		if err = agg.Flush(applyTx); err != nil {
			return err
		}
		if err = execStage.Update(applyTx, stageProgress); err != nil {
			return err
		}
	}

	if blockSnapshots.Cfg().Produce {
		if err := agg.BuildFilesInBackground(chainDb); err != nil {
			return err
		}
	}

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func blockWithSenders(db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	if tx == nil {
		tx, err = db.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, _, err = blockReader.BlockWithSenders(context.Background(), tx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func processResultQueue(rws *exec22.TxTaskQueue, outputTxNum *atomic2.Uint64, rs *state.State22, agg *state2.Aggregator22, applyTx kv.Tx, triggerCount, outputBlockNum, repeatCount *atomic2.Uint64, resultsSize *atomic2.Int64, onSuccess func()) error {
	var txTask *exec22.TxTask
	for rws.Len() > 0 && (*rws)[0].TxNum == outputTxNum.Load() {
		txTask = heap.Pop(rws).(*exec22.TxTask)
		resultsSize.Add(-txTask.ResultsSize)
		if txTask.Error != nil || !rs.ReadsValid(txTask.ReadLists) {
			rs.AddWork(txTask)
			repeatCount.Inc()
			continue
			//fmt.Printf("Rolled back %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		}

		if err := rs.ApplyState(applyTx, txTask, agg); err != nil {
			return fmt.Errorf("State22.Apply: %w", err)
		}
		triggerCount.Add(rs.CommitTxNum(txTask.Sender, txTask.TxNum))
		outputTxNum.Inc()
		onSuccess()
		if err := rs.ApplyHistory(txTask, agg); err != nil {
			return fmt.Errorf("State22.Apply: %w", err)
		}
		//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
	}
	if txTask != nil {
		outputBlockNum.Store(txTask.BlockNum)
	}
	return nil
}

func reconstituteStep(last bool,
	workerCount int, ctx context.Context, db kv.RwDB, txNum uint64, dirs datadir.Dirs,
	as *libstate.AggregatorStep, chainDb kv.RwDB, blockReader services.FullBlockReader,
	chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis, engine consensus.Engine,
	batchSize datasize.ByteSize, s *StageState, blockNum uint64, total uint64,
) error {
	var err error
	var startOk, endOk bool
	startTxNum, endTxNum := as.TxNumRange()
	var startBlockNum, endBlockNum uint64 // First block which is not covered by the history snapshot files
	if err := chainDb.View(ctx, func(tx kv.Tx) error {
		startOk, startBlockNum, err = rawdb.TxNums.FindBlockNum(tx, startTxNum)
		if err != nil {
			return err
		}
		if startBlockNum > 0 {
			startBlockNum--
			startTxNum, err = rawdb.TxNums.Min(tx, startBlockNum)
			if err != nil {
				return err
			}
		}
		endOk, endBlockNum, err = rawdb.TxNums.FindBlockNum(tx, endTxNum)
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

	fmt.Printf("startTxNum = %d, endTxNum = %d, startBlockNum = %d, endBlockNum = %d\n", startTxNum, endTxNum, startBlockNum, endBlockNum)
	var maxTxNum uint64 = startTxNum

	workCh := make(chan *exec22.TxTask, workerCount*4)
	rs := state.NewReconState(workCh)
	scanWorker := exec3.NewScanWorker(txNum, as)

	t := time.Now()
	if err := scanWorker.BitmapAccounts(); err != nil {
		return err
	}
	log.Info("Scan accounts history", "took", time.Since(t))

	t = time.Now()
	if err := scanWorker.BitmapStorage(); err != nil {
		return err
	}
	log.Info("Scan storage history", "took", time.Since(t))

	t = time.Now()
	if err := scanWorker.BitmapCode(); err != nil {
		return err
	}
	log.Info("Scan code history", "took", time.Since(t))
	bitmap := scanWorker.Bitmap()

	var wg sync.WaitGroup
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	log.Info("Ready to replay", "transactions", bitmap.GetCardinality(), "out of", txNum)
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
		if roTxs[i], err = db.BeginRo(ctx); err != nil {
			return err
		}
		if chainTxs[i], err = chainDb.BeginRo(ctx); err != nil {
			return err
		}
	}
	for i := 0; i < workerCount; i++ {
		var localAs *libstate.AggregatorStep
		if i == 0 {
			localAs = as
		} else {
			localAs = as.Clone()
		}
		reconWorkers[i] = exec3.NewReconWorker(lock.RLocker(), &wg, rs, localAs, blockReader, chainConfig, logger, genesis, engine, chainTxs[i])
		reconWorkers[i].SetTx(roTxs[i])
		reconWorkers[i].SetChainTx(chainTxs[i])
	}
	wg.Add(workerCount)

	rollbackCount := uint64(0)
	prevCount := rs.DoneCount()
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].Run()
	}
	commitThreshold := batchSize.Bytes()
	prevRollbackCount := uint64(0)
	prevTime := time.Now()
	reconDone := make(chan struct{})
	var bn uint64
	go func() {
		for {
			select {
			case <-reconDone:
				return
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
				syncMetrics[stages.Execution].Set(bn)
				log.Info(fmt.Sprintf("[%s] State reconstitution", s.LogPrefix()), "overall progress", fmt.Sprintf("%.2f%%", progress),
					"step progress", fmt.Sprintf("%.2f%%", stepProgress),
					"tx/s", fmt.Sprintf("%.1f", speedTx), "workCh", fmt.Sprintf("%d/%d", len(workCh), cap(workCh)),
					"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "queue.len", rs.QueueLen(), "blk", bn,
					"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(commitThreshold)),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				if sizeEstimate >= commitThreshold {
					t := time.Now()
					if err = func() error {
						lock.Lock()
						defer lock.Unlock()
						for i := 0; i < workerCount; i++ {
							roTxs[i].Rollback()
						}
						if err := db.Update(ctx, func(tx kv.RwTx) error {
							if err = rs.Flush(tx); err != nil {
								return err
							}
							return nil
						}); err != nil {
							return err
						}
						for i := 0; i < workerCount; i++ {
							if roTxs[i], err = db.BeginRo(ctx); err != nil {
								return err
							}
							reconWorkers[i].SetTx(roTxs[i])
						}
						return nil
					}(); err != nil {
						panic(err)
					}
					log.Info(fmt.Sprintf("[%s] State reconstitution, commit", s.LogPrefix()), "took", time.Since(t))
				}
			}
		}
	}()

	var inputTxNum uint64 = startTxNum
	var b *types.Block
	var txKey [8]byte
	getHeaderFunc := func(hash common2.Hash, number uint64) (h *types.Header) {
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
	for bn = startBlockNum; bn <= endBlockNum; bn++ {
		t = time.Now()
		b, err = blockWithSenders(chainDb, nil, blockReader, bn)
		if err != nil {
			return err
		}
		if b == nil {
			fmt.Printf("could not find block %d\n", bn)
			panic("")
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, bn)
		signer := *types.MakeSigner(chainConfig, bn)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common2.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */)
		rules := chainConfig.Rules(bn, b.Time())

		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			if bitmap.Contains(inputTxNum) {
				binary.BigEndian.PutUint64(txKey[:], inputTxNum)
				txTask := &exec22.TxTask{
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
				workCh <- txTask
			}
			inputTxNum++
		}

		core.BlockExecutionTimer.UpdateDuration(t)
	}
	close(workCh)
	wg.Wait()
	reconDone <- struct{}{} // Complete logging and committing go-routine
	for i := 0; i < workerCount; i++ {
		roTxs[i].Rollback()
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err = rs.Flush(tx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	plainStateCollector := etl.NewCollector("recon plainState", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector("recon code", dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector("recon plainContract", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainContractCollector.Close()
	var transposedKey []byte
	if err = db.View(ctx, func(roTx kv.Tx) error {
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.PlainStateR, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.PlainStateR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return plainStateCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.PlainStateD, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.PlainStateD, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], v...)
			transposedKey = append(transposedKey, k...)
			return plainStateCollector.Collect(transposedKey, nil)
		}); err != nil {
			return err
		}
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.CodeR, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.CodeR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return codeCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.CodeD, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.CodeD, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], v...)
			transposedKey = append(transposedKey, k...)
			return codeCollector.Collect(transposedKey, nil)
		}); err != nil {
			return err
		}
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.PlainContractR, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.PlainContractR, nil, func(k, v []byte) error {
			transposedKey = append(transposedKey[:0], k[8:]...)
			transposedKey = append(transposedKey, k[:8]...)
			return plainContractCollector.Collect(transposedKey, v)
		}); err != nil {
			return err
		}
		kv.ReadAhead(ctx, db, atomic2.NewBool(false), kv.PlainContractD, nil, math.MaxUint32)
		if err = roTx.ForEach(kv.PlainContractD, nil, func(k, v []byte) error {
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
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if err = tx.ClearBucket(kv.PlainStateR); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.PlainStateD); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.CodeR); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.CodeD); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.PlainContractR); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.PlainContractD); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err = chainDb.Update(ctx, func(tx kv.RwTx) error {
		var lastKey []byte
		var lastVal []byte
		if err = plainStateCollector.Load(tx, kv.PlainState, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			lastVal = append(lastVal[:0], v...)
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
		if err = codeCollector.Load(tx, kv.Code, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			lastVal = append(lastVal[:0], v...)
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
		if err = plainContractCollector.Load(tx, kv.PlainContractCode, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if !bytes.Equal(k[:len(k)-8], lastKey) {
				if lastKey != nil {
					if e := next(lastKey, lastKey, lastVal); e != nil {
						return e
					}
				}
				lastKey = append(lastKey[:0], k[:len(k)-8]...)
			}
			lastVal = append(lastVal[:0], v...)
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

func ReconstituteState(ctx context.Context, s *StageState, dirs datadir.Dirs, workerCount int, batchSize datasize.ByteSize, chainDb kv.RwDB,
	blockReader services.FullBlockReader,
	logger log.Logger, agg *state2.Aggregator22, engine consensus.Engine,
	chainConfig *params.ChainConfig, genesis *core.Genesis) (err error) {
	startTime := time.Now()
	defer agg.EnableMadvNormal().DisableReadAhead()
	blockSnapshots := blockReader.(WithSnapshots).Snapshots()

	var ok bool
	var blockNum uint64 // First block which is not covered by the history snapshot files
	if err := chainDb.View(ctx, func(tx kv.Tx) error {
		ok, blockNum, err = rawdb.TxNums.FindBlockNum(tx, agg.EndTxNumMinimax())
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("mininmax txNum not found in snapshot blocks: %d", agg.EndTxNumMinimax())
	}
	if blockNum == 0 {
		return fmt.Errorf("not enough transactions in the history data")
	}
	blockNum--
	var txNum uint64
	if err := chainDb.View(ctx, func(tx kv.Tx) error {
		txNum, err = rawdb.TxNums.Max(tx, blockNum)
		if err != nil {
			return err
		}
		txNum++
		return nil
	}); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Blocks execution, reconstitution", s.LogPrefix()), "fromBlock", s.BlockNumber, "toBlock", blockNum, "toTxNum", txNum)

	reconDbPath := filepath.Join(dirs.DataDir, "recondb")
	dir.Recreate(reconDbPath)
	db, err := kv2.NewMDBX(log.New()).Path(reconDbPath).
		Flags(func(u uint) uint {
			return mdbx.UtterlyNoSync | mdbx.NoMetaSync | mdbx.NoMemInit | mdbx.LifoReclaim | mdbx.WriteMap
		}).
		WriteMergeThreshold(2 * 8192).
		PageSize(uint64(64 * datasize.KB)).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).
		Open()
	if err != nil {
		return err
	}
	defer db.Close()
	defer os.RemoveAll(reconDbPath)

	// Incremental reconstitution, step by step (snapshot range by snapshot range)
	defer blockSnapshots.EnableReadAhead().DisableReadAhead()
	aggSteps := agg.MakeSteps()
	for step, as := range aggSteps {
		log.Info("Step of incremental reconstitution", "step", step+1, "out of", len(aggSteps), "workers", workerCount)
		if err := reconstituteStep(step+1 == len(aggSteps), workerCount, ctx, db,
			txNum, dirs, as, chainDb, blockReader, chainConfig, logger, genesis,
			engine, batchSize, s, blockNum, txNum,
		); err != nil {
			return err
		}
	}
	db.Close()
	plainStateCollector := etl.NewCollector("recon plainState", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector("recon code", dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector("recon plainContract", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainContractCollector.Close()
	fillWorker := exec3.NewFillWorker(txNum, aggSteps[len(aggSteps)-1])
	t := time.Now()
	fillWorker.FillAccounts(plainStateCollector)
	log.Info("Filled accounts", "took", time.Since(t))
	t = time.Now()
	fillWorker.FillStorage(plainStateCollector)
	log.Info("Filled storage", "took", time.Since(t))
	t = time.Now()
	fillWorker.FillCode(codeCollector, plainContractCollector)
	log.Info("Filled code", "took", time.Since(t))
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
	log.Info("Reconstitution done", "in", time.Since(startTime))
	return nil
}

func idxStepsInDB(tx kv.Tx) float64 {
	fst, _ := kv.FirstKey(tx, kv.TracesToKeys)
	lst, _ := kv.LastKey(tx, kv.TracesToKeys)
	if len(fst) > 0 && len(lst) > 0 {
		fstTxNum := binary.BigEndian.Uint64(fst)
		lstTxNum := binary.BigEndian.Uint64(lst)

		return float64(lstTxNum-fstTxNum) / float64(ethconfig.HistoryV3AggregationStep)
	}
	return 0
}
