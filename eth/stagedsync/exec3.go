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
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"github.com/torquem-ch/mdbx-go/mdbx"
	atomic2 "go.uber.org/atomic"

	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbhelpers"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
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

func (p *Progress) Log(rs *state.StateV3, rwsLen int, queueSize, doneCount, inputBlockNum, outputBlockNum, outTxNum, repeatCount uint64, resultsSize uint64, resultCh chan *exec22.TxTask, idxStepsAmountInDB float64) {
	ExecStepsInDB.Set(uint64(idxStepsAmountInDB * 100))
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	queueLen := rs.QueueLen()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	speedTx := float64(doneCount-p.prevCount) / (float64(interval) / float64(time.Second))
	//speedBlock := float64(outputBlockNum-p.prevOutputBlockNum) / (float64(interval) / float64(time.Second))
	var repeatRatio float64
	if doneCount > p.prevCount {
		repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(doneCount-p.prevCount)
	}
	log.Info(fmt.Sprintf("[%s] Transaction replay", p.logPrefix),
		//"workers", workerCount,
		"blk", outputBlockNum,
		//"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"pipe", fmt.Sprintf("%d/%d->%d/%d->%d/%d", queueLen, queueSize, rwsLen, queueSize, len(resultCh), cap(resultCh)),
		"resultsSize", common.ByteCount(resultsSize),
		"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		"workers", p.workersCount,
		"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"idxStepsInDB", fmt.Sprintf("%.2f", idxStepsAmountInDB),
		"inBlk", atomic.LoadUint64(&inputBlockNum),
		"step", fmt.Sprintf("%.1f", float64(outTxNum)/float64(ethconfig.HistoryV3AggregationStep)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	//var txNums []string
	//for _, t := range rws {
	//	txNums = append(txNums, fmt.Sprintf("%d", t.TxNum))
	//}
	//s := strings.Join(txNums, ",")
	//log.Info(fmt.Sprintf("[%s] Transaction replay queue", logPrefix), "txNums", s)

	p.prevTime = currentTime
	p.prevCount = doneCount
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, workerCount int, cfg ExecuteBlockCfg, applyTx kv.RwTx,
	parallel bool, rs *state.StateV3, logPrefix string,
	logger log.Logger,
	maxBlockNum uint64,
) error {
	batchSize, chainDb := cfg.batchSize, cfg.db
	blockReader := cfg.blockReader
	agg, engine := cfg.agg, cfg.engine
	chainConfig, genesis := cfg.chainConfig, cfg.genesis
	blockSnapshots := blockReader.(WithSnapshots).Snapshots()

	useExternalTx := applyTx != nil
	if !useExternalTx && !parallel {
		var err error
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
	var maxTxNum uint64
	var outputTxNum = atomic2.NewUint64(0)
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

		var err error
		maxTxNum, err = rawdbv3.TxNums.Max(applyTx, maxBlockNum)
		if err != nil {
			return err
		}
		if block > 0 {
			_outputTxNum, err := rawdbv3.TxNums.Max(applyTx, execStage.BlockNumber)
			if err != nil {
				return err
			}
			outputTxNum.Store(_outputTxNum)
			outputTxNum.Inc()
			inputTxNum = outputTxNum.Load()
		}
	} else {
		if err := chainDb.View(ctx, func(tx kv.Tx) error {
			var err error
			maxTxNum, err = rawdbv3.TxNums.Max(tx, maxBlockNum)
			if err != nil {
				return err
			}
			if block > 0 {
				_outputTxNum, err := rawdbv3.TxNums.Max(tx, execStage.BlockNumber)
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

	queueSize := workerCount // workerCount * 4 // when wait cond can be moved inside txs loop
	execWorkers, applyWorker, resultCh, stopWorkers, waitWorkers := exec3.NewWorkersPool(lock.RLocker(), ctx, parallel, chainDb, rs, blockReader, chainConfig, logger, genesis, engine, workerCount+1)
	defer stopWorkers()
	applyWorker.DiscardReadList()

	var rwsLock sync.Mutex
	rwsReceiveCond := sync.NewCond(&rwsLock)

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

		applyWorker.ResetTx(tx)

		notifyReceived := func() { rwsReceiveCond.Signal() }
		var t time.Time
		var lastBlockNum uint64
		for outputTxNum.Load() < maxTxNum {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case txTask, ok := <-resultCh:
				if !ok {
					return nil
				}
				if txTask.BlockNum > lastBlockNum {
					if lastBlockNum > 0 {
						core.BlockExecutionTimer.UpdateDuration(t)
					}
					lastBlockNum = txTask.BlockNum
					t = time.Now()
				}
				if err := func() error {
					rwsLock.Lock()
					defer rwsLock.Unlock()
					resultsSize.Add(txTask.ResultsSize)
					heap.Push(rws, txTask)
					if err := processResultQueue(rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize, notifyReceived, applyWorker); err != nil {
						return err
					}
					return nil
				}(); err != nil {
					return err
				}

				syncMetrics[stages.Execution].Set(outputBlockNum.Load())
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

	var rwLoopErrCh chan error

	var rwLoopWg sync.WaitGroup
	if parallel {
		// `rwLoop` lives longer than `applyLoop`
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

			defer applyLoopWg.Wait()
			applyCtx, cancelApplyCtx := context.WithCancel(ctx)
			defer cancelApplyCtx()
			applyLoopWg.Add(1)
			go applyLoop(applyCtx, rwLoopErrCh)
			for outputTxNum.Load() < maxTxNum {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case <-logEvery.C:
					rwsLock.Lock()
					rwsLen := rws.Len()
					rwsLock.Unlock()

					stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
					progress.Log(rs, rwsLen, uint64(queueSize), rs.DoneCount(), inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh, stepsInDB)
				case <-pruneEvery.C:
					if rs.SizeEstimate() < commitThreshold {
						// too much steps in db will slow-down everything: flush and prune
						// it means better spend time for pruning, before flushing more data to db
						// also better do it now - instead of before Commit() - because Commit does block execution
						stepsInDB := rawdbhelpers.IdxStepsCountV3(tx)
						if stepsInDB > 5 && rs.SizeEstimate() < uint64(float64(commitThreshold)*0.2) {
							if err = agg.Prune(ctx, ethconfig.HistoryV3AggregationStep*2); err != nil { // prune part of retired data, before commit
								panic(err)
							}
						}

						if err = agg.Flush(ctx, tx); err != nil {
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
								case txTask, ok := <-resultCh:
									if !ok {
										return nil
									}
									resultsSize.Add(txTask.ResultsSize)
									heap.Push(rws, txTask)
								default:
									drained = true
								}
							}
							applyWorker.ResetTx(tx)
							if err := processResultQueue(rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize, func() {}, applyWorker); err != nil {
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
							case txTask, ok := <-resultCh:
								if !ok {
									return nil
								}
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
						if err := rs.Flush(ctx, tx, logPrefix, logEvery); err != nil {
							return err
						}
						t2 = time.Since(tt)

						tt = time.Now()
						if err := agg.Flush(ctx, tx); err != nil {
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
					go applyLoop(applyCtx, rwLoopErrCh)

					log.Info("Committed", "time", time.Since(commitStart), "drain", t1, "rs.flush", t2, "agg.flush", t3, "tx.commit", t4)
				}
			}
			if err = rs.Flush(ctx, tx, logPrefix, logEvery); err != nil {
				return err
			}
			if err = agg.Flush(ctx, tx); err != nil {
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

		rwLoopErrCh = make(chan error, 1)

		defer rwLoopWg.Wait()
		rwLoopCtx, rwLoopCancel := context.WithCancel(ctx)
		defer rwLoopCancel()
		rwLoopWg.Add(1)
		go func() {
			defer close(rwLoopErrCh)
			defer rwLoopWg.Done()

			defer applyLoopWg.Wait()
			defer rs.Finish()

			if err := rwLoop(rwLoopCtx); err != nil {
				rwLoopErrCh <- err
			}
		}()
	}

	if block < blockSnapshots.BlocksAvailable() {
		agg.KeepInDB(0)
		defer agg.KeepInDB(ethconfig.HistoryV3AggregationStep)
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
	}

	_, isPoSa := cfg.engine.(consensus.PoSA)
	//isBor := cfg.chainConfig.Bor != nil

	var b *types.Block
	var blockNum uint64
	var err error
Loop:
	for blockNum = block; blockNum <= maxBlockNum; blockNum++ {
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
		getHashFn := func(n uint64) common.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */)

		if parallel {
			select {
			case err := <-rwLoopErrCh:
				if err != nil {
					return err
				}
			default:
			}

			func() {
				needWait := rs.QueueLen() > queueSize
				if !needWait {
					return
				}
				rwsLock.Lock()
				defer rwsLock.Unlock()
				for rs.QueueLen() > queueSize || rws.Len() > queueSize || resultsSize.Load() >= resultsThreshold || rs.SizeEstimate() >= commitThreshold {
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
				Withdrawals:     b.Withdrawals(),
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					panic(err)
				}

				if sender, ok := txs[txIndex].GetSender(); ok {
					txTask.Sender = &sender
				} else {
					sender, err := signer.Sender(txTask.Tx)
					if err != nil {
						return err
					}
					txTask.Sender = &sender
					log.Warn("[Execution] expencive lazy sender recovery", "blockNum", txTask.BlockNum, "txIdx", txTask.TxIndex)
				}
			}

			if parallel {
				if txTask.TxIndex >= 0 && txTask.TxIndex < len(txs) {
					if ok := rs.RegisterSender(txTask); ok {
						rs.AddWork(txTask)
					}
				} else {
					rs.AddWork(txTask)
				}
			} else {
				count++
				applyWorker.RunTxTask(txTask)
				if err := func() error {
					if txTask.Final && !isPoSa {
						gasUsed += txTask.UsedGas
						if gasUsed != txTask.Header.GasUsed {
							if txTask.BlockNum > 0 { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
								return fmt.Errorf("gas used by execution: %d, in header: %d, headerNum=%d, %x", gasUsed, txTask.Header.GasUsed, txTask.Header.Number.Uint64(), txTask.Header.Hash())
							}
						}
						gasUsed = 0
					} else {
						gasUsed += txTask.UsedGas
					}
					return nil
				}(); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
						return err
					} else {
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
					return fmt.Errorf("StateV3.Apply: %w", err)
				}
				triggerCount.Add(rs.CommitTxNum(txTask.Sender, txTask.TxNum))
				outputTxNum.Inc()
				outputBlockNum.Store(txTask.BlockNum)
				if err := rs.ApplyHistory(txTask, agg); err != nil {
					return fmt.Errorf("StateV3.Apply: %w", err)
				}
			}
			stageProgress = blockNum
			inputTxNum++
		}

		if !parallel {
			syncMetrics[stages.Execution].Set(blockNum)

			select {
			case <-logEvery.C:
				stepsInDB := rawdbhelpers.IdxStepsCountV3(applyTx)
				progress.Log(rs, rws.Len(), uint64(queueSize), count, inputBlockNum.Load(), outputBlockNum.Load(), outputTxNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh, stepsInDB)
				if rs.SizeEstimate() < commitThreshold {
					break
				}

				var t1, t2, t3, t4 time.Duration
				commitStart := time.Now()
				if err := func() error {
					t1 = time.Since(commitStart)
					tt := time.Now()
					if err := rs.Flush(ctx, applyTx, logPrefix, logEvery); err != nil {
						return err
					}
					t2 = time.Since(tt)

					tt = time.Now()
					if err := agg.Flush(ctx, applyTx); err != nil {
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
			agg.BuildFilesInBackground()
		}
	}

	if parallel {
		if err := <-rwLoopErrCh; err != nil {
			return err
		}
		rwLoopWg.Wait()
		waitWorkers()
	} else {
		if err = rs.Flush(ctx, applyTx, logPrefix, logEvery); err != nil {
			return err
		}
		if err = agg.Flush(ctx, applyTx); err != nil {
			return err
		}
		if err = execStage.Update(applyTx, stageProgress); err != nil {
			return err
		}
	}

	if blockSnapshots.Cfg().Produce {
		agg.BuildFilesInBackground()
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

func processResultQueue(rws *exec22.TxTaskQueue, outputTxNum *atomic2.Uint64, rs *state.StateV3, agg *state2.AggregatorV3, applyTx kv.Tx, triggerCount, outputBlockNum, repeatCount *atomic2.Uint64, resultsSize *atomic2.Int64, onSuccess func(), applyWorker *exec3.Worker) error {
	var txTask *exec22.TxTask
	for rws.Len() > 0 && (*rws)[0].TxNum == outputTxNum.Load() {
		txTask = heap.Pop(rws).(*exec22.TxTask)
		resultsSize.Add(-txTask.ResultsSize)
		if txTask.Error != nil || !rs.ReadsValid(txTask.ReadLists) {
			repeatCount.Inc()

			//rs.AddWork(txTask)
			//repeatCount.Inc()
			//continue

			// immediately retry once
			applyWorker.RunTxTask(txTask)
			if txTask.Error != nil {
				return txTask.Error
				//log.Info("second fail", "blk", txTask.BlockNum, "txn", txTask.BlockNum)
				//rs.AddWork(txTask)
				//continue
			}
		}

		if err := rs.ApplyState(applyTx, txTask, agg); err != nil {
			return fmt.Errorf("StateV3.Apply: %w", err)
		}
		triggerCount.Add(rs.CommitTxNum(txTask.Sender, txTask.TxNum))
		outputTxNum.Inc()
		onSuccess()
		if err := rs.ApplyHistory(txTask, agg); err != nil {
			return fmt.Errorf("StateV3.Apply: %w", err)
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
	chainConfig *chain.Config, logger log.Logger, genesis *core.Genesis, engine consensus.Engine,
	batchSize datasize.ByteSize, s *StageState, blockNum uint64, total uint64,
) error {
	var startOk, endOk bool
	startTxNum, endTxNum := as.TxNumRange()
	var startBlockNum, endBlockNum uint64 // First block which is not covered by the history snapshot files
	if err := chainDb.View(ctx, func(tx kv.Tx) (err error) {
		startOk, startBlockNum, err = rawdbv3.TxNums.FindBlockNum(tx, startTxNum)
		if err != nil {
			return err
		}
		if startBlockNum > 0 {
			startBlockNum--
			startTxNum, err = rawdbv3.TxNums.Min(tx, startBlockNum)
			if err != nil {
				return err
			}
		}
		endOk, endBlockNum, err = rawdbv3.TxNums.FindBlockNum(tx, endTxNum)
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

	log.Info(fmt.Sprintf("[%s] Reconstitution", s.LogPrefix()), "startTxNum", startTxNum, "endTxNum", endTxNum, "startBlockNum", startBlockNum, "endBlockNum", endBlockNum)

	var maxTxNum = startTxNum

	workCh := make(chan *exec22.TxTask, workerCount*4)
	rs := state.NewReconState(workCh)
	scanWorker := exec3.NewScanWorker(txNum, as)

	t := time.Now()
	if err := scanWorker.BitmapAccounts(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Scan accounts history", s.LogPrefix()), "took", time.Since(t))
	}

	t = time.Now()
	if err := scanWorker.BitmapStorage(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Scan storage history", s.LogPrefix()), "took", time.Since(t))
	}

	t = time.Now()
	if err := scanWorker.BitmapCode(); err != nil {
		return err
	}
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Scan code history", s.LogPrefix()), "took", time.Since(t))
	}
	bitmap := scanWorker.Bitmap()

	var wg sync.WaitGroup
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	log.Info(fmt.Sprintf("[%s] Ready to replay", s.LogPrefix()), "transactions", bitmap.GetCardinality(), "out of", txNum)
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
	//var bn uint64
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
				log.Info(fmt.Sprintf("[%s] State reconstitution", s.LogPrefix()), "overall progress", fmt.Sprintf("%.2f%%", progress),
					"step progress", fmt.Sprintf("%.2f%%", stepProgress),
					"tx/s", fmt.Sprintf("%.1f", speedTx), "workCh", fmt.Sprintf("%d/%d", len(workCh), cap(workCh)),
					"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "queue.len", rs.QueueLen(), "blk", syncMetrics[stages.Execution].Get(),
					"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(commitThreshold)),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				if sizeEstimate >= commitThreshold {
					t := time.Now()
					if err := func() error {
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

	var err error // avoid declare global mutable variable
	for bn := startBlockNum; bn <= endBlockNum; bn++ {
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
		getHashFn := func(n uint64) common.Hash {
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
					Withdrawals:     b.Withdrawals(),
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
		syncMetrics[stages.Execution].Set(bn)
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

	plainStateCollector := etl.NewCollector(fmt.Sprintf("%s recon plainState", s.LogPrefix()), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector(fmt.Sprintf("%s recon code", s.LogPrefix()), dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector(fmt.Sprintf("%s recon plainContract", s.LogPrefix()), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
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
	logger log.Logger, agg *state2.AggregatorV3, engine consensus.Engine,
	chainConfig *chain.Config, genesis *core.Genesis) (err error) {
	startTime := time.Now()
	defer agg.EnableMadvNormal().DisableReadAhead()
	blockSnapshots := blockReader.(WithSnapshots).Snapshots()
	defer blockSnapshots.EnableReadAhead().DisableReadAhead()

	// force merge snapshots before reconstitution, to allign domains progress
	// un-finished merge can happen at "kill -9" during merge
	if err := agg.MergeLoop(ctx, estimate.CompressSnapshot.Workers()); err != nil {
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

	var ok bool
	var blockNum uint64 // First block which is not covered by the history snapshot files
	var txNum uint64
	if err := chainDb.View(ctx, func(tx kv.Tx) error {
		_, toTxNum := lastStep.TxNumRange()
		ok, blockNum, err = rawdbv3.TxNums.FindBlockNum(tx, toTxNum)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("blockNum for mininmaxTxNum=%d not found", toTxNum)
		}
		if blockNum == 0 {
			return fmt.Errorf("not enough transactions in the history data")
		}
		blockNum--
		txNum, err = rawdbv3.TxNums.Max(tx, blockNum)
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
		PageSize(uint64(8 * datasize.KB)).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).
		Open()
	if err != nil {
		return err
	}
	defer db.Close()
	defer os.RemoveAll(reconDbPath)

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
	plainStateCollector := etl.NewCollector(fmt.Sprintf("%s recon plainState", s.LogPrefix()), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector(fmt.Sprintf("%s recon code", s.LogPrefix()), dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector(fmt.Sprintf("%s recon plainContract", s.LogPrefix()), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainContractCollector.Close()

	fillWorker := exec3.NewFillWorker(txNum, aggSteps[len(aggSteps)-1])
	t := time.Now()
	fillWorker.FillAccounts(plainStateCollector)
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Filled accounts", s.LogPrefix()), "took", time.Since(t))
	}
	t = time.Now()
	fillWorker.FillStorage(plainStateCollector)
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Filled storage", s.LogPrefix()), "took", time.Since(t))
	}
	t = time.Now()
	fillWorker.FillCode(codeCollector, plainContractCollector)
	if time.Since(t) > 5*time.Second {
		log.Info(fmt.Sprintf("[%s] Filled code", s.LogPrefix()), "took", time.Since(t))
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
	log.Info(fmt.Sprintf("[%s] Reconstitution done", s.LogPrefix()), "in", time.Since(startTime))
	return nil
}
