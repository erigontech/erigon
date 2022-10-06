package stagedsync

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	atomic2 "go.uber.org/atomic"
	"golang.org/x/sync/semaphore"
)

func NewProgress(prevOutputBlockNum, commitThreshold uint64) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum, commitThreshold: commitThreshold}
}

type Progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
	commitThreshold    uint64
}

func (p *Progress) Log(logPrefix string, rs *state.State22, rws state.TxTaskQueue, count, inputBlockNum, outputBlockNum, repeatCount uint64, resultsSize uint64, resultCh chan *state.TxTask) {
	var m runtime.MemStats
	common.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	speedTx := float64(count-p.prevCount) / (float64(interval) / float64(time.Second))
	speedBlock := float64(outputBlockNum-p.prevOutputBlockNum) / (float64(interval) / float64(time.Second))
	var repeatRatio float64
	if count > p.prevCount {
		repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(count-p.prevCount)
	}
	log.Info(fmt.Sprintf("[%s] Transaction replay", logPrefix),
		//"workers", workerCount,
		"at blk", outputBlockNum,
		"input blk", atomic.LoadUint64(&inputBlockNum),
		"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"resultCh", fmt.Sprintf("%d/%d", len(resultCh), cap(resultCh)),
		"resultQueue", rws.Len(),
		"resultsSize", common.ByteCount(resultsSize),
		"repeatRatio", fmt.Sprintf("%.2f%%", repeatRatio),
		"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(p.commitThreshold)),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	p.prevTime = currentTime
	p.prevCount = count
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

func Exec3(ctx context.Context,
	execStage *StageState, workerCount int, batchSize datasize.ByteSize, chainDb kv.RwDB, applyTx kv.RwTx,
	rs *state.State22, blockReader services.FullBlockReader,
	logger log.Logger, agg *state2.Aggregator22, engine consensus.Engine,
	maxBlockNum uint64, chainConfig *params.ChainConfig,
	genesis *core.Genesis,
) (err error) {
	parallel := workerCount > 1
	useExternalTx := applyTx != nil
	if !useExternalTx && !parallel {
		applyTx, err = chainDb.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer applyTx.Rollback()
	}
	if !useExternalTx {
		defer blockReader.(WithSnapshots).Snapshots().EnableMadvNormal().DisableReadAhead()
	}

	var block, stageProgress uint64
	var outputTxNum, maxTxNum = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var inputTxNum uint64
	var inputBlockNum, outputBlockNum = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var count uint64
	var repeatCount, triggerCount = atomic2.NewUint64(0), atomic2.NewUint64(0)
	var resultsSize = atomic2.NewInt64(0)
	var lock sync.RWMutex
	var rws state.TxTaskQueue
	var rwsLock sync.Mutex

	if execStage.BlockNumber > 0 {
		stageProgress = execStage.BlockNumber
		block = execStage.BlockNumber + 1
	}

	// erigon3 execution doesn't support power-off shutdown yet. it need to do quite a lot of work on exit
	// too keep consistency
	// will improve it in future versions
	interruptCh := ctx.Done()
	ctx = context.Background()
	queueSize := workerCount * 4
	var wg sync.WaitGroup
	reconWorkers, resultCh, clear := exec3.NewWorkersPool(lock.RLocker(), parallel, chainDb, &wg, rs, blockReader, chainConfig, logger, genesis, engine, workerCount)
	defer clear()
	if !parallel {
		reconWorkers[0].ResetTx(applyTx)
		agg.SetTx(applyTx)
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
			outputTxNum.Add(1)
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
				outputTxNum.Add(1)
				inputTxNum = outputTxNum.Load()
			}
			return nil
		}); err != nil {
			return err
		}
	}

	commitThreshold := batchSize.Bytes() * 4
	resultsThreshold := int64(batchSize.Bytes() * 4)
	progress := NewProgress(block, commitThreshold)
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	rwsReceiveCond := sync.NewCond(&rwsLock)
	heap.Init(&rws)
	agg.SetTxNum(inputTxNum)
	if parallel {
		// Go-routine gathering results from the workers
		go func() {
			tx, err := chainDb.BeginRw(ctx)
			if err != nil {
				panic(err)
			}
			defer tx.Rollback()
			agg.SetTx(tx)
			defer rs.Finish()
			for outputTxNum.Load() < maxTxNum.Load() {
				select {
				case txTask := <-resultCh:
					//fmt.Printf("Saved %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
					func() {
						rwsLock.Lock()
						defer rwsLock.Unlock()
						resultsSize.Add(txTask.ResultsSize)
						heap.Push(&rws, txTask)
						processResultQueue(&rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize)
						rwsReceiveCond.Signal()
					}()
				case <-logEvery.C:
					progress.Log(execStage.LogPrefix(), rs, rws, rs.DoneCount(), inputBlockNum.Load(), outputBlockNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh)
					sizeEstimate := rs.SizeEstimate()
					//prevTriggerCount = triggerCount
					if sizeEstimate >= commitThreshold {
						commitStart := time.Now()
						log.Info("Committing...")
						err := func() error {
							rwsLock.Lock()
							defer rwsLock.Unlock()
							// Drain results (and process) channel because read sets do not carry over
							for {
								var drained bool
								for !drained {
									select {
									case txTask := <-resultCh:
										resultsSize.Add(txTask.ResultsSize)
										heap.Push(&rws, txTask)
									default:
										drained = true
									}
								}
								processResultQueue(&rws, outputTxNum, rs, agg, tx, triggerCount, outputBlockNum, repeatCount, resultsSize)
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
								txTask := heap.Pop(&rws).(*state.TxTask)
								resultsSize.Add(-txTask.ResultsSize)
								rs.AddWork(txTask)
								syncMetrics[stages.Execution].Set(txTask.BlockNum)
							}
							if err := rs.Flush(tx); err != nil {
								return err
							}
							tx.CollectMetrics()
							if err = execStage.Update(tx, outputBlockNum.Load()); err != nil {
								return err
							}
							//TODO: can't commit - because we are in the middle of the block. Need make sure that we are always processed whole block.
							if err = tx.Commit(); err != nil {
								return err
							}
							if tx, err = chainDb.BeginRw(ctx); err != nil {
								return err
							}
							for i := 0; i < len(reconWorkers); i++ {
								reconWorkers[i].ResetTx(nil)
							}
							agg.SetTx(tx)
							return nil
						}()
						if err != nil {
							panic(err)
						}
						log.Info("Committed", "time", time.Since(commitStart))
					}
				}
			}
			if err = tx.Commit(); err != nil {
				panic(err)
			}
		}()
	}

	var b *types.Block
	var blockNum uint64
loop:
	for blockNum = block; blockNum <= maxBlockNum; blockNum++ {
		inputBlockNum.Store(blockNum)
		rules := chainConfig.Rules(blockNum)
		b, err = blockWithSenders(chainDb, applyTx, blockReader, blockNum)
		if err != nil {
			return err
		}
		if parallel {
			func() {
				rwsLock.Lock()
				defer rwsLock.Unlock()
				for rws.Len() > queueSize || resultsSize.Load() >= resultsThreshold || rs.SizeEstimate() >= commitThreshold {
					rwsReceiveCond.Wait()
				}
			}()
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:  blockNum,
				Rules:     rules,
				Block:     b,
				TxNum:     inputTxNum,
				TxIndex:   txIndex,
				BlockHash: b.Hash(),
				Final:     txIndex == len(txs),
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(*types.MakeSigner(chainConfig, txTask.BlockNum), txTask.Block.Header().BaseFee, txTask.Rules)
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
			if parallel {
				stageProgress = blockNum
			} else {
				count++
				reconWorkers[0].RunTxTask(txTask)
				if txTask.Error == nil {
					if err := rs.Apply(reconWorkers[0].Tx(), txTask, agg); err != nil {
						panic(fmt.Errorf("State22.Apply: %w", err))
					}
					outputTxNum.Add(1)
					outputBlockNum.Store(txTask.BlockNum)
					//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
				} else {
					return fmt.Errorf("rolled back %d block %d txIndex %d, err = %v", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, txTask.Error)
				}

				stageProgress = blockNum
			}

			inputTxNum++
		}

		if rs.SizeEstimate() >= commitThreshold {
			commitStart := time.Now()
			log.Info("Committing...")
			if err := rs.Flush(applyTx); err != nil {
				return err
			}
			if !useExternalTx {
				if err = execStage.Update(applyTx, stageProgress); err != nil {
					return err
				}
				applyTx.CollectMetrics()
				if err := applyTx.Commit(); err != nil {
					return err
				}
				if applyTx, err = chainDb.BeginRw(ctx); err != nil {
					return err
				}
				defer applyTx.Rollback()
				agg.SetTx(applyTx)
				reconWorkers[0].ResetTx(applyTx)
				log.Info("Committed", "time", time.Since(commitStart), "toProgress", stageProgress)
			}
		}

		// Check for interrupts
		select {
		case <-logEvery.C:
			progress.Log(execStage.LogPrefix(), rs, rws, count, inputBlockNum.Load(), outputBlockNum.Load(), repeatCount.Load(), uint64(resultsSize.Load()), resultCh)
		case <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next run will start with block %d", blockNum))
			maxTxNum.Store(inputTxNum)
			break loop
		default:
		}
	}
	if parallel {
		wg.Wait()
		if err = chainDb.Update(ctx, func(tx kv.RwTx) error {
			if err = rs.Flush(tx); err != nil {
				return err
			}
			if err = execStage.Update(tx, stageProgress); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	} else {
		if err = rs.Flush(applyTx); err != nil {
			return err
		}
		if err = execStage.Update(applyTx, stageProgress); err != nil {
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

func processResultQueue(rws *state.TxTaskQueue, outputTxNum *atomic2.Uint64, rs *state.State22, agg *state2.Aggregator22, applyTx kv.Tx,
	triggerCount, outputBlockNum, repeatCount *atomic2.Uint64, resultsSize *atomic2.Int64) {
	for rws.Len() > 0 && (*rws)[0].TxNum == outputTxNum.Load() {
		txTask := heap.Pop(rws).(*state.TxTask)
		resultsSize.Add(-txTask.ResultsSize)
		if txTask.Error == nil && rs.ReadsValid(txTask.ReadLists) {
			if err := rs.Apply(applyTx, txTask, agg); err != nil {
				panic(fmt.Errorf("State22.Apply: %w", err))
			}
			triggerCount.Add(rs.CommitTxNum(txTask.Sender, txTask.TxNum))
			outputTxNum.Add(1)
			outputBlockNum.Store(txTask.BlockNum)
			//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		} else {
			rs.AddWork(txTask)
			repeatCount.Add(1)
			//fmt.Printf("Rolled back %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		}
	}
}

func ReconstituteState(ctx context.Context, s *StageState, dirs datadir.Dirs, workerCount int, batchSize datasize.ByteSize, chainDb kv.RwDB,
	blockReader services.FullBlockReader,
	logger log.Logger, agg *state2.Aggregator22, engine consensus.Engine,
	chainConfig *params.ChainConfig, genesis *core.Genesis) (err error) {
	defer agg.EnableMadvNormal().DisableReadAhead()

	reconDbPath := filepath.Join(dirs.DataDir, "recondb")
	dir.Recreate(reconDbPath)
	limiterB := semaphore.NewWeighted(int64(runtime.NumCPU()*2 + 1))
	db, err := kv2.NewMDBX(log.New()).Path(reconDbPath).RoTxsLimiter(limiterB).
		WriteMergeThreshold(8192).
		PageSize(uint64(16 * datasize.KB)).
		WriteMap().WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).
		Open()
	if err != nil {
		return err
	}
	defer db.Close()
	defer os.RemoveAll(reconDbPath)

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
	fmt.Printf("Max blockNum = %d\n", blockNum)
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

	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	var wg sync.WaitGroup
	workCh := make(chan *state.TxTask, workerCount*64)
	rs := state.NewReconState(workCh)
	var fromKey, toKey []byte
	bigCount := big.NewInt(int64(workerCount))
	bigStep := big.NewInt(0x100000000)
	bigStep.Div(bigStep, bigCount)
	bigCurrent := big.NewInt(0)
	fillWorkers := make([]*exec3.FillWorker, workerCount)
	doneCount := atomic2.NewUint64(0)
	for i := 0; i < workerCount; i++ {
		fromKey = toKey
		if i == workerCount-1 {
			toKey = nil
		} else {
			bigCurrent.Add(bigCurrent, bigStep)
			toKey = make([]byte, 4)
			bigCurrent.FillBytes(toKey)
		}
		//fmt.Printf("%d) Fill worker [%x] - [%x]\n", i, fromKey, toKey)
		fillWorkers[i] = exec3.NewFillWorker(txNum, doneCount, agg, fromKey, toKey)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	doneCount.Store(0)
	accountCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		accountCollectorsX[i] = etl.NewCollector("account scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		accountCollectorsX[i].LogLvl(log.LvlDebug)
		go fillWorkers[i].BitmapAccounts(accountCollectorsX[i])
	}
	t := time.Now()
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Scan accounts history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	log.Info("Scan accounts history", "took", time.Since(t))

	accountCollectorX := etl.NewCollector("account scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize*2))
	defer accountCollectorX.Close()
	accountCollectorX.LogLvl(log.LvlDebug)
	for i := 0; i < workerCount; i++ {
		if err = accountCollectorsX[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return accountCollectorX.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		accountCollectorsX[i].Close()
		accountCollectorsX[i] = nil
	}
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		return accountCollectorX.Load(tx, kv.XAccount, etl.IdentityLoadFunc, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	accountCollectorX.Close()
	accountCollectorX = nil
	doneCount.Store(0)
	storageCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		storageCollectorsX[i] = etl.NewCollector("storage scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		storageCollectorsX[i].LogLvl(log.LvlDebug)
		go fillWorkers[i].BitmapStorage(storageCollectorsX[i])
	}
	t = time.Now()
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Scan storage history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	log.Info("Scan storage history", "took", time.Since(t))

	storageCollectorX := etl.NewCollector("storage scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize*2))
	defer storageCollectorX.Close()
	storageCollectorX.LogLvl(log.LvlDebug)
	for i := 0; i < workerCount; i++ {
		if err = storageCollectorsX[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return storageCollectorX.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		storageCollectorsX[i].Close()
		storageCollectorsX[i] = nil
	}
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		return storageCollectorX.Load(tx, kv.XStorage, etl.IdentityLoadFunc, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	storageCollectorX.Close()
	storageCollectorX = nil
	doneCount.Store(0)
	codeCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		codeCollectorsX[i] = etl.NewCollector("code scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		codeCollectorsX[i].LogLvl(log.LvlDebug)
		go fillWorkers[i].BitmapCode(codeCollectorsX[i])
	}
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Scan code history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	codeCollectorX := etl.NewCollector("code scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize*2))
	defer codeCollectorX.Close()
	codeCollectorX.LogLvl(log.LvlDebug)
	var bitmap roaring64.Bitmap
	for i := 0; i < workerCount; i++ {
		bitmap.Or(fillWorkers[i].Bitmap())
		if err = codeCollectorsX[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return codeCollectorX.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		codeCollectorsX[i].Close()
		codeCollectorsX[i] = nil
	}
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		return codeCollectorX.Load(tx, kv.XCode, etl.IdentityLoadFunc, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	codeCollectorX.Close()
	codeCollectorX = nil
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
		reconWorkers[i] = exec3.NewReconWorker(lock.RLocker(), &wg, rs, agg, blockReader, chainConfig, logger, genesis, engine, chainTxs[i])
		reconWorkers[i].SetTx(roTxs[i])
	}
	wg.Add(workerCount)
	count := uint64(0)
	rollbackCount := uint64(0)
	total := bitmap.GetCardinality()
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].Run()
	}
	commitThreshold := batchSize.Bytes() * 4
	prevCount := uint64(0)
	prevRollbackCount := uint64(0)
	prevTime := time.Now()
	reconDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-reconDone:
				return
			case <-logEvery.C:
				var m runtime.MemStats
				common.ReadMemStats(&m)
				sizeEstimate := rs.SizeEstimate()
				count = rs.DoneCount()
				rollbackCount = rs.RollbackCount()
				currentTime := time.Now()
				interval := currentTime.Sub(prevTime)
				speedTx := float64(count-prevCount) / (float64(interval) / float64(time.Second))
				progress := 100.0 * float64(count) / float64(total)
				var repeatRatio float64
				if count > prevCount {
					repeatRatio = 100.0 * float64(rollbackCount-prevRollbackCount) / float64(count-prevCount)
				}
				prevTime = currentTime
				prevCount = count
				prevRollbackCount = rollbackCount
				log.Info("State reconstitution", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", progress),
					"tx/s", fmt.Sprintf("%.1f", speedTx), "workCh", fmt.Sprintf("%d/%d", len(workCh), cap(workCh)),
					"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio),
					"buffer", fmt.Sprintf("%s/%s", common.ByteCount(sizeEstimate), common.ByteCount(commitThreshold)),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				if sizeEstimate >= commitThreshold {
					err := func() error {
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
					}()
					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	defer blockReader.(WithSnapshots).Snapshots().EnableReadAhead().DisableReadAhead()

	var inputTxNum uint64
	var b *types.Block
	var txKey [8]byte
	for bn := uint64(0); bn <= blockNum; bn++ {
		rules := chainConfig.Rules(bn)
		b, err = blockWithSenders(chainDb, nil, blockReader, blockNum)
		if err != nil {
			return err
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			if bitmap.Contains(inputTxNum) {
				binary.BigEndian.PutUint64(txKey[:], inputTxNum)
				txTask := &state.TxTask{
					BlockNum:  bn,
					Block:     b,
					Rules:     rules,
					TxNum:     inputTxNum,
					TxIndex:   txIndex,
					BlockHash: b.Hash(),
					Final:     txIndex == len(txs),
				}
				if txIndex >= 0 && txIndex < len(txs) {
					txTask.Tx = txs[txIndex]
					txTask.TxAsMessage, err = txTask.Tx.AsMessage(*types.MakeSigner(chainConfig, txTask.BlockNum), txTask.Block.Header().BaseFee, txTask.Rules)
					if err != nil {
						panic(err)
					}
				}
				workCh <- txTask
			}
			inputTxNum++
		}
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
	roTx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	if err = roTx.ForEach(kv.PlainStateR, nil, func(k, v []byte) error {
		return plainStateCollector.Collect(k[8:], v)
	}); err != nil {
		return err
	}
	if err = roTx.ForEach(kv.CodeR, nil, func(k, v []byte) error {
		return codeCollector.Collect(k[8:], v)
	}); err != nil {
		return err
	}
	if err = roTx.ForEach(kv.PlainContractR, nil, func(k, v []byte) error {
		return plainContractCollector.Collect(k[8:], v)
	}); err != nil {
		return err
	}
	roTx.Rollback()

	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if err = tx.ClearBucket(kv.PlainStateR); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.CodeR); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.PlainContractR); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	plainStateCollectors := make([]*etl.Collector, workerCount)
	codeCollectors := make([]*etl.Collector, workerCount)
	plainContractCollectors := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		plainStateCollectors[i] = etl.NewCollector(fmt.Sprintf("plainState %d", i), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer plainStateCollectors[i].Close()
		codeCollectors[i] = etl.NewCollector(fmt.Sprintf("code %d", i), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer codeCollectors[i].Close()
		plainContractCollectors[i] = etl.NewCollector(fmt.Sprintf("plainContract %d", i), dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer plainContractCollectors[i].Close()
	}
	doneCount.Store(0)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillAccounts(plainStateCollectors[i])
	}
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Filling accounts", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	doneCount.Store(0)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillStorage(plainStateCollectors[i])
	}
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Filling storage", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	doneCount.Store(0)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillCode(codeCollectors[i], plainContractCollectors[i])
	}
	for doneCount.Load() < uint64(workerCount) {
		<-logEvery.C
		var m runtime.MemStats
		common.ReadMemStats(&m)
		var p float64
		for i := 0; i < workerCount; i++ {
			if total := fillWorkers[i].Total(); total > 0 {
				p += float64(fillWorkers[i].Progress()) / float64(total)
			}
		}
		p *= 100.0
		log.Info("Filling code", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
			"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		)
	}
	// Load all collections into the main collector
	for i := 0; i < workerCount; i++ {
		if err = plainStateCollectors[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return plainStateCollector.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		plainStateCollectors[i].Close()
		if err = codeCollectors[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return codeCollector.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		codeCollectors[i].Close()
		if err = plainContractCollectors[i].Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return plainContractCollector.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		plainContractCollectors[i].Close()
	}
	if err = chainDb.Update(ctx, func(tx kv.RwTx) error {
		if err = tx.ClearBucket(kv.PlainState); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.Code); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.PlainContractCode); err != nil {
			return err
		}
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
	return nil
}
