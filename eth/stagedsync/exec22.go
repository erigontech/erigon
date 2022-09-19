package stagedsync

import (
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

func NewProgress(prevOutputBlockNum uint64) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum}
}

type Progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
}

func (p *Progress) Log(rs *state.State22, rws state.TxTaskQueue, count, inputBlockNum, outputBlockNum, repeatCount uint64, resultsSize uint64) {
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
	log.Info("Transaction replay",
		//"workers", workerCount,
		"at blk", outputBlockNum,
		"input blk", atomic.LoadUint64(&inputBlockNum),
		"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"result queue", rws.Len(),
		"results size", common.ByteCount(resultsSize),
		"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio),
		"buffer", common.ByteCount(sizeEstimate),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	p.prevTime = currentTime
	p.prevCount = count
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}

func Exec22(ctx context.Context,
	execStage *StageState, workerCount int, chainDb kv.RwDB, applyTx kv.RwTx,
	rs *state.State22, blockReader services.FullBlockReader,
	allSnapshots *snapshotsync.RoSnapshots,
	logger log.Logger, agg *state2.Aggregator22, engine consensus.Engine,
	maxBlockNum uint64, chainConfig *params.ChainConfig,
	genesis *core.Genesis, initialCycle bool,
) (err error) {

	var block, stageProgress uint64
	var outputTxNum, inputTxNum, maxTxNum uint64
	var count, repeatCount, triggerCount uint64
	var resultsSize int64
	var inputBlockNum, outputBlockNum uint64
	var lock sync.RWMutex
	var rws state.TxTaskQueue
	var rwsLock sync.Mutex

	if execStage.BlockNumber > 0 {
		stageProgress = execStage.BlockNumber
		block = execStage.BlockNumber + 1
	}

	// erigon22 execution doesn't support power-off shutdown yet. it need to do quite a lot of work on exit
	// too keep consistency
	// will improve it in future versions
	interruptCh := ctx.Done()
	ctx = context.Background()
	parallel := workerCount > 1
	queueSize := workerCount * 4
	var wg sync.WaitGroup
	reconWorkers, resultCh, clear := exec22.NewWorkersPool(lock.RLocker(), parallel, chainDb, &wg, rs, blockReader, allSnapshots, chainConfig, logger, genesis, engine, workerCount)
	defer clear()
	if !parallel {
		reconWorkers[0].ResetTx(applyTx)
		agg.SetTx(applyTx)
		maxTxNum, err = rawdb.TxNums.Max(applyTx, maxBlockNum)
		if err != nil {
			return err
		}
		if block > 0 {
			outputTxNum, err = rawdb.TxNums.Max(applyTx, execStage.BlockNumber)
			if err != nil {
				return err
			}
			inputTxNum = outputTxNum
		}
	} else {
		if err := chainDb.View(ctx, func(tx kv.Tx) error {
			maxTxNum, err = rawdb.TxNums.Max(tx, maxBlockNum)
			if err != nil {
				return err
			}
			if block > 0 {
				outputTxNum, err = rawdb.TxNums.Max(tx, execStage.BlockNumber)
				if err != nil {
					return err
				}
				inputTxNum = outputTxNum
			}
			return nil
		}); err != nil {
			return err
		}
	}
	commitThreshold := uint64(1024 * 1024 * 1024)
	resultsThreshold := int64(1024 * 1024 * 1024)
	progress := NewProgress(block)
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
			for atomic.LoadUint64(&outputTxNum) < atomic.LoadUint64(&maxTxNum) {
				select {
				case txTask := <-resultCh:
					//fmt.Printf("Saved %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
					func() {
						rwsLock.Lock()
						defer rwsLock.Unlock()
						atomic.AddInt64(&resultsSize, txTask.ResultsSize)
						heap.Push(&rws, txTask)
						processResultQueue(&rws, &outputTxNum, rs, agg, tx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
						rwsReceiveCond.Signal()
					}()
				case <-logEvery.C:
					progress.Log(rs, rws, rs.DoneCount(), inputBlockNum, outputBlockNum, repeatCount, uint64(atomic.LoadInt64(&resultsSize)))
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
										atomic.AddInt64(&resultsSize, txTask.ResultsSize)
										heap.Push(&rws, txTask)
									default:
										drained = true
									}
								}
								processResultQueue(&rws, &outputTxNum, rs, agg, tx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
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
								atomic.AddInt64(&resultsSize, -txTask.ResultsSize)
								rs.AddWork(txTask)
							}
							if err := rs.Flush(tx); err != nil {
								return err
							}
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

	var header *types.Header
	var blockNum uint64
loop:
	for blockNum = block; blockNum <= maxBlockNum; blockNum++ {
		atomic.StoreUint64(&inputBlockNum, blockNum)
		rules := chainConfig.Rules(blockNum)
		if header, err = blockReader.HeaderByNumber(ctx, applyTx, blockNum); err != nil {
			return err
		}
		blockHash := header.Hash()
		b, _, err := blockReader.BlockWithSenders(ctx, applyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			if parallel {
				func() {
					rwsLock.Lock()
					defer rwsLock.Unlock()
					for rws.Len() > queueSize || atomic.LoadInt64(&resultsSize) >= resultsThreshold || rs.SizeEstimate() >= commitThreshold {
						rwsReceiveCond.Wait()
					}
				}()
			}
			txTask := &state.TxTask{
				Header:    header,
				BlockNum:  blockNum,
				Rules:     rules,
				Block:     b,
				TxNum:     inputTxNum,
				TxIndex:   txIndex,
				BlockHash: blockHash,
				Final:     txIndex == len(txs),
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
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
					outputTxNum++
					outputBlockNum = txTask.BlockNum
					//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
				} else {
					return fmt.Errorf("rolled back %d block %d txIndex %d, err = %v", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, txTask.Error)
				}

				stageProgress = blockNum
				select {
				case <-logEvery.C:
					progress.Log(rs, rws, count, inputBlockNum, outputBlockNum, repeatCount, uint64(atomic.LoadInt64(&resultsSize)))
					sizeEstimate := rs.SizeEstimate()
					//prevTriggerCount = triggerCount
					if sizeEstimate >= commitThreshold {
						commitStart := time.Now()
						log.Info("Committing...")
						if err := rs.Flush(applyTx); err != nil {
							return err
						}
						if !initialCycle {
							if err = execStage.Update(applyTx, stageProgress); err != nil {
								return err
							}
							if err := applyTx.Commit(); err != nil {
								return err
							}
							if applyTx, err = chainDb.BeginRw(ctx); err != nil {
								return err
							}
							agg.SetTx(applyTx)
							reconWorkers[0].ResetTx(applyTx)
						}
						log.Info("Committed", "time", time.Since(commitStart))
					}
				default:
				}
			}
			inputTxNum++
		}
		// Check for interrupts
		select {
		case <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next run will start with block %d", blockNum))
			atomic.StoreUint64(&maxTxNum, inputTxNum)
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

	return nil
}

func processResultQueue(rws *state.TxTaskQueue, outputTxNum *uint64, rs *state.State22, agg *state2.Aggregator22, applyTx kv.Tx,
	triggerCount *uint64, outputBlockNum *uint64, repeatCount *uint64, resultsSize *int64) {
	for rws.Len() > 0 && (*rws)[0].TxNum == *outputTxNum {
		txTask := heap.Pop(rws).(*state.TxTask)
		atomic.AddInt64(resultsSize, -txTask.ResultsSize)
		if txTask.Error == nil && rs.ReadsValid(txTask.ReadLists) {
			if err := rs.Apply(applyTx, txTask, agg); err != nil {
				panic(fmt.Errorf("State22.Apply: %w", err))
			}
			*triggerCount += rs.CommitTxNum(txTask.Sender, txTask.TxNum)
			atomic.AddUint64(outputTxNum, 1)
			atomic.StoreUint64(outputBlockNum, txTask.BlockNum)
			//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		} else {
			rs.AddWork(txTask)
			*repeatCount++
			//fmt.Printf("Rolled back %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		}
	}
}

func Recon22(ctx context.Context, s *StageState, dirs datadir.Dirs, workerCount int, chainDb kv.RwDB, db kv.RwDB,
	blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	logger log.Logger, agg *state2.Aggregator22, engine consensus.Engine,
	chainConfig *params.ChainConfig, genesis *core.Genesis) (err error) {

	var ok bool
	var blockNum uint64
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
		return nil
	}); err != nil {
		return err
	}

	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	var wg sync.WaitGroup
	workCh := make(chan *state.TxTask, 128)
	rs := state.NewReconState(workCh)
	var fromKey, toKey []byte
	bigCount := big.NewInt(int64(workerCount))
	bigStep := big.NewInt(0x100000000)
	bigStep.Div(bigStep, bigCount)
	bigCurrent := big.NewInt(0)
	fillWorkers := make([]*exec22.FillWorker, workerCount)
	var doneCount uint64
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
		fillWorkers[i] = exec22.NewFillWorker(txNum, &doneCount, agg, fromKey, toKey)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	doneCount = 0
	accountCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		accountCollectorsX[i] = etl.NewCollector("account scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		go fillWorkers[i].BitmapAccounts(accountCollectorsX[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
	accountCollectorX := etl.NewCollector("account scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer accountCollectorX.Close()
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
		return accountCollectorX.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return tx.Put(kv.XAccount, k, v)
		}, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	accountCollectorX.Close()
	accountCollectorX = nil
	doneCount = 0
	storageCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		storageCollectorsX[i] = etl.NewCollector("storage scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		go fillWorkers[i].BitmapStorage(storageCollectorsX[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
	storageCollectorX := etl.NewCollector("storage scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer storageCollectorX.Close()
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
		return storageCollectorX.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return tx.Put(kv.XStorage, k, v)
		}, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	storageCollectorX.Close()
	storageCollectorX = nil
	doneCount = 0
	codeCollectorsX := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		codeCollectorsX[i] = etl.NewCollector("code scan X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
		go fillWorkers[i].BitmapCode(codeCollectorsX[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
	codeCollectorX := etl.NewCollector("code scan total X", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer codeCollectorX.Close()
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
		return codeCollectorX.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return tx.Put(kv.XCode, k, v)
		}, etl.TransformArgs{})
	}); err != nil {
		return err
	}
	codeCollectorX.Close()
	codeCollectorX = nil
	log.Info("Ready to replay", "transactions", bitmap.GetCardinality(), "out of", txNum)
	var lock sync.RWMutex
	reconWorkers := make([]*exec22.ReconWorker, workerCount)
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
		reconWorkers[i] = exec22.NewReconWorker(lock.RLocker(), &wg, rs, agg, blockReader, allSnapshots, chainConfig, logger, genesis, engine, chainTxs[i])
		reconWorkers[i].SetTx(roTxs[i])
	}
	wg.Add(workerCount)
	count := uint64(0)
	rollbackCount := uint64(0)
	total := bitmap.GetCardinality()
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].Run()
	}
	commitThreshold := uint64(256 * 1024 * 1024)
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
				log.Info("State reconstitution", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", progress), "tx/s", fmt.Sprintf("%.1f", speedTx), "repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "buffer", common.ByteCount(sizeEstimate),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
				)
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
	var inputTxNum uint64
	var header *types.Header
	var txKey [8]byte
	for bn := uint64(0); bn <= blockNum; bn++ {
		if header, err = blockReader.HeaderByNumber(ctx, nil, bn); err != nil {
			panic(err)
		}
		blockHash := header.Hash()
		b, _, err := blockReader.BlockWithSenders(ctx, nil, blockHash, bn)
		if err != nil {
			panic(err)
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			if bitmap.Contains(inputTxNum) {
				binary.BigEndian.PutUint64(txKey[:], inputTxNum)
				txTask := &state.TxTask{
					Header:    header,
					BlockNum:  bn,
					Block:     b,
					TxNum:     inputTxNum,
					TxIndex:   txIndex,
					BlockHash: blockHash,
					Final:     txIndex == len(txs),
				}
				if txIndex >= 0 && txIndex < len(txs) {
					txTask.Tx = txs[txIndex]
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
	cursor, err := roTx.Cursor(kv.PlainStateR)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var k, v []byte
	for k, v, err = cursor.First(); err == nil && k != nil; k, v, err = cursor.Next() {
		if err = plainStateCollector.Collect(k[8:], v); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	cursor.Close()
	if cursor, err = roTx.Cursor(kv.CodeR); err != nil {
		return err
	}
	defer cursor.Close()
	for k, v, err = cursor.First(); err == nil && k != nil; k, v, err = cursor.Next() {
		if err = codeCollector.Collect(k[8:], v); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	cursor.Close()
	if cursor, err = roTx.Cursor(kv.PlainContractR); err != nil {
		return err
	}
	defer cursor.Close()
	for k, v, err = cursor.First(); err == nil && k != nil; k, v, err = cursor.Next() {
		if err = plainContractCollector.Collect(k[8:], v); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	cursor.Close()
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
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillAccounts(plainStateCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillStorage(plainStateCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].FillCode(codeCollectors[i], plainContractCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
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
