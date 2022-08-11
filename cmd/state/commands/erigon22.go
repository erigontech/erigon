package commands

import (
	"container/heap"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/state/state22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	datadir2 "github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

var (
	reset   bool
	workers int
)

func init() {
	erigon22Cmd.Flags().BoolVar(&reset, "reset", false, "Resets the state database and static files")
	erigon22Cmd.Flags().IntVar(&workers, "workers", 1, "Number of workers")
	withDataDir(erigon22Cmd)
	rootCmd.AddCommand(erigon22Cmd)
	withChain(erigon22Cmd)
}

var erigon22Cmd = &cobra.Command{
	Use:   "erigon22",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 histoty (ugrade 2)",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon22(genesis, logger)
	},
}

func Erigon22(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	var err error
	ctx := context.Background()
	tmpDir := filepath.Join(datadir, "tmp")
	reconDbPath := path.Join(datadir, "db22")
	if reset && dir.Exist(reconDbPath) {
		if err = os.RemoveAll(reconDbPath); err != nil {
			return err
		}
	}
	dir.MustExist(reconDbPath)
	limiter := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).RoTxsLimiter(limiter).Open()
	if err != nil {
		return err
	}
	chainDbPath := path.Join(datadir, "chaindata")
	chainDb, err := kv2.NewMDBX(logger).Path(chainDbPath).RoTxsLimiter(limiter).Readonly().Open()
	if err != nil {
		return err
	}
	startTime := time.Now()
	var blockReader services.FullBlockReader
	var allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	maxBlockNum := allSnapshots.BlocksAvailable() + 1
	txNums := make([]uint64, maxBlockNum)
	if err = allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err = b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}

	engine := initConsensusEngine(chainConfig, logger, allSnapshots)
	sentryControlServer, err := sentry.NewMultiClient(
		db,
		"",
		chainConfig,
		common.Hash{},
		engine,
		1,
		nil,
		ethconfig.Defaults.Sync,
		blockReader,
		false,
	)
	if err != nil {
		return err
	}
	cfg := ethconfig.Defaults
	cfg.DeprecatedTxPool.Disable = true
	cfg.Dirs = datadir2.New(datadir)
	cfg.Snapshot = allSnapshots.Cfg()
	stagedSync, err := stages2.NewStagedSync(context.Background(), logger, db, p2p.Config{}, &cfg, sentryControlServer, datadir, &stagedsync.Notifications{}, nil, allSnapshots, nil, nil)
	if err != nil {
		return err
	}
	var execStage *stagedsync.StageState
	if err := db.View(ctx, func(tx kv.Tx) error {
		execStage, err = stagedSync.StageState(stages.Execution, tx, db)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !reset {
		block = execStage.BlockNumber + 1
	}

	rs := state.NewState22()
	aggDir := path.Join(datadir, "agg22")
	if reset && dir.Exist(aggDir) {
		if err = os.RemoveAll(aggDir); err != nil {
			return err
		}
	}
	dir.MustExist(aggDir)
	agg, err := libstate.NewAggregator22(aggDir, AggregationStep)
	if err != nil {
		return err
	}
	defer agg.Close()
	var lock sync.RWMutex

	workerCount := workers
	queueSize := workerCount * 4
	var applyTx kv.RwTx
	var wg sync.WaitGroup
	reconWorkers, resultCh, clear := state22.NewWorkersPool(lock.RLocker(), db, chainDb, &wg, rs, blockReader, allSnapshots, txNums, chainConfig, logger, genesis, engine, workerCount)
	defer clear()
	if workerCount <= 1 {
		applyTx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if applyTx != nil {
				applyTx.Rollback()
			}
		}()
		reconWorkers[0].ResetTx(applyTx, nil)
		agg.SetTx(applyTx)
	}
	commitThreshold := uint64(1024 * 1024 * 1024)
	resultsThreshold := int64(1024 * 1024 * 1024)
	count := uint64(0)
	repeatCount := uint64(0)
	triggerCount := uint64(0)
	resultsSize := int64(0)
	progress := newProgress(block)
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var rws state.TxTaskQueue
	var rwsLock sync.Mutex
	rwsReceiveCond := sync.NewCond(&rwsLock)
	heap.Init(&rws)
	var outputTxNum uint64
	if block > 0 {
		outputTxNum = txNums[block-1]
	}
	var inputBlockNum, outputBlockNum uint64
	// Go-routine gathering results from the workers
	var maxTxNum = txNums[len(txNums)-1]
	if workerCount > 1 {
		go func() {
			defer func() {
				if applyTx != nil {
					applyTx.Rollback()
				}
			}()
			if applyTx, err = db.BeginRw(ctx); err != nil {
				panic(err)
			}
			agg.SetTx(applyTx)
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
						processResultQueue(&rws, &outputTxNum, rs, agg, applyTx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
						rwsReceiveCond.Signal()
					}()
				case <-logEvery.C:
					progress.log(rs, rws, inputBlockNum, outputBlockNum, repeatCount, uint64(atomic.LoadInt64(&resultsSize)))
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
								processResultQueue(&rws, &outputTxNum, rs, agg, applyTx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
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
							if err = applyTx.Commit(); err != nil {
								return err
							}
							for i := 0; i < len(reconWorkers); i++ {
								reconWorkers[i].ResetTx(nil, nil)
							}
							if err := db.Update(ctx, func(tx kv.RwTx) error { return rs.Flush(tx) }); err != nil {
								return err
							}
							if applyTx, err = db.BeginRw(ctx); err != nil {
								return err
							}
							agg.SetTx(applyTx)
							return nil
						}()
						if err != nil {
							panic(err)
						}
						log.Info("Committed", "time", time.Since(commitStart))
					}
				}
			}
			if err = applyTx.Commit(); err != nil {
				panic(err)
			}
		}()
	}
	var inputTxNum uint64
	if block > 0 {
		inputTxNum = txNums[block-1]
	}
	var header *types.Header
	var blockNum uint64
loop:
	for blockNum = block; blockNum < maxBlockNum; blockNum++ {
		atomic.StoreUint64(&inputBlockNum, blockNum)
		rules := chainConfig.Rules(blockNum)
		if header, err = blockReader.HeaderByNumber(ctx, nil, blockNum); err != nil {
			return err
		}
		blockHash := header.Hash()
		b, _, err := blockReader.BlockWithSenders(ctx, nil, blockHash, blockNum)
		if err != nil {
			return err
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			if workerCount > 1 {
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
				if workerCount > 1 {
					if ok := rs.RegisterSender(txTask); ok {
						rs.AddWork(txTask)
					}
				}
			} else if workerCount > 1 {
				rs.AddWork(txTask)
			}
			if workerCount == 1 {
				count++
				reconWorkers[0].RunTxTask(txTask)
				if txTask.Error == nil {
					if err := rs.Apply(txTask.Rules.IsSpuriousDragon, reconWorkers[0].Tx(), txTask, agg); err != nil {
						panic(err)
					}
					outputTxNum++
					outputBlockNum = txTask.BlockNum
					//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
				} else {
					return fmt.Errorf("Rolled back %d block %d txIndex %d, err = %v\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, txTask.Error)
				}
				select {
				case <-logEvery.C:
					progress.log(rs, rws, inputBlockNum, outputBlockNum, repeatCount, uint64(atomic.LoadInt64(&resultsSize)))
					sizeEstimate := rs.SizeEstimate()
					//prevTriggerCount = triggerCount
					if sizeEstimate >= commitThreshold {
						commitStart := time.Now()
						log.Info("Committing...")
						if err = applyTx.Commit(); err != nil {
							return err
						}
						reconWorkers[0].ResetTx(nil, nil)
						if err = db.Update(ctx, func(tx kv.RwTx) error { return rs.Flush(tx) }); err != nil {
							return err
						}
						if applyTx, err = db.BeginRw(ctx); err != nil {
							return err
						}
						agg.SetTx(applyTx)
						reconWorkers[0].ResetTx(applyTx, nil)
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
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next run will start with block %d", blockNum+1))
			atomic.StoreUint64(&maxTxNum, inputTxNum)
			break loop
		default:
		}
	}
	if workerCount > 1 {
		wg.Wait()
	} else {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}
	for i := 0; i < len(reconWorkers); i++ {
		reconWorkers[i].ResetTx(nil, nil)
	}
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if err = rs.Flush(tx); err != nil {
			return err
		}
		if err = execStage.Update(tx, blockNum); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		log.Info("Transaction replay complete", "duration", time.Since(startTime))
		log.Info("Computing hashed state")
		tmpDir := filepath.Join(datadir, "tmp")
		if err = tx.ClearBucket(kv.HashedAccounts); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.HashedStorage); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.ContractCode); err != nil {
			return err
		}
		if err = stagedsync.PromoteHashedStateCleanly("recon", tx, stagedsync.StageHashStateCfg(db, tmpDir), ctx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	var rootHash common.Hash
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if rootHash, err = stagedsync.RegenerateIntermediateHashes("recon", tx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, tmpDir, blockReader, nil /* HeaderDownload */), common.Hash{}, make(chan struct{}, 1)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if rootHash != header.Root {
		log.Error("Incorrect root hash", "expected", fmt.Sprintf("%x", header.Root))
	}
	return nil
}

func processResultQueue(rws *state.TxTaskQueue, outputTxNum *uint64, rs *state.State22, agg *libstate.Aggregator22, applyTx kv.Tx,
	triggerCount *uint64, outputBlockNum *uint64, repeatCount *uint64, resultsSize *int64) {
	for rws.Len() > 0 && (*rws)[0].TxNum == *outputTxNum {
		txTask := heap.Pop(rws).(*state.TxTask)
		atomic.AddInt64(resultsSize, -txTask.ResultsSize)
		if txTask.Error == nil && rs.ReadsValid(txTask.ReadLists) {
			if err := rs.Apply(txTask.Rules.IsSpuriousDragon, applyTx, txTask, agg); err != nil {
				panic(err)
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

type progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
}

func newProgress(prevOutputBlockNum uint64) *progress {
	return &progress{prevTime: time.Now(), prevOutputBlockNum: block}
}
func (p *progress) log(rs *state.State22, rws state.TxTaskQueue, inputBlockNum, outputBlockNum, repeatCount uint64, resultsSize uint64) {
	var m runtime.MemStats
	libcommon.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	count := rs.DoneCount()
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
		"at block", outputBlockNum,
		"input block", atomic.LoadUint64(&inputBlockNum),
		"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"result queue", rws.Len(),
		"results size", libcommon.ByteCount(resultsSize),
		"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio),
		"buffer", libcommon.ByteCount(sizeEstimate),
		"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
	)
	p.prevTime = currentTime
	p.prevCount = count
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}
