package commands

import (
	"container/heap"
	"context"
	"errors"
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
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

func init() {
	withBlock(recon1Cmd)
	withDataDir(recon1Cmd)
	rootCmd.AddCommand(recon1Cmd)
}

var recon1Cmd = &cobra.Command{
	Use:   "recon1",
	Short: "Exerimental command to reconstitute the state at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon1(genesis, logger)
	},
}

type ReconWorker1 struct {
	lock         sync.Locker
	wg           *sync.WaitGroup
	rs           *state.ReconState1
	blockReader  services.FullBlockReader
	allSnapshots *snapshotsync.RoSnapshots
	stateWriter  *state.StateReconWriter1
	stateReader  *state.StateReconReader1
	getHeader    func(hash common.Hash, number uint64) *types.Header
	ctx          context.Context
	engine       consensus.Engine
	txNums       []uint64
	chainConfig  *params.ChainConfig
	logger       log.Logger
	genesis      *core.Genesis
	resultCh     chan state.TxTask
}

func NewReconWorker1(lock sync.Locker, wg *sync.WaitGroup, rs *state.ReconState1,
	blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	txNums []uint64, chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis,
	resultCh chan state.TxTask,
) *ReconWorker1 {
	return &ReconWorker1{
		lock:         lock,
		wg:           wg,
		rs:           rs,
		blockReader:  blockReader,
		allSnapshots: allSnapshots,
		ctx:          context.Background(),
		stateWriter:  state.NewStateReconWriter1(rs),
		stateReader:  state.NewStateReconReader1(rs),
		txNums:       txNums,
		chainConfig:  chainConfig,
		logger:       logger,
		genesis:      genesis,
		resultCh:     resultCh,
	}
}

func (rw *ReconWorker1) SetTx(tx kv.Tx) {
	rw.stateReader.SetTx(tx)
}

func (rw *ReconWorker1) run() {
	defer rw.wg.Done()
	rw.getHeader = func(hash common.Hash, number uint64) *types.Header {
		h, err := rw.blockReader.Header(rw.ctx, nil, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	rw.engine = initConsensusEngine(rw.chainConfig, rw.logger, rw.allSnapshots)
	for txTask, ok := rw.rs.Schedule(); ok; txTask, ok = rw.rs.Schedule() {
		rw.runTxTask(&txTask)
		rw.resultCh <- txTask // Needs to have outside of the lock
	}
}

func (rw *ReconWorker1) runTxTask(txTask *state.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	txTask.Error = nil
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateWriter.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()
	ibs := state.New(rw.stateReader)
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == txTask.BlockNum && txTask.TxIndex == -1
	var err error
	if txTask.BlockNum == 0 && txTask.TxIndex == -1 {
		//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock()
		if err != nil {
			panic(err)
		}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txTask.TxNum, txTask.BlockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.TxIndex == -1 {
		// Block initialisation
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
			// End of block transaction in a block
			if _, _, err := rw.engine.Finalize(rw.chainConfig, txTask.Header, ibs, txTask.Block.Transactions(), txTask.Block.Uncles(), nil /* receipts */, nil, nil, nil); err != nil {
				panic(fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err))
			}
		}
	} else {
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		txHash := txTask.Tx.Hash()
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, txTask.BlockNum)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		ibs.Prepare(txHash, txTask.BlockHash, txTask.TxIndex)
		getHashFn := core.GetHashFn(txTask.Header, rw.getHeader)
		blockContext := core.NewEVMBlockContext(txTask.Header, getHashFn, rw.engine, nil /* author */, contractHasTEVM)
		msg, err := txTask.Tx.AsMessage(*types.MakeSigner(rw.chainConfig, txTask.BlockNum), txTask.Header.BaseFee, txTask.Rules)
		if err != nil {
			panic(err)
		}
		txContext := core.NewEVMTxContext(msg)
		vmenv := vm.NewEVM(blockContext, txContext, ibs, rw.chainConfig, vmConfig)
		if _, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */); err != nil {
			txTask.Error = err
			//fmt.Printf("error=%v\n", err)
		}
		// Update the state with pending changes
		ibs.SoftFinalise()
	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if txTask.Error == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		//for addr, bal := range txTask.BalanceIncreaseSet {
		//	fmt.Printf("[%x]=>[%d]\n", addr, &bal)
		//}
		if err = ibs.MakeWriteSet(txTask.Rules, rw.stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadKeys, txTask.ReadVals = rw.stateReader.ReadSet()
		txTask.WriteKeys, txTask.WriteVals = rw.stateWriter.WriteSet()
		size := (20 + 32) * len(txTask.BalanceIncreaseSet)
		for _, bb := range txTask.ReadKeys {
			for _, b := range bb {
				size += len(b)
			}
		}
		for _, bb := range txTask.ReadVals {
			for _, b := range bb {
				size += len(b)
			}
		}
		for _, bb := range txTask.WriteKeys {
			for _, b := range bb {
				size += len(b)
			}
		}
		for _, bb := range txTask.WriteVals {
			for _, b := range bb {
				size += len(b)
			}
		}
		txTask.ResultsSize = int64(size)
	}
}

func processResultQueue(rws *state.TxTaskQueue, outputTxNum *uint64, rs *state.ReconState1, applyTx kv.Tx,
	triggerCount *uint64, outputBlockNum *uint64, repeatCount *uint64, resultsSize *int64) {
	for rws.Len() > 0 && (*rws)[0].TxNum == *outputTxNum {
		txTask := heap.Pop(rws).(state.TxTask)
		atomic.AddInt64(resultsSize, -txTask.ResultsSize)
		if txTask.Error == nil && rs.ReadsValid(txTask.ReadKeys, txTask.ReadVals) {
			if err := rs.Apply(txTask.Rules.IsSpuriousDragon, applyTx, txTask.WriteKeys, txTask.WriteVals, txTask.BalanceIncreaseSet); err != nil {
				panic(err)
			}
			*triggerCount += rs.CommitTxNum(txTask.Sender, txTask.TxNum)
			*outputTxNum++
			*outputBlockNum = txTask.BlockNum
			//fmt.Printf("Applied %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		} else {
			rs.AddWork(txTask)
			*repeatCount++
			//fmt.Printf("Rolled back %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		}
	}
}

func Recon1(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	ctx := context.Background()
	reconDbPath := path.Join(datadir, "recon1db")
	var err error
	if _, err = os.Stat(reconDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err = os.RemoveAll(reconDbPath); err != nil {
		return err
	}
	limiter := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).RoTxsLimiter(limiter).Open()
	if err != nil {
		return err
	}
	startTime := time.Now()
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := make([]uint64, allSnapshots.BlocksAvailable()+1)
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
	blockNum := block + 1
	txNum := txNums[blockNum-1]
	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	workerCount := runtime.NumCPU()
	workCh := make(chan state.TxTask, 128)
	rs := state.NewReconState1()
	var lock sync.RWMutex
	reconWorkers := make([]*ReconWorker1, workerCount)
	var applyTx kv.Tx
	defer func() {
		if applyTx != nil {
			applyTx.Rollback()
		}
	}()
	if applyTx, err = db.BeginRo(ctx); err != nil {
		return err
	}
	roTxs := make([]kv.Tx, workerCount)
	defer func() {
		for i := 0; i < workerCount; i++ {
			if roTxs[i] != nil {
				roTxs[i].Rollback()
			}
		}
	}()
	for i := 0; i < workerCount; i++ {
		roTxs[i], err = db.BeginRo(ctx)
		if err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	resultCh := make(chan state.TxTask, 128)
	for i := 0; i < workerCount; i++ {
		reconWorkers[i] = NewReconWorker1(lock.RLocker(), &wg, rs, blockReader, allSnapshots, txNums, chainConfig, logger, genesis, resultCh)
		reconWorkers[i].SetTx(roTxs[i])
	}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].run()
	}
	commitThreshold := uint64(1024 * 1024 * 1024)
	resultsThreshold := int64(1024 * 1024 * 1024)
	count := uint64(0)
	repeatCount := uint64(0)
	triggerCount := uint64(0)
	total := txNum
	prevCount := uint64(0)
	prevRepeatCount := uint64(0)
	//prevTriggerCount := uint64(0)
	resultsSize := int64(0)
	prevTime := time.Now()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var rws state.TxTaskQueue
	var rwsLock sync.Mutex
	rwsReceiveCond := sync.NewCond(&rwsLock)
	heap.Init(&rws)
	var outputTxNum uint64
	var inputBlockNum, outputBlockNum uint64
	var prevOutputBlockNum uint64
	// Go-routine gathering results from the workers
	go func() {
		defer rs.Finish()
		for outputTxNum < txNum {
			select {
			case txTask := <-resultCh:
				//fmt.Printf("Saved %d block %d txIndex %d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
				func() {
					rwsLock.Lock()
					defer rwsLock.Unlock()
					atomic.AddInt64(&resultsSize, txTask.ResultsSize)
					heap.Push(&rws, txTask)
					processResultQueue(&rws, &outputTxNum, rs, applyTx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
					rwsReceiveCond.Signal()
				}()
			case <-logEvery.C:
				var m runtime.MemStats
				libcommon.ReadMemStats(&m)
				sizeEstimate := rs.SizeEstimate()
				count = rs.DoneCount()
				currentTime := time.Now()
				interval := currentTime.Sub(prevTime)
				speedTx := float64(count-prevCount) / (float64(interval) / float64(time.Second))
				speedBlock := float64(outputBlockNum-prevOutputBlockNum) / (float64(interval) / float64(time.Second))
				progress := 100.0 * float64(count) / float64(total)
				var repeatRatio float64
				if count > prevCount {
					repeatRatio = 100.0 * float64(repeatCount-prevRepeatCount) / float64(count-prevCount)
				}
				log.Info("Transaction replay",
					//"workers", workerCount,
					"at block", outputBlockNum,
					"input block", atomic.LoadUint64(&inputBlockNum),
					"progress", fmt.Sprintf("%.2f%%", progress),
					"blk/s", fmt.Sprintf("%.1f", speedBlock),
					"tx/s", fmt.Sprintf("%.1f", speedTx),
					//"repeats", repeatCount-prevRepeatCount,
					//"triggered", triggerCount-prevTriggerCount,
					"result queue", rws.Len(),
					"results size", libcommon.ByteCount(uint64(atomic.LoadInt64(&resultsSize))),
					"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio),
					"buffer", libcommon.ByteCount(sizeEstimate),
					"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
				)
				prevTime = currentTime
				prevCount = count
				prevOutputBlockNum = outputBlockNum
				prevRepeatCount = repeatCount
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
							processResultQueue(&rws, &outputTxNum, rs, applyTx, &triggerCount, &outputBlockNum, &repeatCount, &resultsSize)
							if rws.Len() == 0 {
								break
							}
						}
						rwsReceiveCond.Signal()
						lock.Lock()
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
							txTask := heap.Pop(&rws).(state.TxTask)
							atomic.AddInt64(&resultsSize, -txTask.ResultsSize)
							rs.AddWork(txTask)
						}
						applyTx.Rollback()
						for i := 0; i < workerCount; i++ {
							roTxs[i].Rollback()
						}
						rwTx, err := db.BeginRw(ctx)
						if err != nil {
							return err
						}
						if err = rs.Flush(rwTx); err != nil {
							return err
						}
						if err = rwTx.Commit(); err != nil {
							return err
						}
						if applyTx, err = db.BeginRo(ctx); err != nil {
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
					log.Info("Committed", "time", time.Since(commitStart))
				}
			}
		}
	}()
	var inputTxNum uint64
	var header *types.Header
	for blockNum := uint64(0); blockNum <= block; blockNum++ {
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
			func() {
				rwsLock.Lock()
				defer rwsLock.Unlock()
				for rws.Len() > 128 || atomic.LoadInt64(&resultsSize) >= resultsThreshold || rs.SizeEstimate() >= commitThreshold {
					rwsReceiveCond.Wait()
				}
			}()
			txTask := state.TxTask{
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
				if ok := rs.RegisterSender(txTask); ok {
					rs.AddWork(txTask)
				}
			} else {
				rs.AddWork(txTask)
			}
			inputTxNum++
		}
	}
	close(workCh)
	wg.Wait()
	applyTx.Rollback()
	for i := 0; i < workerCount; i++ {
		roTxs[i].Rollback()
	}
	rwTx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if err = rs.Flush(rwTx); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	log.Info("Transaction replay complete", "duration", time.Since(startTime))
	log.Info("Computing hashed state")
	tmpDir := filepath.Join(datadir, "tmp")
	if err = stagedsync.PromoteHashedStateCleanly("recon", rwTx, stagedsync.StageHashStateCfg(db, tmpDir), ctx); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	var rootHash common.Hash
	if rootHash, err = stagedsync.RegenerateIntermediateHashes("recon", rwTx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, tmpDir, blockReader, nil /* HeaderDownload */), common.Hash{}, make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rootHash != header.Root {
		log.Error("Incorrect root hash", "expected", fmt.Sprintf("%x", header.Root))
	}
	return nil
}
