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
		rw.runTxTask(txTask)
	}
}

func (rw *ReconWorker1) runTxTask(txTask state.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	txTask.Error = nil
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateWriter.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()
	rules := rw.chainConfig.Rules(txTask.BlockNum)
	ibs := state.New(rw.stateReader)
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == txTask.BlockNum && txTask.TxIndex == -1
	var err error
	if txTask.BlockNum == 0 && txTask.TxIndex == -1 {
		fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock()
		if err != nil {
			panic(err)
		}
	} else if daoForkTx {
		fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txTask.TxNum, txTask.BlockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.TxIndex == -1 {
		// Block initialisation
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
			// End of block transaction in a block
			if _, _, err := rw.engine.Finalize(rw.chainConfig, txTask.Header, ibs, txTask.Block.Transactions(), txTask.Block.Uncles(), nil /* receipts */, nil, nil, nil); err != nil {
				panic(fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err))
			}
		}
	} else {
		fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		txHash := txTask.Tx.Hash()
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, gas=%d, input=[%x]\n", txNum, blockNum, txIndex, txn.GetGas(), txn.GetData())
		usedGas := new(uint64)
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, txTask.BlockNum)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		ibs.Prepare(txHash, txTask.BlockHash, txTask.TxIndex)
		vmConfig.SkipAnalysis = core.SkipAnalysis(rw.chainConfig, txTask.BlockNum)
		blockContext := core.NewEVMBlockContext(txTask.Header, rw.getHeader, rw.engine, nil /* author */, contractHasTEVM)
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, ibs, rw.chainConfig, vmConfig)
		msg, err := txTask.Tx.AsMessage(*types.MakeSigner(rw.chainConfig, txTask.BlockNum), txTask.Header.BaseFee, rules)
		if err != nil {
			panic(err)
		}
		txContext := core.NewEVMTxContext(msg)

		// Update the evm with the new transaction context.
		vmenv.Reset(txContext, ibs)

		result, err := core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			txTask.Error = err
		}
		// Update the state with pending changes
		ibs.SoftFinalise()
		*usedGas += result.UsedGas
	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if txTask.Error == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		for addr, bal := range txTask.BalanceIncreaseSet {
			fmt.Printf("[%x]=>[%d]\n", addr, &bal)
		}
		if err = ibs.MakeWriteSet(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadKeys, txTask.ReadVals = rw.stateReader.ReadSet()
		txTask.WriteKeys, txTask.WriteVals = rw.stateWriter.WriteSet()
	}
	rw.resultCh <- txTask
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
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
	if err != nil {
		return err
	}
	startTime := time.Now()
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	var snapshotsPath string
	if snapdir != "" {
		snapshotsPath = snapdir
	} else {
		snapshotsPath = path.Join(datadir, "snapshots")
	}
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), snapshotsPath)
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
	rs := state.NewReconState1(workCh)
	var lock sync.RWMutex
	reconWorkers := make([]*ReconWorker1, workerCount)
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
	commitThreshold := uint64(256 * 1024 * 1024)
	count := uint64(0)
	rollbackCount := uint64(0)
	total := txNum
	prevCount := uint64(0)
	prevRollbackCount := uint64(0)
	prevTime := time.Now()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var rws state.TxTaskQueue
	heap.Init(&rws)
	var outputTxNum uint64
	// Go-routine gathering results from the workers
	go func() {
		for outputTxNum < txNum {
			select {
			case txTask := <-resultCh:
				if txTask.TxNum == outputTxNum {
					// Try to apply without placing on the queue first
					if txTask.Error == nil && rs.ReadsValid(txTask.ReadKeys, txTask.ReadVals) {
						rs.Apply(txTask.WriteKeys, txTask.WriteVals, txTask.BalanceIncreaseSet)
						rs.CommitTxNum(txTask.Sender, txTask.TxNum)
						outputTxNum++
					} else {
						rs.RollbackTx(txTask)
					}
				} else {
					heap.Push(&rws, txTask)
				}
				for rws.Len() > 0 && rws[0].TxNum == outputTxNum {
					txTask = heap.Pop(&rws).(state.TxTask)
					if txTask.Error == nil && rs.ReadsValid(txTask.ReadKeys, txTask.ReadVals) {
						rs.Apply(txTask.WriteKeys, txTask.WriteVals, txTask.BalanceIncreaseSet)
						rs.CommitTxNum(txTask.Sender, txTask.TxNum)
						outputTxNum++
					} else {
						rs.RollbackTx(txTask)
					}
				}
			case <-logEvery.C:
				var m runtime.MemStats
				libcommon.ReadMemStats(&m)
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
				log.Info("Transaction replay", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", progress), "tx/s", fmt.Sprintf("%.1f", speedTx), "repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "buffer", libcommon.ByteCount(sizeEstimate),
					"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
				)
				if sizeEstimate >= commitThreshold {
					commitStart := time.Now()
					log.Info("Committing...")
					err := func() error {
						lock.Lock()
						defer lock.Unlock()
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
	for blockNum := uint64(0); blockNum <= block; blockNum++ {
		header, err := blockReader.HeaderByNumber(ctx, nil, blockNum)
		if err != nil {
			panic(err)
		}
		blockHash := header.Hash()
		b, _, err := blockReader.BlockWithSenders(ctx, nil, blockHash, blockNum)
		if err != nil {
			panic(err)
		}
		txs := b.Transactions()
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			txTask := state.TxTask{
				Header:    header,
				BlockNum:  blockNum,
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
			}
			workCh <- txTask
			inputTxNum++
		}
	}
	close(workCh)
	wg.Wait()
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
	if _, err = stagedsync.RegenerateIntermediateHashes("recon", rwTx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, tmpDir, blockReader, nil /* HeaderDownload */), common.Hash{}, make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	return nil
}
