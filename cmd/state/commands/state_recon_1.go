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
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/holiman/uint256"
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
	lock          sync.Locker
	wg            *sync.WaitGroup
	rs            *state.ReconState1
	blockReader   services.FullBlockReader
	allSnapshots  *snapshotsync.RoSnapshots
	stateWriter   *state.StateReconWriter1
	stateReader   *state.StateReconReader1
	firstBlock    bool
	lastBlockNum  uint64
	lastBlockHash common.Hash
	lastHeader    *types.Header
	lastRules     *params.Rules
	getHeader     func(hash common.Hash, number uint64) *types.Header
	ctx           context.Context
	engine        consensus.Engine
	txNums        []uint64
	chainConfig   *params.ChainConfig
	logger        log.Logger
	genesis       *core.Genesis
	resultCh      chan ReadWriteSet
}

func NewReconWorker1(lock sync.Locker, wg *sync.WaitGroup, rs *state.ReconState1,
	blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	txNums []uint64, chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis,
	resultCh chan ReadWriteSet,
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
	rw.firstBlock = true
	rw.getHeader = func(hash common.Hash, number uint64) *types.Header {
		h, err := rw.blockReader.Header(rw.ctx, nil, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	rw.engine = initConsensusEngine(rw.chainConfig, rw.logger, rw.allSnapshots)
	for txNum, ok := rw.rs.Schedule(); ok; txNum, ok = rw.rs.Schedule() {
		rw.runTxNum(txNum)
	}
}

func (rw *ReconWorker1) runTxNum(txNum uint64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txNum)
	rw.stateWriter.SetTxNum(txNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()
	noop := state.NewNoopWriter()
	// Find block number
	blockNum := uint64(sort.Search(len(rw.txNums), func(i int) bool {
		return rw.txNums[i] > txNum
	}))
	if rw.firstBlock || blockNum != rw.lastBlockNum {
		var err error
		if rw.lastHeader, err = rw.blockReader.HeaderByNumber(rw.ctx, nil, blockNum); err != nil {
			panic(err)
		}
		rw.lastBlockNum = blockNum
		rw.lastBlockHash = rw.lastHeader.Hash()
		rw.lastRules = rw.chainConfig.Rules(blockNum)
		rw.firstBlock = false
	}
	var txSender *common.Address
	var startTxNum uint64
	if blockNum > 0 {
		startTxNum = rw.txNums[blockNum-1]
	}
	ibs := state.New(rw.stateReader)
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == blockNum && txNum == rw.txNums[blockNum-1]
	var err error
	if blockNum == 0 {
		//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txNum, blockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock()
		if err != nil {
			panic(err)
		}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rw.lastRules, noop); err != nil {
			panic(err)
		}
	} else if txNum+1 == rw.txNums[blockNum] {
		//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txNum, blockNum)
		// End of block transaction in a block
		block, _, err := rw.blockReader.BlockWithSenders(rw.ctx, nil, rw.lastBlockHash, blockNum)
		if err != nil {
			panic(err)
		}
		if _, _, err := rw.engine.Finalize(rw.chainConfig, rw.lastHeader, ibs, block.Transactions(), block.Uncles(), nil /* receipts */, nil, nil, nil); err != nil {
			panic(fmt.Errorf("finalize of block %d failed: %w", blockNum, err))
		}
	} else {
		txIndex := txNum - startTxNum - 1
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := rw.blockReader.TxnByIdxInBlock(rw.ctx, nil, blockNum, int(txIndex))
		if err != nil {
			panic(err)
		}
		sender, ok := txn.GetSender()
		if !ok {
			panic(fmt.Sprintf("could not find sender for txHash=%x", txn.Hash()))
		}
		if !rw.rs.RegisterSender(sender, txNum) {
			return
		}
		txSender = &sender
		txHash := txn.Hash()
		gp := new(core.GasPool).AddGas(txn.GetGas())
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, gas=%d, input=[%x]\n", txNum, blockNum, txIndex, txn.GetGas(), txn.GetData())
		usedGas := new(uint64)
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, blockNum)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		ibs.Prepare(txHash, rw.lastBlockHash, int(txIndex))
		_, _, err = core.ApplyTransaction(rw.chainConfig, rw.getHeader, rw.engine, nil, gp, ibs, noop, rw.lastHeader, txn, usedGas, vmConfig, contractHasTEVM)
		if err != nil {
			panic(fmt.Errorf("could not apply tx %d [%x] failed: %w", txIndex, txHash, err))
		}
	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	balanceIncreaseSet := ibs.BalanceIncreaseSet()
	if err = ibs.MakeWriteSet(rw.lastRules, rw.stateWriter); err != nil {
		panic(err)
	}
	readKeys, readVals := rw.stateReader.ReadSet()
	writeKeys, writeVals := rw.stateWriter.WriteSet()
	rw.resultCh <- ReadWriteSet{
		txNum:              txNum,
		sender:             txSender,
		balanceIncreaseSet: balanceIncreaseSet,
		readKeys:           readKeys,
		readVals:           readVals,
		writeKeys:          writeKeys,
		writeVals:          writeVals,
	}
}

// ReadWriteSet contains ReadSet, WriteSet and BalanceIncrease of a transaction,
// which is processed by a single thread that writes into the ReconState1 and
// flushes to the database
type ReadWriteSet struct {
	txNum              uint64
	sender             *common.Address
	balanceIncreaseSet map[common.Address]uint256.Int
	readKeys           map[string][][]byte
	readVals           map[string][][]byte
	writeKeys          map[string][][]byte
	writeVals          map[string][][]byte
}

type ReadWriteSetHeap []ReadWriteSet

func (h ReadWriteSetHeap) Len() int {
	return len(h)
}

func (h ReadWriteSetHeap) Less(i, j int) bool {
	return h[i].txNum < h[j].txNum
}

func (h ReadWriteSetHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ReadWriteSetHeap) Push(a interface{}) {
	*h = append(*h, a.(ReadWriteSet))
}

func (h *ReadWriteSetHeap) Pop() interface{} {
	c := *h
	*h = c[:len(c)-1]
	return c[len(c)-1]
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
	var wg sync.WaitGroup
	rs := state.NewReconState1(txNum)
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
	resultCh := make(chan ReadWriteSet, 128)
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
	reconDone := make(chan struct{})
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var rws ReadWriteSetHeap
	heap.Init(&rws)
	var nextTxNum uint64 // Next txNum to accept
	go func() {
		for {
			select {
			case <-reconDone:
				return
			case readWriteSet := <-resultCh:
				if readWriteSet.txNum == nextTxNum {
					// Try to apply without placing on the queue first
					if rs.ReadsValid(readWriteSet.readKeys, readWriteSet.readVals) {
						rs.Apply(readWriteSet.writeKeys, readWriteSet.writeVals, readWriteSet.balanceIncreaseSet)
						rs.CommitTxNum(readWriteSet.sender, readWriteSet.txNum)
						nextTxNum++
					} else {
						rs.RollbackTxNum(readWriteSet.txNum)
					}
				} else {
					heap.Push(&rws, readWriteSet)
				}
				for rws.Len() > 0 && rws[0].txNum == nextTxNum {
					readWriteSet = heap.Pop(&rws).(ReadWriteSet)
					if rs.ReadsValid(readWriteSet.readKeys, readWriteSet.readVals) {
						rs.Apply(readWriteSet.writeKeys, readWriteSet.writeVals, readWriteSet.balanceIncreaseSet)
						rs.CommitTxNum(readWriteSet.sender, readWriteSet.txNum)
						nextTxNum++
					} else {
						rs.RollbackTxNum(readWriteSet.txNum)
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
				}
			}
		}
	}()
	wg.Wait()
	reconDone <- struct{}{} // Complete logging and committing go-routine
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
	if _, err = stagedsync.RegenerateIntermediateHashes("recon", rwTx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, tmpDir, blockReader), common.Hash{}, make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	return nil
}
