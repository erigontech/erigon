package commands

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/state22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
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
	withBlock(reconCmd)
	withChain(reconCmd)
	withDataDir(reconCmd)
	rootCmd.AddCommand(reconCmd)
}

var reconCmd = &cobra.Command{
	Use:   "recon",
	Short: "Exerimental command to reconstitute the state from state history at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon(genesis, logger)
	},
}

type ReconWorker struct {
	lock         sync.Locker
	wg           *sync.WaitGroup
	rs           *state.ReconState
	blockReader  services.FullBlockReader
	allSnapshots *snapshotsync.RoSnapshots
	stateWriter  *state.StateReconWriter
	stateReader  *state.HistoryReaderNoState
	getHeader    func(hash common.Hash, number uint64) *types.Header
	ctx          context.Context
	engine       consensus.Engine
	chainConfig  *params.ChainConfig
	logger       log.Logger
	genesis      *core.Genesis
	epoch        state22.EpochReader
	chain        state22.ChainReader
	isPoSA       bool
	posa         consensus.PoSA
}

func NewReconWorker(lock sync.Locker, wg *sync.WaitGroup, rs *state.ReconState,
	a *libstate.Aggregator22, blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis, engine consensus.Engine,
	chainTx kv.Tx,
) *ReconWorker {
	ac := a.MakeContext()
	rw := &ReconWorker{
		lock:         lock,
		wg:           wg,
		rs:           rs,
		blockReader:  blockReader,
		allSnapshots: allSnapshots,
		ctx:          context.Background(),
		stateWriter:  state.NewStateReconWriter(ac, rs),
		stateReader:  state.NewHistoryReaderNoState(ac, rs),
		chainConfig:  chainConfig,
		logger:       logger,
		genesis:      genesis,
		engine:       engine,
	}
	rw.epoch = state22.NewEpochReader(chainTx)
	rw.chain = state22.NewChainReader(chainConfig, chainTx, blockReader)
	rw.posa, rw.isPoSA = engine.(consensus.PoSA)
	return rw
}

func (rw *ReconWorker) SetTx(tx kv.Tx) {
	rw.stateReader.SetTx(tx)
}

func (rw *ReconWorker) run() {
	defer rw.wg.Done()
	rw.getHeader = func(hash common.Hash, number uint64) *types.Header {
		h, err := rw.blockReader.Header(rw.ctx, nil, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	for txTask, ok := rw.rs.Schedule(); ok; txTask, ok = rw.rs.Schedule() {
		rw.runTxTask(txTask)
	}
}

func (rw *ReconWorker) runTxTask(txTask *state.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetError()
	rw.stateWriter.SetTxNum(txTask.TxNum)
	noop := state.NewNoopWriter()
	rules := rw.chainConfig.Rules(txTask.BlockNum)
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
		// For Genesis, rules should be empty, so that empty accounts can be included
		rules = &params.Rules{}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txNum, blockNum)
			// End of block transaction in a block
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine)
			}
			if _, _, err := rw.engine.Finalize(rw.chainConfig, txTask.Header, ibs, txTask.Block.Transactions(), txTask.Block.Uncles(), nil /* receipts */, rw.epoch, rw.chain, syscall); err != nil {
				panic(fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err))
			}
		}
	} else if txTask.TxIndex == -1 {
		// Block initialisation
		if rw.isPoSA {
			systemcontracts.UpgradeBuildInSystemContract(chainConfig, txTask.Header.Number, ibs)
		}
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine)
		}
		rw.engine.Initialize(rw.chainConfig, rw.chain, rw.epoch, txTask.Header, txTask.Block.Transactions(), txTask.Block.Uncles(), syscall)
	} else {
		if rw.isPoSA {
			if isSystemTx, err := rw.posa.IsSystemTransaction(txTask.Tx, txTask.Header); err != nil {
				panic(err)
			} else if isSystemTx {
				return
			}
		}
		txHash := txTask.Tx.Hash()
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, txTask.BlockNum)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		getHashFn := core.GetHashFn(txTask.Header, rw.getHeader)
		blockContext := core.NewEVMBlockContext(txTask.Header, getHashFn, rw.engine, nil /* author */, contractHasTEVM)
		ibs.Prepare(txHash, txTask.BlockHash, txTask.TxIndex)
		msg, err := txTask.Tx.AsMessage(*types.MakeSigner(rw.chainConfig, txTask.BlockNum), txTask.Header.BaseFee, rules)
		if err != nil {
			panic(err)
		}
		txContext := core.NewEVMTxContext(msg)
		vmenv := vm.NewEVM(blockContext, txContext, ibs, rw.chainConfig, vmConfig)
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, evm=%p\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, vmenv)
		_, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			panic(fmt.Errorf("could not apply tx %d [%x] failed: %w", txTask.TxIndex, txHash, err))
		}
		if err = ibs.FinalizeTx(rules, noop); err != nil {
			panic(err)
		}
	}
	if dependency, ok := rw.stateReader.ReadError(); ok {
		//fmt.Printf("rollback %d\n", txNum)
		rw.rs.RollbackTx(txTask, dependency)
	} else {
		if err = ibs.CommitBlock(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		//fmt.Printf("commit %d\n", txNum)
		rw.rs.CommitTxNum(txTask.TxNum)
	}
}

type FillWorker struct {
	txNum          uint64
	doneCount      *uint64
	ac             *libstate.Aggregator22Context
	fromKey, toKey []byte
	currentKey     []byte
	bitmap         roaring64.Bitmap
	total          uint64
	progress       uint64
}

func NewFillWorker(txNum uint64, doneCount *uint64, a *libstate.Aggregator22, fromKey, toKey []byte) *FillWorker {
	fw := &FillWorker{
		txNum:     txNum,
		doneCount: doneCount,
		ac:        a.MakeContext(),
		fromKey:   fromKey,
		toKey:     toKey,
	}
	return fw
}

func (fw *FillWorker) Total() uint64 {
	return atomic.LoadUint64(&fw.total)
}

func (fw *FillWorker) Progress() uint64 {
	return atomic.LoadUint64(&fw.progress)
}

func (fw *FillWorker) fillAccounts(plainStateCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateAccountsHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.currentKey = key
		if len(val) > 0 {
			var a accounts.Account
			a.Reset()
			pos := 0
			nonceBytes := int(val[pos])
			pos++
			if nonceBytes > 0 {
				a.Nonce = bytesToUint64(val[pos : pos+nonceBytes])
				pos += nonceBytes
			}
			balanceBytes := int(val[pos])
			pos++
			if balanceBytes > 0 {
				a.Balance.SetBytes(val[pos : pos+balanceBytes])
				pos += balanceBytes
			}
			codeHashBytes := int(val[pos])
			pos++
			if codeHashBytes > 0 {
				copy(a.CodeHash[:], val[pos:pos+codeHashBytes])
				pos += codeHashBytes
			}
			incBytes := int(val[pos])
			pos++
			if incBytes > 0 {
				a.Incarnation = bytesToUint64(val[pos : pos+incBytes])
			}
			if a.Incarnation > 0 {
				a.Incarnation = state.FirstContractIncarnation
			}
			value := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(value)
			if err := plainStateCollector.Collect(key, value); err != nil {
				panic(err)
			}
			//fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
		}
	}
}

func (fw *FillWorker) fillStorage(plainStateCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateStorageHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.currentKey = key
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(key[:20], state.FirstContractIncarnation, key[20:])
		if len(val) > 0 {
			if err := plainStateCollector.Collect(compositeKey, val); err != nil {
				panic(err)
			}
			//fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
		}
	}
}

func (fw *FillWorker) fillCode(codeCollector, plainContractCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateCodeHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.currentKey = key
		compositeKey := dbutils.PlainGenerateStoragePrefix(key, state.FirstContractIncarnation)
		if len(val) > 0 {
			codeHash, err := common.HashData(val)
			if err != nil {
				panic(err)
			}
			if err = codeCollector.Collect(codeHash[:], val); err != nil {
				panic(err)
			}
			if err = plainContractCollector.Collect(compositeKey, codeHash[:]); err != nil {
				panic(err)
			}
			//fmt.Printf("Code [%x] => %d\n", compositeKey, len(val))
		}
	}
}

func (fw *FillWorker) ResetProgress() {
	fw.total = 0
	fw.progress = 0
}

func (fw *FillWorker) bitmapAccounts() {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateAccountsReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		txNum, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
	}
}

func (fw *FillWorker) bitmapStorage() {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateStorageReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		txNum, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
	}
}

func (fw *FillWorker) bitmapCode() {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateCodeReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		txNum, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
	}
}

func Recon(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	ctx := context.Background()
	aggPath := filepath.Join(datadir, "agg22")
	agg, err := libstate.NewAggregator22(aggPath, AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	reconDbPath := path.Join(datadir, "recondb")
	if _, err = os.Stat(reconDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err = os.RemoveAll(reconDbPath); err != nil {
		return err
	}
	startTime := time.Now()
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
	if err != nil {
		return err
	}
	limiter := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
	chainDbPath := path.Join(datadir, "chaindata")
	chainDb, err := kv2.NewMDBX(logger).Path(chainDbPath).RoTxsLimiter(limiter).Readonly().Open()
	if err != nil {
		return err
	}
	var blockReader services.FullBlockReader
	allSnapshots := snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
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
	endTxNumMinimax := agg.EndTxNumMinimax()
	fmt.Printf("Max txNum in files: %d\n", endTxNumMinimax)
	blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
		return txNums[i] > endTxNumMinimax
	}))
	if blockNum == uint64(len(txNums)) {
		return fmt.Errorf("mininmax txNum not found in snapshot blocks: %d", endTxNumMinimax)
	}
	if blockNum == 0 {
		return fmt.Errorf("not enough transactions in the history data")
	}
	if block+1 > blockNum {
		return fmt.Errorf("specified block %d which is higher than available %d", block, blockNum)
	}
	fmt.Printf("Max blockNum = %d\n", blockNum)
	blockNum = block + 1
	txNum := txNums[blockNum-1]
	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup
	workCh := make(chan *state.TxTask, 128)
	rs := state.NewReconState(workCh)
	var fromKey, toKey []byte
	bigCount := big.NewInt(int64(workerCount))
	bigStep := big.NewInt(0x100000000)
	bigStep.Div(bigStep, bigCount)
	bigCurrent := big.NewInt(0)
	fillWorkers := make([]*FillWorker, workerCount)
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
		fillWorkers[i] = NewFillWorker(txNum, &doneCount, agg, fromKey, toKey)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].bitmapAccounts()
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Scan accounts history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
	}
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].bitmapStorage()
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Scan storage history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
	}
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].bitmapCode()
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Scan code history", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
	}
	var bitmap roaring64.Bitmap
	for i := 0; i < workerCount; i++ {
		bitmap.Or(&fillWorkers[i].bitmap)
	}
	log.Info("Ready to replay", "transactions", bitmap.GetCardinality(), "out of", txNum)
	var lock sync.RWMutex
	reconWorkers := make([]*ReconWorker, workerCount)
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
	engine := initConsensusEngine(chainConfig, logger, allSnapshots)
	for i := 0; i < workerCount; i++ {
		reconWorkers[i] = NewReconWorker(lock.RLocker(), &wg, rs, agg, blockReader, allSnapshots, chainConfig, logger, genesis, engine, chainTxs[i])
		reconWorkers[i].SetTx(roTxs[i])
	}
	wg.Add(workerCount)
	count := uint64(0)
	rollbackCount := uint64(0)
	total := bitmap.GetCardinality()
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].run()
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
				log.Info("State reconstitution", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", progress), "tx/s", fmt.Sprintf("%.1f", speedTx), "repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio), "buffer", libcommon.ByteCount(sizeEstimate),
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
	for bn := uint64(0); bn < blockNum; bn++ {
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
	plainStateCollector := etl.NewCollector("recon plainState", datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer plainStateCollector.Close()
	codeCollector := etl.NewCollector("recon code", datadir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer codeCollector.Close()
	plainContractCollector := etl.NewCollector("recon plainContract", datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
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
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	if err = rwTx.ClearBucket(kv.PlainStateR); err != nil {
		return err
	}
	if err = rwTx.ClearBucket(kv.CodeR); err != nil {
		return err
	}
	if err = rwTx.ClearBucket(kv.PlainContractR); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	plainStateCollectors := make([]*etl.Collector, workerCount)
	codeCollectors := make([]*etl.Collector, workerCount)
	plainContractCollectors := make([]*etl.Collector, workerCount)
	for i := 0; i < workerCount; i++ {
		plainStateCollectors[i] = etl.NewCollector(fmt.Sprintf("plainState %d", i), datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer plainStateCollectors[i].Close()
		codeCollectors[i] = etl.NewCollector(fmt.Sprintf("code %d", i), datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer codeCollectors[i].Close()
		plainContractCollectors[i] = etl.NewCollector(fmt.Sprintf("plainContract %d", i), datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer plainContractCollectors[i].Close()
	}
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].fillAccounts(plainStateCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Filling accounts", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
	}
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].fillStorage(plainStateCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Filling storage", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
	}
	doneCount = 0
	for i := 0; i < workerCount; i++ {
		fillWorkers[i].ResetProgress()
		go fillWorkers[i].fillCode(codeCollectors[i], plainContractCollectors[i])
	}
	for atomic.LoadUint64(&doneCount) < uint64(workerCount) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)
			var p float64
			for i := 0; i < workerCount; i++ {
				if total := fillWorkers[i].Total(); total > 0 {
					p += float64(fillWorkers[i].Progress()) / float64(total)
				}
			}
			p *= 100.0
			log.Info("Filling code", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", p),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
		}
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
	rwTx, err = db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if err = plainStateCollector.Load(rwTx, kv.PlainState, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	plainStateCollector.Close()
	if err = codeCollector.Load(rwTx, kv.Code, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	codeCollector.Close()
	if err = plainContractCollector.Load(rwTx, kv.PlainContractCode, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	plainContractCollector.Close()
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	log.Info("Reconstitution complete", "duration", time.Since(startTime))
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
