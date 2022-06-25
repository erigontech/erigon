package commands

import (
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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(reconCmd)
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
	lock          sync.Locker
	wg            *sync.WaitGroup
	rs            *state.ReconState
	blockReader   services.FullBlockReader
	allSnapshots  *snapshotsync.RoSnapshots
	stateWriter   *state.StateReconWriter
	stateReader   *state.HistoryReaderNoState
	firstBlock    bool
	lastBlockNum  uint64
	lastBlockHash common.Hash
	lastHeader    *types.Header
	lastSigner    *types.Signer
	lastRules     *params.Rules
	getHeader     func(hash common.Hash, number uint64) *types.Header
	ctx           context.Context
	engine        consensus.Engine
	txNums        []uint64
	chainConfig   *params.ChainConfig
	logger        log.Logger
	genesis       *core.Genesis
}

func NewReconWorker(lock sync.Locker, wg *sync.WaitGroup, rs *state.ReconState,
	a *libstate.Aggregator, blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	txNums []uint64, chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis,
) *ReconWorker {
	return &ReconWorker{
		lock:         lock,
		wg:           wg,
		rs:           rs,
		blockReader:  blockReader,
		allSnapshots: allSnapshots,
		ctx:          context.Background(),
		stateWriter:  state.NewStateReconWriter(a, rs),
		stateReader:  state.NewHistoryReaderNoState(a, rs),
		txNums:       txNums,
		chainConfig:  chainConfig,
		logger:       logger,
		genesis:      genesis,
	}
}

func (rw *ReconWorker) SetTx(tx kv.Tx) {
	rw.stateReader.SetTx(tx)
}

func (rw *ReconWorker) run() {
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
	rw.wg.Done()
}

func (rw *ReconWorker) runTxNum(txNum uint64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txNum)
	rw.stateWriter.SetTxNum(txNum)
	noop := state.NewNoopWriter()
	// Find block number
	blockNum := uint64(sort.Search(len(rw.txNums), func(i int) bool {
		return rw.txNums[i] > txNum
	}))
	if rw.firstBlock || blockNum > rw.lastBlockNum {
		var err error
		if rw.lastHeader, err = rw.blockReader.HeaderByNumber(rw.ctx, nil, blockNum); err != nil {
			panic(err)
		}
		rw.lastBlockNum = blockNum
		rw.lastBlockHash = rw.lastHeader.Hash()
		rw.lastSigner = types.MakeSigner(rw.chainConfig, blockNum)
		rw.lastRules = rw.chainConfig.Rules(blockNum)
		rw.firstBlock = false
	}
	var startTxNum uint64
	if blockNum > 0 {
		startTxNum = rw.txNums[blockNum-1]
	}
	ibs := state.New(rw.stateReader)
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == blockNum && txNum == rw.txNums[blockNum-1]
	var err error
	if blockNum == 0 {
		fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txNum, blockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock()
		if err != nil {
			panic(err)
		}
	} else if daoForkTx {
		fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rw.lastRules, noop); err != nil {
			panic(err)
		}
	} else if txNum+1 == rw.txNums[blockNum] {
		fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txNum, blockNum)
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
		fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := rw.blockReader.TxnByIdxInBlock(rw.ctx, nil, blockNum, int(txIndex))
		if err != nil {
			panic(err)
		}
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*rw.lastSigner, rw.lastHeader.BaseFee, rw.lastRules)
		if err != nil {
			panic(err)
		}
		gp := new(core.GasPool).AddGas(msg.Gas())
		usedGas := new(uint64)
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, blockNum)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		ibs.Prepare(txHash, rw.lastBlockHash, int(txIndex))
		_, _, err = core.ApplyTransaction(rw.chainConfig, rw.getHeader, rw.engine, nil, gp, ibs, noop, rw.lastHeader, txn, usedGas, vmConfig, contractHasTEVM)
		if err != nil {
			panic(fmt.Errorf("could not apply tx %d [%x] failed: %w", txIndex, txHash, err))
		}
	}
	if err = ibs.CommitBlock(rw.lastRules, rw.stateWriter); err != nil {
		panic(err)
	}
	if err = ibs.Error(); err == nil {
		rw.rs.CommitTxNum(txNum)
	} else if rse, ok := err.(state.RequiredStateError); ok {
		rw.rs.RollbackTxNum(txNum, rse.StateTxNum)
	} else {
		panic(err)
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
	aggPath := filepath.Join(datadir, "erigon22")
	agg, err := libstate.NewAggregator(aggPath, AggregationStep)
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
	bitmap := agg.ReconBitmap(txNum)
	fmt.Printf("Bitmap length = %d\n", bitmap.GetCardinality())

	rs := state.NewReconState(bitmap)
	workerCount := 8
	var lock sync.RWMutex
	var wg sync.WaitGroup
	reconWorkers := make([]*ReconWorker, workerCount)
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
	for i := 0; i < workerCount; i++ {
		reconWorkers[i] = NewReconWorker(lock.RLocker(), &wg, rs, agg, blockReader, allSnapshots, txNums, chainConfig, logger, genesis)
		reconWorkers[i].SetTx(roTxs[i])
	}
	wg.Add(workerCount)
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	count := uint64(0)
	prevCommitCount := uint64(0)
	total := bitmap.GetCardinality()
	for i := 0; i < workerCount; i++ {
		go reconWorkers[i].run()
	}
	for count < total {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			libcommon.ReadMemStats(&m)

			count = rs.DoneCount()
			progress := 100.0 * float64(count) / float64(total)
			log.Info("State reconstitution", "workers", workerCount, "progress", fmt.Sprintf("%.2f%%", progress),
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
			)
			if prevCommitCount+1_000_000 <= count {
				prevCommitCount = count
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
	log.Info("Completed tx replay phase")
	rwTx, err = db.BeginRw(ctx)
	if err != nil {
		return err
	}
	count = 0
	accountsIt := agg.IterateAccountsHistory(txNum)
	for accountsIt.HasNext() {
		key, val := accountsIt.Next()
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
			if pos >= len(val) {
				fmt.Printf("panic ReadAccountData(%x)=>[%x]\n", key, val)
			}
			incBytes := int(val[pos])
			pos++
			if incBytes > 0 {
				a.Incarnation = bytesToUint64(val[pos : pos+incBytes])
			}
			value := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(value)
			if err = rwTx.Put(kv.PlainState, key, value); err != nil {
				return err
			}
			fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "accounts", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	count = 0
	storageIt := agg.IterateStorageHistory(txNum)
	for storageIt.HasNext() {
		key, val := storageIt.Next()
		if len(val) > 0 {
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(key[:20], state.FirstContractIncarnation, key[20:])
			if err = rwTx.Put(kv.PlainState, compositeKey, val); err != nil {
				return err
			}
			fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "storage", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	count = 0
	codeIt := agg.IterateCodeHistory(txNum)
	for codeIt.HasNext() {
		key, val := codeIt.Next()
		if len(val) > 0 {
			codeHash := crypto.Keccak256(val)
			if err = rwTx.Put(kv.Code, codeHash[:], val); err != nil {
				return err
			}
			compositeKey := dbutils.PlainGenerateStoragePrefix(key, state.FirstContractIncarnation)
			if err = rwTx.Put(kv.PlainContractCode, compositeKey, codeHash[:]); err != nil {
				return err
			}
			fmt.Printf("Code [%x] => [%x]\n", compositeKey, val)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "code", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	log.Info("Computing hashed state")
	tmpDir := filepath.Join(datadir, "tmp")
	if err = stagedsync.PromoteHashedStateCleanly("recon", rwTx, stagedsync.StageHashStateCfg(nil, tmpDir), make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	if _, err = stagedsync.RegenerateIntermediateHashes("recon", rwTx, stagedsync.StageTrieCfg(nil, false /* checkRoot */, false /* saveHashesToDB */, tmpDir, blockReader), common.Hash{}, make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	return nil
}
