package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debugprint"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
)

var stateStags = &cobra.Command{
	Use: "state_stages",
	Short: `Run all StateStages (which happen after senders) in loop.
Examples: 
--unwind=1 --unwind.every=10  # 10 blocks forward, 1 block back, 10 blocks forward, ...
--unwind=10 --unwind.every=1  # 1 block forward, 10 blocks back, 1 blocks forward, ...
--unwind=10  # 10 blocks back, then stop
--integrity.fast=false --integrity.slow=false # Performs DB integrity checks each step. You can disable slow or fast checks.
--block # Stop at exact blocks
--chaindata.reference # When finish all cycles, does comparison to this db file.
		`,
	Example: "go run ./cmd/integration state_stages --datadir=... --verbosity=3 --unwind=100 --unwind.every=100000 --block=2000000",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		cfg := &node.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)
		miningConfig := params.MiningConfig{}
		utils.SetupMinerCobra(cmd, &miningConfig)
		db := openDB(path.Join(cfg.DataDir, "erigon", "chaindata"), true)
		defer db.Close()

		//c := cmd.Flags().String(utils.ChainFlag.Name, utils.ChainFlag.Value, utils.ChainFlag.Usage)
		//if c != nil {
		//	chain = *c
		//}
		if err := syncBySmallSteps(db, miningConfig, ctx); err != nil {
			log.Error("Error", "err", err)
			return nil
		}

		if referenceChaindata != "" {
			if err := compareStates(ctx, chaindata, referenceChaindata); err != nil {
				log.Error(err.Error())
				return nil
			}
		}
		return nil
	},
}

var loopIhCmd = &cobra.Command{
	Use: "loop_ih",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
		defer db.Close()

		if unwind == 0 {
			unwind = 1
		}
		if err := loopIh(db, ctx, unwind); err != nil {
			log.Error("Error", "err", err)
			return err
		}

		return nil
	},
}

var loopExecCmd = &cobra.Command{
	Use: "loop_exec",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
		defer db.Close()
		if unwind == 0 {
			unwind = 1
		}
		if err := loopExec(db, ctx, unwind); err != nil {
			log.Error("Error", "err", err)
			return nil
		}

		return nil
	},
}

func init() {
	withDatadir2(stateStags)
	withReferenceChaindata(stateStags)
	withUnwind(stateStags)
	withUnwindEvery(stateStags)
	withBlock(stateStags)
	withIntegrityChecks(stateStags)
	withMining(stateStags)
	withChain(stateStags)

	rootCmd.AddCommand(stateStags)

	withDatadir(loopIhCmd)
	withBatchSize(loopIhCmd)
	withUnwind(loopIhCmd)
	withChain(loopIhCmd)

	rootCmd.AddCommand(loopIhCmd)

	withDatadir(loopExecCmd)
	withBatchSize(loopExecCmd)
	withUnwind(loopExecCmd)
	withChain(loopExecCmd)

	rootCmd.AddCommand(loopExecCmd)
}

func syncBySmallSteps(db ethdb.RwKV, miningConfig params.MiningConfig, ctx context.Context) error {
	sm, engine, chainConfig, vmConfig, txPool, stateStages, mining, _, miningResultCh := newSync(ctx, db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tmpDir := path.Join(datadir, etl.TmpDirName)
	must(clearUnwindStack(tx, ctx))
	quit := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	expectedAccountChanges := make(map[uint64]*changeset.ChangeSet)
	expectedStorageChanges := make(map[uint64]*changeset.ChangeSet)
	changeSetHook := func(blockNum uint64, csw *state.ChangeSetWriter) {
		accountChanges, err := csw.GetAccountChanges()
		if err != nil {
			panic(err)
		}
		expectedAccountChanges[blockNum] = accountChanges

		storageChanges, err := csw.GetStorageChanges()
		if err != nil {
			panic(err)
		}
		if storageChanges.Len() > 0 {
			expectedStorageChanges[blockNum] = storageChanges
		}
	}

	stateStages.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders,
		stages.CreateHeadersSnapshot,
		stages.CreateBodiesSnapshot,
		stages.CreateStateSnapshot,
		stages.TxPool, // TODO: enable TxPool stage
		stages.Finish)

	execCfg := stagedsync.StageExecuteBlocksCfg(db, sm.Receipts, sm.CallTraces, sm.TEVM, 0, batchSize, changeSetHook, chainConfig, engine, vmConfig, tmpDir)

	execUntilFunc := func(execToBlock uint64) func(stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx ethdb.RwTx) error {
		return func(s *stagedsync.StageState, unwinder stagedsync.Unwinder, tx ethdb.RwTx) error {
			if err := stagedsync.SpawnExecuteBlocksStage(s, unwinder, tx, execToBlock, quit, execCfg, nil); err != nil {
				return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
			}
			return nil
		}
	}
	senderAtBlock := progress(tx, stages.Senders)
	execAtBlock := progress(tx, stages.Execution)

	var stopAt = senderAtBlock
	onlyOneUnwind := block == 0 && unwindEvery == 0 && unwind > 0
	backward := unwindEvery < unwind
	if onlyOneUnwind {
		stopAt = progress(tx, stages.Execution) - unwind
	} else if block > 0 && block < senderAtBlock {
		stopAt = block
	} else if backward {
		stopAt = 1
	}

	traceStart := func() {
		vmConfig.Tracer = vm.NewStructLogger(&vm.LogConfig{})
		vmConfig.Debug = true
	}
	traceStop := func(id int) {
		if !vmConfig.Debug {
			return
		}
		w, err3 := os.Create(fmt.Sprintf("trace_%d.txt", id))
		if err3 != nil {
			panic(err3)
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent(" ", " ")
		for _, l := range core.FormatLogs(vmConfig.Tracer.(*vm.StructLogger).StructLogs()) {
			if err2 := encoder.Encode(l); err2 != nil {
				panic(err2)
			}
		}
		if err2 := w.Close(); err2 != nil {
			panic(err2)
		}

		vmConfig.Tracer = nil
		vmConfig.Debug = false
	}
	_, _ = traceStart, traceStop

	for (!backward && execAtBlock < stopAt) || (backward && execAtBlock > stopAt) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// All stages forward to `execStage + unwindEvery` block
		execAtBlock = progress(tx, stages.Execution)
		execToBlock := block
		if unwindEvery > 0 || unwind > 0 {
			if execAtBlock+unwindEvery > unwind {
				execToBlock = execAtBlock + unwindEvery - unwind
			} else {
				break
			}
		}
		if backward {
			if execToBlock < stopAt {
				execToBlock = stopAt
			}
		} else {
			if execToBlock > stopAt {
				execToBlock = stopAt + 1
				unwind = 0
			}
		}

		stateStages.MockExecFunc(stages.Execution, execUntilFunc(execToBlock))
		_ = stateStages.SetCurrentStage(stages.Execution)
		if err := stateStages.Run(db, tx); err != nil {
			return err
		}

		if integrityFast {
			if err := checkChanges(expectedAccountChanges, tx, expectedStorageChanges, execAtBlock, sm.History); err != nil {
				return err
			}
			integrity.Trie(tx, integritySlow, quit)
		}
		//receiptsInDB := rawdb.ReadReceiptsByNumber(tx, progress(tx, stages.Execution)+1)

		//if err := tx.RollbackAndBegin(context.Background()); err != nil {
		//	return err
		//}
		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		execAtBlock = progress(tx, stages.Execution)
		if execAtBlock == stopAt {
			break
		}

		nextBlock, _, err := rawdb.ReadBlockByNumberWithSenders(tx, execAtBlock+1)
		if err != nil {
			panic(err)
		}

		if miningConfig.Enabled && nextBlock != nil && nextBlock.Header().Coinbase != (common.Address{}) {
			miningWorld := stagedsync.StageMiningCfg(true)

			miningConfig.Etherbase = nextBlock.Header().Coinbase
			miningConfig.ExtraData = nextBlock.Header().Extra
			miningStages, err := mining.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpDir, batchSize, quit, nil, txPool, false, miningWorld, nil)
			if err != nil {
				panic(err)
			}
			// Use all non-mining fields from nextBlock
			miningStages.MockExecFunc(stages.MiningCreateBlock, func(s *stagedsync.StageState, u stagedsync.Unwinder, tx ethdb.RwTx) error {
				err = stagedsync.SpawnMiningCreateBlockStage(s, tx,
					stagedsync.StageMiningCreateBlockCfg(db,
						miningConfig,
						*chainConfig,
						engine,
						txPool,
						tmpDir),
					miningWorld.Block,
					quit)
				miningWorld.Block.Uncles = nextBlock.Uncles()
				miningWorld.Block.Header.Time = nextBlock.Header().Time
				miningWorld.Block.Header.GasLimit = nextBlock.Header().GasLimit
				miningWorld.Block.Header.Difficulty = nextBlock.Header().Difficulty
				miningWorld.Block.Header.Nonce = nextBlock.Header().Nonce
				miningWorld.Block.LocalTxs = types.NewTransactionsFixedOrder(nextBlock.Transactions())
				miningWorld.Block.RemoteTxs = types.NewTransactionsFixedOrder(nil)
				//debugprint.Headers(miningWorld.Block.Header, nextBlock.Header())
				return err
			})
			//miningStages.MockExecFunc(stages.MiningFinish, func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
			//debugprint.Transactions(nextBlock.Transactions(), miningWorld.Block.Txs)
			//debugprint.Receipts(miningWorld.Block.Receipts, receiptsInDB)
			//return stagedsync.SpawnMiningFinishStage(s, tx, miningWorld.Block, cc.Engine(), chainConfig, quit)
			//})

			if err := miningStages.Run(db, tx); err != nil {
				return err
			}
			tx.Rollback()
			tx, err = db.BeginRw(context.Background())
			if err != nil {
				return err
			}
			defer tx.Rollback()
			minedBlock := <-miningResultCh
			checkMinedBlock(nextBlock, minedBlock, chainConfig)
		}

		// Unwind all stages to `execStage - unwind` block
		if unwind == 0 {
			continue
		}

		to := execAtBlock - unwind
		if err := stateStages.UnwindTo(to, tx, common.Hash{}); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	return nil
}

func checkChanges(expectedAccountChanges map[uint64]*changeset.ChangeSet, db ethdb.Tx, expectedStorageChanges map[uint64]*changeset.ChangeSet, execAtBlock uint64, historyEnabled bool) error {
	for blockN := range expectedAccountChanges {
		if err := checkChangeSet(db, blockN, expectedAccountChanges[blockN], expectedStorageChanges[blockN]); err != nil {
			return err
		}
		delete(expectedAccountChanges, blockN)
		delete(expectedStorageChanges, blockN)
	}

	if historyEnabled {
		if err := checkHistory(db, dbutils.AccountChangeSetBucket, execAtBlock); err != nil {
			return err
		}
		if err := checkHistory(db, dbutils.StorageChangeSetBucket, execAtBlock); err != nil {
			return err
		}
	}
	return nil
}

func checkMinedBlock(b1, b2 *types.Block, chainConfig *params.ChainConfig) {
	h1 := b1.Header()
	h2 := b2.Header()
	if h1.Root != h2.Root ||
		(chainConfig.IsByzantium(b1.NumberU64()) && h1.ReceiptHash != h2.ReceiptHash) ||
		h1.TxHash != h2.TxHash ||
		h1.ParentHash != h2.ParentHash ||
		h1.UncleHash != h2.UncleHash ||
		h1.GasUsed != h2.GasUsed ||
		!bytes.Equal(h1.Extra, h2.Extra) {
		debugprint.Headers(h1, h2)
		panic("blocks are not same")
	}
}

func loopIh(db ethdb.RwKV, ctx context.Context, unwind uint64) error {
	ch := ctx.Done()
	_, _, _, _, _, sync, _, _, _ := newSync(ctx, db)
	tmpdir := path.Join(datadir, etl.TmpDirName)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Execution, stages.Translation, stages.AccountHistoryIndex, stages.StorageHistoryIndex, stages.TxPool, stages.TxLookup, stages.Finish)
	if err = sync.Run(db, tx); err != nil {
		return err
	}
	execStage := stage(sync, tx, stages.HashState)
	to := execStage.BlockNumber - unwind
	_ = sync.SetCurrentStage(stages.HashState)
	u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: to}
	if err = stagedsync.UnwindHashStateStage(u, stage(sync, tx, stages.HashState), tx, stagedsync.StageHashStateCfg(db, tmpdir), ch); err != nil {
		return err
	}
	_ = sync.SetCurrentStage(stages.IntermediateHashes)
	u = &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: to}
	if err = stagedsync.UnwindIntermediateHashesStage(u, stage(sync, tx, stages.IntermediateHashes), tx, stagedsync.StageTrieCfg(db, true, true, tmpdir), ch); err != nil {
		return err
	}
	_ = clearUnwindStack(tx, ctx)
	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()

	sync.DisableStages(stages.IntermediateHashes)
	_ = sync.SetCurrentStage(stages.HashState)
	if err = sync.Run(db, tx); err != nil {
		return err
	}
	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()

	sync.DisableStages(stages.HashState)
	sync.EnableStages(stages.IntermediateHashes)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_ = sync.SetCurrentStage(stages.IntermediateHashes)
		t := time.Now()
		if err = sync.Run(db, tx); err != nil {
			return err
		}
		log.Warn("loop", "time", time.Since(t).String())
		tx.Rollback()
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
}

func loopExec(db ethdb.RwKV, ctx context.Context, unwind uint64) error {
	ch := ctx.Done()
	_, engine, chainConfig, vmConfig, _, sync, _, _, _ := newSync(ctx, db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_ = clearUnwindStack(tx, context.Background())
	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()
	sync.DisableAllStages()
	sync.EnableStages(stages.Execution)
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	from := progress(tx, stages.Execution)
	to := from + unwind
	cfg := stagedsync.StageExecuteBlocksCfg(db, true, false, false, 0, batchSize, nil, chainConfig, engine, vmConfig, tmpDBPath)

	// set block limit of execute stage
	sync.MockExecFunc(stages.Execution, func(stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx ethdb.RwTx) error {
		if err = stagedsync.SpawnExecuteBlocksStage(stageState, sync, tx, to, ch, cfg, nil); err != nil {
			return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
		}
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_ = sync.SetCurrentStage(stages.Execution)
		t := time.Now()
		if err = sync.Run(db, tx); err != nil {
			return err
		}
		fmt.Printf("loop time: %s\n", time.Since(t))
		tx.Rollback()
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
}

func checkChangeSet(db ethdb.Tx, blockNum uint64, expectedAccountChanges *changeset.ChangeSet, expectedStorageChanges *changeset.ChangeSet) error {
	i := 0
	sort.Sort(expectedAccountChanges)
	err := changeset.Walk(db, dbutils.AccountChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
		c := expectedAccountChanges.Changes[i]
		i++
		if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
			return true, nil
		}

		fmt.Printf("Unexpected account changes in block %d\n", blockNum)
		fmt.Printf("In the database: ======================\n")
		fmt.Printf("0x%x: %x\n", k, v)
		fmt.Printf("Expected: ==========================\n")
		fmt.Printf("0x%x %x\n", c.Key, c.Value)
		return false, fmt.Errorf("check change set failed")
	})
	if err != nil {
		return err
	}
	if expectedAccountChanges.Len() != i {
		return fmt.Errorf("db has less changets")
	}
	if expectedStorageChanges == nil {
		expectedStorageChanges = changeset.NewChangeSet()
	}

	i = 0
	sort.Sort(expectedStorageChanges)
	err = changeset.Walk(db, dbutils.StorageChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
		c := expectedStorageChanges.Changes[i]
		i++
		if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
			return true, nil
		}

		fmt.Printf("Unexpected storage changes in block %d\n", blockNum)
		fmt.Printf("In the database: ======================\n")
		fmt.Printf("0x%x: %x\n", k, v)
		fmt.Printf("Expected: ==========================\n")
		fmt.Printf("0x%x %x\n", c.Key, c.Value)
		return false, fmt.Errorf("check change set failed")
	})
	if err != nil {
		return err
	}
	if expectedStorageChanges.Len() != i {
		return fmt.Errorf("db has less changets")
	}

	return nil
}

func checkHistory(db ethdb.Tx, changeSetBucket string, blockNum uint64) error {
	indexBucket := changeset.Mapper[changeSetBucket].IndexBucket
	blockNumBytes := dbutils.EncodeBlockNumber(blockNum)
	if err := changeset.Walk(db, changeSetBucket, blockNumBytes, 0, func(blockN uint64, address, v []byte) (bool, error) {
		k := dbutils.CompositeKeyWithoutIncarnation(address)
		bm, innerErr := bitmapdb.Get64(db, indexBucket, k, blockN-1, blockN+1)
		if innerErr != nil {
			return false, innerErr
		}
		if !bm.Contains(blockN) {
			return false, fmt.Errorf("checkHistory failed: bucket=%s,block=%d,addr=%x", changeSetBucket, blockN, k)
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}
