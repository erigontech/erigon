package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debugprint"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/log/v3"
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
		ctx, _ := common2.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)
		miningConfig := params.MiningConfig{}
		utils.SetupMinerCobra(cmd, &miningConfig)
		logger := log.New()
		db := openDB(path.Join(cfg.DataDir, "chaindata"), logger, true)
		defer db.Close()

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
		ctx, _ := common2.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
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
		ctx, _ := common2.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
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
	withDataDir2(stateStags)
	withReferenceChaindata(stateStags)
	withUnwind(stateStags)
	withUnwindEvery(stateStags)
	withBlock(stateStags)
	withIntegrityChecks(stateStags)
	withMining(stateStags)
	withChain(stateStags)
	withHeimdall(stateStags)

	rootCmd.AddCommand(stateStags)

	withDataDir(loopIhCmd)
	withBatchSize(loopIhCmd)
	withUnwind(loopIhCmd)
	withChain(loopIhCmd)
	withHeimdall(loopIhCmd)

	rootCmd.AddCommand(loopIhCmd)

	withDataDir(loopExecCmd)
	withBatchSize(loopExecCmd)
	withUnwind(loopExecCmd)
	withChain(loopExecCmd)
	withHeimdall(loopExecCmd)

	rootCmd.AddCommand(loopExecCmd)
}

func syncBySmallSteps(db kv.RwDB, miningConfig params.MiningConfig, ctx context.Context) error {
	pm, engine, chainConfig, vmConfig, stateStages, miningStages, miner := newSync(ctx, db, &miningConfig)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tmpDir := filepath.Join(datadir, etl.TmpDirName)
	quit := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	expectedAccountChanges := make(map[uint64]*changeset.ChangeSet)
	expectedStorageChanges := make(map[uint64]*changeset.ChangeSet)
	changeSetHook := func(blockNum uint64, csw *state.ChangeSetWriter) {
		if csw == nil {
			return
		}
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

	stateStages.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders)

	execCfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, changeSetHook, chainConfig, engine, vmConfig, nil, false, tmpDir, getBlockReader(chainConfig, db))

	execUntilFunc := func(execToBlock uint64) func(firstCycle bool, badBlockUnwind bool, stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx) error {
		return func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx) error {
			if err := stagedsync.SpawnExecuteBlocksStage(s, unwinder, tx, execToBlock, ctx, execCfg, firstCycle); err != nil {
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
		if err := stateStages.Run(db, tx, false); err != nil {
			return err
		}

		if integrityFast {
			if err := checkChanges(expectedAccountChanges, tx, expectedStorageChanges, execAtBlock, pm.History.PruneTo(execToBlock)); err != nil {
				return err
			}
			integrity.Trie(db, tx, integritySlow, ctx)
		}
		//receiptsInDB := rawdb.ReadReceiptsByNumber(tx, progress(tx, stages.Execution)+1)

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		execAtBlock = progress(tx, stages.Execution)

		if execAtBlock == stopAt {
			break
		}

		nextBlock, _, err := rawdb.CanonicalBlockByNumberWithSenders(tx, execAtBlock+1)
		if err != nil {
			panic(err)
		}

		if miner.MiningConfig.Enabled && nextBlock != nil && nextBlock.Coinbase() != (common.Address{}) {
			miner.MiningConfig.Etherbase = nextBlock.Coinbase()
			miner.MiningConfig.ExtraData = nextBlock.Extra()
			miningStages.MockExecFunc(stages.MiningCreateBlock, func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx) error {
				err = stagedsync.SpawnMiningCreateBlockStage(s, tx,
					stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, nil, nil, nil, tmpDir),
					quit)
				if err != nil {
					return err
				}
				miner.MiningBlock.Uncles = nextBlock.Uncles()
				miner.MiningBlock.Header.Time = nextBlock.Time()
				miner.MiningBlock.Header.GasLimit = nextBlock.GasLimit()
				miner.MiningBlock.Header.Difficulty = nextBlock.Difficulty()
				miner.MiningBlock.Header.Nonce = nextBlock.Nonce()
				miner.MiningBlock.LocalTxs = types.NewTransactionsFixedOrder(nextBlock.Transactions())
				miner.MiningBlock.RemoteTxs = types.NewTransactionsFixedOrder(nil)
				//debugprint.Headers(miningWorld.Block.Header, nextBlock.Header())
				return err
			})
			//miningStages.MockExecFunc(stages.MiningFinish, func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
			//debugprint.Transactions(nextBlock.Transactions(), miningWorld.Block.Txs)
			//debugprint.Receipts(miningWorld.Block.Receipts, receiptsInDB)
			//return stagedsync.SpawnMiningFinishStage(s, tx, miningWorld.Block, cc.Engine(), chainConfig, quit)
			//})

			_ = miningStages.SetCurrentStage(stages.MiningCreateBlock)
			if err := miningStages.Run(db, tx, false); err != nil {
				return err
			}
			tx.Rollback()
			tx, err = db.BeginRw(context.Background())
			if err != nil {
				return err
			}
			defer tx.Rollback()
			minedBlock := <-miner.MiningResultCh
			checkMinedBlock(nextBlock, minedBlock, chainConfig)
		}

		// Unwind all stages to `execStage - unwind` block
		if unwind == 0 {
			continue
		}

		to := execAtBlock - unwind
		stateStages.UnwindTo(to, common.Hash{})

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

func checkChanges(expectedAccountChanges map[uint64]*changeset.ChangeSet, tx kv.Tx, expectedStorageChanges map[uint64]*changeset.ChangeSet, execAtBlock, prunedTo uint64) error {
	checkHistoryFrom := execAtBlock
	if prunedTo > checkHistoryFrom {
		checkHistoryFrom = prunedTo
	}
	for blockN := range expectedAccountChanges {
		if blockN <= checkHistoryFrom {
			continue
		}
		if err := checkChangeSet(tx, blockN, expectedAccountChanges[blockN], expectedStorageChanges[blockN]); err != nil {
			return err
		}
		delete(expectedAccountChanges, blockN)
		delete(expectedStorageChanges, blockN)
	}

	if err := checkHistory(tx, kv.AccountChangeSet, checkHistoryFrom); err != nil {
		return err
	}
	if err := checkHistory(tx, kv.StorageChangeSet, checkHistoryFrom); err != nil {
		return err
	}
	return nil
}

func checkMinedBlock(b1, b2 *types.Block, chainConfig *params.ChainConfig) {
	if b1.Root() != b2.Root() ||
		(chainConfig.IsByzantium(b1.NumberU64()) && b1.ReceiptHash() != b2.ReceiptHash()) ||
		b1.TxHash() != b2.TxHash() ||
		b1.ParentHash() != b2.ParentHash() ||
		b1.UncleHash() != b2.UncleHash() ||
		b1.GasUsed() != b2.GasUsed() ||
		!bytes.Equal(b1.Extra(), b2.Extra()) { // TODO: Extra() doesn't need to be a copy for a read-only compare
		// Header()'s deep-copy doesn't matter here since it will panic anyway
		debugprint.Headers(b1.Header(), b2.Header())
		panic("blocks are not same")
	}
}

func loopIh(db kv.RwDB, ctx context.Context, unwind uint64) error {
	_, _, chainConfig, _, sync, _, _ := newSync(ctx, db, nil)
	tmpdir := filepath.Join(datadir, etl.TmpDirName)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Execution, stages.Translation, stages.AccountHistoryIndex, stages.StorageHistoryIndex, stages.TxLookup, stages.Finish)
	if err = sync.Run(db, tx, false); err != nil {
		return err
	}
	execStage := stage(sync, tx, nil, stages.HashState)
	to := execStage.BlockNumber - unwind
	_ = sync.SetCurrentStage(stages.HashState)
	u := &stagedsync.UnwindState{ID: stages.HashState, UnwindPoint: to}
	if err = stagedsync.UnwindHashStateStage(u, stage(sync, tx, nil, stages.HashState), tx, stagedsync.StageHashStateCfg(db, tmpdir), ctx); err != nil {
		return err
	}
	_ = sync.SetCurrentStage(stages.IntermediateHashes)
	u = &stagedsync.UnwindState{ID: stages.IntermediateHashes, UnwindPoint: to}
	if err = stagedsync.UnwindIntermediateHashesStage(u, stage(sync, tx, nil, stages.IntermediateHashes), tx, stagedsync.StageTrieCfg(db, true, true, tmpdir, getBlockReader(chainConfig, db)), ctx); err != nil {
		return err
	}
	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()

	sync.DisableStages(stages.IntermediateHashes)
	_ = sync.SetCurrentStage(stages.HashState)
	if err = sync.Run(db, tx, false); err != nil {
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
		if err = sync.Run(db, tx, false); err != nil {
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

func loopExec(db kv.RwDB, ctx context.Context, unwind uint64) error {
	pm, engine, chainConfig, vmConfig, sync, _, _ := newSync(ctx, db, nil)
	tmpdir := filepath.Join(datadir, etl.TmpDirName)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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

	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil, false, tmpdir, getBlockReader(chainConfig, db))

	// set block limit of execute stage
	sync.MockExecFunc(stages.Execution, func(firstCycle bool, badBlockUnwind bool, stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx) error {
		if err = stagedsync.SpawnExecuteBlocksStage(stageState, sync, tx, to, ctx, cfg, false); err != nil {
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
		if err = sync.Run(db, tx, false); err != nil {
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

func checkChangeSet(db kv.Tx, blockNum uint64, expectedAccountChanges *changeset.ChangeSet, expectedStorageChanges *changeset.ChangeSet) error {
	i := 0
	sort.Sort(expectedAccountChanges)
	err := changeset.ForPrefix(db, kv.AccountChangeSet, dbutils.EncodeBlockNumber(blockNum), func(blockN uint64, k, v []byte) error {
		c := expectedAccountChanges.Changes[i]
		i++
		if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
			return nil
		}

		fmt.Printf("Unexpected account changes in block %d\n", blockNum)
		fmt.Printf("In the database: ======================\n")
		fmt.Printf("0x%x: %x\n", k, v)
		fmt.Printf("Expected: ==========================\n")
		fmt.Printf("0x%x %x\n", c.Key, c.Value)
		return fmt.Errorf("check change set failed")
	})
	if err != nil {
		return err
	}
	if expectedAccountChanges.Len() != i {
		return fmt.Errorf("db has less changesets")
	}
	if expectedStorageChanges == nil {
		expectedStorageChanges = changeset.NewChangeSet()
	}

	i = 0
	sort.Sort(expectedStorageChanges)
	err = changeset.ForPrefix(db, kv.StorageChangeSet, dbutils.EncodeBlockNumber(blockNum), func(blockN uint64, k, v []byte) error {
		c := expectedStorageChanges.Changes[i]
		i++
		if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
			return nil
		}

		fmt.Printf("Unexpected storage changes in block %d\n", blockNum)
		fmt.Printf("In the database: ======================\n")
		fmt.Printf("0x%x: %x\n", k, v)
		fmt.Printf("Expected: ==========================\n")
		fmt.Printf("0x%x %x\n", c.Key, c.Value)
		return fmt.Errorf("check change set failed")
	})
	if err != nil {
		return err
	}
	if expectedStorageChanges.Len() != i {
		return fmt.Errorf("db has less changesets")
	}

	return nil
}

func checkHistory(tx kv.Tx, changeSetBucket string, blockNum uint64) error {
	indexBucket := changeset.Mapper[changeSetBucket].IndexBucket
	blockNumBytes := dbutils.EncodeBlockNumber(blockNum)
	if err := changeset.ForEach(tx, changeSetBucket, blockNumBytes, func(blockN uint64, address, v []byte) error {
		k := dbutils.CompositeKeyWithoutIncarnation(address)
		from := blockN
		if from > 0 {
			from--
		}
		bm, innerErr := bitmapdb.Get64(tx, indexBucket, k, from, blockN+1)
		if innerErr != nil {
			return innerErr
		}
		if !bm.Contains(blockN) {
			return fmt.Errorf("checkHistory failed: bucket=%s,block=%d,addr=%x", changeSetBucket, blockN, k)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
