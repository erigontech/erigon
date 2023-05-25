package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debugprint"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

var stateStages = &cobra.Command{
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
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "integration"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}
		ctx, _ := common2.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		ethConfig.Genesis = core.GenesisBlockByChainName(chain)
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)
		miningConfig := params.MiningConfig{}
		utils.SetupMinerCobra(cmd, &miningConfig)
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := syncBySmallSteps(db, miningConfig, ctx, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		if referenceChaindata != "" {
			if err := compareStates(ctx, chaindata, referenceChaindata); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Error(err.Error())
				}
				return
			}
		}
	},
}

var loopIhCmd = &cobra.Command{
	Use: "loop_ih",
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "integration"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}
		ctx, _ := common2.RootContext()
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if unwind == 0 {
			unwind = 1
		}
		if err := loopIh(db, ctx, unwind, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var loopExecCmd = &cobra.Command{
	Use: "loop_exec",
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "integration"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}
		ctx, _ := common2.RootContext()
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()
		if unwind == 0 {
			unwind = 1
		}
		if err := loopExec(db, ctx, unwind, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withConfig(stateStages)
	withDataDir2(stateStages)
	withReferenceChaindata(stateStages)
	withUnwind(stateStages)
	withUnwindEvery(stateStages)
	withBlock(stateStages)
	withIntegrityChecks(stateStages)
	withMining(stateStages)
	withChain(stateStages)
	withHeimdall(stateStages)
	withWorkers(stateStages)
	rootCmd.AddCommand(stateStages)

	withConfig(loopIhCmd)
	withDataDir(loopIhCmd)
	withBatchSize(loopIhCmd)
	withUnwind(loopIhCmd)
	withChain(loopIhCmd)
	withHeimdall(loopIhCmd)
	rootCmd.AddCommand(loopIhCmd)

	withConfig(loopExecCmd)
	withDataDir(loopExecCmd)
	withBatchSize(loopExecCmd)
	withUnwind(loopExecCmd)
	withChain(loopExecCmd)
	withHeimdall(loopExecCmd)
	withWorkers(loopExecCmd)
	rootCmd.AddCommand(loopExecCmd)
}

func syncBySmallSteps(db kv.RwDB, miningConfig params.MiningConfig, ctx context.Context, logger1 log.Logger) error {
	dirs := datadir.New(datadirCli)
	sn, agg := allSnapshots(ctx, db, logger1)
	defer sn.Close()
	defer agg.Close()
	engine, vmConfig, stateStages, miningStages, miner := newSync(ctx, db, &miningConfig, logger1)
	chainConfig, historyV3, pm := fromdb.ChainConfig(db), kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	quit := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	expectedAccountChanges := make(map[uint64]*historyv2.ChangeSet)
	expectedStorageChanges := make(map[uint64]*historyv2.ChangeSet)
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

	stateStages.DisableStages(stages.Snapshots, stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders)
	changesAcc := shards.NewAccumulator()

	genesis := core.GenesisBlockByChainName(chain)
	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = int(workers)
	syncCfg.ReconWorkerCount = int(reconWorkers)

	br, _ := blocksIO(db, logger1)
	execCfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, changeSetHook, chainConfig, engine, vmConfig, changesAcc, false, false, historyV3, dirs,
		br, nil, genesis, syncCfg, agg)

	execUntilFunc := func(execToBlock uint64) func(firstCycle bool, badBlockUnwind bool, stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
		return func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
			if err := stagedsync.SpawnExecuteBlocksStage(s, unwinder, tx, execToBlock, ctx, execCfg, firstCycle, logger); err != nil {
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
		vmConfig.Tracer = logger.NewStructLogger(&logger.LogConfig{})
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
		for _, l := range logger.FormatLogs(vmConfig.Tracer.(*logger.StructLogger).StructLogs()) {
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
		if err := stateStages.Run(db, tx, false /* firstCycle */); err != nil {
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

		hash, err := rawdb.ReadCanonicalHash(tx, execAtBlock+1)
		if err != nil {
			return fmt.Errorf("failed ReadCanonicalHash: %w", err)
		}
		nextBlock, _, err := br.BlockWithSenders(context.Background(), tx, hash, execAtBlock+1)
		if err != nil {
			panic(err)
		}

		if miner.MiningConfig.Enabled && nextBlock != nil && nextBlock.Coinbase() != (common2.Address{}) {
			miner.MiningConfig.Etherbase = nextBlock.Coinbase()
			miner.MiningConfig.ExtraData = nextBlock.Extra()
			miningStages.MockExecFunc(stages.MiningCreateBlock, func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				err = stagedsync.SpawnMiningCreateBlockStage(s, tx,
					stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, nil, nil, nil, dirs.Tmp, br),
					quit, logger)
				if err != nil {
					return err
				}
				miner.MiningBlock.Uncles = nextBlock.Uncles()
				miner.MiningBlock.Header.Time = nextBlock.Time()
				miner.MiningBlock.Header.GasLimit = nextBlock.GasLimit()
				miner.MiningBlock.Header.Difficulty = nextBlock.Difficulty()
				miner.MiningBlock.Header.Nonce = nextBlock.Nonce()
				miner.MiningBlock.PreparedTxs = types.NewTransactionsFixedOrder(nextBlock.Transactions())
				//debugprint.Headers(miningWorld.Block.Header, nextBlock.Header())
				return err
			})
			//miningStages.MockExecFunc(stages.MiningFinish, func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
			//debugprint.Transactions(nextBlock.Transactions(), miningWorld.Block.Txs)
			//debugprint.Receipts(miningWorld.Block.Receipts, receiptsInDB)
			//return stagedsync.SpawnMiningFinishStage(s, tx, miningWorld.Block, cc.Engine(), chainConfig, quit)
			//})

			_ = miningStages.SetCurrentStage(stages.MiningCreateBlock)
			if err := miningStages.Run(db, tx, false /* firstCycle */); err != nil {
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
		stateStages.UnwindTo(to, common2.Hash{})

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// allow backward loop
		if unwind > 0 && unwindEvery > 0 {
			stopAt -= unwind
		}
	}

	return nil
}

func checkChanges(expectedAccountChanges map[uint64]*historyv2.ChangeSet, tx kv.Tx, expectedStorageChanges map[uint64]*historyv2.ChangeSet, execAtBlock, prunedTo uint64) error {
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

func checkMinedBlock(b1, b2 *types.Block, chainConfig *chain2.Config) {
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

func loopIh(db kv.RwDB, ctx context.Context, unwind uint64, logger log.Logger) error {
	sn, agg := allSnapshots(ctx, db, logger)
	defer sn.Close()
	defer agg.Close()
	_, _, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)
	dirs := datadir.New(datadirCli)
	historyV3 := kvcfg.HistoryV3.FromDB(db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	sync.DisableStages(stages.Snapshots, stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Execution, stages.Translation, stages.AccountHistoryIndex, stages.StorageHistoryIndex, stages.TxLookup, stages.Finish)
	if err = sync.Run(db, tx, false /* firstCycle */); err != nil {
		return err
	}
	execStage := stage(sync, tx, nil, stages.HashState)
	to := execStage.BlockNumber - unwind
	_ = sync.SetCurrentStage(stages.HashState)
	u := &stagedsync.UnwindState{ID: stages.HashState, UnwindPoint: to}
	if err = stagedsync.UnwindHashStateStage(u, stage(sync, tx, nil, stages.HashState), tx, stagedsync.StageHashStateCfg(db, dirs, historyV3), ctx, logger); err != nil {
		return err
	}
	_ = sync.SetCurrentStage(stages.IntermediateHashes)
	u = &stagedsync.UnwindState{ID: stages.IntermediateHashes, UnwindPoint: to}
	br, _ := blocksIO(db, logger)
	if err = stagedsync.UnwindIntermediateHashesStage(u, stage(sync, tx, nil, stages.IntermediateHashes), tx, stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp,
		br, nil, historyV3, agg), ctx, logger); err != nil {
		return err
	}
	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()

	sync.DisableStages(stages.IntermediateHashes)
	_ = sync.SetCurrentStage(stages.HashState)
	if err = sync.Run(db, tx, false /* firstCycle */); err != nil {
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
		if err = sync.Run(db, tx, false /* firstCycle */); err != nil {
			return err
		}
		logger.Warn("loop", "time", time.Since(t).String())
		tx.Rollback()
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
}

func loopExec(db kv.RwDB, ctx context.Context, unwind uint64, logger log.Logger) error {
	chainConfig := fromdb.ChainConfig(db)
	dirs, pm := datadir.New(datadirCli), fromdb.PruneMode(db)
	sn, agg := allSnapshots(ctx, db, logger)
	defer sn.Close()
	defer agg.Close()
	engine, vmConfig, sync, _, _ := newSync(ctx, db, nil, logger)

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
	historyV3, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		return err
	}
	from := progress(tx, stages.Execution)
	to := from + unwind

	genesis := core.GenesisBlockByChainName(chain)
	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = int(workers)
	syncCfg.ReconWorkerCount = int(reconWorkers)

	initialCycle := false
	br, _ := blocksIO(db, logger)
	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ false, historyV3, dirs, br, nil, genesis, syncCfg, agg)

	// set block limit of execute stage
	sync.MockExecFunc(stages.Execution, func(firstCycle bool, badBlockUnwind bool, stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
		if err = stagedsync.SpawnExecuteBlocksStage(stageState, sync, tx, to, ctx, cfg, initialCycle, logger); err != nil {
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
		if err = sync.Run(db, tx, initialCycle); err != nil {
			return err
		}
		log.Info("[Integration] ", "loop time", time.Since(t))
		tx.Rollback()
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
}

func checkChangeSet(db kv.Tx, blockNum uint64, expectedAccountChanges *historyv2.ChangeSet, expectedStorageChanges *historyv2.ChangeSet) error {
	i := 0
	sort.Sort(expectedAccountChanges)
	err := historyv2.ForPrefix(db, kv.AccountChangeSet, hexutility.EncodeTs(blockNum), func(blockN uint64, k, v []byte) error {
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
		expectedStorageChanges = historyv2.NewChangeSet()
	}

	i = 0
	sort.Sort(expectedStorageChanges)
	err = historyv2.ForPrefix(db, kv.StorageChangeSet, hexutility.EncodeTs(blockNum), func(blockN uint64, k, v []byte) error {
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
	indexBucket := historyv2.Mapper[changeSetBucket].IndexBucket
	blockNumBytes := hexutility.EncodeTs(blockNum)
	if err := historyv2.ForEach(tx, changeSetBucket, blockNumBytes, func(blockN uint64, address, v []byte) error {
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
