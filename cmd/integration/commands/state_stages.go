package commands

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/integrity"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/spf13/cobra"
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
	Example: "go run ./cmd/integration state_stages --chaindata=... --verbosity=3 --unwind=100 --unwind.every=100000 --block=2000000",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		miningConfig := &params.MiningConfig{}
		utils.SetupMinerCobra(cmd, miningConfig)
		if err := syncBySmallSteps(db, miningConfig, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}

		if referenceChaindata != "" {
			if err := compareStates(ctx, chaindata, referenceChaindata); err != nil {
				log.Error(err.Error())
				return err
			}

		}
		return nil
	},
}

var loopIhCmd = &cobra.Command{
	Use: "loop_ih",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
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
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()
		if unwind == 0 {
			unwind = 1
		}
		if err := loopExec(db, ctx, unwind); err != nil {
			log.Error("Error", "err", err)
			return err
		}

		return nil
	},
}

func init() {
	withChaindata(stateStags)
	withReferenceChaindata(stateStags)
	withUnwind(stateStags)
	withUnwindEvery(stateStags)
	withBlock(stateStags)
	withBatchSize(stateStags)
	withIntegrityChecks(stateStags)
	withMining(stateStags)

	rootCmd.AddCommand(stateStags)

	withChaindata(loopIhCmd)
	withBatchSize(loopIhCmd)
	withUnwind(loopIhCmd)

	rootCmd.AddCommand(loopIhCmd)

	withChaindata(loopExecCmd)
	withBatchSize(loopExecCmd)
	withUnwind(loopExecCmd)

	rootCmd.AddCommand(loopExecCmd)
}

func syncBySmallSteps(db ethdb.Database, miningConfig *params.MiningConfig, ctx context.Context) error {
	tmpDir := path.Join(datadir, etl.TmpDirName)
	must(clearUnwindStack(db, ctx))
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

	var tx ethdb.DbWithPendingMutations = ethdb.NewTxDbWithoutTransaction(db, ethdb.RW)
	defer tx.Rollback()

	sm, cc, chainConfig, vmConfig, txPool, st, mining, cache := newSync2(db, tx)
	mux := new(event.TypeMux)
	minedBlockSub := mux.Subscribe(core.NewMinedBlockEvent{})

	execUntilFunc := func(execToBlock uint64) func(stageState *stagedsync.StageState, unwinder stagedsync.Unwinder) error {
		return func(s *stagedsync.StageState, unwinder stagedsync.Unwinder) error {
			if err := stagedsync.SpawnExecuteBlocksStage(
				s, tx,
				chainConfig, cc, vmConfig,
				quit,
				stagedsync.ExecuteBlockStageParams{
					ToBlock:       execToBlock, // limit execution to the specified block
					WriteReceipts: sm.Receipts,
					Cache:         cache,
					BatchSize:     batchSize,
					ChangeSetHook: changeSetHook,
				}); err != nil {
				return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
			}
			return nil
		}
	}

	var stateHash common.Hash
	execTrieFunc := func(s *stagedsync.StageState, unwinder stagedsync.Unwinder) error {
		var err error
		stateHash, err = stagedsync.SpawnIntermediateHashesStage(s, tx, true /* checkRoot */, cache, "", quit)
		fmt.Printf("got: %x\n", stateHash)
		return err
	}

	var err1 error
	tx, err1 = tx.Begin(ctx, ethdb.RW)
	if err1 != nil {
		return err1
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

	var minedBlock *types.Block
	minedBlocks := make(chan *types.Block, 1) // convert to non-blocking channel
	go func() {
		for msg := range minedBlockSub.Chan() {
			minedBlocks <- msg.Data.(core.NewMinedBlockEvent).Block
		}
	}()

	for (!backward && execAtBlock < stopAt) || (backward && execAtBlock > stopAt) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := tx.CommitAndBegin(context.Background()); err != nil {
			return err
		}

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

		stateStages, err2 := st.Prepare(nil, chainConfig, cc, vmConfig, db, tx, "integration_test", sm, tmpDir, cache, batchSize, quit, nil, txPool, func() error { return nil }, false, nil)
		if err2 != nil {
			panic(err2)
		}
		stateStages.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders)

		if miningConfig.Enabled {
			miningStages, err := mining.Prepare(nil, chainConfig, cc, vmConfig, db, tx, "integration_test", sm, tmpDir, cache, batchSize, quit, nil, txPool, func() error { return nil }, false,
				stagedsync.NewMiningStagesParameters(miningConfig, mux, true, nil, nil, miningTransactions(tx, execToBlock), nil),
			)
			if err != nil {
				panic(err)
			}

			// set block limit of execute state
			stateStages.MockExecFunc(stages.Execution, execUntilFunc(execToBlock-1))
			if err := stateStages.Run(db, tx); err != nil {
				return err
			}

			if integrityFast {
				if err := checkChanges(expectedAccountChanges, tx, expectedStorageChanges, execAtBlock, sm.History); err != nil {
					return err
				}
				integrity.Trie(tx.(ethdb.HasTx).Tx(), integritySlow, quit)
			}

			if err := tx.CommitAndBegin(context.Background()); err != nil {
				return err
			}
			if err := miningStages.Run(db, tx); err != nil {
				return err
			}
			if err := tx.RollbackAndBegin(context.Background()); err != nil {
				return err
			}

			minedBlock = <-minedBlocks
		}

		stateStages.MockExecFunc(stages.Execution, execUntilFunc(execToBlock))
		stateStages.MockExecFunc(stages.IntermediateHashes, execTrieFunc)
		_ = stateStages.SetCurrentStage(stages.Execution)
		if err := stateStages.Run(db, tx); err != nil {
			return err
		}
		if err := tx.CommitAndBegin(context.Background()); err != nil {
			return err
		}
		if miningConfig.Enabled {
			if stateHash != minedBlock.Header().Hash() {
				panic(fmt.Errorf("state hash: %x, mined hash: %x", stateHash, minedBlock.Header().Hash()))
			}
		}
		execAtBlock = progress(tx, stages.Execution)
		if execAtBlock == stopAt {
			break
		}

		// Unwind all stages to `execStage - unwind` block
		if unwind == 0 {
			continue
		}

		to := execAtBlock - unwind
		if err := stateStages.UnwindTo(to, tx); err != nil {
			return err
		}

		if err := tx.CommitAndBegin(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func checkChanges(expectedAccountChanges map[uint64]*changeset.ChangeSet, db ethdb.Database, expectedStorageChanges map[uint64]*changeset.ChangeSet, execAtBlock uint64, historyEnabled bool) error {
	for blockN := range expectedAccountChanges {
		if err := checkChangeSet(db, blockN, expectedAccountChanges[blockN], expectedStorageChanges[blockN]); err != nil {
			return err
		}
		delete(expectedAccountChanges, blockN)
		delete(expectedStorageChanges, blockN)
	}

	if historyEnabled {
		if err := checkHistory(db, dbutils.PlainAccountChangeSetBucket, execAtBlock); err != nil {
			return err
		}
		if err := checkHistory(db, dbutils.PlainStorageChangeSetBucket, execAtBlock); err != nil {
			return err
		}
	}
	return nil
}

func miningTransactions(tx ethdb.Getter, blockNum uint64) map[common.Address]types.Transactions {
	nextBlock, err := rawdb.ReadBlockByNumberWithSenders(tx, blockNum)
	if err != nil {
		panic(err)
	}
	localTxs := make(map[common.Address]types.Transactions, nextBlock.Transactions().Len())
	senders := nextBlock.Body().SendersFromTxs()
	for i, txn := range nextBlock.Transactions() {
		localTxs[senders[i]] = append(localTxs[senders[i]], txn)
	}
	return localTxs
}

func loopIh(db ethdb.Database, ctx context.Context, unwind uint64) error {
	ch := ctx.Done()
	var tx ethdb.DbWithPendingMutations = ethdb.NewTxDbWithoutTransaction(db, ethdb.RW)
	defer tx.Rollback()

	_, bc, _, st, _, cache, progress := newSync(ch, db, tx, nil)
	defer bc.Stop()

	var err error
	tx, err = tx.Begin(ctx, ethdb.RW)
	if err != nil {
		return err
	}

	_ = clearUnwindStack(tx, context.Background())
	st.DisableStages(stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Execution, stages.AccountHistoryIndex, stages.StorageHistoryIndex, stages.TxPool, stages.TxLookup, stages.Finish)
	if err = st.Run(db, tx); err != nil {
		return err
	}
	execStage := progress(stages.HashState)
	to := execStage.BlockNumber - unwind
	_ = st.SetCurrentStage(stages.HashState)
	u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: to}
	if err = stagedsync.UnwindHashStateStage(u, progress(stages.HashState), tx, cache, path.Join(datadir, etl.TmpDirName), ch); err != nil {
		return err
	}
	_ = st.SetCurrentStage(stages.IntermediateHashes)
	u = &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: to}
	if err = stagedsync.UnwindIntermediateHashesStage(u, progress(stages.IntermediateHashes), tx, cache, path.Join(datadir, etl.TmpDirName), ch); err != nil {
		return err
	}
	_ = clearUnwindStack(tx, context.Background())
	_ = tx.CommitAndBegin(context.Background())

	st.DisableStages(stages.IntermediateHashes)
	_ = st.SetCurrentStage(stages.HashState)
	if err = st.Run(db, tx); err != nil {
		return err
	}
	_ = tx.CommitAndBegin(context.Background())

	st.DisableStages(stages.HashState)
	st.EnableStages(stages.IntermediateHashes)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_ = st.SetCurrentStage(stages.IntermediateHashes)
		t := time.Now()
		if err = st.Run(db, tx); err != nil {
			return err
		}
		log.Warn("loop", "time", time.Since(t).String())
		tx.Rollback()
		tx, err = tx.Begin(ctx, ethdb.RW)
		if err != nil {
			return err
		}
	}
}

func loopExec(db ethdb.Database, ctx context.Context, unwind uint64) error {
	ch := ctx.Done()
	var tx ethdb.DbWithPendingMutations = ethdb.NewTxDbWithoutTransaction(db, ethdb.RW)
	defer tx.Rollback()

	cc, bc, _, st, _, cache, progress := newSync(ch, db, tx, nil)
	defer bc.Stop()

	var err error
	tx, err = tx.Begin(ctx, ethdb.RW)
	if err != nil {
		return err
	}

	_ = clearUnwindStack(tx, context.Background())
	_ = tx.CommitAndBegin(ctx)
	st.DisableAllStages()
	st.EnableStages(stages.Execution)
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	from := progress(stages.Execution).BlockNumber
	to := from + unwind
	// set block limit of execute stage
	st.MockExecFunc(stages.Execution, func(stageState *stagedsync.StageState, unwinder stagedsync.Unwinder) error {
		if err = stagedsync.SpawnExecuteBlocksStage(
			stageState, tx,
			bc.Config(), cc, bc.GetVMConfig(),
			ch,
			stagedsync.ExecuteBlockStageParams{
				ToBlock:       to, // limit execution to the specified block
				WriteReceipts: true,
				BatchSize:     batchSize,
				Cache:         cache,
				ChangeSetHook: nil,
			}); err != nil {
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

		_ = st.SetCurrentStage(stages.Execution)
		t := time.Now()
		if err = st.Run(db, tx); err != nil {
			return err
		}
		fmt.Printf("loop time: %s\n", time.Since(t))
		tx.Rollback()
		tx, err = tx.Begin(ctx, ethdb.RW)
		if err != nil {
			return err
		}
	}
}

func checkChangeSet(db ethdb.Database, blockNum uint64, expectedAccountChanges *changeset.ChangeSet, expectedStorageChanges *changeset.ChangeSet) error {
	i := 0
	sort.Sort(expectedAccountChanges)
	err := changeset.Walk(db, dbutils.PlainAccountChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
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
	err = changeset.Walk(db, dbutils.PlainStorageChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
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

func checkHistory(db ethdb.Database, changeSetBucket string, blockNum uint64) error {
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
