package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var stateStags = &cobra.Command{
	Use: "state_stages",
	Short: `
		Move all StateStages (4,5,6,7,8) forward. 
		Stops at Stage 3 progress or at "--block".
		Each iteration test will move forward "--unwind_every" blocks, then unwind "--unwind" blocks.
		Use reset_state command to re-run this test.
		`,
	Example: "go run ./cmd/integration state_stages --chaindata=... --verbosity=3 --unwind=100 --unwind_every=100000 --block=2000000",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		err := syncBySmallSteps(ctx, chaindata)
		if err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

func init() {
	withChaindata(stateStags)
	withUnwind(stateStags)
	withUnwindEvery(stateStags)
	withBlock(stateStags)

	rootCmd.AddCommand(stateStags)
}

func syncBySmallSteps(ctx context.Context, chaindata string) error {
	core.UsePlainStateExecution = true
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	blockchain, chainErr := newBlockChain(db)
	if chainErr != nil {
		return chainErr
	}
	defer blockchain.Stop()
	ch := make(chan struct{})
	defer close(ch)

	senderStageProgress := progress(db, stages.Senders).BlockNumber

	var stopAt = senderStageProgress
	if block > 0 && block < senderStageProgress {
		stopAt = block
	}

	expectedAccountChanges := make(map[uint64][]byte)
	expectedStorageChanges := make(map[uint64][]byte)
	changeSetHook := func(blockNum uint64, csw *state.ChangeSetWriter) {
		accountChanges, err := csw.GetAccountChanges()
		if err != nil {
			panic(err)
		}
		expectedAccountChanges[blockNum], err = changeset.EncodeAccountsPlain(accountChanges)
		if err != nil {
			panic(err)
		}

		storageChanges, err := csw.GetStorageChanges()
		if err != nil {
			panic(err)
		}
		if storageChanges.Len() > 0 {
			expectedStorageChanges[blockNum], err = changeset.EncodeStoragePlain(storageChanges)
			if err != nil {
				panic(err)
			}
		}
	}

	for progress(db, stages.Execution).BlockNumber+unwindEvery < stopAt {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// All stages forward to `execStage + unwindEvery` block
		{
			stage := progress(db, stages.Execution)
			execToBlock := stage.BlockNumber + unwindEvery

			if err := stagedsync.SpawnExecuteBlocksStage(stage, db, blockchain, execToBlock, ch, nil, false, changeSetHook); err != nil {
				return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.IntermediateHashes)
			if err := stagedsync.SpawnIntermediateHashesStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("spawnIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.HashState)
			if err := stagedsync.SpawnHashStateStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("spawnHashStateStage: %w", err)
			}
		}

		{
			stage7 := progress(db, stages.AccountHistoryIndex)
			stage8 := progress(db, stages.StorageHistoryIndex)
			if err := stagedsync.SpawnAccountHistoryIndex(stage7, db, "", ch); err != nil {
				return fmt.Errorf("spawnAccountHistoryIndex: %w", err)
			}
			if err := stagedsync.SpawnStorageHistoryIndex(stage8, db, "", ch); err != nil {
				return fmt.Errorf("spawnStorageHistoryIndex: %w", err)
			}
		}

		{
			stage9 := progress(db, stages.TxLookup)
			if err := stagedsync.SpawnTxLookup(stage9, db, "", ch); err != nil {
				return fmt.Errorf("spawnTxLookup: %w", err)
			}
		}

		for blockN := range expectedAccountChanges {
			if err := checkChangeSet(db, blockN, expectedAccountChanges[blockN], expectedStorageChanges[blockN]); err != nil {
				panic(err)
			}
			delete(expectedAccountChanges, blockN)
		}

		// Unwind all stages to `execStage - unwind` block
		if unwind == 0 {
			continue
		}

		execStage := progress(db, stages.Execution)
		to := execStage.BlockNumber - unwind
		{
			u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: to}
			stage9 := progress(db, stages.TxLookup)
			if err := stagedsync.UnwindTxLookup(u, stage9, db, "", ch); err != nil {
				return fmt.Errorf("unwindTxLookup: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.StorageHistoryIndex, UnwindPoint: to}
			if err := stagedsync.UnwindStorageHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("unwindStorageHistoryIndex: %w", err)
			}

			u = &stagedsync.UnwindState{Stage: stages.AccountHistoryIndex, UnwindPoint: to}
			if err := stagedsync.UnwindAccountHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("unwindAccountHistoryIndex: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: to}
			stage := progress(db, stages.HashState)
			if err := stagedsync.UnwindHashStateStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("unwindHashStateStage: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: to}
			stage := progress(db, stages.IntermediateHashes)
			if err := stagedsync.UnwindIntermediateHashesStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("unwindIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.Execution)
			u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: to}
			if err := stagedsync.UnwindExecutionStage(u, stage, db); err != nil {
				return fmt.Errorf("unwindExecutionStage: %w", err)
			}
		}
	}

	return nil
}

func progress(db ethdb.Getter, stage stages.SyncStage) *stagedsync.StageState {
	stageProgress, _, err := stages.GetStageProgress(db, stage)
	if err != nil {
		panic(err)
	}
	return &stagedsync.StageState{Stage: stage, BlockNumber: stageProgress}
}

func checkChangeSet(db *ethdb.ObjectDatabase, blockNum uint64, expectedAccountChanges []byte, expectedStorageChanges []byte) error {
	dbAccountChanges, err := db.GetChangeSetByBlock(false /* storage */, blockNum)
	if err != nil {
		return err
	}

	if !bytes.Equal(dbAccountChanges, expectedAccountChanges) {
		fmt.Printf("Unexpected account changes in block %d\nIn the database: ======================\n", blockNum)
		if err = changeset.AccountChangeSetPlainBytes(dbAccountChanges).Walk(func(k, v []byte) error {
			fmt.Printf("0x%x: %x\n", k, v)
			return nil
		}); err != nil {
			return err
		}
		fmt.Printf("Expected: ==========================\n")
		if err = changeset.AccountChangeSetPlainBytes(expectedAccountChanges).Walk(func(k, v []byte) error {
			fmt.Printf("0x%x %x\n", k, v)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	dbStorageChanges, err := db.GetChangeSetByBlock(true /* storage */, blockNum)
	if err != nil {
		return err
	}
	equal := true
	if !bytes.Equal(dbStorageChanges, expectedStorageChanges) {
		var addrs [][]byte
		var keys [][]byte
		var vals [][]byte
		if err = changeset.StorageChangeSetPlainBytes(dbStorageChanges).Walk(func(k, v []byte) error {
			addrs = append(addrs, common.CopyBytes(k[:common.AddressLength]))
			keys = append(keys, common.CopyBytes(k[common.AddressLength+common.IncarnationLength:]))
			vals = append(vals, common.CopyBytes(v))
			return nil
		}); err != nil {
			return err
		}
		i := 0
		if err = changeset.StorageChangeSetPlainBytes(expectedStorageChanges).Walk(func(k, v []byte) error {
			if !equal {
				return nil
			}
			if i >= len(addrs) {
				equal = false
				return nil
			}
			if !bytes.Equal(k[:common.AddressLength], addrs[i]) {
				equal = false
				return nil
			}
			if !bytes.Equal(k[common.AddressLength+common.IncarnationLength:], keys[i]) {
				equal = false
				return nil
			}
			if !bytes.Equal(v, vals[i]) {
				equal = false
				return nil
			}
			i++
			return nil
		}); err != nil {
			return err
		}
	}
	if !equal {
		fmt.Printf("Unexpected storage changes in block %d\nIn the database: ======================\n", blockNum)
		if err = changeset.StorageChangeSetPlainBytes(dbStorageChanges).Walk(func(k, v []byte) error {
			fmt.Printf("0x%x: [%x]\n", k, v)
			return nil
		}); err != nil {
			return err
		}
		fmt.Printf("Expected: ==========================\n")
		if err = changeset.StorageChangeSetPlainBytes(expectedStorageChanges).Walk(func(k, v []byte) error {
			fmt.Printf("0x%x: [%x]\n", k, v)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}
	return nil
}
