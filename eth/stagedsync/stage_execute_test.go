package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
)

func TestExec(t *testing.T) {
	ctx := context.Background()
	db1, db2 := memdb.NewTestDB(t), memdb.NewTestDB(t)
	cfg := ExecuteBlockCfg{}

	t.Run("UnwindExecutionStagePlainStatic", func(t *testing.T) {
		require := require.New(t)
		tx1, err := db1.BeginRw(ctx)
		require.NoError(err)
		defer tx1.Rollback()
		tx2, err := db2.BeginRw(ctx)
		require.NoError(err)
		defer tx2.Rollback()

		generateBlocks(t, 1, 25, plainWriterGen(tx1), staticCodeStaticIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), staticCodeStaticIncarnations)

		err = stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode, kv.ContractTEVMCode)
	})
	t.Run("UnwindExecutionStagePlainWithIncarnationChanges", func(t *testing.T) {
		require := require.New(t)
		tx1, err := db1.BeginRw(ctx)
		require.NoError(err)
		defer tx1.Rollback()
		tx2, err := db2.BeginRw(ctx)
		require.NoError(err)
		defer tx2.Rollback()

		generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeWithIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

		err = stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})
	t.Run("UnwindExecutionStagePlainWithCodeChanges", func(t *testing.T) {
		t.Skip("not supported yet, to be restored")
		require := require.New(t)
		tx1, _ := db1.BeginRw(ctx)
		defer tx1.Rollback()
		tx2, _ := db2.BeginRw(ctx)
		defer tx2.Rollback()

		generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		if err != nil {
			t.Errorf("error while saving progress: %v", err)
		}
		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})

	t.Run("PruneExecution", func(t *testing.T) {
		require := require.New(t)
		tx, err := db1.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()

		generateBlocks(t, 1, 20, plainWriterGen(tx), changeCodeIndepenentlyOfIncarnations)
		err = stages.SaveStageProgress(tx, stages.Execution, 20)
		require.NoError(err)

		available, err := changeset.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)

		s := &PruneState{ID: stages.Execution, ForwardProgress: 20}
		// check pruning distance > than current stage progress
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(100), Receipts: prune.Distance(101), CallTraces: prune.Distance(200)}}, ctx, false)
		require.NoError(err)

		available, err = changeset.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)
		available, err = changeset.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)

		// pruning distance, first run
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(5),
			Receipts: prune.Distance(10), CallTraces: prune.Distance(15)}}, ctx, false)
		require.NoError(err)

		available, err = changeset.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
		available, err = changeset.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)

		// pruning distance, second run
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(5),
			Receipts: prune.Distance(15), CallTraces: prune.Distance(25)}}, ctx, false)
		require.NoError(err)

		available, err = changeset.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
		available, err = changeset.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
	})
}

func apply(tx kv.RwTx, agg *libstate.Aggregator22, rs *state.State22, stateWriter *state.StateWriter22) func(blockNumber uint64) {
	agg.SetTx(tx)
	return func(blockNumber uint64) {
		txTask := &state.TxTask{
			Header:     nil,
			BlockNum:   blockNumber,
			Rules:      params.TestRules,
			Block:      nil,
			TxNum:      blockNumber,
			TxIndex:    0,
			Final:      true,
			WriteLists: stateWriter.WriteSet(),
		}
		txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
		if err := rs.Apply(tx, txTask, agg); err != nil {
			panic(err)
		}
	}
}

func TestExec22(t *testing.T) {
	ctx := context.Background()
	db1, db2 := memdb.NewTestDB(t), memdb.NewTestDB(t)
	tmpDir := t.TempDir()
	agg, err := libstate.NewAggregator22(tmpDir, ethconfig.HistoryV2AggregationStep)
	require.NoError(t, err)
	err = agg.ReopenFiles()
	require.NoError(t, err)
	cfg := ExecuteBlockCfg{exec22: true, agg: agg}
	rs := state.NewState22()
	stateWriter := state.NewStateWriter22(rs)

	t.Run("UnwindExecutionStagePlainStatic", func(t *testing.T) {
		require := require.New(t)
		tx1, err := db1.BeginRw(ctx)
		require.NoError(err)
		defer tx1.Rollback()
		tx2, err := db2.BeginRw(ctx)
		require.NoError(err)
		defer tx2.Rollback()

		for i := uint64(0); i < 50; i++ {
			err = rawdb.TxNums.Append(tx1, i, i)
			require.NoError(err)
			err = rawdb.TxNums.Append(tx2, i, i)
			require.NoError(err)
		}

		generateBlocks2(t, 1, 25, stateWriter, apply(tx1, agg, rs, stateWriter), staticCodeStaticIncarnations)
		err = rs.Flush(tx1)
		require.NoError(err)

		generateBlocks2(t, 1, 50, stateWriter, apply(tx2, agg, rs, stateWriter), staticCodeStaticIncarnations)
		err = rs.Flush(tx2)
		require.NoError(err)

		err = stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})
	//t.Run("UnwindExecutionStagePlainWithIncarnationChanges", func(t *testing.T) {
	//	require := require.New(t)
	//	tx1, err := db1.BeginRw(ctx)
	//	require.NoError(err)
	//	defer tx1.Rollback()
	//	tx2, err := db2.BeginRw(ctx)
	//	require.NoError(err)
	//	defer tx2.Rollback()
	//
	//	for i := uint64(0); i < 50; i++ {
	//		err = rawdb.TxNums.Append(tx1, i, i)
	//		require.NoError(err)
	//		err = rawdb.TxNums.Append(tx2, i, i)
	//		require.NoError(err)
	//	}
	//
	//	generateBlocks2(t, 1, 25, stateWriter, apply(tx1, agg, rs, stateWriter), changeCodeWithIncarnations)
	//	err = rs.Flush(tx1)
	//	require.NoError(err)
	//
	//	generateBlocks2(t, 1, 50, stateWriter, apply(tx2, agg, rs, stateWriter), changeCodeWithIncarnations)
	//	err = rs.Flush(tx2)
	//	require.NoError(err)
	//
	//	err = stages.SaveStageProgress(tx2, stages.Execution, 50)
	//	require.NoError(err)
	//
	//	u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
	//	s := &StageState{ID: stages.Execution, BlockNumber: 50}
	//	err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
	//	require.NoError(err)
	//	tx2.ForEach(kv.PlainState, nil, func(k, v []byte) error {
	//		if len(k) == 20 {
	//			fmt.Printf("b1: %x, %x\n", k, v)
	//		} else {
	//			fmt.Printf("b2: %x, %x\n", k, v)
	//		}
	//		return nil
	//	})
	//
	//	compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	//})
	//t.Run("UnwindExecutionStagePlainWithCodeChanges", func(t *testing.T) {
	//	t.Skip("not supported yet, to be restored")
	//	require := require.New(t)
	//	tx1, _ := db1.BeginRw(ctx)
	//	defer tx1.Rollback()
	//	tx2, _ := db2.BeginRw(ctx)
	//	defer tx2.Rollback()
	//
	//	generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
	//	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)
	//
	//	err := stages.SaveStageProgress(tx2, stages.Execution, 50)
	//	if err != nil {
	//		t.Errorf("error while saving progress: %v", err)
	//	}
	//	u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
	//	s := &StageState{ID: stages.Execution, BlockNumber: 50}
	//	err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
	//	require.NoError(err)
	//
	//	compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	//})
	//
	//t.Run("PruneExecution", func(t *testing.T) {
	//	require := require.New(t)
	//	tx, err := db1.BeginRw(ctx)
	//	require.NoError(err)
	//	defer tx.Rollback()
	//
	//	generateBlocks(t, 1, 20, plainWriterGen(tx), changeCodeIndepenentlyOfIncarnations)
	//	err = stages.SaveStageProgress(tx, stages.Execution, 20)
	//	require.NoError(err)
	//
	//	available, err := changeset.AvailableFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(1), available)
	//
	//	s := &PruneState{ID: stages.Execution, ForwardProgress: 20}
	//	cfgCopy := cfg
	//	cfgCopy.prune = prune.Mode{History: prune.Distance(100), Receipts: prune.Distance(101), CallTraces: prune.Distance(200)}
	//	// check pruning distance > than current stage progress
	//	err = PruneExecutionStage(s, tx, cfgCopy, ctx, false)
	//	require.NoError(err)
	//
	//	available, err = changeset.AvailableFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(1), available)
	//	available, err = changeset.AvailableStorageFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(1), available)
	//
	//	// pruning distance, first run
	//	cfgCopy = cfg
	//	cfgCopy.prune = prune.Mode{History: prune.Distance(5),
	//		Receipts: prune.Distance(10), CallTraces: prune.Distance(15)}
	//	err = PruneExecutionStage(s, tx, cfgCopy, ctx, false)
	//	require.NoError(err)
	//
	//	available, err = changeset.AvailableFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(15), available)
	//	available, err = changeset.AvailableStorageFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(15), available)
	//
	//	// pruning distance, second run
	//	cfgCopy = cfg
	//	cfgCopy.prune = prune.Mode{History: prune.Distance(5),
	//		Receipts: prune.Distance(15), CallTraces: prune.Distance(25)}
	//	err = PruneExecutionStage(s, tx, cfgCopy, ctx, false)
	//	require.NoError(err)
	//
	//	available, err = changeset.AvailableFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(15), available)
	//	available, err = changeset.AvailableStorageFrom(tx)
	//	require.NoError(err)
	//	require.Equal(uint64(15), available)
	//})
}
