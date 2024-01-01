package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/datadir"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
)

func TestExec(t *testing.T) {
	if ethconfig.EnableHistoryV4InTest {
		t.Skip()
	}
	logger := log.New()
	tmp := t.TempDir()
	_, db1, _ := temporal.NewTestDB(t, datadir.New(tmp), nil)
	_, db2, _ := temporal.NewTestDB(t, datadir.New(tmp), nil)

	ctx := context.Background()
	cfg := ExecuteBlockCfg{}

	t.Run("UnwindExecutionStagePlainStatic", func(t *testing.T) {
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		generateBlocks(t, 1, 25, plainWriterGen(tx1), staticCodeStaticIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), staticCodeStaticIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false, logger)
		require.NoError(err)

		compareCurrentState(t, newAgg(t, logger), tx1, tx2, kv.PlainState, kv.PlainContractCode, kv.ContractTEVMCode)
	})
	t.Run("UnwindExecutionStagePlainWithIncarnationChanges", func(t *testing.T) {
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeWithIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false, logger)
		require.NoError(err)

		compareCurrentState(t, newAgg(t, logger), tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})
	t.Run("UnwindExecutionStagePlainWithCodeChanges", func(t *testing.T) {
		t.Skip("not supported yet, to be restored")
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		if err != nil {
			t.Errorf("error while saving progress: %v", err)
		}
		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false, logger)
		require.NoError(err)

		compareCurrentState(t, newAgg(t, logger), tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})

	t.Run("PruneExecution", func(t *testing.T) {
		require, tx := require.New(t), memdb.BeginRw(t, db1)

		generateBlocks(t, 1, 20, plainWriterGen(tx), changeCodeIndepenentlyOfIncarnations)
		err := stages.SaveStageProgress(tx, stages.Execution, 20)
		require.NoError(err)

		available, err := historyv2.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)

		s := &PruneState{ID: stages.Execution, ForwardProgress: 20}
		// check pruning distance > than current stage progress
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(100), Receipts: prune.Distance(101), CallTraces: prune.Distance(200)}}, ctx, false)
		require.NoError(err)

		available, err = historyv2.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)
		available, err = historyv2.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(1), available)

		// pruning distance, first run
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(5),
			Receipts: prune.Distance(10), CallTraces: prune.Distance(15)}}, ctx, false)
		require.NoError(err)

		available, err = historyv2.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
		available, err = historyv2.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)

		// pruning distance, second run
		err = PruneExecutionStage(s, tx, ExecuteBlockCfg{prune: prune.Mode{History: prune.Distance(5),
			Receipts: prune.Distance(15), CallTraces: prune.Distance(25)}}, ctx, false)
		require.NoError(err)

		available, err = historyv2.AvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
		available, err = historyv2.AvailableStorageFrom(tx)
		require.NoError(err)
		require.Equal(uint64(15), available)
	})
}

func apply(tx kv.RwTx, logger log.Logger) (beforeBlock, afterBlock testGenHook, w state.StateWriter) {
	domains := libstate.NewSharedDomains(tx, logger)

	rs := state.NewStateV3(domains, logger)
	stateWriter := state.NewStateWriterBufferedV3(rs)
	stateWriter.SetTx(tx)

	return func(n, from, numberOfBlocks uint64) {
			stateWriter.SetTxNum(context.Background(), n)
			stateWriter.ResetWriteSet()
		}, func(n, from, numberOfBlocks uint64) {
			txTask := &state.TxTask{
				BlockNum:   n,
				Rules:      params.TestRules,
				TxNum:      n,
				TxIndex:    0,
				Final:      true,
				WriteLists: stateWriter.WriteSet(),
			}
			txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
			rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			if err := rs.ApplyState4(context.Background(), txTask); err != nil {
				panic(err)
			}
			_, err := rs.Domains().ComputeCommitment(context.Background(), true, txTask.BlockNum, "")
			if err != nil {
				panic(err)
			}

			if n == from+numberOfBlocks-1 {
				if err := domains.Flush(context.Background(), tx); err != nil {
					panic(err)
				}
			}
		}, stateWriter
}

func newAgg(t *testing.T, logger log.Logger) *libstate.AggregatorV3 {
	t.Helper()
	dirs, ctx := datadir.New(t.TempDir()), context.Background()
	agg, err := libstate.NewAggregatorV3(ctx, dirs, ethconfig.HistoryV3AggregationStep, nil, logger)
	require.NoError(t, err)
	err = agg.OpenFolder(false)
	require.NoError(t, err)
	return agg
}
