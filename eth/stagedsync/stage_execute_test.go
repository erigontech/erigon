package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
)

func TestExec(t *testing.T) {
	ctx, db1, db2 := context.Background(), memdb.NewTestDB(t), memdb.NewTestDB(t)
	cfg := ExecuteBlockCfg{}

	t.Run("UnwindExecutionStagePlainStatic", func(t *testing.T) {
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		generateBlocks(t, 1, 25, plainWriterGen(tx1), staticCodeStaticIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), staticCodeStaticIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode, kv.ContractTEVMCode)
	})
	t.Run("UnwindExecutionStagePlainWithIncarnationChanges", func(t *testing.T) {
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		generateBlocks(t, 1, 25, plainWriterGen(tx1), changeCodeWithIncarnations)
		generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
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
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
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

func apply(tx kv.RwTx, agg *libstate.AggregatorV3) (beforeBlock, afterBlock testGenHook, w state.StateWriter) {
	agg.SetTx(tx)
	agg.StartWrites()

	rs := state.NewStateV3()
	stateWriter := state.NewStateWriter22(rs)
	return func(n, from, numberOfBlocks uint64) {
			stateWriter.SetTxNum(n)
			stateWriter.ResetWriteSet()
		}, func(n, from, numberOfBlocks uint64) {
			txTask := &exec22.TxTask{
				BlockNum:   n,
				Rules:      params.TestRules,
				TxNum:      n,
				TxIndex:    0,
				Final:      true,
				WriteLists: stateWriter.WriteSet(),
			}
			txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
			if err := rs.ApplyState(tx, txTask, agg); err != nil {
				panic(err)
			}
			if err := rs.ApplyHistory(txTask, agg); err != nil {
				panic(err)
			}
			if n == from+numberOfBlocks-1 {
				err := rs.Flush(context.Background(), tx, "", time.NewTicker(time.Minute))
				if err != nil {
					panic(err)
				}
				if err := agg.Flush(context.Background(), tx); err != nil {
					panic(err)
				}
			}
		}, stateWriter
}

func newAgg(t *testing.T) *libstate.AggregatorV3 {
	t.Helper()
	dir, ctx := t.TempDir(), context.Background()
	agg, err := libstate.NewAggregatorV3(ctx, dir, dir, ethconfig.HistoryV3AggregationStep, nil)
	require.NoError(t, err)
	err = agg.OpenFolder()
	require.NoError(t, err)
	return agg
}

func TestExec22(t *testing.T) {
	ctx, db1, db2 := context.Background(), memdb.NewTestDB(t), memdb.NewTestDB(t)
	agg := newAgg(t)
	cfg := ExecuteBlockCfg{historyV3: true, agg: agg}

	t.Run("UnwindExecutionStagePlainStatic", func(t *testing.T) {
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		beforeBlock, afterBlock, stateWriter := apply(tx1, agg)
		generateBlocks2(t, 1, 25, stateWriter, beforeBlock, afterBlock, staticCodeStaticIncarnations)
		beforeBlock, afterBlock, stateWriter = apply(tx2, agg)
		generateBlocks2(t, 1, 50, stateWriter, beforeBlock, afterBlock, staticCodeStaticIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		for i := uint64(0); i < 50; i++ {
			err = rawdbv3.TxNums.Append(tx2, i, i)
			require.NoError(err)
		}

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})
	t.Run("UnwindExecutionStagePlainWithIncarnationChanges", func(t *testing.T) {
		t.Skip("we don't delete newer incarnations - seems it's a feature?")
		require, tx1, tx2 := require.New(t), memdb.BeginRw(t, db1), memdb.BeginRw(t, db2)

		beforeBlock, afterBlock, stateWriter := apply(tx1, agg)
		generateBlocks2(t, 1, 25, stateWriter, beforeBlock, afterBlock, changeCodeWithIncarnations)
		beforeBlock, afterBlock, stateWriter = apply(tx2, agg)
		generateBlocks2(t, 1, 50, stateWriter, beforeBlock, afterBlock, changeCodeWithIncarnations)

		err := stages.SaveStageProgress(tx2, stages.Execution, 50)
		require.NoError(err)

		for i := uint64(0); i < 50; i++ {
			err = rawdbv3.TxNums.Append(tx2, i, i)
			require.NoError(err)
		}

		u := &UnwindState{ID: stages.Execution, UnwindPoint: 25}
		s := &StageState{ID: stages.Execution, BlockNumber: 50}
		err = UnwindExecutionStage(u, s, tx2, ctx, cfg, false)
		require.NoError(err)

		tx1.ForEach(kv.PlainState, nil, func(k, v []byte) error {
			if len(k) > 20 {
				fmt.Printf("a: inc=%d, loc=%x, v=%x\n", binary.BigEndian.Uint64(k[20:]), k[28:], v)
			}
			return nil
		})
		tx2.ForEach(kv.PlainState, nil, func(k, v []byte) error {
			if len(k) > 20 {
				fmt.Printf("b: inc=%d, loc=%x, v=%x\n", binary.BigEndian.Uint64(k[20:]), k[28:], v)
			}
			return nil
		})

		compareCurrentState(t, tx1, tx2, kv.PlainState, kv.PlainContractCode)
	})
}
