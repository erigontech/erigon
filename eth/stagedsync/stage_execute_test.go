package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestUnwindExecutionStagePlainStatic(t *testing.T) {
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), staticCodeStaticIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), staticCodeStaticIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true})
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithIncarnationChanges(t *testing.T) {
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true})
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithCodeChanges(t *testing.T) {
	t.Skip("not supported yet, to be restored")
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true})
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}
