package stagedsync

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestUnwindExecutionStagePlainStatic(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), staticCodeStaticIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), staticCodeStaticIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100, nil)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, true)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithIncarnationChanges(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100, nil)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	core.UsePlainStateExecution = true
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, true)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithCodeChanges(t *testing.T) {
	t.Skip("not supported yet, to be restored")
	db1 := ethdb.NewMemDatabase()
	defer db1.Close()
	tx1, err := db1.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	tx2, err := db2.Begin(context.Background(), ethdb.RW)
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)

	err = stages.SaveStageProgress(tx2, stages.Execution, 100, nil)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, true)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}
