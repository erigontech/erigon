package stagedsync

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestUnwindExecutionStageHashedStatic(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, hashedWriterGen(initialDb), staticCodeStaticIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, hashedWriterGen(mutation), staticCodeStaticIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}

	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestUnwindExecutionStageHashedWithIncarnationChanges(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, hashedWriterGen(initialDb), changeCodeWithIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, hashedWriterGen(mutation), changeCodeWithIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestUnwindExecutionStageHashedWithCodeChanges(t *testing.T) {
	t.Skip("not supported yet, to be restored")
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, hashedWriterGen(initialDb), changeCodeIndepenentlyOfIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, hashedWriterGen(mutation), changeCodeIndepenentlyOfIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestUnwindExecutionStagePlainStatic(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, plainWriterGen(initialDb), staticCodeStaticIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, plainWriterGen(mutation), staticCodeStaticIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	core.UsePlainStateExecution = true
	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithIncarnationChanges(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, plainWriterGen(initialDb), changeCodeWithIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, plainWriterGen(mutation), changeCodeWithIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	core.UsePlainStateExecution = true
	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithCodeChanges(t *testing.T) {
	t.Skip("not supported yet, to be restored")
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, plainWriterGen(initialDb), changeCodeIndepenentlyOfIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, plainWriterGen(mutation), changeCodeIndepenentlyOfIncarnations)

	err := stages.SaveStageProgress(mutation, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	core.UsePlainStateExecution = true
	err = unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}
