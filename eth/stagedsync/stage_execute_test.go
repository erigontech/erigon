package stagedsync

import (
	"testing"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
)

func TestUnwindExecutionStagePlainStatic(t *testing.T) {
	_, tx1 := ethdb.NewTestTx(t)
	_, tx2 := ethdb.NewTestTx(t)

	generateBlocks(t, 1, 50, plainWriterGen(tx1), staticCodeStaticIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), staticCodeStaticIncarnations)

	err := stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true}, nil)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket, dbutils.ContractTEVMCodeBucket)
}

func TestUnwindExecutionStagePlainWithIncarnationChanges(t *testing.T) {
	_, tx1 := ethdb.NewTestTx(t)
	_, tx2 := ethdb.NewTestTx(t)

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true}, nil)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func TestUnwindExecutionStagePlainWithCodeChanges(t *testing.T) {
	t.Skip("not supported yet, to be restored")
	_, tx1 := ethdb.NewTestTx(t)
	_, tx2 := ethdb.NewTestTx(t)

	generateBlocks(t, 1, 50, plainWriterGen(tx1), changeCodeIndepenentlyOfIncarnations)
	generateBlocks(t, 1, 100, plainWriterGen(tx2), changeCodeIndepenentlyOfIncarnations)

	err := stages.SaveStageProgress(tx2, stages.Execution, 100)
	if err != nil {
		t.Errorf("error while saving progress: %v", err)
	}
	u := &UnwindState{Stage: stages.Execution, UnwindPoint: 50}
	s := &StageState{Stage: stages.Execution, BlockNumber: 100}
	err = UnwindExecutionStage(u, s, tx2, nil, ExecuteBlockCfg{writeReceipts: true}, nil)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}
