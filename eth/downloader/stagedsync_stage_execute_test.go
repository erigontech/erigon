package downloader

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	staticCodeStaticIncarnations         = iota // no incarnation changes, no code changes
	changeCodeWithIncarnations                  // code changes with incarnation
	changeCodeIndepenentlyOfIncarnations        // code changes with and without incarnation
)

func TestUnwindExecutionStageHashedStatic(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 50, hashedWriterGen(initialDb), staticCodeStaticIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, hashedWriterGen(mutation), staticCodeStaticIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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
	generateBlocks(t, 50, hashedWriterGen(initialDb), changeCodeWithIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, hashedWriterGen(mutation), changeCodeWithIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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
	generateBlocks(t, 50, hashedWriterGen(initialDb), changeCodeIndepenentlyOfIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, hashedWriterGen(mutation), changeCodeIndepenentlyOfIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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
	generateBlocks(t, 50, plainWriterGen(initialDb), staticCodeStaticIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, plainWriterGen(mutation), staticCodeStaticIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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
	generateBlocks(t, 50, plainWriterGen(initialDb), changeCodeWithIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, plainWriterGen(mutation), changeCodeWithIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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
	generateBlocks(t, 50, plainWriterGen(initialDb), changeCodeIndepenentlyOfIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 100, plainWriterGen(mutation), changeCodeIndepenentlyOfIncarnations)

	err := SaveStageProgress(mutation, Execution, 100)
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

func generateBlocks(t *testing.T, numberOfBlocks uint64, stateWriterGen stateWriterGen, difficulty int) {
	from := uint64(1)
	ctx := context.Background()
	acc1 := accounts.NewAccount()
	acc1.Incarnation = 1
	acc := &acc1
	acc.Initialised = true
	var addr common.Address = common.HexToAddress("0x1234567890")
	acc.Balance.SetUint64(0)
	for blockNumber := from; blockNumber < from+numberOfBlocks; blockNumber++ {
		updateIncarnation := difficulty != staticCodeStaticIncarnations && blockNumber%10 == 0
		newAcc := acc.SelfCopy()
		newAcc.Balance.SetUint64(blockNumber)
		if updateIncarnation {
			newAcc.Incarnation = acc.Incarnation + 1
		}
		blockWriter := stateWriterGen(blockNumber)

		var oldValue, newValue uint256.Int
		newValue.SetOne()
		var location common.Hash
		location.SetBytes(big.NewInt(int64(blockNumber)).Bytes())

		if blockNumber == 1 {
			err := blockWriter.CreateContract(addr)
			if err != nil {
				t.Fatal(err)
			}
		}
		if blockNumber == 1 || updateIncarnation || difficulty == changeCodeIndepenentlyOfIncarnations {
			code := []byte(fmt.Sprintf("acc-code-%v", blockNumber))
			codeHash, _ := common.HashData(code)
			if err := blockWriter.UpdateAccountCode(addr, newAcc.Incarnation, codeHash, code); err != nil {
				t.Fatal(err)
			}
			newAcc.CodeHash = codeHash
		}
		if err := blockWriter.WriteAccountStorage(ctx, addr, newAcc.Incarnation, &location, &oldValue, &newValue); err != nil {
			t.Fatal(err)
		}
		if err := blockWriter.UpdateAccountData(ctx, addr, acc /* original */, newAcc /* new account */); err != nil {
			t.Fatal(err)
		}
		if err := blockWriter.WriteChangeSets(); err != nil {
			t.Fatal(err)
		}
		acc = newAcc
	}
}

func compareCurrentState(
	t *testing.T,
	db1 ethdb.Database,
	db2 ethdb.Database,
	buckets ...[]byte,
) {
	for _, bucket := range buckets {
		compareBucket(t, db1, db2, bucket)
	}
}

func compareBucket(t *testing.T, db1, db2 ethdb.Database, bucketName []byte) {
	var err error

	bucket1 := make(map[string][]byte)
	err = db1.Walk(bucketName, nil, 0, func(k, v []byte) (bool, error) {
		bucket1[string(k)] = v
		return true, nil
	})
	assert.Nil(t, err)

	bucket2 := make(map[string][]byte)
	err = db2.Walk(bucketName, nil, 0, func(k, v []byte) (bool, error) {
		bucket2[string(k)] = v
		return true, nil
	})
	assert.Nil(t, err)

	assert.Equal(t, bucket1 /*expected*/, bucket2 /*actual*/)
}

type stateWriterGen func(uint64) state.WriterWithChangeSets

func hashedWriterGen(db ethdb.Database) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewDbStateWriter(db, db, blockNum)
	}
}

func plainWriterGen(db ethdb.Database) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewPlainStateWriter(db, db, blockNum)
	}
}
