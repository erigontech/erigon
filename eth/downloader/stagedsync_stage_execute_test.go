package downloader

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"

	"github.com/stretchr/testify/assert"
)

const (
	staticCodeStaticIncarnations         = iota // no incarnation changes, no code changes
	changeCodeWithIncarnations                  // code changes with incarnation
	changeCodeIndepenentlyOfIncarnations        // code changes with and without incarnation
)

func TestUnwindExecutionStageHashedStatic(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 50, hashedWriterGen(initialDb), staticCodeStaticIncarnations)

	mutation := ethdb.NewMemDatabase()
	generateBlocks(t, 1, 100, hashedWriterGen(mutation), staticCodeStaticIncarnations)

	SaveStageProgress(mutation, Execution, 100)
	err := unwindExecutionStage(50, mutation)
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

	SaveStageProgress(mutation, Execution, 100)
	err := unwindExecutionStage(50, mutation)
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

	SaveStageProgress(mutation, Execution, 100)
	err := unwindExecutionStage(50, mutation)
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

	SaveStageProgress(mutation, Execution, 100)
	core.UsePlainStateExecution = true
	err := unwindExecutionStage(50, mutation)
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

	SaveStageProgress(mutation, Execution, 100)
	core.UsePlainStateExecution = true
	err := unwindExecutionStage(50, mutation)
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

	SaveStageProgress(mutation, Execution, 100)
	core.UsePlainStateExecution = true
	err := unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func generateBlocks(t *testing.T, from uint64, numberOfBlocks uint64, stateWriterGen stateWriterGen, difficulty int) {
	ctx := context.Background()
	acc1 := accounts.NewAccount()
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

		var oldValue common.Hash
		var newValue common.Hash
		newValue[0] = 1
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

	assert.Equal(t, bucket1, bucket2)
}

type stateWriterGen func(uint64) state.WriterWithChangeSets

func hashedWriterGen(db ethdb.Database) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewDbStateWriter(db, db, blockNum, make(map[common.Address]uint64))
	}
}

func plainWriterGen(db ethdb.Database) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewPlainStateWriter(db, db, blockNum, make(map[common.Address]uint64))
	}
}
