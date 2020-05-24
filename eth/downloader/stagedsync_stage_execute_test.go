package downloader

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"

	"github.com/stretchr/testify/assert"
)

func TestUnwindExecutionStageHashed(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()
	generateBlocks(t, 0, 50, hashedWriterGen(initialDb))

	mutation := initialDb.NewBatch()
	generateBlocks(t, 50, 50, hashedWriterGen(mutation))

	SaveStageProgress(mutation, Execution, 100)
	err := unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestUnwindExecutionStagePlain(t *testing.T) {
	initialDb := ethdb.NewMemDatabase()

	generateBlocks(t, 0, 50, plainWriterGen(initialDb))

	mutation := initialDb.NewBatch()
	generateBlocks(t, 50, 50, plainWriterGen(mutation))

	SaveStageProgress(mutation, Execution, 100)
	core.UsePlainStateExecution = true
	err := unwindExecutionStage(50, mutation)
	if err != nil {
		t.Errorf("error while unwinding state: %v", err)
	}

	compareCurrentState(t, initialDb, mutation, dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket)
}

func generateBlocks(t *testing.T, from uint64, numberOfBlocks int, stateWriterGen stateWriterGen) {
	t.Error("implement me")
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
