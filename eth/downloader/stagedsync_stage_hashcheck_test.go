package downloader

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestPromoteHashedStateClearState(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	executeBlocks(db1, 50, getHashedStateWriter)

	executeBlocks(db2, 50, getPlainStateWriter)

	promoteHashedState(db2, 0)

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	executeBlocks(db1, 50, getHashedStateWriter)
	executeBlocks(db2, 50, getPlainStateWriter)
	promoteHashedState(db2, 0)

	executeBlocks(db1, 50, getHashedStateWriter)
	executeBlocks(db2, 50, getPlainStateWriter)

	promoteHashedState(db2, 50)

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	executeBlocks(db1, 100, getHashedStateWriter)
	executeBlocks(db2, 50, getHashedStateWriter)
	executeBlocks(db2, 50, getPlainStateWriter)

	promoteHashedState(db2, 50)

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func getHashedStateWriter(db ethdb.Database, blockNum uint64) state.WriterWithChangeSets {
	uncommitedIncarnations := make(map[common.Address]uint64)
	return state.NewDbStateWriter(db, db, blockNum, uncommitedIncarnations)
}

func getPlainStateWriter(db ethdb.Database, blockNum uint64) state.WriterWithChangeSets {
	uncommitedIncarnations := make(map[common.Address]uint64)
	return state.NewPlainStateWriter(db, db, blockNum, uncommitedIncarnations)
}

func executeBlocks(
	db ethdb.Database,
	numberOfBlocks uint64,
	getWriterFunc func(ethdb.Database, uint64) state.WriterWithChangeSets,
) {
	panic("implement me!")
}
