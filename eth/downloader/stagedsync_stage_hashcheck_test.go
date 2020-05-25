package downloader

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestPromoteHashedStateClearState(t *testing.T) {
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)

	generateBlocks(t, 1, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 0)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	t.Skip("not implemented yet")
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 0)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	generateBlocks(t, 51, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 = db2.NewBatch()
	err = promoteHashedState(m2, 50)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	t.Skip("not implemented yet")
	db1 := ethdb.NewMemDatabase()
	db2 := ethdb.NewMemDatabase()

	generateBlocks(t, 1, 100, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(db1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(db2), changeCodeWithIncarnations)

	m2 := db2.NewBatch()
	err := promoteHashedState(m2, 50)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	_, err = m2.Commit()
	if err != nil {
		t.Errorf("error while commiting state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}
