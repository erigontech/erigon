package stagedsync

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func getTmpDir() string {
	name, err := ioutil.TempDir("", "geth-tests-staged-sync")
	if err != nil {
		panic(err)
	}
	return name
}

func TestPromoteHashedStateClearState(t *testing.T) {
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

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
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

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	generateBlocks(t, 51, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = tx2.CommitAndBegin(context.Background())
	require.NoError(t, err)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 50}, 50, 101, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
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

	generateBlocks(t, 1, 100, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{}, 50, 101, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}

func TestUnwindHashed(t *testing.T) {
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

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	u := &UnwindState{UnwindPoint: 50}
	s := &StageState{BlockNumber: 100}
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.CurrentStateBucket)
}
