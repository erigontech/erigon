package stagedsync

import (
	"context"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/turbo-geth/common"
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
	err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket, dbutils.ContractCodeBucket)
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

	err = promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 50}, 50, 101, tx2, nil, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
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

	err = promoteHashedStateIncrementally("logPrefix", &StageState{}, 50, 101, tx2, nil, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	compareCurrentState(t, db1, db2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
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
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, nil, getTmpDir(), nil)
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Errorf("error while committing state: %v", err)
	}

	compareCurrentState(t, db1, db2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
}

func TestPromoteIncrementallyShutdown(t *testing.T) {

	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, common.ErrStopped},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelFuncExec {
				cancel()
			}
			db := ethdb.NewMemDatabase()
			defer db.Close()

			generateBlocks(t, 1, 10, plainWriterGen(db), changeCodeWithIncarnations)
			if err := promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 1}, 1, 10, db, nil, getTmpDir(), ctx.Done()); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown promoteHashedStateIncrementally, got: %v, expected: %v", err, tc.errExp)
			}
		})

	}

}

func TestPromoteHashedStateCleanlyShutdown(t *testing.T) {

	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, common.ErrStopped},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			defer cancel()
			if tc.cancelFuncExec {
				cancel()
			}

			db := ethdb.NewMemDatabase()
			defer db.Close()

			generateBlocks(t, 1, 10, plainWriterGen(db), changeCodeWithIncarnations)

			if err := PromoteHashedStateCleanly("logPrefix", db, getTmpDir(), ctx.Done()); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown promoteHashedStateCleanly , got: %v, expected: %v", err, tc.errExp)
			}

		})
	}
}

func TestUnwindHashStateShutdown(t *testing.T) {

	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, common.ErrStopped},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelFuncExec {
				cancel()
			}

			db := ethdb.NewMemDatabase()
			defer db.Close()

			generateBlocks(t, 1, 10, plainWriterGen(db), changeCodeWithIncarnations)

			err := PromoteHashedStateCleanly("logPrefix", db, getTmpDir(), nil)
			require.NoError(t, err)

			u := &UnwindState{UnwindPoint: 5}
			s := &StageState{BlockNumber: 10}
			if err = unwindHashStateStageImpl("logPrefix", u, s, db, nil, getTmpDir(), ctx.Done()); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown unwindHashStateStageImpl, got: %v, expected: %v", err, tc.errExp)
			}

		})
	}

}
