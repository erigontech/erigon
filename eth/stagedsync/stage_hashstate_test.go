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
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, getTmpDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket, dbutils.ContractCodeBucket)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	cfg := StageHashStateCfg(db2, getTmpDir())
	err = PromoteHashedStateCleanly("logPrefix", tx2, cfg, nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	generateBlocks(t, 51, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 50}, 50, 101, tx2, cfg, nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 100, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{}, 50, 101, tx2, StageHashStateCfg(db2, getTmpDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	compareCurrentState(t, tx1, tx2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
}

func TestUnwindHashed(t *testing.T) {
	db1 := ethdb.NewMemKV()
	defer db1.Close()
	tx1, err := db1.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx1.Rollback()

	db2 := ethdb.NewMemKV()
	defer db2.Close()
	tx2, err := db2.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx2.Rollback()

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, getTmpDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	u := &UnwindState{UnwindPoint: 50}
	s := &StageState{BlockNumber: 100}
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, StageHashStateCfg(db2, getTmpDir()), nil)
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, dbutils.HashedAccountsBucket, dbutils.HashedStorageBucket)
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
			db := ethdb.NewMemKV()
			defer db.Close()
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			if err := promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 1}, 1, 10, tx, StageHashStateCfg(db, getTmpDir()), ctx.Done()); !errors.Is(err, tc.errExp) {
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

			db := ethdb.NewMemKV()
			defer db.Close()
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)

			if err := PromoteHashedStateCleanly("logPrefix", tx, StageHashStateCfg(db, getTmpDir()), ctx.Done()); !errors.Is(err, tc.errExp) {
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

			db := ethdb.NewMemKV()
			defer db.Close()
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			cfg := StageHashStateCfg(db, getTmpDir())
			err = PromoteHashedStateCleanly("logPrefix", tx, cfg, nil)
			require.NoError(t, err)

			u := &UnwindState{UnwindPoint: 5}
			s := &StageState{BlockNumber: 10}
			if err = unwindHashStateStageImpl("logPrefix", u, s, tx, cfg, ctx.Done()); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown unwindHashStateStageImpl, got: %v, expected: %v", err, tc.errExp)
			}

		})
	}

}
