package stagedsync

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
)

func TestPromoteHashedStateClearState(t *testing.T) {
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, t.TempDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage, kv.ContractCode)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	cfg := StageHashStateCfg(db2, t.TempDir())
	err := PromoteHashedStateCleanly("logPrefix", tx2, cfg, nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	generateBlocks(t, 51, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 50}, 50, 101, tx2, cfg, nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 100, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := promoteHashedStateIncrementally("logPrefix", &StageState{}, 50, 101, tx2, StageHashStateCfg(db2, t.TempDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
}

func TestUnwindHashed(t *testing.T) {
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, t.TempDir()), nil)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	u := &UnwindState{UnwindPoint: 50}
	s := &StageState{BlockNumber: 100}
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, StageHashStateCfg(db2, t.TempDir()), nil)
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
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
			db, tx := memdb.NewTestTx(t)
			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			if err := promoteHashedStateIncrementally("logPrefix", &StageState{BlockNumber: 1}, 1, 10, tx, StageHashStateCfg(db, t.TempDir()), ctx.Done()); !errors.Is(err, tc.errExp) {
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

			db, tx := memdb.NewTestTx(t)

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)

			if err := PromoteHashedStateCleanly("logPrefix", tx, StageHashStateCfg(db, t.TempDir()), ctx.Done()); !errors.Is(err, tc.errExp) {
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

			db, tx := memdb.NewTestTx(t)

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			cfg := StageHashStateCfg(db, t.TempDir())
			err := PromoteHashedStateCleanly("logPrefix", tx, cfg, nil)
			require.NoError(t, err)

			u := &UnwindState{UnwindPoint: 5}
			s := &StageState{BlockNumber: 10}
			if err = unwindHashStateStageImpl("logPrefix", u, s, tx, cfg, ctx.Done()); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown unwindHashStateStageImpl, got: %v, expected: %v", err, tc.errExp)
			}

		})
	}

}
