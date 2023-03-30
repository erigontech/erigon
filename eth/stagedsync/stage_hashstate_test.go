package stagedsync

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func TestPromoteHashedStateClearState(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	historyV3 := false
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, dirs, historyV3, nil), context.Background())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage, kv.ContractCode)
}

func TestPromoteHashedStateIncremental(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	historyV3 := false
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	cfg := StageHashStateCfg(db2, dirs, historyV3, nil)
	err := PromoteHashedStateCleanly("logPrefix", tx2, cfg, context.Background())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	generateBlocks(t, 51, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err = promoteHashedStateIncrementally("logPrefix", 50, 101, tx2, cfg, context.Background(), false /* quiet */)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
}

func TestPromoteHashedStateIncrementalMixed(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	historyV3 := false
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 100, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, hashedWriterGen(tx2), changeCodeWithIncarnations)
	generateBlocks(t, 51, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := promoteHashedStateIncrementally("logPrefix", 50, 101, tx2, StageHashStateCfg(db2, dirs, historyV3, nil), context.Background(), false /* quiet */)
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
}

func TestUnwindHashed(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	historyV3 := false
	_, tx1 := memdb.NewTestTx(t)
	db2, tx2 := memdb.NewTestTx(t)

	generateBlocks(t, 1, 50, hashedWriterGen(tx1), changeCodeWithIncarnations)
	generateBlocks(t, 1, 50, plainWriterGen(tx2), changeCodeWithIncarnations)

	err := PromoteHashedStateCleanly("logPrefix", tx2, StageHashStateCfg(db2, dirs, historyV3, nil), context.Background())
	if err != nil {
		t.Errorf("error while promoting state: %v", err)
	}
	u := &UnwindState{UnwindPoint: 50}
	s := &StageState{BlockNumber: 100}
	err = unwindHashStateStageImpl("logPrefix", u, s, tx2, StageHashStateCfg(db2, dirs, historyV3, nil), context.Background())
	if err != nil {
		t.Errorf("error while unwind state: %v", err)
	}

	compareCurrentState(t, tx1, tx2, kv.HashedAccounts, kv.HashedStorage)
}

func TestPromoteIncrementallyShutdown(t *testing.T) {
	historyV3 := false

	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, libcommon.ErrStopped},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelFuncExec {
				cancel()
			}
			db, tx := memdb.NewTestTx(t)
			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			if err := promoteHashedStateIncrementally("logPrefix", 1, 10, tx, StageHashStateCfg(db, dirs, historyV3, nil), ctx, false /* quiet */); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown promoteHashedStateIncrementally, got: %v, expected: %v", err, tc.errExp)
			}
		})

	}

}

func TestPromoteHashedStateCleanlyShutdown(t *testing.T) {
	historyV3 := false

	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, context.Canceled},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			ctx, cancel := context.WithCancel(context.Background())

			defer cancel()
			if tc.cancelFuncExec {
				cancel()
			}

			db, tx := memdb.NewTestTx(t)

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)

			if err := PromoteHashedStateCleanly("logPrefix", tx, StageHashStateCfg(db, dirs, historyV3, nil), ctx); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown promoteHashedStateCleanly , got: %v, expected: %v", err, tc.errExp)
			}

		})
	}
}

func TestUnwindHashStateShutdown(t *testing.T) {
	historyV3 := false
	tt := []struct {
		name           string
		cancelFuncExec bool
		errExp         error
	}{
		{"cancel", true, libcommon.ErrStopped},
		{"no cancel", false, nil},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelFuncExec {
				cancel()
			}

			db, tx := memdb.NewTestTx(t)

			generateBlocks(t, 1, 10, plainWriterGen(tx), changeCodeWithIncarnations)
			cfg := StageHashStateCfg(db, dirs, historyV3, nil)
			err := PromoteHashedStateCleanly("logPrefix", tx, cfg, ctx)
			if tc.cancelFuncExec {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}

			u := &UnwindState{UnwindPoint: 5}
			s := &StageState{BlockNumber: 10}
			if err = unwindHashStateStageImpl("logPrefix", u, s, tx, cfg, ctx); !errors.Is(err, tc.errExp) {
				t.Errorf("error does not match expected error while shutdown unwindHashStateStageImpl, got: %v, expected: %v", err, tc.errExp)
			}

		})
	}

}
