package stagedsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestShouldWaitForReadAhead(t *testing.T) {
	oldReadAheadWait := dbg.ReadAheadWait
	t.Cleanup(func() { dbg.ReadAheadWait = oldReadAheadWait })
	dbg.ReadAheadWait = false
	require.False(t, shouldWaitForReadAhead(false))
	require.False(t, shouldWaitForReadAhead(true))
	dbg.ReadAheadWait = true
	require.False(t, shouldWaitForReadAhead(false))
	require.True(t, shouldWaitForReadAhead(true))
}

type blockedWarmupDB struct {
	kv.RoDB
	started chan context.Context
}

func (db *blockedWarmupDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	select {
	case db.started <- ctx:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func newExecutorWithBlockedWarmup(t *testing.T) (*parallelExecutor, context.Context) {
	t.Helper()
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead) })
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	readAheader := exec.NewBlockReadAheader()
	pe, tx := newResumeTestExec(t, db, chain.AllProtocolChanges)
	pe.cfg.readAheader = readAheader
	pe.workerCount = 1
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopWaiting()
		readAheader.WaitForWarmup(waitCtx)
		require.NoError(t, waitCtx.Err(), "warmup did not stop after cancellation")
	})
	warmupDB := &blockedWarmupDB{started: make(chan context.Context, 1)}
	readAheader.AddHeaderAndBody(ctx, warmupDB, tx,
		&types.Header{Number: *uint256.NewInt(1)},
		&types.Body{Transactions: []types.Transaction{&types.AccessListTx{}}})
	select {
	case warmupCtx := <-warmupDB.started:
		return pe, warmupCtx
	case <-time.After(5 * time.Second):
		t.Fatal("warmup did not start")
		return nil, nil
	}
}

func TestExecutorNormalStopPreservesWarmup(t *testing.T) {
	for _, kind := range []stopKind{stopReachedMax, stopMoreWork} {
		t.Run(kind.String(), func(t *testing.T) {
			pe, warmupCtx := newExecutorWithBlockedWarmup(t)
			execCtx, finish, err := pe.run(t.Context())
			defer func() {
				finish(nil)
				require.NoError(t, pe.wait())
				require.NoError(t, warmupCtx.Err(), "normal teardown must preserve warmup")
			}()
			require.NoError(t, err)

			cause := &stopCause{kind: kind}
			require.NoError(t, warmupCtx.Err(), "warmup must remain active before the stop")
			pe.cancelExecLoop(cause)

			require.ErrorIs(t, context.Cause(execCtx), cause)
			require.NoError(t, warmupCtx.Err(), "normal stops preserve warmup for commitment")
		})
	}
}

type failingStageReadDB struct {
	kv.TemporalRwDB
	ctx context.Context
	err error
}

func (db *failingStageReadDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	// Worker reads use a child context and must succeed to isolate setup failures.
	if ctx.Done() == db.ctx.Done() {
		return nil, db.err
	}
	return db.TemporalRwDB.BeginTemporalRo(ctx)
}

func TestExecutorFailureCancelsWarmup(t *testing.T) {
	failure := errors.New("executor failure")
	for _, tc := range []struct {
		name           string
		faults         chaos_monkey.Faults
		setupErr       error
		preserveWarmup bool
	}{
		{name: "worker startup", faults: chaos_monkey.Faults{WorkerError: failure}},
		{name: "exec loop", faults: chaos_monkey.Faults{ExecLoopPanic: failure}},
		{name: "commitment setup", setupErr: failure},
		{name: "cancelled setup", setupErr: context.Canceled, preserveWarmup: true},
		{name: "cancelled setup with failure", setupErr: errors.Join(context.Canceled, failure)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pe, warmupCtx := newExecutorWithBlockedWarmup(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = chaos_monkey.WithFaults(ctx, tc.faults)
			tx, err := pe.cfg.db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			cfg := pe.cfg
			cfg.vmConfig = &vm.Config{}
			cfg.syncCfg = ethconfig.Defaults.Sync
			cfg.syncCfg.ExecWorkerCount = 1
			cfg.blockReader = frozenBlockReader{}
			if tc.setupErr != nil {
				cfg.db = &failingStageReadDB{TemporalRwDB: cfg.db, ctx: ctx, err: tc.setupErr}
			}
			require.NoError(t, warmupCtx.Err(), "warmup must remain active before execution")
			out, err := ExecV3(ctx, cfg, pe.doms, tx, stages.ModeApplyingBlocks, true, "test",
				execRange{blockNum: 1, maxBlockNum: 1}, &singleBlockSource{done: true}, pe.logger)
			require.Nil(t, out.verdict)
			if tc.setupErr != nil {
				require.ErrorIs(t, err, tc.setupErr)
				require.ErrorContains(t, err, "commitmentCalculator: open roTx")
			} else {
				require.ErrorContains(t, err, failure.Error())
			}
			if tc.preserveWarmup {
				require.NoError(t, warmupCtx.Err(), "cancellation alone must preserve warmup")
			} else {
				require.ErrorIs(t, warmupCtx.Err(), context.Canceled, "executor returned a failure with warmup still active")
			}
		})
	}
}
