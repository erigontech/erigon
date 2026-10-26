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
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
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

func TestExecutorStopCancelsWarmup(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cause        error
		cancelWarmup bool
	}{
		{"reached max", &stopCause{kind: stopReachedMax}, false},
		{"more work", &stopCause{kind: stopMoreWork}, false},
		{"bad block", &stopCause{kind: stopBadBlock}, true},
		{"operational failure", &stopCause{kind: stopOperational, err: errors.New("database failed")}, true},
		{"untyped failure", errors.New("apply loop failed"), true},
		{"unknown stop", &stopCause{kind: stopKind(255)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pe, warmupCtx := newExecutorWithBlockedWarmup(t)
			execCtx, finish, err := pe.run(t.Context())
			defer func() {
				finish(nil)
				require.NoError(t, pe.wait())
				if !tc.cancelWarmup {
					require.NoError(t, warmupCtx.Err(), "normal teardown must preserve warmup")
				}
			}()
			require.NoError(t, err)

			pe.cancelExecLoop(tc.cause)

			require.ErrorIs(t, context.Cause(execCtx), tc.cause)
			if tc.cancelWarmup {
				require.ErrorIs(t, warmupCtx.Err(), context.Canceled)
			} else {
				require.NoError(t, warmupCtx.Err(), "normal stops preserve warmup for commitment")
			}
		})
	}
}

func TestExecutorFailureCancelsWarmup(t *testing.T) {
	failure := errors.New("executor failure")
	for name, faults := range map[string]chaos_monkey.Faults{
		"worker startup": {WorkerError: failure},
		"exec loop":      {ExecLoopPanic: failure},
	} {
		t.Run(name, func(t *testing.T) {
			pe, warmupCtx := newExecutorWithBlockedWarmup(t)
			pe.enableChaosMonkey = true
			pe.chaosFaults = faults
			execCtx, finish, err := pe.run(t.Context())
			defer func() {
				finish(nil)
				if faults.ExecLoopPanic != nil {
					require.ErrorContains(t, pe.wait(), failure.Error())
				}
				require.ErrorIs(t, warmupCtx.Err(), context.Canceled, "executor returned a failure with warmup still active")
			}()
			if faults.WorkerError != nil {
				require.ErrorIs(t, err, failure)
				return
			}
			require.NoError(t, err)
			select {
			case <-execCtx.Done():
				require.ErrorContains(t, context.Cause(execCtx), failure.Error())
			case <-time.After(5 * time.Second):
				t.Fatal("executor failure did not cancel its context")
			}
		})
	}
}
