package stagedsync

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
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

func TestExecutorStopCancelsWarmup(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	defer dbg.SetReadAhead(oldReadAhead)
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
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			readAheader := exec.NewBlockReadAheader()
			pe, tx := newResumeTestExec(t, db, chain.AllProtocolChanges)
			pe.cfg.readAheader = readAheader
			pe.workerCount = 1
			ctx, cancel := context.WithCancel(t.Context())
			defer func() {
				cancel()
				readAheader.WaitForWarmup(context.Background())
			}()
			warmupDB := &blockedWarmupDB{started: make(chan context.Context, 1)}
			readAheader.AddHeaderAndBody(ctx, warmupDB, tx,
				&types.Header{Number: *uint256.NewInt(1)},
				&types.Body{Transactions: []types.Transaction{&types.AccessListTx{}}})
			warmupCtx := <-warmupDB.started
			execCtx, finish, err := pe.run(t.Context())
			defer func() { require.NoError(t, finish(nil)) }()
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
