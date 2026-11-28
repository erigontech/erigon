package stagedsync

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/types"
)

func newReuseReplay(t *testing.T) (*ephemeralReplay, *blockreplay.Fixture) {
	t.Helper()
	fx := loadPoolFixture(t)
	require.NotNil(t, fx.Outputs)
	t.Setenv("EPHEMERAL_WORKERS", "4")
	r, closeFn := setupEphemeralReplay(t, fx)
	t.Cleanup(closeFn)
	return r, fx
}

// runOnParent executes r.block over a fresh copy of the parent state and checks the post-state.
func runOnParent(t *testing.T, r *ephemeralReplay, fx *blockreplay.Fixture) error {
	t.Helper()
	tx, doms, ws := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()
	if err := r.exec(tx, doms); err != nil {
		return err
	}
	return r.verify(tx, doms, ws, fx.Outputs)
}

func idleWorkers(r *ephemeralReplay) []*exec.WorkerContext {
	r.cfg.workers.mu.Lock()
	defer r.cfg.workers.mu.Unlock()
	return append([]*exec.WorkerContext(nil), r.cfg.workers.idle...)
}

func TestExecV3ReusesWorkersOverARewoundState(t *testing.T) {
	r, fx := newReuseReplay(t)
	require.NoError(t, runOnParent(t, r, fx))
	workers := idleWorkers(r)
	require.Len(t, workers, 5)

	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, slices.Equal(workers, idleWorkers(r)), "the second call did not reuse the first call's workers")
}

func TestExecV3ReusedWorkersAfterAnInvalidBlock(t *testing.T) {
	r, fx := newReuseReplay(t)
	valid := r.block
	h := types.CopyHeader(valid.HeaderNoCopy())
	h.GasUsed++
	r.block = valid.WithSeal(h)
	require.ErrorContains(t, runOnParent(t, r, fx), "gas used")
	workers := idleWorkers(r)
	require.NotEmpty(t, workers)

	r.block = valid
	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, slices.Equal(workers, idleWorkers(r)))
}

func TestExecV3ReusedWorkersAfterACancelledCall(t *testing.T) {
	r, fx := newReuseReplay(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.ctx = ctx
	c := countPool(t)
	poolOnRoTxOpen = func() {
		c.opens.Add(1)
		cancel()
	}
	require.Error(t, runOnParent(t, r, fx))
	require.Positive(t, c.opens.Load(), "no pool worker ran before the cancel")
	workers := idleWorkers(r)
	require.NotEmpty(t, workers)

	r.ctx = context.Background()
	poolOnRoTxOpen = nil
	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, slices.Equal(workers, idleWorkers(r)))
}

func TestWorkerStartupErrorKeepsIdleWorkers(t *testing.T) {
	pe, _ := newResumeTestExec(t, newResumeTestDB(t), chain.AllProtocolChanges)
	pe.workerCount = 1
	pe.cfg.workers = &execWorkerCache{}
	worker := exec.NewWorkerContext(t.Context(), true, nil, pe.cfg.db, pe.cfg.blockReader, pe.cfg.chainConfig, pe.cfg.genesis, pe.cfg.engine, pe.cfg.dirs, pe.logger)
	pe.cfg.workers.put([]*exec.WorkerContext{worker})
	t.Cleanup(func() { closeWorkers(pe.cfg.workers.take(1)) })

	pe.enableChaosMonkey = true
	failure := errors.New("worker startup failed")
	pe.chaosFaults.WorkerError = failure
	_, finish, err := pe.run(t.Context())
	require.ErrorIs(t, err, failure)
	finish(nil)

	require.Len(t, pe.cfg.workers.idle, 1, "a startup failure took the worker out of the cache")
	require.Same(t, worker, pe.cfg.workers.idle[0])
}
