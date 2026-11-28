package stagedsync

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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

// takenDuringCall reports whether every pool read tx of the next call was opened
// while the cache was empty, so the call ran on the workers it took.
func takenDuringCall(t *testing.T, r *ephemeralReplay, onOpen func()) func() bool {
	t.Helper()
	var opened, cacheHeld atomic.Bool
	poolOnRoTxOpen = func() {
		opened.Store(true)
		if len(idleWorkers(r)) > 0 {
			cacheHeld.Store(true)
		}
		if onOpen != nil {
			onOpen()
		}
	}
	t.Cleanup(func() { poolOnRoTxOpen = nil })
	return func() bool { return opened.Load() && !cacheHeld.Load() }
}

// runWithoutFunds runs the block over a parent where the first tx's sender has no
// balance, so a worker that kept the first run's account reads lets the block pass.
func runWithoutFunds(t *testing.T, r *ephemeralReplay, fx *blockreplay.Fixture) error {
	t.Helper()
	tx, doms, _ := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()
	signer := types.MakeSigner(r.cfg.chainConfig, r.num, r.block.Time())
	sender, err := signer.Sender(r.block.Transactions()[0])
	require.NoError(t, err)
	acc, err := state.NewReaderV3(doms.AsGetter(tx)).ReadAccountData(sender)
	require.NoError(t, err)
	require.NotNil(t, acc)
	acc.Balance.Clear()
	addr := sender.Value()
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(acc), ephemeralSeedTxNum, nil))
	return r.exec(tx, doms)
}

func TestExecV3ReusesWorkersOverARewoundState(t *testing.T) {
	r, fx := newReuseReplay(t)
	require.NoError(t, runOnParent(t, r, fx))
	workers := idleWorkers(r)
	require.Len(t, workers, 5)

	ranOnCached := takenDuringCall(t, r, nil)
	require.ErrorContains(t, runWithoutFunds(t, r, fx), "insufficient funds", "a reused worker read the first run's sender balance")
	require.True(t, ranOnCached(), "the second call did not run on the cached workers")
	require.True(t, slices.Equal(workers, idleWorkers(r)), "the second call did not reuse the first call's workers")

	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, slices.Equal(workers, idleWorkers(r)))
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
	ranOnCached := takenDuringCall(t, r, nil)
	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, ranOnCached(), "the call after an invalid block did not run on the cached workers")
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
	tx, doms, _ := r.newDomains(t, fx)
	src := &singleBlockSource{block: r.block, num: r.num, parent: r.parent}
	out, err := ExecV3(r.ctx, r.cfg, doms, tx, stages.ModeApplyingBlocks, false, "replay", r.rng, src, r.logger)
	doms.Close()
	tx.Rollback()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, out.verdict, "a cancelled call gave a block verdict")
	require.Positive(t, c.opens.Load(), "no pool worker ran before the cancel")
	workers := idleWorkers(r)
	require.NotEmpty(t, workers)

	r.ctx = context.Background()
	ranOnCached := takenDuringCall(t, r, nil)
	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, ranOnCached(), "the call after a cancel did not run on the cached workers")
	require.True(t, slices.Equal(workers, idleWorkers(r)))
}

func TestWorkerStartupErrorKeepsIdleWorkers(t *testing.T) {
	pe, _ := newResumeTestExec(t, newResumeTestDB(t), chain.AllProtocolChanges)
	pe.workerCount = 1
	pe.cfg.workers = &ExecWorkerCache{}
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

func TestExecV3ConfigsSharingACacheReuseOneWorkerSet(t *testing.T) {
	r, fx := newReuseReplay(t)
	shared := &ExecWorkerCache{}
	other := r.cfg
	other.workers = &ExecWorkerCache{}
	r.cfg = r.cfg.ShareWorkers(shared)
	other = other.ShareWorkers(shared)

	require.NoError(t, runOnParent(t, r, fx))
	workers := idleWorkers(r)
	require.Len(t, workers, 5)

	r.cfg = other
	ranOnCached := takenDuringCall(t, r, nil)
	require.NoError(t, runOnParent(t, r, fx))
	require.True(t, ranOnCached(), "the second config did not run on the set the first one parked")
	require.True(t, slices.Equal(workers, idleWorkers(r)))
}
