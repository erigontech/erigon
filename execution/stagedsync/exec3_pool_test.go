package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const poolTestTxs = 64

func loadPoolFixture(t *testing.T) *blockreplay.Fixture {
	t.Helper()
	fx, err := blockreplay.Load("../blockreplay/testdata/block-25604144.gob")
	require.NoError(t, err)
	return fx
}

const counterTxGas = 26010

var counterAddr = common.HexToAddress("0x00000000000000000000000000000000000000d3")

// counterChainFixture rewrites the committed mainnet block into n calls from
// distinct senders to a contract that increments storage slot 0, so every tx
// reads the value its predecessor writes. Each call costs counterTxGas.
func counterChainFixture(t *testing.T, n int) *blockreplay.Fixture {
	t.Helper()
	fx := loadPoolFixture(t)
	orig, err := fx.Block()
	require.NoError(t, err)
	header := orig.HeaderNoCopy()
	template := fx.Accounts[fx.Senders[0]]
	to := counterAddr
	code := []byte{0x5f, 0x54, 0x60, 0x01, 0x01, 0x5f, 0x55, 0x00}
	contract := template
	contract.Nonce = 1
	contract.CodeHash = crypto.Keccak256Hash(code)
	fx.Accounts[to] = contract
	fx.Code[to] = code
	fx.Storage[to] = map[[32]byte][32]byte{{}: uint256.NewInt(1).Bytes32()}
	gasPrice := new(uint256.Int).Mul(header.BaseFee, uint256.NewInt(2))

	txs := make([]types.Transaction, n)
	receipts := make([]*types.Receipt, n)
	senders := make([][20]byte, n)
	for i := range txs {
		sender := common.BigToAddress(uint256.NewInt(uint64(0x5e0000 + i)).ToBig())
		senders[i] = sender
		acct := template
		acct.Nonce = 0
		acct.Balance = uint256.NewInt(1_000_000_000_000_000_000).Bytes32()
		fx.Accounts[sender] = acct
		tx := &types.LegacyTx{
			CommonTx: types.CommonTx{To: &to, GasLimit: 50000, V: *uint256.NewInt(27), R: *uint256.NewInt(1), S: *uint256.NewInt(1)},
			GasPrice: *gasPrice,
		}
		tx.SetSender(accounts.InternAddress(sender))
		txs[i] = tx
		receipts[i] = &types.Receipt{Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: uint64(i+1) * counterTxGas}
	}
	h := types.CopyHeader(header)
	h.GasUsed = uint64(n) * counterTxGas
	zero := uint64(0)
	h.BlobGasUsed = &zero
	block := types.NewBlock(h, txs, nil, receipts, orig.Withdrawals(), nil)

	var buf bytes.Buffer
	require.NoError(t, block.EncodeRLP(&buf))
	fx.BlockRLP = buf.Bytes()
	fx.Senders = senders
	fx.Outputs = nil
	return fx
}

func runPoolReplay(t *testing.T, ctx context.Context, fx *blockreplay.Fixture, workers string, check func(*state.ReaderV3)) error {
	t.Helper()
	t.Setenv("EPHEMERAL_WORKERS", workers)
	r, closeFn := setupEphemeralReplay(t, fx)
	defer closeFn()
	r.ctx = ctx
	tx, doms := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()
	if err := r.exec(tx, doms); err != nil {
		return err
	}
	if check != nil {
		check(state.NewReaderV3(doms.AsGetter(tx)))
	}
	return nil
}

type poolCounts struct {
	opens   atomic.Int64
	mu      sync.Mutex
	maxRuns int
}

func (c *poolCounts) runs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxRuns
}

func countPool(t *testing.T) *poolCounts {
	t.Helper()
	c := &poolCounts{}
	poolOnRoTxOpen = func() { c.opens.Add(1) }
	poolOnRun = func(runs int) {
		c.mu.Lock()
		c.maxRuns = max(c.maxRuns, runs)
		c.mu.Unlock()
	}
	t.Cleanup(func() { poolOnRoTxOpen, poolOnRun = nil, nil })
	return c
}

func TestPoolReadTxsBoundedByWorkers(t *testing.T) {
	const workers = 4
	fx := loadPoolFixture(t)
	b, err := fx.Block()
	require.NoError(t, err)
	require.Greater(t, len(b.Transactions()), 10*workers)

	c := countPool(t)
	require.NoError(t, runPoolReplay(t, context.Background(), fx, "4", nil))
	opens := c.opens.Load()
	require.Positive(t, opens, "no pool worker opened a read tx: the run measured nothing")
	require.LessOrEqual(t, opens, int64(workers+1), "pool opened more read txs than it has workers (%d txs in the block)", len(b.Transactions()))
}

// A task's first poolNamedReruns re-runs wait for their named writer; every later re-run
// waits for the predecessor to commit and so reads committed state: a tx runs at most
// poolNamedReruns+2 times, however long the dependent chain. A re-run that never waits for
// the predecessor scales with the chain towards the incarnation limit (tasks+8) and fails the block.
func TestPoolDependencyChainRunsBoundedByNamedReruns(t *testing.T) {
	fx := counterChainFixture(t, poolTestTxs)
	for _, workers := range []string{"4", "16"} {
		c := countPool(t)
		require.NoError(t, runPoolReplay(t, context.Background(), fx, workers, func(r *state.ReaderV3) {
			counter, _, err := r.ReadAccountStorage(accounts.InternAddress(counterAddr), accounts.InternKey(common.Hash{}))
			require.NoError(t, err)
			require.Equal(t, uint64(poolTestTxs+1), counter.Uint64(), "the chain lost an increment with %s workers", workers)
		}))
		require.Positive(t, c.opens.Load(), "no pool worker ran: the run measured nothing")
		require.LessOrEqual(t, c.runs(), poolNamedReruns+2, "a tx in a %d-call chain ran more than %d times with %s workers", poolTestTxs, poolNamedReruns+2, workers)
	}
}

func TestPoolReexecSignalIsLatched(t *testing.T) {
	be := &blockExecutor{poolTasks: make([]*poolTask, 1)}
	be.signalSelfLoopReexec(0)
	pt := &poolTask{}
	be.poolTasks[0] = pt

	be.signalSelfLoopReexec(0)
	be.signalSelfLoopReexec(0)
	require.Equal(t, taskRerunRequested, pt.state.Load(), "two signals before the task looks must latch as one request")

	pt.state.Store(taskFinished)
	be.signalSelfLoopReexec(0)
	require.Equal(t, taskFinished, pt.state.Load(), "a finished task must ignore the signal")
}

func TestPoolShutdownMidBlockDoesNotHang(t *testing.T) {
	fx := loadPoolFixture(t)
	t.Setenv("EPHEMERAL_WORKERS", "4")
	r, closeFn := setupEphemeralReplay(t, fx)
	t.Cleanup(closeFn)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = ctx
	tx, doms := r.newDomains(t, fx)
	t.Cleanup(tx.Rollback)
	t.Cleanup(doms.Close)
	c := countPool(t)
	poolOnRoTxOpen = func() {
		c.opens.Add(1)
		cancel()
	}
	done := make(chan error, 1)
	go func() { done <- r.exec(tx, doms) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("ExecV3 did not return within 30s after workersCtx was cancelled mid-block")
	}
	require.Positive(t, c.opens.Load(), "no pool worker ran before the cancel: the run measured nothing")
}

// poolUnit runs f in a synctest bubble on an executor with one block of the given
// task count. slots are the pool's slots, in channel order.
func poolUnit(t *testing.T, tasks int, slots []*exec.WorkerContext, f func(t *testing.T, pe *parallelExecutor, be *blockExecutor)) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		pe := &parallelExecutor{}
		pe.workersCtx = ctx
		pe.results = make(chan *exec.TxResult, 8)
		pe.pool = newTaskPool(pe, slots)
		be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(1), nil, discardConsumers(1), false, nil)
		be.pool = pe.pool
		be.committedFrontier.Store(-1)
		be.poolTasks = make([]*poolTask, tasks)
		be.writers = make([]writerGate, tasks)
		for i := range tasks {
			be.tasks = append(be.tasks, &execTask{Task: &exec.TxTask{TxIndex: i}, index: i})
		}
		f(t, pe, be)
		cancel()
		pe.runWG.Wait()
	})
}

func newPoolUnitTask(be *blockExecutor, i int) *poolTask {
	pt := &poolTask{be: be, tv: &taskVersion{execTask: be.tasks[i], version: state.Version{BlockNum: 1, TxIndex: i}}}
	be.poolTasks[i] = pt
	return pt
}

func requireOperational(t *testing.T, pe *parallelExecutor, msg string) *exec.TxResult {
	t.Helper()
	select {
	case r := <-pe.results:
		require.True(t, r.Operational, "result is not operational: %v", r.Err)
		require.ErrorContains(t, r.Err, msg)
		return r
	default:
		require.FailNow(t, "no operational result was sent", "want an error containing %q", msg)
		return nil
	}
}

func TestPoolPublishedTaskReleasesItsGoroutine(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		require.False(t, pt.afterPublish(pe.pool), "a task with no re-exec request must free its goroutine")
		require.Equal(t, taskReleased, pt.state.Load())
	})
}

func TestPoolSignalBeforePublishReruns(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		be.signalSelfLoopReexec(2)
		res := make(chan bool, 1)
		go func() { res <- pt.afterPublish(pe.pool) }()
		synctest.Wait()
		require.Empty(t, res, "a re-exec signal must wait for the predecessor to commit")
		be.signalCommitted(1)
		synctest.Wait()
		require.True(t, <-res, "a signal that arrived before publish must keep the goroutine")
		require.Equal(t, state.Incarnation(1), pt.tv.version.Incarnation)
		require.Equal(t, taskRunning, pt.state.Load())
	})
}

func TestPoolSignalResumesReleasedTaskAfterPredecessorCommit(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		pt.state.Store(taskReleased)
		be.signalSelfLoopReexec(2)
		synctest.Wait()
		require.Equal(t, state.Incarnation(0), pt.tv.version.Incarnation, "the re-run must wait for the predecessor to commit")
		be.signalCommitted(1)
		synctest.Wait()
		require.Equal(t, state.Incarnation(1), pt.tv.version.Incarnation)
	})
}

func TestPoolIncarnationLimitIsOperational(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 1)
		limit := len(be.tasks) + 8
		pt.tv.version.Incarnation = state.Incarnation(limit - 1)

		require.True(t, pt.bumpInc(pe.pool), "incarnation %d is within the limit %d", limit, limit)
		require.Empty(t, pe.results)

		require.False(t, pt.bumpInc(pe.pool), "incarnation %d is past the limit %d", limit+1, limit)
		requireOperational(t, pe, "exceeded the self-loop incarnation limit")
	})
}

func TestPoolRerunWaitsForPredecessorAfterNamedReruns(t *testing.T) {
	for _, tc := range []struct{ reruns, target int }{{0, 0}, {poolNamedReruns - 1, 0}, {poolNamedReruns, 1}} {
		poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
			pt := newPoolUnitTask(be, 2)
			pt.reruns = tc.reruns
			res := make(chan bool, 1)
			go func() { res <- pt.rerun(pe.pool, 0) }()
			synctest.Wait()
			require.Empty(t, res, "the task must wait, not re-run in place")
			if tc.target == 1 {
				be.signalCommitted(0)
				synctest.Wait()
				require.Empty(t, res, "after %d named re-runs the named writer is not enough", tc.reruns)
			}
			be.signalCommitted(tc.target)
			synctest.Wait()
			require.True(t, <-res, "after %d named re-runs", tc.reruns)
			require.Equal(t, state.Incarnation(1), pt.tv.version.Incarnation)
		})
	}
}

func TestPoolWaitCommittedSeesAFireBeforeItBlocks(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		be.signalCommitted(1)
		require.True(t, pt.waitCommitted(pe.pool, 1))
	})
}

type failingRoDB struct {
	kv.TemporalRwDB
	err error
}

func (d failingRoDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) { return nil, d.err }

func TestPoolRoTxOpenFailureIsOperational(t *testing.T) {
	poolUnit(t, 3, []*exec.WorkerContext{nil}, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		boom := errors.New("read tx limit")
		pe.cfg.db = failingRoDB{err: boom}
		pt := newPoolUnitTask(be, 1)

		pe.pool.push(pt)
		synctest.Wait()
		r := requireOperational(t, pe, "open roTx")
		require.ErrorIs(t, r.Err, boom)
	})
}

type fakeRoTx struct{ kv.TemporalTx }

func TestPoolRunPanicIsOperationalAndReturnsSlot(t *testing.T) {
	poolUnit(t, 3, []*exec.WorkerContext{nil}, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		<-pe.pool.slots
		pt := newPoolUnitTask(be, 1)

		pt.run(pe.pool, poolSlot{tx: fakeRoTx{}})
		requireOperational(t, pe, "self-loop worker panic (block 1 tx 1)")
		require.Len(t, pe.pool.slots, 1, "a panicking task must return its slot")
	})
}

func newParkContext(t *testing.T) *exec.WorkerContext {
	t.Helper()
	return exec.NewWorkerContext(t.Context(), true, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
}

func TestPoolParkGivesUpTheSlotAndTakesOneBack(t *testing.T) {
	poolUnit(t, 3, []*exec.WorkerContext{nil}, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		<-pe.pool.slots
		tx := fakeRoTx{}
		own := newParkContext(t)
		pt := newPoolUnitTask(be, 2)
		pt.slot = poolSlot{tx: tx, w: own}
		res := make(chan bool, 1)
		go func() { res <- be.parkReader(2, 1, func() int { return 1 }) }()
		synctest.Wait()

		require.Empty(t, res, "the reader must block")
		require.Len(t, pe.pool.slots, 1, "a parked reader must give its slot back")
		given := <-pe.pool.slots
		require.Equal(t, kv.TemporalTx(tx), given.tx)
		require.Nil(t, given.w, "the parked EVM stays on its context")

		other := newParkContext(t)
		pe.pool.slots <- poolSlot{tx: tx, w: other}
		be.fireWriter(1)
		synctest.Wait()
		require.True(t, <-res)
		require.Same(t, own, pt.slot.w)
		require.Equal(t, kv.TemporalTx(tx), pt.slot.tx)
		require.Equal(t, []*exec.WorkerContext{other}, pe.pool.spare, "the context of the slot taken back must be reusable")
	})
}

func TestPoolParkSkipsWaitWhenCellAlreadyResolved(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		require.True(t, be.parkReader(2, 1, func() int { return state.UnknownDep }))
		require.Nil(t, pt.slot.tx)
		require.Equal(t, 0, cap(pe.pool.slots))
	})
}

// The named writer has already fired, and a different writer now holds the cell. Waiting
// on the named writer's gate would never end, so the reader must re-read and park on the new one.
func TestPoolParkReReadsWhenAnotherWriterNowHoldsTheCell(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		pt := newPoolUnitTask(be, 2)
		require.True(t, be.parkReader(2, 0, func() int { return 1 }), "the reader must re-read, not wait on a writer that will not fire")
		require.Nil(t, pt.slot.tx)
	})
}

func TestPoolParkReturnsFalseOnShutdownHoldingASlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pe := &parallelExecutor{}
		pe.workersCtx = ctx
		pe.pool = newTaskPool(pe, []*exec.WorkerContext{nil})
		be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(1), nil, discardConsumers(1), false, nil)
		be.pool = pe.pool
		be.poolTasks = make([]*poolTask, 2)
		be.writers = make([]writerGate, 2)
		for i := range 2 {
			be.tasks = append(be.tasks, &execTask{Task: &exec.TxTask{TxIndex: i}, index: i})
		}
		<-pe.pool.slots
		tx := fakeRoTx{}
		pt := newPoolUnitTask(be, 1)
		pt.slot = poolSlot{tx: tx, w: newParkContext(t)}
		res := make(chan bool, 1)
		go func() { res <- be.parkReader(1, 0, func() int { return 0 }) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.False(t, <-res)
		require.Equal(t, kv.TemporalTx(tx), pt.slot.tx, "the run continues until the EVM sees the cancel, so it needs its tx")
		pe.runWG.Wait()
	})
}

func TestPoolBlockEndAndSystemTasksDoNotPark(t *testing.T) {
	poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
		be.tasks[0] = &execTask{Task: &exec.TxTask{TxIndex: -1}, index: 0}
		be.tasks[1] = &execTask{Task: &exec.TxTask{TxIndex: 0}, index: 1}
		be.tasks[2] = &execTask{Task: &exec.TxTask{TxIndex: 1, Txs: types.Transactions{nil}}, index: 2}
		blocked := func() int { t.Fatal("must not check a cell it will not wait on"); return 0 }
		require.False(t, be.parkReader(-1, -2, blocked), "the block-init task must not park")
		require.False(t, be.parkReader(1, 0, blocked), "the block-end task must not park")
	})
}

func TestPoolBALSeededBlockMatchesUnseeded(t *testing.T) {
	prevAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = prevAssert })
	fx := loadPoolFixture(t)
	require.NotNil(t, fx.Outputs)
	t.Setenv("EPHEMERAL_WORKERS", "4")
	r, closeFn := setupEphemeralReplay(t, fx)
	t.Cleanup(closeFn)
	r.cfg.experimentalBAL = true
	var captured types.BlockAccessList
	r.cfg.SetBALSink(func(_ uint64, bal types.BlockAccessList) { captured = bal })

	derive := func(seed types.BlockAccessList) types.BlockAccessList {
		captured = nil
		tx, doms := r.newDomains(t, fx)
		defer tx.Rollback()
		defer doms.Close()
		require.NoError(t, r.execSeeded(tx, doms, seed))
		require.NoError(t, r.verify(tx, doms, fx.Outputs))
		require.NotNil(t, captured, "no BAL derived")
		return captured
	}
	unseeded := derive(nil)
	seeded := derive(unseeded)
	require.Equal(t, unseeded.Hash(), seeded.Hash(), "executing against a seeded versionMap changed the derived BAL")
}

func TestPoolSettleWaitsForReadTargetUnlessBAL(t *testing.T) {
	for _, hasBAL := range []bool{false, true} {
		poolUnit(t, 3, nil, func(t *testing.T, pe *parallelExecutor, be *blockExecutor) {
			be.versionMap.HasBAL = hasBAL
			pt := newPoolUnitTask(be, 2)
			type out struct {
				ok, valid bool
			}
			res := make(chan out, 1)
			go func() {
				ok, valid, _ := pt.settle(pe.pool, &exec.TxResult{}, 0)
				res <- out{ok, valid}
			}()
			synctest.Wait()
			if !hasBAL {
				require.Empty(t, res, "a non-BAL block must wait for the read target to commit")
				be.signalCommitted(0)
				synctest.Wait()
			}
			require.Equal(t, out{true, true}, <-res)
		})
	}
}
