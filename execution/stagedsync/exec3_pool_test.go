package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/blockreplay"
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
	to := common.HexToAddress("0x00000000000000000000000000000000000000d3")
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

func runPoolReplay(t *testing.T, ctx context.Context, fx *blockreplay.Fixture, workers string) error {
	t.Helper()
	t.Setenv("EPHEMERAL_WORKERS", workers)
	r, closeFn := setupEphemeralReplay(t, fx)
	defer closeFn()
	r.ctx = ctx
	tx, doms := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()
	return r.exec(tx, doms)
}

func resetPoolCounters() {
	poolRoTxOpens.Store(0)
	poolMaxRuns.Store(0)
}

func TestPoolReadTxsBoundedByWorkers(t *testing.T) {
	const workers = 4
	fx := loadPoolFixture(t)
	b, err := fx.Block()
	require.NoError(t, err)
	require.Greater(t, len(b.Transactions()), 10*workers)

	resetPoolCounters()
	require.NoError(t, runPoolReplay(t, context.Background(), fx, "4"))
	opens := poolRoTxOpens.Load()
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
		resetPoolCounters()
		require.NoError(t, runPoolReplay(t, context.Background(), fx, workers))
		require.Positive(t, poolRoTxOpens.Load(), "no pool worker ran: the run measured nothing")
		require.LessOrEqual(t, poolMaxRuns.Load(), int64(poolNamedReruns+2), "a tx in a %d-call chain ran more than %d times with %s workers", poolTestTxs, poolNamedReruns+2, workers)
	}
}

func TestPoolReexecSignalRequeuesParkedTask(t *testing.T) {
	pe := &parallelExecutor{}
	pe.workersCtx = t.Context()
	pe.pool = newTaskPool(pe, nil)
	be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(1), nil, discardConsumers(1), false, nil)
	be.pool = pe.pool
	pt := &poolTask{be: be, tv: &taskVersion{execTask: &execTask{index: 0}}, next: psReexec, parked: true}
	be.poolTasks = []*poolTask{pt}

	be.signalSelfLoopReexec(0)
	require.False(t, pt.parked, "the re-exec signal left the task parked")
	require.Equal(t, 1, pe.pool.queue.Len(), "the re-exec signal did not queue the task on the pool")
	require.Same(t, pt, pe.pool.pop())

	be.signalSelfLoopReexec(0)
	require.True(t, pt.pending, "a signal for a running task must latch")
	require.Equal(t, 0, pe.pool.queue.Len(), "a signal for a running task queued it a second time")

	pt.fin = true
	pt.pending = false
	be.signalSelfLoopReexec(0)
	require.False(t, pt.pending, "a finalized task must ignore the signal")
	require.Equal(t, 0, pe.pool.queue.Len())
}

func TestPoolShutdownMidBlockDoesNotHang(t *testing.T) {
	fx := loadPoolFixture(t)
	resetPoolCounters()
	t.Setenv("EPHEMERAL_WORKERS", "4")
	r, closeFn := setupEphemeralReplay(t, fx)
	t.Cleanup(closeFn)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = ctx
	tx, doms := r.newDomains(t, fx)
	t.Cleanup(tx.Rollback)
	t.Cleanup(doms.Close)
	go func() {
		for poolRoTxOpens.Load() == 0 {
			time.Sleep(50 * time.Microsecond)
		}
		cancel()
	}()
	done := make(chan error, 1)
	go func() { done <- r.exec(tx, doms) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		fmt.Fprintln(os.Stderr, "FAIL: ExecV3 did not return within 30s after workersCtx was cancelled mid-block")
		os.Exit(1)
	}
	require.Positive(t, poolRoTxOpens.Load(), "no pool worker ran before the cancel: the run measured nothing")
}

func newPoolUnit(t *testing.T, tasks int) (*parallelExecutor, *blockExecutor) {
	t.Helper()
	pe := &parallelExecutor{}
	pe.workersCtx = t.Context()
	pe.results = make(chan *exec.TxResult, 8)
	pe.pool = newTaskPool(pe, nil)
	be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(1), nil, discardConsumers(1), false, nil)
	be.pool = pe.pool
	be.versionMap = state.NewVersionMap(nil)
	be.wakeAt = map[int][]*poolTask{}
	be.committedFrontier.Store(-1)
	be.poolTasks = make([]*poolTask, tasks)
	be.runInc = make([]atomic.Int64, tasks)
	for i := range tasks {
		be.tasks = append(be.tasks, &execTask{Task: &exec.TxTask{TxIndex: i}, index: i})
	}
	return pe, be
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

func TestPoolStepConsumesSignalLatchedBeforePark(t *testing.T) {
	for _, pending := range []bool{true, false} {
		pe, be := newPoolUnit(t, 3)
		pt := newPoolUnitTask(be, 2)
		pt.next = psRecheck
		pt.result = &exec.TxResult{}
		pt.pending = pending

		pt.step(pe.pool, nil)

		require.Len(t, pe.results, 1, "the validated result was not sent")
		if pending {
			require.False(t, pt.parked, "a task parked although a re-exec signal arrived before it parked")
			require.False(t, pt.pending, "the latched signal was not consumed")
			require.Equal(t, []*poolTask{pt}, be.wakeAt[1], "the task was not queued to re-run after its predecessor commits")
		} else {
			require.True(t, pt.parked, "a task with no signal must park")
			require.Empty(t, be.wakeAt)
		}
	}
}

func TestPoolSignalWaitsForPredecessorCommit(t *testing.T) {
	pe, be := newPoolUnit(t, 3)
	pt := newPoolUnitTask(be, 2)
	pt.next = psReexec
	pt.parked = true

	be.signalSelfLoopReexec(2)
	require.Equal(t, 0, pe.pool.queue.Len(), "a re-exec signal queued the task before its predecessor committed")
	require.Equal(t, []*poolTask{pt}, be.wakeAt[1])

	be.committedFrontier.Store(1)
	be.signalCommitted(1)
	require.Equal(t, 1, pe.pool.queue.Len(), "the predecessor's commit did not queue the task")
}

func TestPoolIncarnationLimitIsOperational(t *testing.T) {
	pe, be := newPoolUnit(t, 3)
	pt := newPoolUnitTask(be, 1)
	limit := len(be.tasks) + 8
	pt.tv.version.Incarnation = state.Incarnation(limit - 1)

	require.True(t, pt.bumpInc(pe.pool), "incarnation %d is within the limit %d", limit, limit)
	require.Empty(t, pe.results)

	require.False(t, pt.bumpInc(pe.pool), "incarnation %d is past the limit %d", limit+1, limit)
	requireOperational(t, pe, "exceeded the self-loop incarnation limit")
	require.True(t, pt.aborted)
}

type failingRoDB struct {
	kv.TemporalRwDB
	err error
}

func (d failingRoDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) { return nil, d.err }

func TestPoolRoTxOpenFailureIsOperational(t *testing.T) {
	pe, be := newPoolUnit(t, 3)
	boom := errors.New("read tx limit")
	pe.cfg.db = failingRoDB{err: boom}
	pt := newPoolUnitTask(be, 1)

	require.False(t, pt.run(pe.pool, &poolWorker{}))
	r := requireOperational(t, pe, "open roTx")
	require.ErrorIs(t, r.Err, boom)
}

func TestPoolStepPanicIsOperational(t *testing.T) {
	pe, be := newPoolUnit(t, 3)
	pt := newPoolUnitTask(be, 1)
	pt.next = psRecheck

	pt.step(pe.pool, nil)
	requireOperational(t, pe, "self-loop worker panic (block 1 tx 1)")
}

func TestPoolBALSeededBlockMatchesUnseeded(t *testing.T) {
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
		pe, be := newPoolUnit(t, 3)
		be.versionMap.HasBAL = hasBAL
		pt := newPoolUnitTask(be, 2)

		proceed := pt.settle(&exec.TxResult{}, 0)

		require.Equal(t, psRecheck, pt.next)
		require.Empty(t, pe.results)
		if hasBAL {
			require.True(t, proceed, "a BAL block must re-check at once, not wait for the read target")
			require.Empty(t, be.wakeAt)
		} else {
			require.False(t, proceed, "a non-BAL block must wait for the read target to commit")
			require.Equal(t, []*poolTask{pt}, be.wakeAt[0])
		}
	}
}
