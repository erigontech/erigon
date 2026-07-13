// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package executiontests

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// TestParallelExec_PreDispatchFailure_SurfacesInsteadOfInfiniteLoop pins the
// error-surfacing invariant at the parallel exec/apply boundary: a failure that
// hits executeBlocks before it dispatches any block must reach the stage as a
// hard error, never be masked as ErrLoopExhausted.
//
// When executeBlocks fails pre-dispatch (a snapshot step-misalignment guard, a
// missing/nil block, or a BAL decode error — here injected by the chaos monkey),
// no block is dispatched: the apply loop returns ErrLoopExhausted{To:0} while
// pe.wait() returns the real error. If those are merged so that
// errors.Is(_, &ErrLoopExhausted{}) stays true, sync.go:runStage classifies the
// result as "more work" (moreWork=true) and PipelineExecutor.RunLoop's
// `for hasMore` re-runs Execution forever with zero progress and nothing logged.
//
// The test drives the real parallel executor (ExecV3, parallel=true) through the
// injected failure and asserts the surfaced error is the real one and is not
// classified as ErrLoopExhausted — the exact property that keeps runStage from
// looping.
func TestParallelExec_PreDispatchFailure_SurfacesInsteadOfInfiniteLoop(t *testing.T) {
	ctx := context.Background()

	// A real 1-block chain gives a fully consistent committed tip (block 1) with
	// real txNums for blocks 0 and 1 — chaos stays disarmed for this insert.
	m := execmoduletester.New(t)
	require.NoError(t, m.InsertChain(makeBlockChain(m.Genesis, 1, m, canonicalSeed)))

	// Give ExecV3 a block to execute past the committed tip. The pre-dispatch
	// fault fires before block 2 is read, so only its txNum is needed — no
	// header or body.
	const maxBlockNum = uint64(2)
	setupTx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer setupTx.Rollback() // safety net; no-op after the Commit below
	_, lastTxNum, err := m.BlockReader.TxnumReader().Last(setupTx)
	require.NoError(t, err)
	require.NoError(t, rawdbv3.TxNums.Append(setupTx, maxBlockNum, lastTxNum+2))
	require.NoError(t, setupTx.Commit())

	chaosErr := errors.New("chaos monkey: simulated pre-dispatch failure (snapshot step misalignment)")
	disarm := chaos_monkey.ArmPreExecutionError(chaosErr)
	defer disarm()

	err = runParallelExecV3(t, m, maxBlockNum)

	// The fix must surface the injected error. Classified as ErrLoopExhausted
	// instead, sync.go:runStage returns moreWork=true and PipelineExecutor.RunLoop
	// re-runs Execution forever with zero progress.
	require.ErrorIs(t, err, chaosErr,
		"the pre-dispatch failure must surface as a hard error, wrapping the original")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"pre-dispatch failure classified as ErrLoopExhausted → runStage loops forever with zero progress")
}

// tipWithUnexecutedBlock2 commits block 1 through the insert pipeline, then
// stores block 2 raw (header, body, canonical hash, txNums) WITHOUT executing
// it: ExecV3 gets a real block to dispatch past the committed tip, and the
// dispatch itself cannot fail and mask the fault under test.
func tipWithUnexecutedBlock2(t *testing.T) (*execmoduletester.ExecModuleTester, *types.Block) {
	t.Helper()
	ctx := context.Background()

	m := execmoduletester.New(t)
	chain := makeBlockChain(m.Genesis, 2, m, canonicalSeed)
	require.NoError(t, m.InsertChain(chain.Slice(0, 1)))

	b2 := chain.Blocks[1]
	setupTx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer setupTx.Rollback() // safety net; no-op after the Commit below
	require.NoError(t, rawdb.WriteHeader(setupTx, b2.Header()))
	require.NoError(t, rawdb.WriteBody(setupTx, b2.Hash(), b2.NumberU64(), b2.Body()))
	require.NoError(t, rawdb.WriteCanonicalHash(setupTx, b2.Hash(), b2.NumberU64()))
	_, lastTxNum, err := m.BlockReader.TxnumReader().Last(setupTx)
	require.NoError(t, err)
	require.NoError(t, rawdbv3.TxNums.Append(setupTx, b2.NumberU64(), lastTxNum+2))
	require.NoError(t, setupTx.Commit())
	return m, b2
}

// runParallelExecV3 drives the real parallel executor from the committed tip
// (block 1) to maxBlockNum, with the flag half of the chaos gate enabled.
func runParallelExecV3(t *testing.T, m *execmoduletester.ExecModuleTester, maxBlockNum uint64) error {
	t.Helper()
	ctx := context.Background()

	syncCfg := m.Cfg().Sync
	syncCfg.ChaosMonkey = true
	execCfg := stagedsync.StageExecuteBlocksCfg(
		m.DB, m.Cfg().Prune, m.Cfg().BatchSize, m.ChainConfig, m.Engine, &vm.Config{},
		m.Notifications, m.Cfg().StateStream, false /*badBlockHalt*/, m.Dirs, m.BlockReader,
		m.Cfg().Genesis, syncCfg, false /*experimentalBAL*/, exec.NewBlockReadAheader(),
	)

	rwTx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, rwTx, m.Log)
	require.NoError(t, err)
	defer doms.Close()

	s := &stagedsync.StageState{
		State:            m.Sync,
		ID:               stages.Execution,
		BlockNumber:      1,
		CurrentSyncCycle: stagedsync.CurrentSyncCycleInfo{IsInitialCycle: true}, // enables the chaos gate
	}
	return stagedsync.ExecV3(ctx, s, nil /*Unwinder*/, execCfg, doms, rwTx, true /*parallel*/, maxBlockNum, m.Log)
}

// TestParallelExec_WorkerPoolDeath_SurfacesInsteadOfHanging pins the worker-pool
// half of the error-surfacing invariant: when the OCC workers die (a worker
// panic — here injected by the chaos monkey at worker start), the batch must
// terminate with the worker's error, not starve. Without the worker pool joined
// into the executor errgroup, dead workers cancel only their own pool context:
// dispatched tasks are never consumed, the exec loop waits forever for results,
// and ExecV3 hangs — on a regression this test fails via the suite timeout with
// a goroutine dump showing the starved exec loop.
func TestParallelExec_WorkerPoolDeath_SurfacesInsteadOfHanging(t *testing.T) {
	// The armed fault kills each worker at Run entry, before it consumes a task.
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated worker panic")
	disarm := chaos_monkey.ArmWorkerError(chaosErr)
	defer disarm()

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorIs(t, err, chaosErr,
		"a dead worker pool must surface its error through the executor group")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"worker-pool death classified as ErrLoopExhausted → runStage retries forever with zero progress")
}

// TestParallelExec_ApplyLoopPanic_SurfacesInsteadOfCommitting pins the apply
// side of the panic-surfacing invariant: a recovered apply-loop panic must fail
// the batch. The apply loop owns post-execution validation while the exec loop
// and calculator complete state and commitment on their own, so a swallowed
// apply panic looks like a clean batch — checkBlocksDrained sees a fully
// drained map — and unvalidated blocks advance the stage as success.
func TestParallelExec_ApplyLoopPanic_SurfacesInsteadOfCommitting(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated apply-loop panic")
	disarm := chaos_monkey.ArmApplyLoopPanic(chaosErr)
	defer disarm()

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorContains(t, err, chaosErr.Error(),
		"a recovered apply-loop panic must fail the batch, not commit unvalidated blocks")
}

// TestParallelExec_ExecLoopPanic_SurfacesInsteadOfRetrying pins the exec side:
// a recovered exec-loop panic must fail the batch. A panic before any block is
// scheduled leaves pe.blockExecutors empty, so checkBlocksDrained cannot catch
// it; the apply loop's close fallback then manufactures a no-cause
// ErrLoopExhausted and runStage retries forever with zero progress.
func TestParallelExec_ExecLoopPanic_SurfacesInsteadOfRetrying(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated exec-loop panic")
	disarm := chaos_monkey.ArmExecLoopPanic(chaosErr)
	defer disarm()

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorContains(t, err, chaosErr.Error(),
		"a recovered exec-loop panic must surface as a hard error")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"exec-loop panic classified as ErrLoopExhausted → runStage retries forever with zero progress")
}
