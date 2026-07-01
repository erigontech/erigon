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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
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
// The test drives the real parallel executor (ExecV3, parallel=true) and mirrors
// runStage's classification each cycle, capping the count so the test itself
// cannot hang: if the error stays ErrLoopExhausted-classified every cycle the cap
// trips; when the real error surfaces on the first cycle, the loop stops.
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

	const injectedErrMsg = "chaos monkey: simulated pre-dispatch failure (snapshot step misalignment)"
	disarm := chaos_monkey.ArmPreExecutionError(errors.New(injectedErrMsg))
	defer disarm()

	syncCfg := m.Cfg().Sync
	syncCfg.ChaosMonkey = true
	execCfg := stagedsync.StageExecuteBlocksCfg(
		m.DB, m.Cfg().Prune, m.Cfg().BatchSize, m.ChainConfig, m.Engine, &vm.Config{},
		m.Notifications, m.Cfg().StateStream, false /*badBlockHalt*/, m.Dirs, m.BlockReader,
		m.Cfg().Genesis, syncCfg, false /*experimentalBAL*/, exec.NewBlockReadAheader(),
	)

	// One stage-loop cycle on a fresh tx/doms. Zero progress is made (the fault
	// fires before dispatch), so the deferred rollback discards it and the next
	// cycle re-opens clean — exactly what RunLoop's CommitCycle does.
	runCycle := func() error {
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

	// RunLoop would spin unbounded on the pre-fix bug; cap it so the test itself
	// cannot hang. Any completed cycle beyond the first means the loop wasn't stopped.
	const maxCycles = 5
	var (
		cyclesRun int
		surfaced  error
		stopped   bool
	)
	for cyclesRun < maxCycles && !stopped {
		cyclesRun++
		execErr := runCycle()

		var exhausted *stagedsync.ErrLoopExhausted
		if errors.As(execErr, &exhausted) {
			continue // runStage → moreWork=true; RunLoop re-runs Execution — the spin
		}
		surfaced = execErr // real error (or nil) → hasMore=false → RunLoop stops
		stopped = true
	}

	require.True(t, stopped,
		"pre-dispatch executeBlocks failure stayed classified as ErrLoopExhausted for all %d cycles: "+
			"sync.go:runStage keeps returning moreWork=true and PipelineExecutor.RunLoop re-runs Execution "+
			"forever with zero progress — the silent infinite loop this test guards against", maxCycles)
	require.Equal(t, 1, cyclesRun, "the fix must surface the error on the first cycle, not loop")
	require.Error(t, surfaced, "the executeBlocks failure must surface as a hard error")
	require.ErrorContains(t, surfaced, "simulated pre-dispatch failure",
		"the surfaced error must be the injected pre-dispatch fault")

	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(surfaced, &exhausted),
		"the surfaced error must not carry ErrLoopExhausted, or sync.go:runStage would loop")
}
