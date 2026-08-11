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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/chaos_monkey"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// A pre-dispatch failure must surface as a hard error. Classifying it as
// ErrLoopExhausted would make the stage retry without progress.
func TestParallelExec_PreDispatchFailure_SurfacesInsteadOfInfiniteLoop(t *testing.T) {
	ctx := context.Background()

	m := execmoduletester.New(t)
	require.NoError(t, m.InsertChain(makeBlockChain(m.Genesis, 1, m, canonicalSeed)))

	// Only tx-number metadata is needed because the fault precedes the block read.
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
	t.Cleanup(disarm)

	err = runParallelExecV3(t, m, maxBlockNum)

	require.ErrorIs(t, err, chaosErr,
		"the pre-dispatch failure must surface as a hard error, wrapping the original")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"pre-dispatch failure classified as ErrLoopExhausted → runStage loops forever with zero progress")
}

// tipWithUnexecutedBlock2 creates a committed block-1 tip and stores block 2
// without executing it, giving fault-injection tests real work to dispatch.
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

// runParallelExecV3 runs one parallel execution batch with chaos hooks enabled.
func runParallelExecV3(t *testing.T, m *execmoduletester.ExecModuleTester, maxBlockNum uint64) error {
	t.Helper()
	return runParallelExecV3WithContext(t, context.Background(), m, maxBlockNum)
}

func runParallelExecV3WithContext(t *testing.T, ctx context.Context, m *execmoduletester.ExecModuleTester, maxBlockNum uint64) error {
	t.Helper()

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

type cancelOnTemporalRoDB struct {
	kv.TemporalRwDB
	done    <-chan struct{}
	cancel  context.CancelFunc
	blocked atomic.Bool
}

func (db *cancelOnTemporalRoDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	if ctx.Done() != db.done || !db.blocked.CompareAndSwap(false, true) {
		return db.TemporalRwDB.BeginTemporalRo(ctx)
	}
	db.cancel()
	return nil, ctx.Err()
}

func (db *cancelOnTemporalRoDB) Agg() any {
	return db.TemporalRwDB.(dbstate.HasAgg).Agg()
}

// A worker-pool failure must cancel the executor and surface instead of leaving
// the exec loop waiting for results.
func TestParallelExec_WorkerPoolDeath_SurfacesInsteadOfHanging(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated worker panic")
	disarm := chaos_monkey.ArmWorkerError(chaosErr)
	t.Cleanup(disarm)

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorIs(t, err, chaosErr,
		"a dead worker pool must surface its error through the executor group")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"worker-pool death classified as ErrLoopExhausted → runStage retries forever with zero progress")
}

func TestParallelExec_EarlySetupCancellationDoesNotHideWorkerFailure(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db := &cancelOnTemporalRoDB{
		TemporalRwDB: m.DB,
		done:         ctx.Done(),
		cancel:       cancel,
	}
	m.DB = db

	chaosErr := errors.New("chaos monkey: simulated worker panic")
	disarm := chaos_monkey.ArmWorkerError(chaosErr)
	t.Cleanup(disarm)

	err := runParallelExecV3WithContext(t, ctx, m, b2.NumberU64())

	require.ErrorIs(t, err, chaosErr)
	require.False(t, commonerrors.IsOnlyCanceled(err))
}

// A recovered apply-loop panic must fail the batch because the apply loop owns
// post-execution validation.
func TestParallelExec_ApplyLoopPanic_SurfacesInsteadOfCommitting(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated apply-loop panic")
	disarm := chaos_monkey.ArmApplyLoopPanic(chaosErr)
	t.Cleanup(disarm)

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorContains(t, err, chaosErr.Error(),
		"a recovered apply-loop panic must fail the batch, not commit unvalidated blocks")
}

// A recovered exec-loop panic must surface instead of looking like a resumable
// empty batch.
func TestParallelExec_ExecLoopPanic_SurfacesInsteadOfRetrying(t *testing.T) {
	m, b2 := tipWithUnexecutedBlock2(t)

	chaosErr := errors.New("chaos monkey: simulated exec-loop panic")
	disarm := chaos_monkey.ArmExecLoopPanic(chaosErr)
	t.Cleanup(disarm)

	err := runParallelExecV3(t, m, b2.NumberU64())

	require.ErrorContains(t, err, chaosErr.Error(),
		"a recovered exec-loop panic must surface as a hard error")
	var exhausted *stagedsync.ErrLoopExhausted
	require.False(t, errors.As(err, &exhausted),
		"exec-loop panic classified as ErrLoopExhausted → runStage retries forever with zero progress")
}
