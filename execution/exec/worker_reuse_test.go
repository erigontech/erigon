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

package exec

import (
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// keepCancelTask resets like a block-init system task: it keeps the EVM's cancellation.
type keepCancelTask struct{ workerTestTask }

func (task *keepCancelTask) Reset(*vm.EVM, *state.IntraBlockState, *calltracer.CallTracer) error {
	return nil
}

func TestWorkerResetKeepsIntraBlockState(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	ibs := w.ibs

	w.SetReader(state.NewNoopReader())
	require.Same(t, ibs, w.ibs, "SetReader rebuilt the worker's IntraBlockState")
	w.EnablePrevBlockReads(state.NewPrevBlockList())
	require.Same(t, ibs, w.ibs, "EnablePrevBlockReads rebuilt the worker's IntraBlockState")
	require.IsType(t, &state.PrevBlockReader{}, w.ibs.StateReader())
}

func TestParkedWorkerRunsUnderTheNextContext(t *testing.T) {
	first, cancel := context.WithCancel(t.Context())
	w := NewWorkerContext(first, false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	cancel()
	w.Park()

	metrics := NewWorkerMetrics()
	w.Rebind(t.Context(), metrics)
	w.SetReader(state.NewNoopReader())
	cancelled := true
	result := w.RunTxTask(&keepCancelTask{workerTestTask{
		TxTask: TxTask{TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1)}},
		execute: func(evm *vm.EVM) *TxResult {
			cancelled = evm.Cancelled()
			return &TxResult{}
		},
	}})

	require.NoError(t, result.Err)
	require.False(t, cancelled, "the previous run's cancellation reached the next run's EVM")
	require.Same(t, metrics, w.metrics)
}

type parkTestTx struct{ kv.TemporalTx }

func TestParkDropsTheRunBindings(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.SetReader(state.NewNoopReader())
	w.EnablePrevBlockReads(state.NewPrevBlockList())
	w.chainTx = parkTestTx{}
	w.rs = &state.StateV3Buffered{}

	versionMap := state.NewVersionMap(nil)
	w.ibs.SetVersionMap(versionMap)
	w.ibs.SetTxContext(1, 0)
	addr := accounts.InternAddress(common.HexToAddress("0xbeef"))
	require.NoError(t, w.ibs.AddBalance(addr, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	w.ibs.AddRefund(1)
	w.ibs.AddLog(&types.Log{})
	callTracer := calltracer.NewCallTracer(nil)
	txCtx := evmtypes.TxContext{Origin: addr}
	blockCtx := evmtypes.BlockContext{GetHash: func(uint64) (common.Hash, error) { return common.Hash{}, nil }}
	w.evm.ResetBetweenBlocks(blockCtx, txCtx, w.ibs, vm.Config{Tracer: callTracer.Tracer().Hooks}, blockCtx.Rules(chain.AllProtocolChanges))

	w.Park()

	require.Nil(t, w.chainTx, "a parked worker keeps the run's read tx")
	require.Nil(t, w.rs, "a parked worker keeps the run's state")
	require.Nil(t, w.stateReader)
	require.Nil(t, w.prevBlockReg)
	require.Nil(t, w.ibs.StateReader())
	require.NotSame(t, versionMap, w.ibs.VersionMap(), "a parked worker keeps the run's version map")
	require.Zero(t, w.ibs.GetRefund())
	require.Empty(t, w.ibs.Logs())
	require.Nil(t, w.evm.Config().Tracer, "a parked worker keeps the last tx's tracer")
	require.Nil(t, w.evm.Context.GetHash, "a parked worker keeps the run's block hash lookup")
	require.Equal(t, evmtypes.TxContext{}, w.evm.TxContext)
}

func TestParkRacingACancelLeavesTheEVMUncancelled(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.Park()
	for range 500 {
		ctx, cancel := context.WithCancel(t.Context())
		w.Rebind(ctx, nil)
		go cancel()
		w.Park()
		require.False(t, w.evm.Cancelled(), "a cancel that raced Park reached the parked EVM")
		cancel()
	}
}

func TestRebindUnderACancelledContextCancelsTheEVM(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.Park()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w.Rebind(ctx, nil)
	require.Eventually(t, w.evm.Cancelled, time.Second, time.Millisecond)
}

func TestParkAndCloseTwice(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	w.Park()
	w.Park()
	w.Close()
	w.Close()
}

func TestParkReleasesContextValues(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "previous execution")
	w := NewWorkerContext(ctx, false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.Park()
	if w.ctx != nil {
		require.Nil(t, w.ctx.Value(contextKey{}), "a parked worker keeps context values of the previous run")
	}
}

func TestParkReleasesTheRunEVM(t *testing.T) {
	w := NewWorkerContext(t.Context(), false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	runEVM := w.evm
	w.Park()
	require.NotSame(t, runEVM, w.evm, "a parked worker keeps the run's EVM and its call-output buffers")
}
