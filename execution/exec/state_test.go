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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type workerTestTask struct {
	TxTask
	execute func(*vm.EVM) *TxResult
}

func (task *workerTestTask) Reset(evm *vm.EVM, ibs *state.IntraBlockState, _ *calltracer.CallTracer) error {
	evm.Reset(evmtypes.TxContext{}, ibs)
	return nil
}

func (task *workerTestTask) Execute(evm *vm.EVM, _ rules.Engine, _ *types.Genesis, _ *state.IntraBlockState,
	_ state.StateWriter, _ *chain.Config, _ rules.ChainReader, _ datadir.Dirs, _ bool,
) *TxResult {
	return task.execute(evm)
}

func TestWorkerCancellationStopsRunningEVM(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	in := NewQueueWithRetry(1)
	defer in.Release()
	results := NewResultsQueue(1, 1)
	w := NewWorker(ctx, false, nil, nil, in, nil, chain.AllProtocolChanges, nil, results, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.SetReader(stubStateReader{})
	started := make(chan struct{})
	task := &workerTestTask{
		TxTask: TxTask{TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1)}},
		execute: func(evm *vm.EVM) *TxResult {
			close(started)
			_, _, _, err := evm.Run(vm.Contract{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}},
				mdgas.MdGas{Execution: 1 << 63}, nil, false)
			return &TxResult{Err: err}
		},
	}
	in.Add(ctx, task)
	done := make(chan error, 1)
	go func() { done <- w.Run() }()
	<-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		w.evm.Cancel()
		<-done
		t.Fatal("worker context cancellation did not interrupt the running EVM")
	}
}

type systemCallCancellationEngine struct {
	rules.Engine
	cancelAfter int
	cancel      func()
	value       uint256.Int
}

func (e *systemCallCancellationEngine) Initialize(_ *chain.Config, _ rules.ChainHeaderReader, header *types.Header,
	ibs *state.IntraBlockState, syscall rules.SysCallCustom, _ log.Logger, _ *tracing.Hooks,
) error {
	contract := accounts.InternAddress(common.HexToAddress("0xcafe"))
	if err := ibs.CreateAccount(contract, false); err != nil {
		return err
	}
	code := []byte{byte(vm.PUSH1), 0, byte(vm.SLOAD), byte(vm.PUSH1), 1, byte(vm.ADD), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP)}
	if err := ibs.SetCode(contract, code, tracing.CodeChangeUnspecified); err != nil {
		return err
	}
	for call := range 2 {
		if call == e.cancelAfter {
			e.cancel()
		}
		if _, err := syscall(contract, nil, ibs, header, false); err != nil {
			return err
		}
	}
	value, err := ibs.GetState(contract, accounts.ZeroKey)
	e.value = value
	return err
}

func TestWorkerCancellationStopsSystemCalls(t *testing.T) {
	for cancelAfter, name := range []string{"before first call", "between calls"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				in := NewQueueWithRetry(1)
				defer in.Release()
				engine := &systemCallCancellationEngine{
					Engine: ethash.NewFaker(), cancelAfter: cancelAfter,
					cancel: func() {
						cancel()
						synctest.Wait()
					},
				}
				w := NewWorker(ctx, false, nil, nil, in, nil, chain.AllProtocolChanges, nil, NewResultsQueue(1, 1), engine, datadir.Dirs{}, log.New())
				defer w.Close()
				w.SetReader(state.NewNoopReader())
				w.stateWriter = state.NewNoopWriter()
				in.Add(ctx, &TxTask{Config: chain.AllProtocolChanges, TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000}})
				require.ErrorIs(t, w.Run(), context.Canceled)
				require.Equal(t, uint64(cancelAfter), engine.value.Uint64(), "cancelled system calls must not write storage")
				require.True(t, w.evm.Cancelled())
			})
		})
	}
}

func TestCancelledWorkerDoesNotInvalidateBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := NewWorker(ctx, false, nil, nil, nil, nil, chain.AllProtocolChanges, nil, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.SetReader(stubStateReader{})
	executed := false
	result := w.RunTxTask(&workerTestTask{
		TxTask: TxTask{TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1)}},
		execute: func(*vm.EVM) *TxResult {
			executed = true
			return &TxResult{}
		},
	})

	require.False(t, executed, "a cancelled worker must not execute another task")
	require.ErrorIs(t, result.Err, context.Canceled)
	require.True(t, result.Operational, "cancellation is not evidence that a block is invalid")
}

func TestNewWorkersPoolForegroundReturnsWait(t *testing.T) {
	in := NewQueueWithRetry(1)
	t.Cleanup(in.Release)

	_, _, _, clear, wait, err := NewWorkersPool(
		context.Background(), WorkerFaults{}, nil, false, nil,
		nil, nil, nil, in, nil, chain.AllProtocolChanges, nil,
		nil, 1, NewWorkerMetrics(), datadir.Dirs{}, log.New(),
	)
	require.NoError(t, err)
	t.Cleanup(clear)
	require.NotNil(t, wait)
	require.NoError(t, wait())
}

// The background pool must keep every worker's failure, mirroring the
// executor-group guarantee: a concurrent second failure is diagnostic
// evidence, not noise to drop on the first-error slot.
func TestNewWorkersPoolPreservesEveryWorkerFailure(t *testing.T) {
	in := NewQueueWithRetry(1)
	t.Cleanup(in.Release)

	boom := errors.New("worker start fault")
	var n atomic.Int64
	faults := WorkerFaults{RunStart: func() error {
		return fmt.Errorf("worker %d: %w", n.Add(1), boom)
	}}

	_, _, _, clear, wait, err := NewWorkersPool(
		context.Background(), faults, nil, true, nil,
		nil, nil, nil, in, nil, chain.AllProtocolChanges, nil,
		nil, 2, NewWorkerMetrics(), datadir.Dirs{}, log.New(),
	)
	require.NoError(t, err)
	t.Cleanup(clear)

	got := wait()
	require.ErrorIs(t, got, boom)
	require.Equal(t, 2, strings.Count(got.Error(), boom.Error()),
		"both workers' failures must survive the join")
}

type failingRoDB struct{ kv.TemporalRoDB }

func (db failingRoDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	return nil, errors.New("disk failure")
}

type stubStateReader struct{ state.StateReader }

// A worker setup failure (chain tx open) is an executor fault: the result must
// carry the operational marker so it cannot become a block verdict downstream.
func TestRunTxTaskMarksWorkerSetupFailureOperational(t *testing.T) {
	in := NewQueueWithRetry(1)
	t.Cleanup(in.Release)
	rws := NewResultsQueue(8, 1)
	w := NewWorker(context.Background(), true, NewWorkerMetrics(), failingRoDB{}, in,
		nil, chain.AllProtocolChanges, nil, rws, nil, datadir.Dirs{}, log.New())
	t.Cleanup(w.Close)
	w.stateReader = stubStateReader{}

	res := w.RunTxTask(&TxTask{})

	require.Error(t, res.Err)
	require.ErrorContains(t, res.Err, "disk failure")
	require.True(t, res.Operational,
		"a chain-tx open failure is an executor fault, never a statement about the block")
}
