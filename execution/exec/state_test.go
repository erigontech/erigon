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
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
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
	for _, traced := range []bool{false, true} {
		t.Run(fmt.Sprintf("traced=%t", traced), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cancelWorker := func() {
					cancel()
					synctest.Wait()
				}
				w := NewWorkerContext(ctx, false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
				defer w.Close()
				w.SetReader(state.NewNoopReader())
				var vmConfig vm.Config
				if traced {
					vmConfig.Tracer = &tracing.Hooks{OnOpcode: func(uint64, byte, uint64, uint64, tracing.OpContext, []byte, int, error) {
						if ctx.Err() == nil {
							cancelWorker()
						}
					}}
				}
				blockCtx := evmtypes.BlockContext{}
				w.evm.ResetBetweenBlocks(blockCtx, evmtypes.TxContext{}, w.ibs, vmConfig, blockCtx.Rules(chain.AllProtocolChanges))
				var executionErr error
				result := w.RunTxTask(&workerTestTask{
					TxTask: TxTask{TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1)}},
					execute: func(evm *vm.EVM) *TxResult {
						if !traced {
							cancelWorker()
						}
						// Finite gas makes a missing cancellation check fail instead of hanging.
						_, _, _, executionErr = evm.Run(vm.Contract{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}},
							mdgas.MdGas{Execution: 1_000}, nil, false)
						return &TxResult{Err: executionErr}
					},
				})
				require.ErrorIs(t, result.Err, context.Canceled)
				require.NoError(t, executionErr, "cancelled EVM must stop before exhausting gas")
				require.True(t, w.evm.Cancelled())
			})
		})
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
				engine := &systemCallCancellationEngine{
					Engine: ethash.NewFaker(), cancelAfter: cancelAfter,
					cancel: func() {
						cancel()
						synctest.Wait()
					},
				}
				w := NewWorkerContext(ctx, false, nil, nil, nil, chain.AllProtocolChanges, nil, engine, datadir.Dirs{}, log.New())
				defer w.Close()
				w.SetReader(state.NewNoopReader())
				w.stateWriter = state.NewNoopWriter()
				result := w.RunTxTask(&TxTask{Config: chain.AllProtocolChanges, TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000}})
				require.ErrorIs(t, result.Err, context.Canceled)
				require.Equal(t, uint64(cancelAfter), engine.value.Uint64(), "cancelled system calls must not write storage")
				require.True(t, w.evm.Cancelled())
			})
		})
	}
}

func TestCancelledWorkerDoesNotInvalidateBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := NewWorkerContext(ctx, false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.SetReader(state.NewNoopReader())
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

func TestWorkerCancellationDuringExecutionReturnsOperationalError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w := NewWorkerContext(ctx, false, nil, nil, nil, chain.AllProtocolChanges, nil, nil, datadir.Dirs{}, log.New())
	defer w.Close()
	w.SetReader(state.NewNoopReader())
	result := w.RunTxTask(&workerTestTask{
		TxTask: TxTask{TxIndex: -1, Header: &types.Header{Number: *uint256.NewInt(1)}},
		execute: func(*vm.EVM) *TxResult {
			cancel()
			return &TxResult{}
		},
	})

	require.ErrorIs(t, result.Err, context.Canceled)
	require.True(t, result.Operational, "cancellation is not evidence that a block is invalid")
}
