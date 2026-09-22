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

package protocol

import (
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type gasChange struct {
	old    mdgas.MdGas
	new    mdgas.MdGas
	reason tracing.GasChangeReason
}

func gasTracingEVM(ibs *state.IntraBlockState, cfg *chain.Config, changes *[]gasChange) *vm.EVM {
	return vm.NewEVM(evmtypes.BlockContext{CanTransfer: CanTransfer, Transfer: misc.Transfer, GasLimit: 30_000_000}, evmtypes.TxContext{}, ibs, cfg, vm.Config{
		NoBaseFee: true, Tracer: &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
			*changes = append(*changes, gasChange{old: old, new: new, reason: reason})
		}},
	})
}

func TestFrameV2RuntimeFailure(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	initial := mdgas.MdGas{Execution: 100, State: 50}
	remaining := mdgas.MdGas{Execution: 10, State: 20}
	var entered []mdgas.MdGas
	var exited []mdgas.MdGasUsage
	hooks := &tracing.Hooks{
		OnEnterV2: func(depth int, typ byte, _, _ accounts.Address, precompile bool, _ []byte, gas mdgas.MdGas, _ uint256.Int, _ []byte) {
			require.Zero(t, depth)
			require.Equal(t, byte(vm.CALL), typ)
			require.True(t, precompile)
			entered = append(entered, gas)
		},
		OnExitV2: func(depth int, _ []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
			require.Zero(t, depth)
			require.ErrorIs(t, err, vm.ErrRuntimeOutOfGas)
			require.True(t, reverted)
			exited = append(exited, gasUsed)
		},
	}
	evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{Tracer: hooks})
	gasUsed := HandleRuntimeFailure(evm, vm.CALL, accounts.ZeroAddress, accounts.InternAddress(common.HexToAddress("0x04")), nil, initial, &remaining, uint256.Int{}, vm.ErrRuntimeOutOfGas)
	require.Equal(t, []mdgas.MdGas{initial}, entered)
	require.Equal(t, mdgas.MdGasUsage{Execution: initial.Execution}, gasUsed)
	require.Equal(t, []mdgas.MdGasUsage{gasUsed}, exited)
	require.Equal(t, mdgas.MdGas{State: initial.State}, remaining)
}

func TestGasChangeV2RuntimeCharges(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	auth, authority := eip2780TestAuthorization()
	require.NoError(t, ibs.SetNonce(authority, auth.Nonce, tracing.NonceChangeUnspecified))
	sender := accounts.InternAddress(common.HexToAddress("0x1000"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2000"))
	require.NoError(t, ibs.SetBalance(sender, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	var changes []gasChange
	evm := gasTracingEVM(ibs, eip2780TestConfig(t), &changes)
	msg := types.NewMessage(sender, recipient, 0, uint256.NewInt(1), 500_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), nil, nil, false, false, true, false, nil)
	msg.SetAuthorizations([]types.Authorization{auth})
	result, err := NewTxnExecutor(evm, msg, NewGasPool(30_000_000, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.EqualValues(t, params.StateGasAuthBase+params.StateGasNewAccount, result.BlockStateGasUsed)
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeTxAuthorization && change.old.Total() == change.new.Total()+params.StateGasAuthBase
	}))
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeCallNewAccount && change.old.Total() == change.new.Total()+params.StateGasNewAccount
	}))
	require.Equal(t, gasChange{new: mdgas.MdGas{Execution: msg.Gas()}, reason: tracing.GasChangeTxInitialBalance}, changes[0])
	require.Equal(t, tracing.GasChangeTxIntrinsicGas, changes[1].reason)
}

func TestGasChangeV2TopLevelRefund(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var changes []gasChange
	evm := gasTracingEVM(ibs, chain.AllProtocolChanges, &changes)
	msg := types.NewMessage(accounts.ZeroAddress, accounts.NilAddress, 0, uint256.NewInt(0), 500_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.REVERT)}, nil, false, false, true, false, nil)
	result, err := NewTxnExecutor(evm, msg, NewGasPool(30_000_000, 0)).Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeCallNewAccount && change.old.Total() == change.new.Total()+params.StateGasNewAccount
	}))
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeRefundAccountCreation && change.new.Total() == change.old.Total()+params.StateGasNewAccount
	}))
	require.Zero(t, result.BlockStateGasUsed)
}

func TestGasChangeV2RuntimeRollback(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	auth, authority := eip2780TestAuthorization()
	require.NoError(t, ibs.SetNonce(authority, auth.Nonce, tracing.NonceChangeUnspecified))
	sender := accounts.InternAddress(common.HexToAddress("0x1000"))
	require.NoError(t, ibs.SetBalance(sender, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	var changes []gasChange
	evm := gasTracingEVM(ibs, eip2780TestConfig(t), &changes)
	msg := types.NewMessage(sender, accounts.InternAddress(common.HexToAddress("0x2000")), 0, uint256.NewInt(1), 100_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0), nil, nil, false, false, true, false, nil)
	msg.SetAuthorizations([]types.Authorization{auth})
	executor := NewTxnExecutor(evm, msg, NewGasPool(30_000_000, 0))
	inFailureFrame := false
	hooks := evm.Config().Tracer
	hooks.OnEnter = func(_ int, _ byte, _ accounts.Address, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
		inFailureFrame = true
	}
	hooks.OnExit = func(_ int, _ []byte, _ uint64, _ error, _ bool) {
		inFailureFrame = false
	}
	record := hooks.OnGasChangeV2
	hooks.OnGasChangeV2 = func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		record(old, new, reason)
		if inFailureFrame && reason != tracing.GasChangeCallLeftOverReturned {
			require.Equal(t, executor.gasRemaining, new)
		}
	}
	result, err := executor.Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeTxAuthorization && change.old.Total() == change.new.Total()+params.StateGasAuthBase
	}))
	require.True(t, slices.ContainsFunc(changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeCallFailedExecution && change.new == (mdgas.MdGas{})
	}))
	require.Zero(t, result.BlockStateGasUsed)
}
