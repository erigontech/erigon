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

package vm

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
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type gasChange struct {
	old    mdgas.MdGas
	new    mdgas.MdGas
	reason tracing.GasChangeReason
}

func TestFrameV2Gas(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  OpCode
	}{
		{name: "call", typ: CALLCODE},
		{name: "create", typ: CREATE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			var entered []mdgas.MdGas
			var exited []mdgas.MdGasUsage
			hooks := &tracing.Hooks{
				OnEnter: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
					t.Fatal("V2 must take precedence")
				},
				OnEnterV2: func(depth int, typ byte, _, _ accounts.Address, _ bool, _ []byte, gas mdgas.MdGas, _ uint256.Int, _ []byte) {
					require.Zero(t, depth)
					require.Equal(t, byte(tc.typ), typ)
					entered = append(entered, gas)
				},
				OnExit: func(_ int, _ []byte, _ uint64, _ error, _ bool) {
					t.Fatal("V2 must take precedence")
				},
				OnExitV2: func(depth int, _ []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
					require.Zero(t, depth)
					require.NoError(t, err)
					require.False(t, reverted)
					exited = append(exited, gasUsed)
				},
			}
			evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
			initial := mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet / 2}
			code := []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE), byte(STOP)}
			var remaining mdgas.MdGas
			var used mdgas.MdGasUsage
			var err error
			if tc.typ == CREATE {
				_, _, remaining, used, err = evm.Create(accounts.ZeroAddress, code, initial, uint256.Int{}, nil, false)
			} else {
				address := accounts.InternAddress(common.HexToAddress("0x1000"))
				require.NoError(t, ibs.SetCode(address, code, tracing.CodeChangeUnspecified))
				_, remaining, used, err = evm.CallCode(accounts.ZeroAddress, address, nil, initial, uint256.Int{})
			}
			require.NoError(t, err)
			require.Equal(t, []mdgas.MdGas{initial}, entered)
			require.Equal(t, mdgas.MdGasUsage{Execution: 12_106, State: params.StateGasPerStorageSet, StateSpill: params.StateGasPerStorageSet / 2}, used)
			require.Equal(t, []mdgas.MdGasUsage{used}, exited)
			require.Equal(t, initial.Execution-remaining.Execution, used.Execution+used.StateSpill)
			require.Zero(t, remaining.State)
		})
	}
}

func TestFrameV2Selfdestruct(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *chain.Config
	}{
		{name: "before EIP-6780", config: chain.TestChainBerlinConfig},
		{name: "Amsterdam", config: chain.AllProtocolChanges},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			var enters int
			var exits int
			hooks := &tracing.Hooks{
				OnEnterV2: func(depth int, typ byte, _, _ accounts.Address, _ bool, _ []byte, gas mdgas.MdGas, _ uint256.Int, _ []byte) {
					if OpCode(typ) == SELFDESTRUCT {
						enters++
						require.Equal(t, 1, depth)
						require.Equal(t, mdgas.MdGas{}, gas)
					}
				},
				OnExitV2: func(depth int, _ []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
					if depth == 1 {
						exits++
						require.Equal(t, mdgas.MdGasUsage{}, gasUsed)
						require.NoError(t, err)
						require.False(t, reverted)
					}
				},
			}
			address := accounts.InternAddress(common.HexToAddress("0x1000"))
			require.NoError(t, ibs.SetCode(address, []byte{byte(PUSH1), 0xff, byte(SELFDESTRUCT)}, tracing.CodeChangeUnspecified))
			evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, tc.config, Config{Tracer: hooks})
			_, _, _, err := evm.CallCode(address, address, nil, mdgas.MdGas{Execution: 200_000}, uint256.Int{})
			require.NoError(t, err)
			require.Equal(t, 1, enters)
			require.Equal(t, 1, exits)
		})
	}
}

func TestOpcodeV2StateGas(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	type step struct {
		pc   uint64
		op   OpCode
		gas  mdgas.MdGas
		cost mdgas.MdGas
	}
	var steps []step
	hooks := &tracing.Hooks{OnOpcodeV2: func(pc uint64, op byte, gas, cost mdgas.MdGas, scope tracing.OpContext, _ []byte, depth int, err error) {
		require.NoError(t, err)
		require.Equal(t, 1, depth)
		require.NotNil(t, scope)
		steps = append(steps, step{pc: pc, op: OpCode(op), gas: gas, cost: cost})
	}}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE), byte(STOP)}
	initial := mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet}
	_, remaining, _, err := evm.Run(contract, initial, nil, false)
	require.NoError(t, err)
	require.Equal(t, []step{
		{pc: 0, op: PUSH1, gas: initial, cost: mdgas.MdGas{Execution: 3}},
		{pc: 2, op: PUSH1, gas: mdgas.MdGas{Execution: initial.Execution - 3, State: initial.State}, cost: mdgas.MdGas{Execution: 3}},
		{pc: 4, op: SSTORE, gas: mdgas.MdGas{Execution: initial.Execution - 6, State: initial.State}, cost: mdgas.MdGas{Execution: params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038, State: params.StateGasPerStorageSet}},
		{pc: 5, op: STOP, gas: remaining},
	}, steps)
}

func TestOpcodeV2ChargeFailure(t *testing.T) {
	sstoreCost := mdgas.MdGas{Execution: params.ColdStorageAccessCostEIP8038 + params.StorageWriteCostEIP8038, State: params.StateGasPerStorageSet}
	for _, tc := range []struct {
		name    string
		code    []byte
		initial mdgas.MdGas
		pc      uint64
		gas     mdgas.MdGas
		cost    mdgas.MdGas
	}{
		{
			name: "constant execution cost", code: []byte{byte(PUSH1), 1},
			initial: mdgas.MdGas{Execution: 2, State: 50},
			gas:     mdgas.MdGas{Execution: 2, State: 50}, cost: mdgas.MdGas{Execution: 3},
		},
		{
			name: "dynamic execution cost", code: []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(MSTORE)},
			initial: mdgas.MdGas{Execution: 11, State: 50}, pc: 4,
			gas: mdgas.MdGas{Execution: 5, State: 50}, cost: mdgas.MdGas{Execution: 6},
		},
		{
			name: "state cost", code: []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE)},
			initial: mdgas.MdGas{Execution: 6 + sstoreCost.Execution + 10, State: sstoreCost.State - 11}, pc: 4,
			gas: mdgas.MdGas{Execution: sstoreCost.Execution + 10, State: sstoreCost.State - 11}, cost: sstoreCost,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			var failures int
			hooks := &tracing.Hooks{
				OnOpcodeV2: func(pc uint64, op byte, gas, cost mdgas.MdGas, _ tracing.OpContext, _ []byte, _ int, err error) {
					if err == nil {
						return
					}
					failures++
					require.ErrorIs(t, err, ErrOutOfGas)
					require.Equal(t, tc.pc, pc)
					require.Equal(t, tc.code[tc.pc], op)
					require.Equal(t, tc.gas, gas)
					require.Equal(t, tc.cost, cost)
				},
				OnFaultV2: func(_ uint64, _ byte, _, _ mdgas.MdGas, _ tracing.OpContext, _ int, _ error) {
					t.Fatal("charging failures must use the opcode hook")
				},
			}
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
			contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			contract.Code = tc.code
			_, _, _, err := evm.Run(contract, tc.initial, nil, false)
			require.ErrorIs(t, err, ErrOutOfGas)
			require.Equal(t, 1, failures)
		})
	}
}

func TestFaultV2(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var faults int
	hooks := &tracing.Hooks{
		OnOpcode: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, err error) {
			require.NoError(t, err)
		},
		OnFault: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ int, _ error) {
			t.Fatal("V2 must take precedence")
		},
		OnFaultV2: func(pc uint64, op byte, gas, cost mdgas.MdGas, scope tracing.OpContext, depth int, err error) {
			faults++
			require.Equal(t, uint64(2), pc)
			require.Equal(t, byte(JUMP), op)
			require.Equal(t, mdgas.MdGas{Execution: 97, State: 50}, gas)
			require.Equal(t, mdgas.MdGas{Execution: 8}, cost)
			require.NotNil(t, scope)
			require.Equal(t, 1, depth)
			require.ErrorIs(t, err, ErrInvalidJump)
		},
	}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH1), 0xff, byte(JUMP)}
	_, _, _, err := evm.Run(contract, mdgas.MdGas{Execution: 100, State: 50}, nil, false)
	require.ErrorIs(t, err, ErrInvalidJump)
	require.Equal(t, 1, faults)
}

func TestGasChangeV2FailedOpcodeCharge(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var changes []gasChange
	hooks := &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		changes = append(changes, gasChange{old: old, new: new, reason: reason})
	}}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(MSTORE)}
	_, remaining, _, err := evm.Run(contract, mdgas.MdGas{Execution: 11}, nil, false)
	require.ErrorIs(t, err, ErrOutOfGas)
	require.Equal(t, mdgas.MdGas{Execution: 2}, remaining)
	require.Equal(t, []gasChange{
		{old: mdgas.MdGas{Execution: 11}, new: mdgas.MdGas{Execution: 8}, reason: tracing.GasChangeCallOpCode},
		{old: mdgas.MdGas{Execution: 8}, new: mdgas.MdGas{Execution: 5}, reason: tracing.GasChangeCallOpCode},
	}, changes)
}

func TestGasChangeV2StateCharge(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var changes []gasChange
	hooks := &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		changes = append(changes, gasChange{old: old, new: new, reason: reason})
	}}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE), byte(STOP)}
	initial := mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet}
	_, remaining, used, err := evm.Run(contract, initial, nil, false)
	require.NoError(t, err)
	require.Len(t, changes, 4)
	previous := initial
	for _, change := range changes {
		require.Equal(t, tracing.GasChangeCallOpCode, change.reason)
		require.Equal(t, previous, change.old)
		previous = change.new
	}
	require.Equal(t, remaining, previous)
	require.Zero(t, remaining.State)
	require.EqualValues(t, params.StateGasPerStorageSet, used.State)
}

func TestGasChangeV2Revert(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	address := accounts.InternAddress(common.HexToAddress("0x1000"))
	code := []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE), byte(PUSH0), byte(PUSH0), byte(REVERT)}
	require.NoError(t, ibs.SetCode(address, code, tracing.CodeChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	initial := mdgas.MdGas{Execution: 200_000, State: 50_000}
	_, remaining, _, err := evm.CallCode(accounts.ZeroAddress, address, nil, initial, uint256.Int{})
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, initial.State, remaining.State)
	require.True(t, slices.ContainsFunc(recorder.changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeRefundRevertedState && change.new.Total() == change.old.Total()+params.StateGasPerStorageSet
	}))
}

func TestGasChangeV2NestedCall(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	parent := accounts.InternAddress(common.HexToAddress("0x1000"))
	child := accounts.InternAddress(common.HexToAddress("0x1001"))
	code := []byte{byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH2), 0x10, 1, byte(GAS), byte(DELEGATECALL), byte(STOP)}
	require.NoError(t, ibs.SetCode(parent, code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(child, []byte{byte(PUSH1), 1, byte(PUSH0), byte(SSTORE)}, tracing.CodeChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	initial := mdgas.MdGas{Execution: 200_000, State: 50_000}
	_, remaining, used, err := evm.CallCode(accounts.ZeroAddress, parent, nil, initial, uint256.Int{})
	require.NoError(t, err)
	require.EqualValues(t, params.StateGasPerStorageSet, used.State)
	var totalUsed int64
	for _, change := range recorder.changes[1:] {
		switch change.reason {
		case tracing.GasChangeCallOpCode:
			totalUsed += int64(change.old.Total() - change.new.Total())
		case tracing.GasChangeCallInitialBalance:
			totalUsed -= int64(change.new.Execution)
		}
	}
	require.Equal(t, int64(initial.Total()-remaining.Total()), totalUsed)
}

func TestGasChangeV2OpcodeReportsCurrentBalance(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var scope *CallContext
	var opcodeGasChanges int
	hooks := &tracing.Hooks{
		OnOpcode: func(_ uint64, _ byte, _ uint64, _ uint64, context tracing.OpContext, _ []byte, _ int, _ error) {
			require.Equal(t, 1, opcodeGasChanges)
			opcodeGasChanges = 0
			scope = context.(*CallContext)
		},
		OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
			if reason == tracing.GasChangeCallOpCode {
				opcodeGasChanges++
				if scope != nil {
					require.Equal(t, scope.Gas(), new)
				}
			}
		},
	}
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	contract := *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
	contract.Code = []byte{byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH2), 0x10, 1, byte(GAS), byte(DELEGATECALL)}
	_, _, _, err := evm.Run(contract, mdgas.MdGas{Execution: 200_000, State: 50_000}, nil, false)
	require.NoError(t, err)
}

type gasTraceRecorder struct {
	t        *testing.T
	balances []mdgas.MdGas
	changes  []gasChange
}

func TestGasChangeV2RestoreChildGasClearsStateGas(t *testing.T) {
	ctx := &CallContext{gas: 100, stateGas: 50}
	var changes []gasChange
	ctx.restoreChildGas(mdgas.MdGas{}, &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		changes = append(changes, gasChange{old: old, new: new, reason: reason})
	}})
	require.Equal(t, mdgas.MdGas{Execution: 100}, ctx.Gas())
	require.Equal(t, []gasChange{{
		old: mdgas.MdGas{Execution: 100, State: 50}, new: mdgas.MdGas{Execution: 100},
		reason: tracing.GasChangeCallLeftOverRefunded,
	}}, changes)
}

func TestGasChangeV2Merge(t *testing.T) {
	ctx := &CallContext{gas: 100, stateGas: 50, stateGasSpill: 100}
	var changes []gasChange
	ctx.mergeChildStateGas(0, &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		changes = append(changes, gasChange{old: old, new: new, reason: reason})
	}})
	require.Equal(t, []gasChange{{
		old: mdgas.MdGas{Execution: 100, State: 50}, new: mdgas.MdGas{Execution: 150},
		reason: tracing.GasChangeCallStateGasReturned,
	}}, changes)
}

func TestGasChangeV2Precompile(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		executionGas          uint64
		remainingExecutionGas uint64
		reason                tracing.GasChangeReason
		err                   error
	}{
		{
			name:                  "success",
			executionGas:          200_000,
			remainingExecutionGas: 200_000 - params.IdentityBaseGas,
			reason:                tracing.GasChangeCallPrecompiledContract,
		},
		{
			name:         "out of gas",
			executionGas: params.IdentityBaseGas - 1,
			reason:       tracing.GasChangeCallFailedExecution,
			err:          ErrOutOfGas,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			recorder := &gasTraceRecorder{t: t}
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
			initial := mdgas.MdGas{Execution: tc.executionGas, State: 50_000}
			_, remaining, used, err := evm.CallCode(accounts.ZeroAddress, accounts.InternAddress(common.HexToAddress("0x04")), nil, initial, uint256.Int{})
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, mdgas.MdGas{Execution: tc.remainingExecutionGas, State: initial.State}, remaining)
			require.Equal(t, mdgas.MdGasUsage{Execution: tc.executionGas - tc.remainingExecutionGas}, used)
			require.Len(t, recorder.changes, 3)
			require.Equal(t, gasChange{old: initial, new: remaining, reason: tc.reason}, recorder.changes[1])
		})
	}
}

func TestGasChangeV2CodeDeposit(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(evmtypes.BlockContext{Transfer: misc.Transfer, CanTransfer: func(_ evmtypes.IntraBlockState, _ accounts.Address, value uint256.Int) (bool, error) {
		return value.IsZero(), nil
	}}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	_, _, _, _, err := evm.Create(accounts.ZeroAddress, []byte{byte(PUSH1), 1, byte(PUSH0), byte(RETURN)},
		mdgas.MdGas{Execution: 200_000, State: 50_000}, uint256.Int{}, nil, false)
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(recorder.changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeCallCodeStorage && change.old.State == change.new.State+params.CostPerStateByte
	}))
}

func TestGasChangeV2FailedCodeDeposit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial mdgas.MdGas
		charges []gasChange
	}{
		{
			name:    "execution gas",
			initial: mdgas.MdGas{Execution: 8, State: params.CostPerStateByte},
		},
		{
			name:    "state gas spill",
			initial: mdgas.MdGas{Execution: 20, State: params.CostPerStateByte - 7},
			charges: []gasChange{{
				old:    mdgas.MdGas{Execution: 12, State: params.CostPerStateByte - 7},
				new:    mdgas.MdGas{Execution: 6, State: params.CostPerStateByte - 7},
				reason: tracing.GasChangeCallCodeStorage,
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			recorder := &gasTraceRecorder{t: t}
			evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
			ret, _, remaining, used, err := evm.Create(accounts.ZeroAddress, []byte{byte(PUSH1), 1, byte(PUSH0), byte(RETURN)},
				tc.initial, uint256.Int{}, nil, false)
			require.ErrorIs(t, err, ErrCodeStoreOutOfGas)
			require.Empty(t, ret)
			require.Equal(t, mdgas.MdGas{State: tc.initial.State}, remaining)
			require.Equal(t, mdgas.MdGasUsage{Execution: tc.initial.Execution}, used)
			var charges []gasChange
			for _, change := range recorder.changes {
				if change.reason == tracing.GasChangeCallCodeStorage {
					charges = append(charges, change)
				}
			}
			require.Equal(t, tc.charges, charges)
		})
	}
}

func TestGasChangeV2NestedCreate(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	address := accounts.InternAddress(common.HexToAddress("0x1000"))
	require.NoError(t, ibs.SetCode(address, []byte{byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(CREATE)}, tracing.CodeChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	_, _, _, err := evm.CallCode(accounts.ZeroAddress, address, nil, mdgas.MdGas{Execution: 200_000, State: 50_000}, uint256.Int{})
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(recorder.changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeCallNewAccount && change.old.Total() == change.new.Total()+params.StateGasNewAccount
	}))
}

func gasTraceBlockContext() evmtypes.BlockContext {
	return evmtypes.BlockContext{Transfer: misc.Transfer, CanTransfer: func(ibs evmtypes.IntraBlockState, address accounts.Address, value uint256.Int) (bool, error) {
		balance, err := ibs.GetBalance(address)
		return balance.Cmp(&value) >= 0, err
	}}
}

func TestGasChangeV2CallStipend(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	parent := accounts.InternAddress(common.HexToAddress("0x1000"))
	child := accounts.InternAddress(common.HexToAddress("0x1001"))
	code := []byte{byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH1), 1, byte(PUSH2), 0x10, 1, byte(GAS), byte(CALL)}
	require.NoError(t, ibs.SetCode(parent, code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(child, []byte{byte(STOP)}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetBalance(accounts.ZeroAddress, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	initial := mdgas.MdGas{Execution: 200_000, State: 50_000}
	_, remaining, _, err := evm.CallCode(accounts.ZeroAddress, parent, nil, initial, uint256.Int{})
	require.NoError(t, err)
	var executionUsed int64
	for _, change := range recorder.changes[1:] {
		switch change.reason {
		case tracing.GasChangeCallOpCode:
			executionUsed += int64(change.old.Total() - change.new.Total())
		case tracing.GasChangeCallInitialBalance:
			executionUsed -= int64(change.new.Execution)
		}
	}
	require.Equal(t, int64(initial.Total()-remaining.Total()), executionUsed)
}

func TestGasChangeV2FailedCreateRefund(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	address := accounts.InternAddress(common.HexToAddress("0x1000"))
	code := []byte{byte(PUSH1), byte(INVALID), byte(PUSH0), byte(MSTORE8), byte(PUSH1), 1, byte(PUSH0), byte(PUSH0), byte(CREATE)}
	require.NoError(t, ibs.SetCode(address, code, tracing.CodeChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	_, remaining, _, err := evm.CallCode(accounts.ZeroAddress, address, nil, mdgas.MdGas{Execution: 200_000, State: 50_000}, uint256.Int{})
	require.NoError(t, err)
	require.EqualValues(t, 50_000, remaining.State)
	require.True(t, slices.ContainsFunc(recorder.changes, func(change gasChange) bool {
		return change.reason == tracing.GasChangeRefundAccountCreation && change.new.Total() == change.old.Total()+params.StateGasNewAccount
	}))
}

func TestGasChangeV2CreateCollision(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	address := accounts.InternAddress(types.CreateAddress(accounts.ZeroAddress.Value(), 0))
	require.NoError(t, ibs.SetNonce(address, 1, tracing.NonceChangeUnspecified))
	recorder := &gasTraceRecorder{t: t}
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
	initial := mdgas.MdGas{Execution: 200_000, State: 50_000}
	_, _, remaining, _, err := evm.Create(accounts.ZeroAddress, nil, initial, uint256.Int{}, nil, false)
	require.ErrorIs(t, err, ErrContractAddressCollision)
	require.Equal(t, mdgas.MdGas{State: initial.State}, remaining)
	require.Equal(t, []gasChange{
		{new: initial, reason: tracing.GasChangeCallInitialBalance},
		{old: initial, new: remaining, reason: tracing.GasChangeCallFailedExecution},
		{old: remaining, reason: tracing.GasChangeCallLeftOverReturned},
	}, recorder.changes)
}

func TestGasChangeV2ChildRefill(t *testing.T) {
	for _, revert := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "revert"}[revert], func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			parent := accounts.InternAddress(common.HexToAddress("0x1000"))
			child := accounts.InternAddress(common.HexToAddress("0x1001"))
			code := []byte{byte(PUSH1), 1, byte(PUSH0), byte(SSTORE), byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH0), byte(PUSH2), 0x10, 1, byte(GAS), byte(DELEGATECALL)}
			childCode := []byte{byte(PUSH0), byte(PUSH0), byte(SSTORE)}
			if revert {
				childCode = append(childCode, byte(PUSH0), byte(PUSH0), byte(REVERT))
			}
			require.NoError(t, ibs.SetCode(parent, code, tracing.CodeChangeUnspecified))
			require.NoError(t, ibs.SetCode(child, childCode, tracing.CodeChangeUnspecified))
			recorder := &gasTraceRecorder{t: t}
			evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: recorder.hooks()})
			_, _, used, err := evm.CallCode(accounts.ZeroAddress, parent, nil, mdgas.MdGas{Execution: 500_000}, uint256.Int{})
			require.NoError(t, err)
			if revert {
				require.EqualValues(t, params.StateGasPerStorageSet, used.State)
			} else {
				require.Zero(t, used.State)
			}
			require.True(t, slices.ContainsFunc(recorder.changes, func(change gasChange) bool {
				return change.reason == tracing.GasChangeCallOpCode && change.new.State == change.old.State+params.StateGasPerStorageSet
			}))
		})
	}
}

func (r *gasTraceRecorder) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
			r.balances = append(r.balances, mdgas.MdGas{})
		},
		OnExit: func(_ int, _ []byte, _ uint64, _ error, _ bool) {
			require.Zero(r.t, r.balances[len(r.balances)-1])
			r.balances = r.balances[:len(r.balances)-1]
		},
		OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
			index := len(r.balances) - 1
			require.Equal(r.t, r.balances[index], old, "%s", reason)
			r.balances[index] = new
			r.changes = append(r.changes, gasChange{old: old, new: new, reason: reason})
		},
	}
}

func TestGasChangeV2FrameBalances(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	address := accounts.InternAddress(common.HexToAddress("0x1000"))
	require.NoError(t, ibs.SetCode(address, []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE)}, tracing.CodeChangeUnspecified))
	var balance mdgas.MdGas
	var changes []gasChange
	hooks := &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		require.Equal(t, balance, old, "%s", reason)
		balance = new
		changes = append(changes, gasChange{old: old, new: new, reason: reason})
	}}
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{Tracer: hooks})
	initial := mdgas.MdGas{Execution: 200_000, State: 2 * params.StateGasPerStorageSet}
	_, remaining, _, err := evm.CallCode(accounts.ZeroAddress, address, nil, initial, uint256.Int{})
	require.NoError(t, err)
	require.NotEmpty(t, changes)
	require.Equal(t, gasChange{new: initial, reason: tracing.GasChangeCallInitialBalance}, changes[0])
	require.Equal(t, gasChange{old: remaining, reason: tracing.GasChangeCallLeftOverReturned}, changes[len(changes)-1])
	require.Zero(t, balance)
}
