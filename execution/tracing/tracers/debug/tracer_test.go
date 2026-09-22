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

package debug

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

func TestTxEndV2Recording(t *testing.T) {
	usage := mdgas.TxnGasUsage{BlockExecutionGasUsed: 40, BlockStateGasUsed: 70, GasRefund: 10}
	var calls int
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{
		OnTxEndV2: func(receipt *types.Receipt, txnGasUsage mdgas.TxnGasUsage, err error) {
			require.Nil(t, receipt)
			require.Equal(t, usage, txnGasUsage)
			require.NoError(t, err)
			calls++
		},
	}}}
	recorder.Hooks().EmitTxEnd(nil, usage, nil)
	require.Equal(t, 1, calls)
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onTxEndV2":{"gasUsed":{"BlockExecutionGasUsed":40,"BlockStateGasUsed":70,"GasRefund":10}}}]}`, string(encoded))
}

func TestTxEndV2RecordingWithoutReceipt(t *testing.T) {
	recorder := &Tracer{flushMode: FlushModeTxn, outputDir: t.TempDir()}
	require.NotPanics(t, func() { recorder.Hooks().EmitTxEnd(nil, mdgas.TxnGasUsage{}, vm.ErrOutOfGas) })
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onTxEndV2":{"gasUsed":{"BlockExecutionGasUsed":0,"BlockStateGasUsed":0,"GasRefund":0},"error":"out of gas"}}]}`, string(encoded))
}

func TestFrameV2Recording(t *testing.T) {
	initial := mdgas.MdGas{Execution: 100, State: 200}
	usage := mdgas.MdGasUsage{Execution: 20, State: -10}
	var legacyEntry []uint64
	var legacyUsage []uint64
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{
		OnEnter: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, gas uint64, _ uint256.Int, _ []byte) {
			legacyEntry = append(legacyEntry, gas)
		},
		OnExit: func(_ int, _ []byte, gasUsed uint64, _ error, _ bool) {
			legacyUsage = append(legacyUsage, gasUsed)
		},
	}}}
	input := []byte{0xab}
	recorder.Hooks().EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, input, initial, uint256.Int{}, nil)
	input[0] = 0
	recorder.Hooks().EmitExit(0, nil, usage, nil, false)
	require.Equal(t, []uint64{100}, legacyEntry)
	require.Equal(t, []uint64{20}, legacyUsage)
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	var recorded struct {
		Traces []map[string]json.RawMessage
	}
	require.NoError(t, json.Unmarshal(encoded, &recorded))
	require.Len(t, recorded.Traces, 2)
	require.Contains(t, recorded.Traces[0], "onEnterV2")
	require.Contains(t, recorded.Traces[1], "onExitV2")
	var enter struct {
		Gas   mdgas.MdGas
		Input string
	}
	var exit struct {
		GasUsed mdgas.MdGasUsage
	}
	require.NoError(t, json.Unmarshal(recorded.Traces[0]["onEnterV2"], &enter))
	require.NoError(t, json.Unmarshal(recorded.Traces[1]["onExitV2"], &exit))
	require.Equal(t, initial, enter.Gas)
	require.Equal(t, "0xab", enter.Input)
	require.Equal(t, usage, exit.GasUsed)
}

func TestOpcodeV2Recording(t *testing.T) {
	gas := mdgas.MdGas{Execution: 100, State: 200}
	cost := mdgas.MdGas{Execution: 10, State: 50}
	scope := &vm.CallContext{Contract: *vm.NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})}
	scope.Memory.Resize(2)
	scope.Memory.Set(0, 2, []byte{0xab, 0xcd})
	var received [][2]mdgas.MdGas
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{
		OnOpcode: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
			t.Fatal("V2 must take precedence")
		},
		OnOpcodeV2: func(pc uint64, op byte, gas, cost mdgas.MdGas, context tracing.OpContext, rData []byte, depth int, err error) {
			require.Equal(t, uint64(42), pc)
			require.Equal(t, byte(vm.SSTORE), op)
			require.Same(t, scope, context)
			require.Equal(t, []byte{1}, rData)
			require.Equal(t, 2, depth)
			require.NoError(t, err)
			received = append(received, [2]mdgas.MdGas{gas, cost})
		},
	}}}
	recorder.Hooks().EmitOpcode(42, byte(vm.SSTORE), gas, cost, scope, []byte{1}, 2, nil)
	require.Equal(t, [][2]mdgas.MdGas{{gas, cost}}, received)
	scope.Memory.Set(0, 2, []byte{0, 0})
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onOpcodeV2":{"pc":42,"op":"SSTORE","gas":{"Execution":100,"State":200},"cost":{"Execution":10,"State":50},"caller":"0x0000000000000000000000000000000000000000","memory":"0xabcd","memSize":2,"returnData":"0x01","depth":2}}]}`, string(encoded))
}

func TestFaultV2Recording(t *testing.T) {
	gas := mdgas.MdGas{Execution: 100, State: 200}
	cost := mdgas.MdGas{Execution: 10, State: 50}
	scope := &vm.CallContext{Contract: *vm.NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})}
	var received [][2]mdgas.MdGas
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{
		OnFault: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ int, _ error) {
			t.Fatal("V2 must take precedence")
		},
		OnFaultV2: func(pc uint64, op byte, gas, cost mdgas.MdGas, context tracing.OpContext, depth int, err error) {
			require.Equal(t, uint64(42), pc)
			require.Equal(t, byte(vm.JUMP), op)
			require.Same(t, scope, context)
			require.Equal(t, 2, depth)
			require.ErrorIs(t, err, vm.ErrInvalidJump)
			received = append(received, [2]mdgas.MdGas{gas, cost})
		},
	}}}
	recorder.Hooks().EmitFault(42, byte(vm.JUMP), gas, cost, scope, 2, vm.ErrInvalidJump)
	require.Equal(t, [][2]mdgas.MdGas{{gas, cost}}, received)
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onFaultV2":{"pc":42,"op":86,"gas":{"Execution":100,"State":200},"cost":{"Execution":10,"State":50},"caller":"0x0000000000000000000000000000000000000000","depth":2,"error":"invalid jump destination"}}]}`, string(encoded))
}

func TestGasChangeV2Recording(t *testing.T) {
	old := mdgas.MdGas{Execution: 100}
	new := mdgas.MdGas{Execution: 200}
	var received [][2]mdgas.MdGas
	var reasons []tracing.GasChangeReason
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		received = append(received, [2]mdgas.MdGas{old, new})
		reasons = append(reasons, reason)
	}}}}
	recorder.Hooks().EmitGasChange(old, new, tracing.GasChangeCallOpCode)
	require.Equal(t, [][2]mdgas.MdGas{{old, new}}, received)
	require.Equal(t, []tracing.GasChangeReason{tracing.GasChangeCallOpCode}, reasons)
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onGasChangeV2":{"old":{"Execution":100,"State":0},"new":{"Execution":200,"State":0},"reason":"GasChangeCallOpCode"}}]}`, string(encoded))
}
