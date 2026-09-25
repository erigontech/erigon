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

package native_test

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/stretchr/testify/require"
)

func TestCallTracerStateGas(t *testing.T) {
	for _, name := range []string{"callTracer", "flatCallTracer"} {
		t.Run(name, func(t *testing.T) {
			tracer, err := tracers.New(name, &tracers.Context{}, json.RawMessage("{}"))
			require.NoError(t, err)
			tracer.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: true}},
				types.NewTransaction(0, accounts.ZeroAddress.Value(), nil, 100_000, nil, nil), accounts.ZeroAddress)
			tracer.EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
				mdgas.MdGas{Execution: 1000, State: 200}, uint256.Int{}, nil)
			for _, child := range []struct {
				op       vm.OpCode
				stateGas uint64
			}{
				{vm.CALL, 100},
				{vm.CREATE, 50},
				{vm.STATICCALL, 0},
			} {
				tracer.EmitEnter(1, byte(child.op), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
					mdgas.MdGas{Execution: 800, State: child.stateGas}, uint256.Int{}, nil)
				tracer.EmitExit(1, nil, mdgas.MdGasUsage{Execution: 20}, nil, false)
			}
			tracer.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 100}, nil, false)
			tracer.EmitTxEnd(&types.Receipt{GasUsed: 100}, mdgas.TxnGasUsage{}, nil)
			encoded, err := tracer.GetResult()
			require.NoError(t, err)
			var frames []map[string]json.RawMessage
			if name == "flatCallTracer" {
				require.NoError(t, json.Unmarshal(encoded, &frames))
				for i, frame := range frames {
					var action map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(frame["action"], &action))
					frames[i] = action
				}
			} else {
				var root map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(encoded, &root))
				require.NoError(t, json.Unmarshal(root["calls"], &frames))
				frames = append([]map[string]json.RawMessage{root}, frames...)
			}
			require.Len(t, frames, 4)
			require.JSONEq(t, `"0x186a0"`, string(frames[0]["gas"]))
			for i, want := range []string{`"0xc8"`, `"0x64"`, `"0x32"`} {
				require.JSONEq(t, want, string(frames[i]["stateGasReservoir"]))
			}
			require.NotContains(t, frames[3], "stateGasReservoir")
		})
	}
}

func TestFlatCallTracerTxnGasUsage(t *testing.T) {
	tracer, err := tracers.New("flatCallTracer", &tracers.Context{}, json.RawMessage("{}"))
	require.NoError(t, err)
	tracer.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: true}},
		types.NewTransaction(0, accounts.ZeroAddress.Value(), nil, 100_000, nil, nil), accounts.ZeroAddress)
	tracer.EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
		mdgas.MdGas{Execution: 1000, State: 200}, uint256.Int{}, nil)
	tracer.EmitEnter(1, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
		mdgas.MdGas{Execution: 800, State: 200}, uint256.Int{}, nil)
	tracer.EmitExit(1, nil, mdgas.MdGasUsage{Execution: 20, State: -30}, nil, false)
	tracer.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 100, State: 50}, nil, false)
	tracer.EmitTxEnd(&types.Receipt{GasUsed: 37_000},
		mdgas.TxnGasUsage{BlockExecutionGasUsed: 30_000, BlockStateGasUsed: 12_000, GasRefund: 5_000}, nil)
	encoded, err := tracer.GetResult()
	require.NoError(t, err)
	var frames []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &frames))
	require.Len(t, frames, 2)
	require.JSONEq(t, `{"gasUsed":"0x9088","regularGasUsed":"0x7530","stateGasUsed":"0x2ee0","gasRefund":"0x1388","output":"0x"}`, string(frames[0]["result"]))
	require.JSONEq(t, `{"gasUsed":"0x14","stateGasUsed":"-0x1e","output":"0x"}`, string(frames[1]["result"]))
	for _, field := range []string{"regularGasUsed", "stateGasUsed", "gasRefund"} {
		require.NotContains(t, frames[0], field)
		require.NotContains(t, frames[1], field)
	}
}

func TestFlatCallTracerTxnGasUsagePresence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		amsterdam bool
		frameErr  error
		txnErr    error
	}{
		{name: "pre-Amsterdam"},
		{name: "zero totals", amsterdam: true},
		{name: "reverted creation", amsterdam: true, frameErr: vm.ErrExecutionReverted},
		{name: "failed creation", amsterdam: true, frameErr: vm.ErrOutOfGas},
		{name: "transaction error", amsterdam: true, txnErr: vm.ErrInsufficientBalance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer, err := tracers.New("flatCallTracer", &tracers.Context{}, json.RawMessage("{}"))
			require.NoError(t, err)
			tracer.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: tc.amsterdam}},
				types.NewTransaction(0, accounts.ZeroAddress.Value(), nil, 100_000, nil, nil), accounts.ZeroAddress)
			tracer.EmitEnter(0, byte(vm.CREATE), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
				mdgas.MdGas{Execution: 1000}, uint256.Int{}, nil)
			tracer.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 100}, tc.frameErr, tc.frameErr != nil)
			tracer.EmitTxEnd(&types.Receipt{GasUsed: 100}, mdgas.TxnGasUsage{}, tc.txnErr)
			encoded, err := tracer.GetResult()
			require.NoError(t, err)
			var frames []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &frames))
			require.Len(t, frames, 1)
			for _, field := range []string{"regularGasUsed", "stateGasUsed", "gasRefund"} {
				require.NotContains(t, frames[0], field)
			}
			if errors.Is(tc.frameErr, vm.ErrOutOfGas) {
				require.NotContains(t, frames[0], "result")
				return
			}
			var result map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(frames[0]["result"], &result))
			for _, field := range []string{"regularGasUsed", "stateGasUsed", "gasRefund"} {
				if tc.amsterdam && tc.txnErr == nil {
					require.JSONEq(t, `"0x0"`, string(result[field]))
				} else {
					require.NotContains(t, result, field)
				}
			}
		})
	}
}

// TestTracerStopRace exercises the concurrent Stop / GetResult path that the
// trace RPC handler uses: a timeout watchdog goroutine calls Stop while the
// main goroutine is still running the trace and will eventually call
// GetResult. Under -race, writes to the interruption reason field must not
// race with reads, for every tracer that implements it.
//
// callTracer and flatCallTracer's GetResult short-circuits when the callstack
// is empty, before loading the reason. For those tracers the test pushes a
// single top-level call frame via OnEnter so GetResult reaches the reason.Load()
// path where the race can be observed under -race.
func TestTracerStopRace(t *testing.T) {
	type setup struct {
		name       string
		needsFrame bool // whether GetResult requires a top-level call frame
	}
	cases := []setup{
		{"callTracer", true},
		{"flatCallTracer", true},
		{"4byteTracer", false},
		{"prestateTracer", false},
	}
	for _, s := range cases {
		t.Run(s.name, func(t *testing.T) {
			const iterations = 1000
			stopErr := errors.New("execution timeout")

			for range iterations {
				tr, err := tracers.New(s.name, &tracers.Context{}, json.RawMessage("{}"))
				require.NoError(t, err)

				if s.needsFrame && tr.HasEnterHook() {
					// Push a single top-level call frame so GetResult doesn't
					// short-circuit before reading the interruption reason.
					tr.EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil, mdgas.MdGas{}, uint256.Int{}, nil)
				}

				start := make(chan struct{})
				var ready sync.WaitGroup
				var wg sync.WaitGroup
				ready.Add(2)
				wg.Go(func() {
					ready.Done()
					<-start
					tr.Stop(stopErr)
				})
				wg.Go(func() {
					ready.Done()
					<-start
					_, _ = tr.GetResult()
				})
				ready.Wait()
				close(start)
				wg.Wait()
			}
		})
	}
}
