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

package jsonrpc

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestDebugTraceCallBlockOverrideKeys(t *testing.T) {
	dialDebug := func(t *testing.T, cfg *chain.Config) (*rpc.Client, map[string]any, hexutil.Uint64) {
		t.Helper()
		c := newBaseFeeTestChain(t, cfg)
		server := rpc.NewServer(50, false, false, true, log.New(), 100)
		require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(c.debugAPI())))
		client := rpc.DialInProc(server, log.New())
		t.Cleanup(func() {
			client.Close()
			server.Stop()
		})
		callArgs := map[string]any{"from": c.bankAddress, "to": "0x0000000000000000000000000000000000000002", "gas": "0x186a0"}
		return client, callArgs, hexutil.Uint64(c.head.Time())
	}
	traceCall := func(client *rpc.Client, callArgs map[string]any, blockOverrides string) error {
		var res json.RawMessage
		config := map[string]any{"tracer": "callTracer", "blockOverrides": json.RawMessage(blockOverrides)}
		return client.CallContext(t.Context(), &res, "debug_traceCall", callArgs, "latest", config)
	}

	t.Run("after the merge", func(t *testing.T) {
		client, callArgs, blockTime := dialDebug(t, chain.AllProtocolChanges)
		for _, overrides := range []string{
			`{"fooBar":"0x1"}`,
			`{"time":"` + blockTime.String() + `","Time":"` + blockTime.String() + `"}`,
			`{"difficulty":"0x1"}`,
			`{"difficulty":"0x0"}`,
		} {
			requireErrorCode(t, traceCall(client, callArgs, overrides), -32602)
		}
		for _, overrides := range []string{
			`{"Time":"` + blockTime.String() + `"}`,
			`{"number":"0x10","prevRandao":"0x0000000000000000000000000000000000000000000000000000000000000001"}`,
			`null`,
		} {
			require.NoError(t, traceCall(client, callArgs, overrides), overrides)
		}
		var res json.RawMessage
		config := json.RawMessage(`{"tracer":"callTracer","tracerConfig":{"blockOverrides":{"fooBar":"0x1"}}}`)
		require.NoError(t, client.CallContext(t.Context(), &res, "debug_traceCall", callArgs, "latest", config))
	})

	t.Run("repeated outer keys", func(t *testing.T) {
		client, callArgs, blockTime := dialDebug(t, chain.AllProtocolChanges)
		ts := `"` + blockTime.String() + `"`
		for _, config := range []string{
			`{"tracer":"callTracer","blockOverrides":{"fooBar":"0x1","time":` + ts + `,"Time":` + ts + `},"BlockOverrides":{}}`,
			`{"tracer":"callTracer","blockOverrides":{"fooBar":"0x1"},"blockOverrides":{}}`,
			`{"tracer":"callTracer","blockOverrides":{},"BlockOverrides":{"fooBar":"0x1"}}`,
		} {
			var res json.RawMessage
			err := client.CallContext(t.Context(), &res, "debug_traceCall", callArgs, "latest", json.RawMessage(config))
			requireErrorCode(t, err, -32602)
		}
		var res json.RawMessage
		config := json.RawMessage(`{"tracer":"callTracer","blockOverrides":{"time":` + ts + `},"BlockOverrides":{"Time":` + ts + `}}`)
		require.NoError(t, client.CallContext(t.Context(), &res, "debug_traceCall", callArgs, "latest", config))
	})

	t.Run("before the merge", func(t *testing.T) {
		client, callArgs, _ := dialDebug(t, chain.TestChainBerlinConfig)
		require.NoError(t, traceCall(client, callArgs, `{"difficulty":"0x1"}`))
	})
}

func TestDebugTraceCallConfigPreservesJSONMerge(t *testing.T) {
	prefilled := func() *ethapi.BlockOverrides {
		number, gasLimit := hexutil.U256(*uint256.NewInt(7)), hexutil.Uint64(30_000_000)
		return &ethapi.BlockOverrides{Number: &number, GasLimit: &gasLimit}
	}
	for _, input := range []string{
		`{"tracer":"callTracer"}`,
		`null`,
		`{"blockOverrides":{"time":"0x10"}}`,
		`{"blockOverrides":{"number":"0x9"}}`,
		`{"blockOverrides":{}}`,
		`{"blockOverrides":null}`,
		`{"blockOverrides":{"time":"0x10"},"BlockOverrides":{"gasLimit":"0x1"}}`,
	} {
		want := tracersConfig.TraceConfig{BlockOverrides: prefilled()}
		require.NoError(t, json.Unmarshal([]byte(input), &want), input)
		got := DebugTraceCallConfig{BlockOverrides: prefilled()}
		require.NoError(t, json.Unmarshal([]byte(input), &got), input)
		require.Equal(t, want, tracersConfig.TraceConfig(got), input)
	}
}
