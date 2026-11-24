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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc"
)

func TestTraceCallStateOverrideForms(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.TestChainOsakaConfig)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", TraceAPI(newTraceApiForTest(m))))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() {
		client.Close()
		server.Stop()
	})

	const target = "0x00000000000000000000000000000000cafe0001"
	// PUSH1 0x2a PUSH1 0 MSTORE PUSH1 0x20 PUSH1 0 RETURN
	stateMap := map[string]any{target: map[string]any{"code": "0x602a60005260206000f3"}}
	const wantOutput = "0x000000000000000000000000000000000000000000000000000000000000002a"
	call := map[string]any{"from": bankAddr.Hex(), "to": target, "gas": "0x10000"}
	traceTypes := []string{TraceTypeTrace}

	for _, tc := range []struct {
		name      string
		overrides any
	}{
		{name: "positional state map", overrides: stateMap},
		{name: "configuration wrapper", overrides: map[string]any{"stateOverrides": stateMap}},
	} {
		t.Run("trace_call "+tc.name, func(t *testing.T) {
			var result TraceCallResult
			require.NoError(t, client.CallContext(t.Context(), &result, "trace_call", call, traceTypes, "latest", tc.overrides))
			require.Equal(t, wantOutput, result.Output.String())
		})
		t.Run("trace_callMany "+tc.name, func(t *testing.T) {
			var results []TraceCallResult
			calls := []any{[]any{call, traceTypes}}
			require.NoError(t, client.CallContext(t.Context(), &results, "trace_callMany", calls, "latest", tc.overrides))
			require.Len(t, results, 1)
			require.Equal(t, wantOutput, results[0].Output.String())
		})
	}

	t.Run("addresses mixed with configuration fields", func(t *testing.T) {
		mixed := map[string]any{target: stateMap[target], "stateOverrides": stateMap}
		for _, params := range [][]any{
			{call, traceTypes, "latest", mixed},
			{[]any{[]any{call, traceTypes}}, "latest", mixed},
		} {
			method := "trace_call"
			if len(params) == 3 {
				method = "trace_callMany"
			}
			var result any
			err := client.CallContext(t.Context(), &result, method, params...)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr, method)
			require.Equal(t, -32602, rpcErr.ErrorCode(), method)
		}
	})
}
