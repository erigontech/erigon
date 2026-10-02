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
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// A call object whose data and input are both set and differ is invalid params in every method
// that takes one. Equal values are accepted, and a null member is the same as an omitted one.
func TestCallDataInputConflict(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	base := newBaseApiForTest(m)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("eth", EthAPI(newEthApiForTest(base, m.DB, nil, nil))))
	require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{}))))
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	// Contract creations whose init code returns the word 42 or the word 1.
	const code42, code1 = "0x602a60005260206000f3", "0x600160005260206000f3"
	bundles := func(call map[string]any) []any {
		return []any{[]any{map[string]any{"transactions": []any{call}}}, map[string]any{"blockNumber": "latest"}}
	}
	methods := map[string]func(call map[string]any) []any{
		"eth_call":             func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_estimateGas":      func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_createAccessList": func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_callMany":         bundles,
		"eth_simulateV1": func(call map[string]any) []any {
			return []any{map[string]any{"blockStateCalls": []any{map[string]any{"calls": []any{call}}}}, "latest"}
		},
		"eth_fillTransaction": func(call map[string]any) []any { return []any{call} },
		"debug_traceCall":     func(call map[string]any) []any { return []any{call, "latest"} },
		"debug_traceCallMany": bundles,
		"trace_call":          func(call map[string]any) []any { return []any{call, []string{TraceTypeTrace}, "latest"} },
		"trace_callMany": func(call map[string]any) []any {
			return []any{[]any{[]any{call, []string{TraceTypeTrace}}}, "latest"}
		},
	}
	with := func(calldata map[string]any) map[string]any {
		// A nonce spares eth_createAccessList and eth_fillTransaction a txpool lookup.
		call := map[string]any{"from": bankAddr, "gas": "0x493e0", "maxFeePerGas": "0x77359400", "maxPriorityFeePerGas": "0x0", "nonce": "0x0"}
		maps.Copy(call, calldata)
		return call
	}

	for method, params := range methods {
		t.Run(method, func(t *testing.T) {
			send := func(t *testing.T, calldata map[string]any) (json.RawMessage, error) {
				t.Helper()
				var result json.RawMessage
				err := client.CallContext(t.Context(), &result, method, params(with(calldata))...)
				return result, err
			}

			want, err := send(t, map[string]any{"data": code42})
			require.NoError(t, err)
			for name, calldata := range map[string]map[string]any{
				"input":       {"input": code42},
				"equal":       {"data": code42, "input": code42},
				"equal bytes": {"data": "0x" + strings.ToUpper(code42[2:]), "input": code42},
				"null input":  {"data": code42, "input": nil},
				"null data":   {"data": nil, "input": code42},
			} {
				t.Run(name, func(t *testing.T) {
					got, err := send(t, calldata)
					require.NoError(t, err)
					require.JSONEq(t, string(want), string(got))
				})
			}

			for name, calldata := range map[string]map[string]any{
				"differ":      {"data": code42, "input": code1},
				"empty input": {"data": code42, "input": "0x"},
				"empty data":  {"data": "0x", "input": code42},
			} {
				t.Run(name, func(t *testing.T) {
					_, err := send(t, calldata)
					var rpcErr rpc.Error
					require.ErrorAs(t, err, &rpcErr)
					require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
					require.ErrorContains(t, err, `both "data" and "input" are set and not equal`)
				})
			}
		})
	}
}
