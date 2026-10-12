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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// No transaction before London can carry maxFeePerGas or maxPriorityFeePerGas, so every method
// that runs a call object rejects them there, zero or not, as it rejects an access list before
// Berlin. A legacy gasPrice still runs, and from London on the dynamic fee fields run as before.
func TestCallDynamicFeesBeforeLondon(t *testing.T) {
	london := chain.TestChainBerlinConfig.Copy()
	london.LondonBlock = common.NewUint64(0)

	// A contract creation whose init code returns GASPRICE.
	const gasPriceCode = "0x3a60005260206000f3"
	const oneGwei, twoGwei = "0x3b9aca00", "0x77359400"
	word := func(hex string) string { return "0x" + common.Bytes2Hex(common.LeftPadBytes(common.FromHex(hex), 32)) }

	bundles := func(call map[string]any) []any {
		return []any{[]any{map[string]any{"transactions": []any{call}}}, map[string]any{"blockNumber": "latest"}}
	}
	simulate := func(blockOverrides map[string]any, validation bool) func(call map[string]any) []any {
		return func(call map[string]any) []any {
			block := map[string]any{"calls": []any{call}}
			if blockOverrides != nil {
				block["blockOverrides"] = blockOverrides
			}
			return []any{map[string]any{"blockStateCalls": []any{block}, "validation": validation}, "latest"}
		}
	}
	// Each key names the method, then the variant.
	methods := map[string]func(call map[string]any) []any{
		"eth_call":                       func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_estimateGas":                func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_createAccessList":           func(call map[string]any) []any { return []any{call, "latest"} },
		"eth_callMany":                   bundles,
		"eth_simulateV1":                 simulate(nil, false),
		"eth_simulateV1 with validation": simulate(nil, true),
		"debug_traceCall":                func(call map[string]any) []any { return []any{call, "latest"} },
		"debug_traceCallMany":            bundles,
		"trace_call":                     func(call map[string]any) []any { return []any{call, []string{TraceTypeTrace}, "latest"} },
		"trace_callMany": func(call map[string]any) []any {
			return []any{[]any{[]any{call, []string{TraceTypeTrace}}}, "latest"}
		},
	}

	type sender func(t *testing.T, method string, params []any) (json.RawMessage, error)
	type caller func(fees map[string]any) map[string]any
	dial := func(t *testing.T, cfg *chain.Config) (sender, caller) {
		m, _, bankAddr := fundedBankGenesis(t, cfg)
		base := newBaseApiForTest(m)
		server := rpc.NewServer(50, false, false, true, log.New(), 100)
		require.NoError(t, server.RegisterName("eth", EthAPI(newEthApiForTest(base, m.DB, nil, nil))))
		require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{}))))
		require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
		client := rpc.DialInProc(server, log.New())
		t.Cleanup(func() { client.Close(); server.Stop() })
		send := func(t *testing.T, method string, params []any) (json.RawMessage, error) {
			t.Helper()
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, method, params...)
			return result, err
		}
		with := func(fees map[string]any) map[string]any {
			// A nonce spares eth_createAccessList a txpool lookup.
			call := map[string]any{"from": bankAddr, "gas": "0x493e0", "nonce": "0x0", "data": gasPriceCode}
			maps.Copy(call, fees)
			return call
		}
		return send, with
	}

	t.Run("before London", func(t *testing.T) {
		send, with := dial(t, chain.TestChainBerlinConfig)
		for name, params := range methods {
			method, _, _ := strings.Cut(name, " ")
			t.Run(name, func(t *testing.T) {
				for name, fees := range map[string]map[string]any{
					"priced dynamic fees": {"maxFeePerGas": twoGwei, "maxPriorityFeePerGas": oneGwei},
					"zero dynamic fees":   {"maxFeePerGas": "0x0", "maxPriorityFeePerGas": "0x0"},
					"zero fee cap":        {"maxFeePerGas": "0x0"},
					"zero tip":            {"maxPriorityFeePerGas": "0x0"},
				} {
					t.Run(name, func(t *testing.T) {
						_, err := send(t, method, params(with(fees)))
						require.ErrorContains(t, err, types.ErrDynamicFeePreLondon.Error())
					})
				}
				for name, fees := range map[string]map[string]any{
					"gas price":           {"gasPrice": oneGwei},
					"zero gas price":      {"gasPrice": "0x0"},
					"no fees":             {},
					"null dynamic fees":   {"maxFeePerGas": nil, "maxPriorityFeePerGas": nil},
					"explicit type":       {"type": "0x2"},
					"type with gas price": {"type": "0x2", "gasPrice": oneGwei},
				} {
					t.Run(name, func(t *testing.T) {
						_, err := send(t, method, params(with(fees)))
						require.NoError(t, err)
					})
				}
			})
		}

		t.Run("eth_call runs a gas price at that price", func(t *testing.T) {
			got, err := send(t, "eth_call", []any{with(map[string]any{"gasPrice": oneGwei}), "latest"})
			require.NoError(t, err)
			require.JSONEq(t, `"`+word(oneGwei)+`"`, string(got))
		})

		// The estimate of a plain transfer to a codeless account takes a shortcut, which still runs
		// the transfer once.
		t.Run("eth_estimateGas of a plain transfer", func(t *testing.T) {
			transfer := func(fees map[string]any) map[string]any {
				call := with(fees)
				delete(call, "data")
				call["to"] = "0x000000000000000000000000000000000000dead"
				return call
			}
			_, err := send(t, "eth_estimateGas", []any{transfer(map[string]any{"maxFeePerGas": twoGwei, "maxPriorityFeePerGas": oneGwei}), "latest"})
			require.ErrorContains(t, err, types.ErrDynamicFeePreLondon.Error())
			_, err = send(t, "eth_estimateGas", []any{transfer(map[string]any{"gasPrice": oneGwei}), "latest"})
			require.NoError(t, err)
		})

		// eth_simulateV1 fills zero dynamic fees into a call when its block has a base fee. Those
		// defaults are not the caller's, so a base fee override before London still runs a call
		// without fees, while fees the caller named are still rejected.
		t.Run("eth_simulateV1 with a base fee override", func(t *testing.T) {
			overridden := simulate(map[string]any{"baseFeePerGas": "0x0"}, false)
			_, err := send(t, "eth_simulateV1", overridden(with(nil)))
			require.NoError(t, err)
			_, err = send(t, "eth_simulateV1", overridden(with(map[string]any{"maxFeePerGas": twoGwei, "maxPriorityFeePerGas": oneGwei})))
			require.ErrorContains(t, err, types.ErrDynamicFeePreLondon.Error())
		})
	})

	t.Run("at London", func(t *testing.T) {
		send, with := dial(t, london)
		for name, params := range methods {
			method, _, _ := strings.Cut(name, " ")
			t.Run(name, func(t *testing.T) {
				_, err := send(t, method, params(with(map[string]any{"maxFeePerGas": twoGwei, "maxPriorityFeePerGas": oneGwei})))
				require.NoError(t, err)
			})
		}

		t.Run("eth_call runs zero dynamic fees", func(t *testing.T) {
			got, err := send(t, "eth_call", []any{with(map[string]any{"maxFeePerGas": "0x0", "maxPriorityFeePerGas": "0x0"}), "latest"})
			require.NoError(t, err)
			require.JSONEq(t, `"`+word("0x0")+`"`, string(got))
		})

		// The genesis base fee is 1 gwei, so a 1 gwei tip under a 2 gwei cap pays 2 gwei.
		t.Run("eth_call runs dynamic fees at the effective price", func(t *testing.T) {
			got, err := send(t, "eth_call", []any{with(map[string]any{"maxFeePerGas": twoGwei, "maxPriorityFeePerGas": oneGwei}), "latest"})
			require.NoError(t, err)
			require.JSONEq(t, `"`+word(twoGwei)+`"`, string(got))
		})
	})
}
