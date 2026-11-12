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

package executiontests

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/node/ethconfig"
)

const (
	engineFixture              = "testdata/engine/bal_cross_block_ripemd160_state_leak.json"
	corruptedAccessListFixture = "testdata/engine/bal_invalid_balance_value.json"
)

func loadEngineTest(t *testing.T, edit func(payloads []any) []any) *engineapitester.EngineTest {
	t.Helper()
	src, err := os.ReadFile(engineFixture)
	require.NoError(t, err)
	var fixtures map[string]map[string]any
	require.NoError(t, json.Unmarshal(src, &fixtures))
	fixture := fixtures["bal_cross_block_ripemd160_state_leak"]
	if edit != nil {
		fixture["engineNewPayloads"] = edit(fixture["engineNewPayloads"].([]any))
	}
	src, err = json.Marshal(fixture)
	require.NoError(t, err)
	var test engineapitester.EngineTest
	require.NoError(t, json.Unmarshal(src, &test))
	return &test
}

// A fixture without engineNewPayloads is not an engine test; an empty list
// is one with no payloads.
func TestEngineTestNeedsPayloads(t *testing.T) {
	src, err := os.ReadFile(engineFixture)
	require.NoError(t, err)
	var fixtures map[string]map[string]any
	require.NoError(t, json.Unmarshal(src, &fixtures))
	fixture := fixtures["bal_cross_block_ripemd160_state_leak"]
	for _, tc := range []struct {
		name     string
		payloads any
		wantErr  bool
	}{
		{"missing", nil, true},
		{"null", json.RawMessage("null"), true},
		{"empty", []any{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delete(fixture, "engineNewPayloads")
			if tc.payloads != nil {
				fixture["engineNewPayloads"] = tc.payloads
			}
			src, err := json.Marshal(fixture)
			require.NoError(t, err)
			err = json.Unmarshal(src, new(engineapitester.EngineTest))
			if tc.wantErr {
				require.ErrorContains(t, err, "fixture has no engineNewPayloads")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEngineTest(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	t.Run("valid chain", func(t *testing.T) {
		require.NoError(t, loadEngineTest(t, nil).RunCLI())
	})

	// Block 1 delivers block 0's list: well formed, but not the one its header
	// commits to.
	t.Run("wrong access list", func(t *testing.T) {
		test := loadEngineTest(t, func(payloads []any) []any {
			params := func(i int) map[string]any {
				return payloads[i].(map[string]any)["params"].([]any)[0].(map[string]any)
			}
			params(1)["blockAccessList"] = params(0)["blockAccessList"]
			return payloads
		})
		require.ErrorContains(t, test.RunCLI(), "payload 1: payload status INVALID")
	})

	// The payload's list matches its header but not its execution: one
	// balance is changed and the block hash recomputed. It is INVALID whether
	// blocks run in parallel or on one worker.
	for _, tc := range []struct {
		name    string
		workers int
		path    string
	}{
		{"corrupted access list in parallel", 0, "parallel"},
		{"corrupted access list on one worker", 1, "sequential"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := os.ReadFile(corruptedAccessListFixture)
			require.NoError(t, err)
			var tests map[string]*engineapitester.EngineTest
			require.NoError(t, json.Unmarshal(src, &tests))
			require.Len(t, tests, 1)
			for _, test := range tests {
				var paths []string
				test.ExecWorkers = tc.workers
				test.ExecutionPathReporter = func(p ethconfig.BlockExecutionPath) { paths = append(paths, p.Path) }
				require.NoError(t, test.RunCLI())
				require.Equal(t, []string{tc.path}, paths)
			}
		})
	}

	// A node with no peers cannot fetch a missing parent; the test must fail
	// with SYNCING rather than wait.
	t.Run("unknown parent", func(t *testing.T) {
		test := loadEngineTest(t, func(payloads []any) []any { return payloads[1:] })
		require.ErrorContains(t, test.RunCLI(), "payload 0: payload status SYNCING")
	})

	// SYNCING is not a rejection, so an expected-invalid payload answered
	// SYNCING still fails the fixture.
	t.Run("unknown parent of an invalid payload", func(t *testing.T) {
		test := loadEngineTest(t, func(payloads []any) []any {
			payloads[1].(map[string]any)["validationError"] = "BlockException.INVALID_BLOCK_HASH"
			return payloads[1:]
		})
		require.ErrorContains(t, test.RunCLI(), "payload 0: payload status SYNCING")
		require.Empty(t, test.Rejections)
	})

	// The RPC handler answers a param it cannot decode, or one too many, with
	// -32602. The bad payload comes last, so the chain still ends where the
	// fixture expects.
	for _, tc := range []struct {
		name string
		edit func(params []any) []any
	}{
		{"undecodable param", func(params []any) []any {
			params[0].(map[string]any)["blockHash"] = "0xzz"
			return params
		}},
		{"extra param", func(params []any) []any { return append(params, "0x") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := loadEngineTest(t, func(payloads []any) []any {
				var bad map[string]any
				src, err := json.Marshal(payloads[0])
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(src, &bad))
				bad["params"] = tc.edit(bad["params"].([]any))
				bad["errorCode"] = "-32602"
				return append(payloads, bad)
			})
			require.NoError(t, test.RunCLI())
			require.Len(t, test.Rejections, 1)
			require.Contains(t, test.Rejections[0].Error, "-32602: ")
		})
	}
}
