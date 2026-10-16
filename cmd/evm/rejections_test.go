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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils/cmdtest"
)

// writeEditedFixture writes a copy of the fixture file src, with edit applied
// to its single fixture, into a temporary directory and returns its path.
func writeEditedFixture(t *testing.T, src string, edit func(fixture map[string]any)) string {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	var fixtures map[string]map[string]any
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 1)
	for _, fixture := range fixtures {
		edit(fixture)
	}
	data, err = json.Marshal(fixtures)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), filepath.Base(src))
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func enginePayloadParams(fixture map[string]any, i int) map[string]any {
	payload := fixture["engineNewPayloads"].([]any)[i].(map[string]any)
	return payload["params"].([]any)[0].(map[string]any)
}

// expectPayload1Invalid makes payload 1 deliver payload 0's access list, which
// its header does not commit to, and has the fixture expect it to be rejected
// with validationError.
func expectPayload1Invalid(validationError string) func(fixture map[string]any) {
	return func(fixture map[string]any) {
		enginePayloadParams(fixture, 1)["blockAccessList"] = enginePayloadParams(fixture, 0)["blockAccessList"]
		fixture["engineNewPayloads"].([]any)[1].(map[string]any)["validationError"] = validationError
		fixture["lastblockhash"] = enginePayloadParams(fixture, 0)["blockHash"]
		delete(fixture, "postState") // the fixture's is block 1's
	}
}

// Each result carries the client's raw error for every block or payload it
// rejected, at that block's index, and an empty list when it rejected none.
// The runner does not check the error against the fixture's expected
// exception: a block rejected for another reason than the one named passes.
func TestRunnersReportRejections(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	const (
		blockFixtures = "../../execution/tests/testdata/delivered_access_list/"
		engineFixture = "../../execution/tests/testdata/engine/bal_cross_block_ripemd160_state_leak.json"
		// The block in invalid_list commits to a list whose accounts are out
		// of order.
		badListOrder = "invalid block access list: account addresses must be strictly increasing"
	)
	wrongReason := func(fixture map[string]any) {
		block := fixture["blocks"].([]any)[0].(map[string]any)
		block["expectException"] = "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"
	}
	for _, tc := range []struct {
		name      string
		command   string
		fixture   func(t *testing.T) string
		index     int
		errSubstr string // empty when nothing is rejected
		noHash    bool   // the block failed to decode, so it has no hash
	}{
		{"block valid", "blocktest", func(*testing.T) string { return blockFixtures + "valid.json" }, 0, "", false},
		{"block rejected", "blocktest", func(*testing.T) string { return blockFixtures + "invalid_list.json" }, 0, badListOrder, false},
		{"block rejected for another reason", "blocktest", func(t *testing.T) string {
			return writeEditedFixture(t, blockFixtures+"invalid_list.json", wrongReason)
		}, 0, badListOrder, false},
		{"block that fails to decode", "blocktest", func(t *testing.T) string {
			return writeEditedFixture(t, blockFixtures+"invalid_list.json", func(fixture map[string]any) {
				fixture["blocks"].([]any)[0].(map[string]any)["rlp"] = "0xc0"
			})
		}, 0, "rlp", true},
		{"engine valid", "enginetest", func(*testing.T) string { return engineFixture }, 0, "", false},
		{"engine rejected for another reason", "enginetest", func(t *testing.T) string {
			return writeEditedFixture(t, engineFixture, expectPayload1Invalid("TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"))
		}, 1, "invalid block hash", false},
		// newPayloadV4 refuses an Amsterdam payload with a JSON-RPC error.
		{"engine rejected by error", "enginetest", func(t *testing.T) string {
			return writeEditedFixture(t, engineFixture, func(fixture map[string]any) {
				expectPayload1Invalid("BlockException.INVALID_BLOCK_HASH")(fixture)
				fixture["engineNewPayloads"].([]any)[1].(map[string]any)["newPayloadVersion"] = "4"
			})
		}, 1, "-32602: unexpected slotNumber before engine_newPayloadV5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := cmdtest.NewTestCmd(t, nil)
			tt.Run("evm-test", tc.command, "--jsonout", tc.fixture(t))
			stdout := tt.Output()
			tt.WaitExit()
			require.Equal(t, 0, tt.ExitStatus())

			var results []map[string]any
			require.NoError(t, json.Unmarshal(stdout, &results))
			require.Len(t, results, 1)
			require.Equal(t, true, results[0]["pass"], results[0]["error"])
			rejections, ok := results[0]["rejections"].([]any)
			require.True(t, ok, "rejections missing: %s", stdout)
			if tc.errSubstr == "" {
				require.Empty(t, rejections)
				return
			}
			require.Len(t, rejections, 1)
			rejection := rejections[0].(map[string]any)
			require.InDelta(t, tc.index, rejection["index"], 0)
			if tc.noHash {
				require.NotContains(t, rejection, "hash")
			} else {
				require.Regexp(t, "^0x[0-9a-f]{64}$", rejection["hash"])
			}
			require.Contains(t, rejection["error"], tc.errSubstr)
		})
	}
}
