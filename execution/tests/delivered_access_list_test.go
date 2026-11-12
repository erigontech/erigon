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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/node/ethconfig"
)

func loadDeliveredAccessListTest(t *testing.T, name string, edit func(fixture map[string]any)) (*testutil.BlockTest, *[]ethconfig.BlockExecutionPath) {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("testdata", "delivered_access_list", name+".json"))
	require.NoError(t, err)
	var fixtures map[string]map[string]any
	require.NoError(t, json.Unmarshal(src, &fixtures))
	fixture := fixtures[name]
	if edit != nil {
		edit(fixture)
	}
	src, err = json.Marshal(fixture)
	require.NoError(t, err)
	bt := new(testutil.BlockTest)
	require.NoError(t, json.Unmarshal(src, bt))
	var paths []ethconfig.BlockExecutionPath
	bt.ExecWorkers = 2
	bt.ExecutionPathReporter = func(p ethconfig.BlockExecutionPath) { paths = append(paths, p) }
	return bt, &paths
}

// The valid fixture's block, delivered as is and with one value in its access
// list changed, so the list's hash differs from the header's. The block test
// drops that list, as sync drops a peer's, and the block imports and runs
// without it, reported as "bad-access-list".
func TestBlockTestDropsMismatchedBlockAccessList(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	for _, tc := range []struct {
		name       string
		edit       func(fixture map[string]any)
		accessList bool
		reason     string
	}{
		{"valid", nil, true, ""},
		{"mismatched_list", func(fixture map[string]any) {
			block := fixture["blocks"].([]any)[0].(map[string]any)
			account := block["blockAccessList"].([]any)[16].(map[string]any)
			account["nonceChanges"].([]any)[1].(map[string]any)["postNonce"] = "0x03"
		}, false, "bad-access-list"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bt, paths := loadDeliveredAccessListTest(t, "valid", tc.edit)
			require.NoError(t, bt.Run(t))
			require.Len(t, *paths, 1)
			require.Equal(t, "parallel", (*paths)[0].Path)
			require.Equal(t, tc.accessList, (*paths)[0].AccessList)
			require.Equal(t, tc.reason, (*paths)[0].Reason)
		})
	}

	// The header commits to a list whose accounts are out of order. The list
	// matches, so it is attached, and the block is rejected before it runs; a
	// dropped list would have let it run and print a line.
	t.Run("invalid_list", func(t *testing.T) {
		bt, paths := loadDeliveredAccessListTest(t, "invalid_list", nil)
		require.NoError(t, bt.Run(t))
		require.Empty(t, *paths)
	})
}
