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

	"github.com/erigontech/erigon/execution/tests/testutil"
)

func TestRevertedStorageReadAfterRecreateHasCorrectBALHash(t *testing.T) {
	fixture, err := os.ReadFile("testdata/missing_eest_coverage/reverted_storage_read_after_recreate_has_correct_bal_hash_github_issue_23407.json")
	require.NoError(t, err)
	blockTests := make(map[string]*testutil.BlockTest)
	require.NoError(t, json.Unmarshal(fixture, &blockTests))
	blockTest := blockTests["eg-witness.min"]
	require.NotNil(t, blockTest)
	require.NoError(t, blockTest.Run(t))
}
