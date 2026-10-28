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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCoinbaseSelfdestructWorkerCount(t *testing.T) {
	previousParallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() { dbg.Exec3Parallel = previousParallel })

	raw, err := os.ReadFile("testdata/coinbase_selfdestruct.json")
	require.NoError(t, err)

	for _, workers := range []int{1, 2, 3, 4, 8, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			previous := ethconfig.Defaults.Sync.ExecWorkerCount
			ethconfig.Defaults.Sync.ExecWorkerCount = workers
			t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = previous })

			var tests map[string]*testutil.BlockTest
			require.NoError(t, json.Unmarshal(raw, &tests))
			require.Len(t, tests, 1)
			for _, bt := range tests {
				require.NoError(t, bt.Run(t))
			}
		})
	}
}
