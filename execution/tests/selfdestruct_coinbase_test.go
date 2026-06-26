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
	"path/filepath"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

// TestSuicideCoinbase pins a pre-existing contract at the coinbase that
// SELFDESTRUCTs and is then revived by the same block's fees/reward (a value
// transfer, no CREATE). The parallel executor keeps a stale pre-SD code hash
// and computes a wrong state root here; it is correct under EXEC3_PARALLEL=false
// and on Cancun, where EIP-6780 keeps the pre-existing contract in place.
func TestSuicideCoinbase(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	if runtime.GOOS == "windows" {
		t.Skip("see TestLegacyBlockchain")
	}

	bt := new(testutil.TestMatcher)
	bt.Whitelist(`suicideCoinbase\.json`)
	dir := filepath.Join(legacyDir, "LegacyTests", "Cancun", "BlockchainTests", "ValidBlocks", "bcStateTests")
	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
