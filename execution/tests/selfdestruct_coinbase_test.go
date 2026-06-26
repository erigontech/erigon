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
// SELFDESTRUCTs in a tx: that tx's priority fee, credited to the coinbase, must
// be burned by the same-tx self-destruct rather than survive into the block's
// final coinbase balance. Pre-Cancun only — EIP-6780 leaves the pre-existing
// contract in place, so Cancun never self-destructs it.
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
