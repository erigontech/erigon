// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

func TestLegacyBlockchain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from rules engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}

	bt := new(testutil.TestMatcher)
	dir := filepath.Join(legacyDir, "BlockchainTests")

	// This directory contains no tests
	bt.SkipLoad(`.*\.meta/.*`)

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}

// TestLegacyCancunBlockchain runs the LegacyTests/Cancun hand-written
// BlockchainTests (ValidBlocks/InvalidBlocks) through the block executor — the
// only path that surfaces parallel-exec consensus bugs (e.g. a coinbase tip not
// burned on a same-tx SELFDESTRUCT), which TestLegacyCancunState's native
// single-transaction path cannot.
func TestLegacyCancunBlockchain(t *testing.T) {
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
	dir := filepath.Join(legacyDir, "LegacyTests", "Cancun", "BlockchainTests")
	bt.SkipLoad(`.*\.meta/.*`)

	// GeneralStateTests run natively via TestLegacyCancunState; re-running all of
	// them through the per-test block executor here is prohibitively slow.
	bt.SkipLoad(`GeneralStateTests`)
	// PoW total-difficulty reorg / multi-chain / fork-transition families: the
	// in-memory harness doesn't do TD-based fork choice, so these fail or flake
	// regardless of executor mode. Plus refundReset's Constantinople divergence.
	// Tracked in erigontech/erigon#22061.
	bt.SkipLoad(`bcMultiChainTest`)
	bt.SkipLoad(`bcForkStressTest`)
	bt.SkipLoad(`bcTotalDifficultyTest`)
	bt.SkipLoad(`TransitionTests`)
	bt.SkipLoad(`bcStateTests/refundReset\.json/refundReset_Constantinople`)

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
