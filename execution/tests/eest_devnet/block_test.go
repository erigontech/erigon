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

package eest_devnet_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

// Only runs EEST tests for current devnet - can "skip" on off-seasons
func TestExecutionSpecBlockchainDevnet(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		// TODO(yperbasis, mh0lt)
		t.Skip("fix me on windows/macOS please")
	}

	t.Parallel()
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dir := filepath.Join("..", "execution-spec-tests", "blockchain_tests_devnet")
	bt := new(testutil.TestMatcher)
	// to run only tests for 1 eip do:
	//bt.Whitelist(`.*amsterdam/eip8024_dupn_swapn_exchange.*`)

	// only run tests for amsterdam, otherwise this takes too long
	bt.Whitelist(`.*for_amsterdam/.*`)
	// static — tested in state test format by TestState
	bt.SkipLoad(`^for_amsterdam/static/state_tests/`)

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
