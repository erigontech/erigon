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
	// There is also a LegacyTests folder, containing blockchain tests generated
	// prior to Istanbul. However, they are all derived from GeneralStateTests,
	// which run natively, so there's no reason to run them here.
}

func TestExecutionSpecBlockchain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testutil.TestMatcher)
	dir := filepath.Join(eestDir, "blockchain_tests")

	// Slow tests — split into their own packages for parallelism
	bt.SkipLoad(`^cancun/eip4844_blobs/test_invalid_negative_excess_blob_gas.json`)
	bt.SkipLoad(`^frontier/scenarios/test_scenarios.json`)
	bt.SkipLoad(`^osaka/eip7939_count_leading_zeros/test_clz_opcode_scenarios.json`)
	bt.SkipLoad(`^prague/eip7623_increase_calldata_cost/test_transaction_validity_type_1_type_2.json`)

	// Very slow tests
	bt.SkipLoad(`^berlin/eip2930_access_list/test_tx_intrinsic_gas.json`)
	bt.SkipLoad(`^cancun/eip4844_blobs/test_sufficient_balance_blob_tx`)
	bt.SkipLoad(`^cancun/eip4844_blobs/test_valid_blob_tx_combinations.json`)
	bt.SkipLoad(`^frontier/opcodes/test_stack_overflow.json`)
	bt.SkipLoad(`^prague/eip2537_bls_12_381_precompiles/test_invalid.json`)
	bt.SkipLoad(`^prague/eip2537_bls_12_381_precompiles/test_valid.json`)

	// Tested in the state test format by TestState
	bt.SkipLoad(`^static/state_tests/`)

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}

// Only runs EEST tests for current devnet - can "skip" on off-seasons
func TestExecutionSpecBlockchainDevnet(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if runtime.GOOS == "windows" {
		// TODO(yperbasis, mh0lt)
		t.Skip("fix me on windows please")
	}

	t.Parallel()
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dir := filepath.Join(eestDir, "blockchain_tests_devnet")
	bt := new(testutil.TestMatcher)
	// to run only tests for 1 eip do:
	//bt.Whitelist(`.*amsterdam/eip8024_dupn_swapn_exchange.*`)

	// static — tested in state test format by TestState
	bt.SkipLoad(`^static/state_tests/`)

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
