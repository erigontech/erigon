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
	if runtime.GOOS == "windows" {
		// TODO(yperbasis, mh0lt)
		t.Skip("fix me on windows please")
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
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_sstore_and_oog.json`)                               // block=1, gas used by execution: 37568, in header: 63573
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_deposit_gas.json`)                      // block=1, gas used by execution: 38601120, in header: 16777216
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_call_oog_reservoir_inflation_detection.json`)   // block=1, gas used by execution: 82640, in header: 214128
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_oog_reservoir_inflation_detection.json`) // block=1, block access list mismatch
	bt.SkipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_subcall_create2_oog.json`)                          // block=1, receiptHash mismatch: 39c2dfd7f2067a536d977a40ae2f2b3f3614bbef0a4eb26f49b8114d29e9805f != 8e4253c0afb1566ce113e97f42df12955f2712695f33de2b2c6ab30f654f8897, headerNum=1, 4da65b36435c944e6f68e86aa3d5ad3b796d54b0f73f5e096b31e05ae4c8d32d
	bt.SkipLoad(`^for_amsterdam/frontier/create/test_create_deposit_oog.json`)                                                          // block=1, receiptHash mismatch: b02a14b5b6c881a265f2b3cde9e15388accd60f6ade5ac416448ae70be388b04 != 4b6910d817802196c2707443f8d462fb49eda48f61b5ae0b986ec93f512068d9, headerNum=1, 8f10f38664c884462846eb91339f5c5b5fe818b7f6b6f766ce19e45d109cb0cf
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_genesis_hash_available.json`)                                                     // block=257, gas used by execution: 62432, in header: 37568
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_value_transfer_gas_calculation.json`)                                             // block=1, gas used by execution: 37443, in header: 168931
	bt.SkipLoad(`^for_amsterdam/frontier/scenarios/test_scenarios.json`)                                                                // block=2, gas used by execution: 54514, in header: 349432

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
