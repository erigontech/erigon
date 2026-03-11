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
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_finalization_burn_logs.json`)                          // block=1, receiptHash mismatch: 432a8b7b999111d19c579e4fab6b9e24b39500f183bee3a6de0d0788063fe069 != 559d56e60af9621877a9db4037f33c2a27411983b44ec54526ce8584f691292e, headerNum=1, 74bf5654f3b79cf5e3131cc27e206737c863efc1c57aad89d01e2a7f6298a8b4
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_finalization_after_priority_fee.json`)    // block=1, receiptHash mismatch: ebab83610f8dfd74c1a394f458de5abd682bb67fc419a331c98ab14fa4a3787d != de22d27f88412395e117fd76b35ffa6781ea18725354cab7665a7b6bdd55df28, headerNum=1, 8798592215fae22cb4c5dd5ccc11a0eb4b6d027af40635c5cd43513ce417da45
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_request_invalid.json`)                 // block=1, gas used by execution: 56234, in header: 150272
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_aborted_storage_access.json`)               // block=1, gas used by execution: 28115, in header: 37568
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_sstore_and_oog.json`)                       // block=1, gas used by execution: 63573, in header: 37568
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size.json`)                          // block=1, gas used by execution: 16777216, in header: 38602294
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_deposit_gas.json`)              // block=1, gas used by execution: 1332100, in header: 38601120
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_via_create.json`)           // block=1, gas used by execution: 16777216, in header: 16645728
	bt.SkipLoad(`^for_amsterdam/cancun/create/test_create_oog_from_eoa_refunds.json`)                                           // block=1, gas used by execution: 131488, in header: 338112
	bt.SkipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_created_in_same_tx_with_revert.json`)             // block=1, receiptHash mismatch: 7e9ba0f7c8ae60792b17cd8f66f4927758071ecc540c3301216a72c38c4ef675 != 835cf74995cac576a1287823eea113f304339b92915937c89204aecabd7bbba0, headerNum=1, 9589cba9978ab301fe6728c7239ca232ee5bdda5c3df0c520b7cfd4fbaaeb7a5
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_cover_revert.json`)                                                       // block=1, gas used by execution: 131488, in header: 169056
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_value_transfer_gas_calculation.json`)                                     // block=1, gas used by execution: 168931, in header: 37443
	bt.SkipLoad(`^for_amsterdam/prague/eip6110_deposits/test_deposit.json`)                                                     // block=1, gas used by execution: 93667, in header: 112704
	bt.SkipLoad(`^for_amsterdam/prague/eip7002_el_triggerable_withdrawals/test_withdrawal_requests.json`)                       // block=1, gas used by execution: 150272, in header: 225408
	bt.SkipLoad(`^for_amsterdam/prague/eip7251_consolidations/test_consolidation_requests.json`)                                // block=1, gas used by execution: 187840, in header: 262976
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_pointer_to_created_from_create_after_oog_call_again.json`) // block=1, gas used by execution: 471814, in header: 473869
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing_failing_tx.json`)                           // block=1, gas used by execution: 472998, in header: 341510
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reentry.json`)                                          // block=1, gas used by execution: 252410, in header: 477818
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reverts.json`)                                          // block=1, gas used by execution: 52133, in header: 177274
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_pointer.json`)                                       // block=1, gas used by execution: 945996, in header: 683020
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_self_sponsored_set_code.json`)                                  // block=1, gas used by execution: 9897862, in header: 9841510
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_sstore.json`)                                       // block=1, gas used by execution: 397862, in header: 341510

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
