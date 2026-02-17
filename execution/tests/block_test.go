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
	"github.com/erigontech/erigon/common/race"
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

	bt := new(testMatcher)
	dir := filepath.Join(legacyDir, "BlockchainTests")

	// This directory contains no tests
	bt.skipLoad(`.*\.meta/.*`)

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
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

	bt := new(testMatcher)
	dir := filepath.Join(eestDir, "blockchain_tests")

	// Slow tests
	bt.slow(`^cancun/eip4844_blobs/test_invalid_negative_excess_blob_gas.json`)
	bt.slow(`^frontier/scenarios/test_scenarios.json`)
	bt.slow(`^osaka/eip7939_count_leading_zeros/test_clz_opcode_scenarios.json`)
	bt.slow(`^prague/eip7623_increase_calldata_cost/test_transaction_validity_type_1_type_2.json`)

	// Very slow tests
	bt.skipLoad(`^berlin/eip2930_access_list/test_tx_intrinsic_gas.json`)
	bt.skipLoad(`^cancun/eip4844_blobs/test_sufficient_balance_blob_tx`)
	bt.skipLoad(`^cancun/eip4844_blobs/test_valid_blob_tx_combinations.json`)
	bt.skipLoad(`^frontier/opcodes/test_stack_overflow.json`)
	bt.skipLoad(`^prague/eip2537_bls_12_381_precompiles/test_invalid.json`)
	bt.skipLoad(`^prague/eip2537_bls_12_381_precompiles/test_valid.json`)

	// Tested in the state test format by TestState
	bt.skipLoad(`^static/state_tests/`)

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}

// Only runs EEST tests for current devnet - can "skip" on off-seasons
func TestExecutionSpecBlockchainDevnet(t *testing.T) {
	const offSeason = false
	if offSeason {
		t.Skip("devnet off-season")
	}
	if testing.Short() {
		t.Skip()
	}
	if race.Enabled {
		// TODO fix -race issues with parallel exec
		t.Skip("skipping from race tests until parallel exec flow is race free")
	}
	t.Parallel()
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dir := filepath.Join(eestDir, "blockchain_tests_devnet")
	bt := new(testMatcher)
	// to run only tests for 1 eip do:
	//bt.whitelist(`.*amsterdam/eip8024_dupn_swapn_exchange.*`)
	bt.whitelist(`.*amsterdam.*`)                                                                              // TODO run tests for older forks too once we fix amsterdam eips, for now focus only on amsterdam eips
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_finalization_selfdestruct_logs.json`)                        // TODO fix error: receiptHash mismatch
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_finalization_after_priority_fee.json`)          // TODO fix error: block access list mismatch
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_to_self_cross_tx_no_log.json`)                  // TODO fix error: block access list mismatch
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_same_tx_via_call.json`)                         // TODO fix error: block #1 insertion into chain failed
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_to_system_address.json`)                        // TODO fix error: block access list mismatch
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_transfer_to_special_address.json`)                           // TODO fix error: block access list mismatch
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_to_self_same_tx.json`)                          // TODO fix error:  block #1 insertion into chain failed
	bt.skipLoad(`.*eip7708_eth_transfer_logs/test_selfdestruct_log_at_fork_transition.json`)                   // TODO file error: block #2 insertion into chain failed: insertion failed for block 2, code: BadBlock
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_7702_delegation_clear.json`)                      // TODO fix error: block access list mismatch
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_7002_clean_sweep.json`)                           // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_7002_request_from_contract.json`)                 // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_consolidation_contract_cross_index.json`)         // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_multiple_withdrawals_same_address.json`)          // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_system_dequeue_consolidations_eip7251.json`)      // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_and_new_contract.json`)                // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_and_selfdestruct.json`)                // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_and_transaction.json`)                 // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_empty_block.json`)                     // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_no_evm_execution.json`)                // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_to_7702_delegation.json`)              // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_to_coinbase.json`)                     // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_to_coinbase_empty_block.json`)         // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_to_nonexistent_account.json`)          // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_to_precompiles.json`)                  // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_largest_amount.json`)                  // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_and_state_access_same_account.json`)   // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_contract_cross_index.json`)            // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_withdrawal_and_value_transfer_same_address.json`) // TODO fix error: can't find diffsets for: 2
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_create_selfdestruct_to_self_with_call.json`)      // TODO fix error: block #1 insertion into chain failed
	bt.skipLoad(`.*eip7928_block_level_access_lists/test_bal_7002_partial_sweep.json`)                         // TODO fix error: can't find diffsets for: 2
	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
