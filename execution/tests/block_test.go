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
	bt := new(testMatcher)
	// to run only tests for 1 eip do:
	//bt.whitelist(`.*amsterdam/eip8024_dupn_swapn_exchange.*`)

	// TODO remove skips
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_contract_creation_tx.json`)                                       // gas used by execution: 53064, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_collision_no_log.json`)                                    // gas used by execution: 197704, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_initcode_stop_emits_log.json`)                             // gas used by execution: 53023, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_insufficient_balance_no_log.json`)                         // gas used by execution: 55226, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_opcode_emits_log.json`)                                    // gas used by execution: 75132, in header: 169056
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_out_of_gas_no_log.json`)                                   // gas used by execution: 253215, in header: 166350
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_failed_create_with_value_no_log.json`)                            // gas used by execution: 495219, in header: 365427
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_finalization_burn_logs.json`)                                     // gas used by execution: 224135, in header: 652744
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_initcode_calls_with_value.json`)                                  // gas used by execution: 62369, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_nested_calls_log_order.json`)                                     // gas used by execution: 139210, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_finalization_after_priority_fee.json`)               // gas used by execution: 100112, in header: 265324
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_mainnet.json`)                                       // gas used by execution: 53603, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_same_tx_via_call.json`)                              // gas used by execution: 72103, in header: 157316
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_then_transfer_same_block.json`)                      // gas used by execution: 82206, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_to_different_address_same_tx.json`)                  // gas used by execution: 60625, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_to_self_cross_tx_no_log.json`)                       // gas used by execution: 79612, in header: 133836
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_to_self_same_tx.json`)                               // gas used by execution: 58024, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_to_system_address.json`)                             // gas used by execution: 53603, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_with_value_emits_log.json`)                          // gas used by execution: 53603, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_transfer_with_all_tx_types.json`)                                 // gas used by execution: 86100, in header: 54004
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7778_block_gas_accounting_without_refunds/test_multiple_refund_types_in_one_tx.json`)         // gas used by execution: 321052, in header: 270020
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7778_block_gas_accounting_without_refunds/test_simple_gas_accounting.json`)                   // gas used by execution: 271002, in header: 270020
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7778_block_gas_accounting_without_refunds/test_varying_calldata_costs.json`)                  // gas used by execution: 51122, in header: 33800
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7843_slotnum/test_slotnum_gas_cost.json`)                                                     // gas used by execution: 45726, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7843_slotnum/test_slotnum_value.json`)                                                        // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_2930_slot_listed_and_unlisted_writes.json`)            // gas used by execution: 67412, in header: 75136
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_2935_query.json`)                                      // gas used by execution: 48102, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_4788_query.json`)                                      // gas used by execution: 50210, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_clean_sweep.json`)                                // gas used by execution: 135769, in header: 187840
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_partial_sweep.json`)                              // gas used by execution: 1655820, in header: 1577856
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_request_from_contract.json`)                      // gas used by execution: 124628, in header: 150272
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_request_invalid.json`)                            // gas used by execution: 124634, in header: 150272
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_delegated_storage_access.json`)                   // gas used by execution: 45209, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_delegation_clear.json`)                           // gas used by execution: 92000, in header: 57000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_delegation_create.json`)                          // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_delegation_update.json`)                          // gas used by execution: 92000, in header: 57000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_double_auth_reset.json`)                          // gas used by execution: 71000, in header: 54004
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_double_auth_swap.json`)                           // gas used by execution: 71000, in header: 54004
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_invalid_chain_id_authorization.json`)             // gas used by execution: 46000, in header: 158490
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_invalid_nonce_authorization.json`)                // gas used by execution: 46000, in header: 158490
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7702_null_address_delegation_no_code_change.json`)     // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_aborted_storage_access.json`)                          // gas used by execution: 5000000, in header: 4962432
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_all_transaction_types.json`)                           // gas used by execution: 243502, in header: 214842
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_call_no_delegation_and_oog_before_target_access.json`) // gas used by execution: 55321, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_call_with_value_in_static_context.json`)               // gas used by execution: 545724, in header: 528624
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_callcode_nested_value_transfer.json`)                  // gas used by execution: 64642, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_code_changes.json`)                                    // gas used by execution: 53247, in header: 132662
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_consolidation_contract_cross_index.json`)              // gas used by execution: 137894, in header: 187840
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create2_collision.json`)                               // gas used by execution: 990207, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_contract_init_revert.json`)                     // gas used by execution: 55860, in header: 32666
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_early_failure.json`)                            // gas used by execution: 58026, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_oog_code_deposit.json`)                         // gas used by execution: 518691, in header: 388898
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_selfdestruct_to_self_with_call.json`)           // gas used by execution: 126992, in header: 244192
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_transaction_empty_code.json`)                   // gas used by execution: 53000, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_cross_tx_storage_revert_to_zero.json`)                 // gas used by execution: 69398, in header: 52298
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_delegated_storage_writes.json`)                        // gas used by execution: 45727, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_gas_limit_boundary.json`)                              // block (index 0) insertion should have failed due to: BlockException.BLOCK_ACCESS_LIST_GAS_LIMIT_EXCEEDED
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_multiple_storage_writes_same_slot.json`)               // gas used by execution: 95136, in header: 78036
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_net_zero_balance_transfer.json`)                       // gas used by execution: 77428, in header: 169056
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_nonexistent_account_access_value_transfer.json`)       // gas used by execution: 55321, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_noop_write_filtering.json`)                            // gas used by execution: 52524, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_selfdestruct_to_7702_delegation.json`)                 // gas used by execution: 77224, in header: 59724
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_sstore_and_oog.json`)                                  // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_sstore_static_context.json`)                           // gas used by execution: 1045726, in header: 1028626
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_system_dequeue_consolidations_eip7251.json`)           // gas used by execution: 241588, in header: 300544
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_transient_storage_not_tracked.json`)                   // gas used by execution: 43312, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_withdrawal_and_new_contract.json`)                     // gas used by execution: 53382, in header: 132662
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_withdrawal_and_selfdestruct.json`)                     // gas used by execution: 53603, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_withdrawal_and_state_access_same_account.json`)        // gas used by execution: 45209, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_withdrawal_contract_cross_index.json`)                 // gas used by execution: 115281, in header: 150272
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_withdrawal_to_7702_delegation.json`)                   // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size.json`)                                     // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 100000000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_deposit_gas.json`)                         // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 39171964
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_external_opcodes.json`)                    // gas used by execution: 102194, in header: 112704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_self_opcodes.json`)                        // gas used by execution: 82207, in header: 75136
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_via_create.json`)                          // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 100000000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_with_max_initcode.json`)                   // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 100000000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_with_max_initcode_mainnet.json`)           // gas used by execution: 102194, in header: 112704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size.json`)                                 // max initcode size exceeded: code size 65536 limit 49152
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_gas_metering.json`)                    // max initcode size exceeded: code size 65536 limit 49152
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_gas_metering_via_create.json`)         // block access list mismatch
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_via_create.json`)                      // gas used by execution: 16777216, in header: 676630
	bt.skipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_over_max_code_size_mainnet.json`)                        // gas used by execution: 16777216, in header: 16645728
	// EIP-8024: decode formulas updated (fix/eip-8024-decode). Tests still skipped pending EIP-8037 (PR#19596).
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8024_dupn_swapn_exchange/`)                                                                                        // needs EIP-8037 state creation gas cost (PR#19596)
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_auth_refund_block_gas_accounting.json`)                                 // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_auth_refund_bypasses_one_fifth_cap.json`)                               // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 17048410
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_auth_with_calldata_and_access_list.json`)                               // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16973274
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_auth_with_multiple_sstores.json`)                                       // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 17123546
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_authorization_exact_state_gas_boundary.json`)                           // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_authorization_state_gas_scaling.json`)                                  // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_authorization_to_precompile_address.json`)                              // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_authorization_with_sstore.json`)                                        // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16973274
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_block_gas_used_with_state_ops.json`)                                    // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_block_state_gas_limit.json`)                                            // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 120000000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_call_insufficient_balance_returns_reservoir.json`)                      // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_call_stack_depth_returns_reservoir.json`)                               // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_call_value_transfer_existing_account_no_state_gas.json`)                // gas used by execution: 77424, in header: 169056
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_call_value_transfer_new_account.json`)                                  // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16908704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_calldata_floor_higher_than_execution_with_state_ops.json`)              // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_calldata_floor_with_sstore.json`)                                       // gas used by execution: 47202, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_charge_draws_entirely_from_reservoir.json`)                             // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16852352
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_charge_spills_to_gas_left.json`)                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16796000
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_child_call_uses_reservoir.json`)                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_child_state_gas_tracked_in_parent.json`)                                // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16852352
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_code_deposit_state_gas_scales_with_size.json`)                          // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 18110880
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create2_address_collision.json`)                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 33554432
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_charges_state_gas.json`)                                         // gas used by execution: 75132, in header: 169056
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_insufficient_balance_returns_reservoir.json`)                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_revert_no_code_deposit_state_gas.json`)                          // gas used by execution: 55232, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_tx_deploys_contract.json`)                                       // gas used by execution: 53006, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_tx_reservoir.json`)                                              // gas used by execution: 53006, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_tx_state_gas.json`)                                              // gas used by execution: 53006, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_create_with_reservoir.json`)                                            // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16908704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_delegatecall_reservoir_passing.json`)                                   // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_delegation_pointer_new_account_state_gas.json`)                         // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 17067194
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_duplicate_signer_authorizations.json`)                                  // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 17094196
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_existing_account_refund.json`)                                          // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_existing_account_refund_enables_sstore.json`)                           // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16973274
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_gas_opcode_excludes_reservoir.json`)                                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 20534016
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_invalid_chain_id_auth_still_charges_intrinsic_state_gas.json`)          // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_invalid_nonce_auth_still_charges_intrinsic_state_gas.json`)             // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_many_authorizations_state_gas.json`)                                    // gas limit too high: address 0x6E019b4cb2cb6feF15a853b3BEaBBeb567E2d75F, gas limit 18362116
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_mixed_new_and_existing_auths.json`)                                     // gas limit too high: address 0x3c78CA7b116dFA834E58f71F5A20470945F6359B, gas limit 17094196
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_mixed_valid_and_invalid_auths.json`)                                    // gas limit too high: address 0x3c78CA7b116dFA834E58f71F5A20470945F6359B, gas limit 17094196
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_multi_tx_block_auth_refund_and_sstore.json`)                            // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_nested_calls_reservoir_passing.json`)                                   // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_pricing_at_various_gas_limits.json`)                                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_pricing_changes_with_block_gas_limit.json`)                             // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_pricing_minimum_cpsb_floor.json`)                                       // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_re_authorization_existing_delegation.json`)                             // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_refund_cap_includes_state_gas.json`)                                    // gas used by execution: 43212, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_refund_with_reservoir_state_gas.json`)                                  // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_allocation_boundary.json`)                                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16777217
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_restored_after_child_full_drain_and_revert.json`)             // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_restored_after_child_spill_and_halt.json`)                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_restored_after_child_spill_and_revert.json`)                  // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_returned_on_oog.json`)                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_returned_on_revert.json`)                                     // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_self_sponsored_authorization.json`)                                     // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16935706
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_selfdestruct_existing_beneficiary_no_state_gas.json`)                   // gas used by execution: 53603, in header: 131488
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_selfdestruct_new_beneficiary_charges_state_gas.json`)                   // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16908704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_selfdestruct_state_gas_from_reservoir.json`)                            // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16908704
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_selfdestruct_to_self_in_create_tx.json`)                                // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 33554432
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sequential_calls_reservoir_restored_between_reverts.json`)              // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_direct_call_same_contract.json`)                                 // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_multiple_slots.json`)                                            // gas used by execution: 131530, in header: 187840
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_restoration_refund.json`)                                        // gas used by execution: 43212, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_state_gas_all_tx_types.json`)                                    // gas used by execution: 43506, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_state_gas_drawn_from_reservoir.json`)                            // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_state_gas_entirely_from_gas_left.json`)                          // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_state_gas_source.json`)                                          // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16965056
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_via_delegation_pointer.json`)                                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16973274
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_zero_to_nonzero.json`)                                           // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/amsterdam/eip8037_state_creation_gas_cost_increase/test_staticcall_passes_reservoir.json`)                                      // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_amsterdam/berlin/eip2929_gas_cost_increases/test_call_insufficient_balance.json`)                                                         // gas used by execution: 54766, in header: 37666
	bt.skipLoad(`^for_amsterdam/berlin/eip2930_access_list/test_account_storage_warm_cold_state.json`)                                                          // gas used by execution: 52163, in header: 37568
	bt.skipLoad(`^for_amsterdam/berlin/eip2930_access_list/test_repeated_address_acl.json`)                                                                     // gas used by execution: 69886, in header: 75136
	bt.skipLoad(`^for_amsterdam/byzantium/eip196_ec_add_mul/test_valid.json`)                                                                                   // gas used by execution: 87994, in header: 112704
	bt.skipLoad(`^for_amsterdam/byzantium/eip198_modexp_precompile/test_modexp.json`)                                                                           // gas used by execution: 106092, in header: 319328
	bt.skipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_call_to_precompile.json`)                                                           // gas used by execution: 270033, in header: 235833
	bt.skipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_call_to_precompile_from_contract_init.json`)                                        // gas used by execution: 370083, in header: 319328
	bt.skipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_nested_call_to_precompile.json`)                                                    // gas used by execution: 338969, in header: 253469
	bt.skipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_reentrant_call_to_precompile.json`)                                                 // gas used by execution: 46404, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/create/test_create_oog_from_eoa_refunds.json`)                                                                           // gas used by execution: 85538, in header: 170230
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_basic_tload_after_store.json`)                                                                       // gas used by execution: 48319, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_basic_tload_gasprice.json`)                                                                          // gas used by execution: 53925, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_basic_tload_other_after_tstore.json`)                                                                // gas used by execution: 48319, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_basic_tload_transaction_begin.json`)                                                                 // gas used by execution: 48213, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_basic_tload_works.json`)                                                                             // gas used by execution: 53425, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_contract_creation.json`)                                                                             // gas used by execution: 194336, in header: 383898
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_gas_usage.json`)                                                                                     // gas used by execution: 43243, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_reentrant_call.json`)                                                                                // gas used by execution: 133666, in header: 99466
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_reentrant_selfdestructing_call.json`)                                                                // gas used by execution: 144550, in header: 335764
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_subcall.json`)                                                                                       // gas used by execution: 95330, in header: 112704
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_tload_after_sstore.json`)                                                                            // gas used by execution: 129848, in header: 150272
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_tload_calls.json`)                                                                                   // gas used by execution: 78358, in header: 75136
	bt.skipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_tstore_clear_after_deployment_tx.json`)                                                              // gas used by execution: 77966, in header: 138532
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_beacon_root_contract_calls.json`)                                                               // gas used by execution: 116540, in header: 150272
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_beacon_root_contract_timestamps.json`)                                                          // gas used by execution: 116540, in header: 150272
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_beacon_root_equal_to_timestamp.json`)                                                           // gas used by execution: 116540, in header: 150272
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_beacon_root_selfdestruct.json`)                                                                 // gas used by execution: 53430, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_multi_block_beacon_root_timestamp_calls.json`)                                                  // gas used by execution: 72320, in header: 75136
	bt.skipLoad(`^for_amsterdam/cancun/eip4788_beacon_root/test_tx_to_beacon_root_contract.json`)                                                               // gas used by execution: 50460, in header: 158490
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blob_gas_subtraction_tx.json`)                                                                        // gas used by execution: 78436, in header: 75136
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blob_tx_attribute_calldata_opcodes.json`)                                                             // gas used by execution: 43142, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blob_tx_attribute_gasprice_opcode.json`)                                                              // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blob_tx_attribute_opcodes.json`)                                                                      // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blob_tx_attribute_value_opcode.json`)                                                                 // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_gas_cost.json`)                                                                              // gas used by execution: 43274, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_invalid_blob_index.json`)                                                                    // gas used by execution: 276432, in header: 413248
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_multiple_txs_in_block.json`)                                                                 // gas used by execution: 204964, in header: 225408
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_opcode_contexts.json`)                                                                       // gas used by execution: 159454, in header: 225408
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_opcode_contexts_tx_types.json`)                                                              // gas used by execution: 48209, in header: 158490
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_blobhash_scenarios.json`)                                                                             // gas used by execution: 159668, in header: 225408
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_correct_decreasing_blob_gas_costs.json`)                                                              // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_correct_excess_blob_gas_calculation.json`)                                                            // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_correct_increasing_blob_gas_costs.json`)                                                              // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip4844_blobs/test_point_evaluation_precompile_gas_usage.json`)                                                          // gas used by execution: 94855, in header: 77755
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_mcopy_huge_memory_expansion.json`)                                                                    // gas used by execution: 16777216, in header: 16739648
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_mcopy_memory_expansion.json`)                                                                         // gas used by execution: 64735, in header: 47634
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_mcopy_on_empty_memory.json`)                                                                          // gas used by execution: 45748, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_no_memory_corruption_on_upper_call_stack_levels.json`)                                                // gas used by execution: 776097, in header: 1277312
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_no_memory_corruption_on_upper_create_stack_levels.json`)                                              // gas used by execution: 805505, in header: 1408800
	bt.skipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_valid_mcopy_operations.json`)                                                                         // gas used by execution: 85793, in header: 75136
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_calling_from_new_contract_to_pre_existing_contract.json`)                                      // gas used by execution: 265350, in header: 565868
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_calling_from_pre_existing_contract_to_new_contract.json`)                                      // gas used by execution: 288369, in header: 608132
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_create_and_destroy_multiple_contracts_same_tx.json`)                                           // gas used by execution: 285488, in header: 696182
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_create_multiple_contracts_destroy_one_then_destroy_other_next_tx.json`)                        // gas used by execution: 284776, in header: 605784
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_create_selfdestruct_same_tx.json`)                                                             // gas used by execution: 460867, in header: 786580
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_create_selfdestruct_same_tx_increased_nonce.json`)                                             // gas used by execution: 585102, in header: 1435802
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_dynamic_create2_selfdestruct_collision.json`)                                                  // gas used by execution: 754570, in header: 795972
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_dynamic_create2_selfdestruct_collision_multi_tx.json`)                                         // gas used by execution: 751253, in header: 570564
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_dynamic_create2_selfdestruct_collision_two_different_transactions.json`)                       // gas used by execution: 369707, in header: 896936
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_parent_creates_child_selfdestruct_one.json`)                                                   // gas used by execution: 269100, in header: 740794
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_recreate_self_destructed_contract_different_txs.json`)                                         // gas used by execution: 230344, in header: 500124
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_recursive_contract_creation_and_selfdestruct.json`)                                            // gas used by execution: 260956, in header: 693834
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_reentrancy_selfdestruct_revert.json`)                                                          // gas used by execution: 85719, in header: 131488
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_self_destructing_initcode.json`)                                                               // gas used by execution: 165642, in header: 376854
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_self_destructing_initcode_create_tx.json`)                                                     // gas used by execution: 83251, in header: 169056
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_created_in_same_tx_with_revert.json`)                                             // gas used by execution: 238037, in header: 443772
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_created_same_block_different_tx.json`)                                            // gas used by execution: 217278, in header: 457860
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_not_created_in_same_tx_with_revert.json`)                                         // gas used by execution: 171609, in header: 206624
	bt.skipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_pre_existing.json`)                                                               // gas used by execution: 368476, in header: 508342
	bt.skipLoad(`^for_amsterdam/cancun/eip7516_blobgasfee/test_blobbasefee_out_of_gas.json`)                                                                    // gas used by execution: 45728, in header: 37568
	bt.skipLoad(`^for_amsterdam/cancun/eip7516_blobgasfee/test_blobbasefee_stack_overflow.json`)                                                                // gas used by execution: 47774, in header: 37568
	bt.skipLoad(`^for_amsterdam/constantinople/eip1014_create2/test_create2_return_data.json`)                                                                  // gas used by execution: 121788, in header: 206624
	bt.skipLoad(`^for_amsterdam/constantinople/eip1014_create2/test_deterministic_deployment.json`)                                                             // gas used by execution: 66586, in header: 49486
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_codeless_with_storage.json`)                                                // gas used by execution: 50812, in header: 37568
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_dynamic_account_overwrite.json`)                                            // gas used by execution: 254802, in header: 437902
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_empty_account_variants.json`)                                               // gas used by execution: 55941, in header: 38841
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_empty_contract_creation.json`)                                              // gas used by execution: 90777, in header: 169056
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_empty_send_value.json`)                                                     // gas used by execution: 82632, in header: 169056
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_new_account.json`)                                                          // gas used by execution: 85565, in header: 169056
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_of_empty.json`)                                                             // gas used by execution: 50812, in header: 37568
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_self.json`)                                                                 // gas used by execution: 65410, in header: 75136
	bt.skipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_via_call.json`)                                                             // gas used by execution: 70593, in header: 75136
	bt.skipLoad(`^for_amsterdam/constantinople/eip145_bitwise_shift/test_combinations.json`)                                                                    // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 25000000
	bt.skipLoad(`^for_amsterdam/frontier/create/test_create_deposit_oog.json`)                                                                                  // gas used by execution: 62065, in header: 131488
	bt.skipLoad(`^for_amsterdam/frontier/create/test_create_one_byte.json`)                                                                                     // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 60000000
	bt.skipLoad(`^for_amsterdam/frontier/create/test_create_suicide_during_transaction_create.json`)                                                            // gas used by execution: 87053, in header: 169056
	bt.skipLoad(`^for_amsterdam/frontier/create/test_create_suicide_store.json`)                                                                                // gas used by execution: 175491, in header: 413248
	bt.skipLoad(`^for_amsterdam/frontier/identity_precompile/test_call_identity_precompile.json`)                                                               // gas used by execution: 65363, in header: 75136
	bt.skipLoad(`^for_amsterdam/frontier/identity_precompile/test_call_identity_precompile_large_params.json`)                                                  // gas used by execution: 2049940, in header: 2032840
	bt.skipLoad(`^for_amsterdam/frontier/identity_precompile/test_identity_precompile_returndata.json`)                                                         // gas used by execution: 43271, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_all_opcodes.json`)                                                                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 50000000
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_call_large_args_offset_size_zero.json`)                                                                   // gas used by execution: 45761, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_call_large_offset_mstore.json`)                                                                           // gas used by execution: 67920, in header: 75136
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_call_memory_expands_on_early_revert.json`)                                                                // gas used by execution: 99617, in header: 206624
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_calldatacopy.json`)                                                                                       // gas used by execution: 70112, in header: 75136
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_calldatacopy_word_copy_oog.json`)                                                                         // gas used by execution: 45957, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_calldataload.json`)                                                                                       // gas used by execution: 46018, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_calldatasize.json`)                                                                                       // gas used by execution: 45758, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_codecopy_word_copy_oog.json`)                                                                             // gas used by execution: 45957, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_constant_gas.json`)                                                                                       // gas used by execution: 95486, in header: 112704
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_cover_revert.json`)                                                                                       // gas used by execution: 75250, in header: 169056
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_dup.json`)                                                                                                // gas used by execution: 396805, in header: 638656
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_gas.json`)                                                                                                // gas used by execution: 101859, in header: 112704
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_genesis_hash_available.json`)                                                                             // gas used by execution: 45358, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_max_stack.json`)                                                                                          // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 50000000
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_push.json`)                                                                                               // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_stack_overflow.json`)                                                                                     // gas used by execution: 46172, in header: 37568
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_swap.json`)                                                                                               // gas used by execution: 354829, in header: 563520
	bt.skipLoad(`^for_amsterdam/frontier/opcodes/test_value_transfer_gas_calculation.json`)                                                                     // gas used by execution: 55044, in header: 37944
	bt.skipLoad(`^for_amsterdam/frontier/precompiles/test_precompile_absence.json`)                                                                             // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 30000000
	bt.skipLoad(`^for_amsterdam/frontier/scenarios/test_scenarios.json`)                                                                                        // gas used by execution: 65443, in header: 48343
	bt.skipLoad(`^for_amsterdam/frontier/validation/test_gas_limit_below_minimum.json`)                                                                         // block (index 0) insertion should have failed due to: BlockException.BLOCK_ACCESS_LIST_GAS_LIMIT_EXCEEDED
	bt.skipLoad(`^for_amsterdam/homestead/identity_precompile/test_identity_return_buffer_modify.json`)                                                         // gas used by execution: 43353, in header: 37568
	bt.skipLoad(`^for_amsterdam/homestead/identity_precompile/test_identity_return_overwrite.json`)                                                             // gas used by execution: 43335, in header: 37568
	bt.skipLoad(`^for_amsterdam/istanbul/eip1344_chainid/test_chainid.json`)                                                                                    // gas used by execution: 43505, in header: 37568
	bt.skipLoad(`^for_amsterdam/istanbul/eip152_blake2/test_blake2b.json`)                                                                                      // gas used by execution: 72154, in header: 75136
	bt.skipLoad(`^for_amsterdam/istanbul/eip152_blake2/test_blake2b_gas_limit.json`)                                                                            // gas used by execution: 72106, in header: 75136
	bt.skipLoad(`^for_amsterdam/istanbul/eip152_blake2/test_blake2b_large_gas_limit.json`)                                                                      // gas used by execution: 172142, in header: 137942
	bt.skipLoad(`^for_amsterdam/osaka/eip7823_modexp_upper_bounds/test_modexp_upper_bounds.json`)                                                               // gas used by execution: 159429, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7825_transaction_gas_limit_cap/test_tx_gas_limit_cap_subcall_context.json`)                                            // gas used by execution: 45741, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_contract_creation_transaction.json`)                                                     // gas used by execution: 123077, in header: 244192
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_contract_initcode.json`)                                                                 // gas used by execution: 123140, in header: 244192
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_modexp_call_operations.json`)                                                            // gas used by execution: 112604, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_modexp_gas_usage_contract_wrapper.json`)                                                 // gas used by execution: 112604, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_modexp_variable_gas_cost.json`)                                                          // gas used by execution: 176928, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_modexp_variable_gas_cost_exceed_tx_gas_cap.json`)                                        // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 120000000
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_vectors_from_eip.json`)                                                                  // gas used by execution: 7984777, in header: 7916377
	bt.skipLoad(`^for_amsterdam/osaka/eip7883_modexp_gas_increase/test_vectors_from_legacy_tests.json`)                                                         // gas used by execution: 115195, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7918_blob_reserve_price/test_reserve_price_boundary.json`)                                                             // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7918_blob_reserve_price/test_reserve_price_various_base_fee_scenarios.json`)                                           // gas used by execution: 43105, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_call_operation.json`)                                                                // gas used by execution: 116547, in header: 150272
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_code_copy_operation.json`)                                                           // gas used by execution: 48146, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_from_set_code.json`)                                                                 // gas used by execution: 114544, in header: 271194
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_initcode_context.json`)                                                              // gas used by execution: 144349, in header: 281760
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_initcode_create.json`)                                                               // gas used by execution: 144409, in header: 281760
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_jump_operation.json`)                                                                // gas used by execution: 50756, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_push_operation_same_value.json`)                                                     // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 30000000
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_stack_not_overflow.json`)                                                            // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 20000000
	bt.skipLoad(`^for_amsterdam/osaka/eip7939_count_leading_zeros/test_clz_with_memory_operation.json`)                                                         // gas used by execution: 48146, in header: 37568
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_call_types.json`)                                                                     // gas used by execution: 96963, in header: 112704
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_contract_creation_transaction.json`)                                                  // gas used by execution: 87802, in header: 169056
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_contract_initcode.json`)                                                              // gas used by execution: 87865, in header: 169056
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_gas.json`)                                                                            // gas used by execution: 96963, in header: 112704
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_invalid.json`)                                                                        // gas used by execution: 77078, in header: 75136
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_modular_comparison.json`)                                                             // gas used by execution: 76658, in header: 75136
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_precompile_will_return_success_with_tx_value.json`)                                   // gas used by execution: 128616, in header: 244192
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_valid.json`)                                                                          // gas used by execution: 96603, in header: 112704
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_wycheproof_extra.json`)                                                               // gas used by execution: 76310, in header: 75136
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_wycheproof_invalid.json`)                                                             // gas used by execution: 76298, in header: 75136
	bt.skipLoad(`^for_amsterdam/osaka/eip7951_p256verify_precompiles/test_wycheproof_valid.json`)                                                               // gas used by execution: 76694, in header: 75136
	bt.skipLoad(`^for_amsterdam/paris/eip7610_create_collision/test_collision_with_create2_revert_in_initcode.json`)                                            // gas used by execution: 10000000, in header: 131488
	bt.skipLoad(`^for_amsterdam/paris/eip7610_create_collision/test_create2_collision_storage.json`)                                                            // gas used by execution: 987912, in header: 262976
	bt.skipLoad(`^for_amsterdam/paris/eip7610_create_collision/test_init_collision_create_opcode.json`)                                                         // gas used by execution: 1974587, in header: 131488
	bt.skipLoad(`^for_amsterdam/paris/eip7610_create_collision/test_init_collision_create_tx.json`)                                                             // gas used by execution: 200000, in header: 131488
	bt.skipLoad(`^for_amsterdam/paris/security/test_tx_selfdestruct_balance_bug.json`)                                                                          // gas used by execution: 195796, in header: 266498
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_call_types.json`)                                                                    // gas used by execution: 90111, in header: 112704
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_gas.json`)                                                                           // gas used by execution: 88971, in header: 112704
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_isogeny_kernel_values.json`)                                                         // gas used by execution: 93874, in header: 112704
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_valid.json`)                                                                         // gas used by execution: 91251, in header: 112704
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_valid_gas_g1msm.json`)                                                               // gas used by execution: 14476618, in header: 13467718
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_valid_gas_g2msm.json`)                                                               // gas used by execution: 14549640, in header: 13831440
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_valid_gas_pairing.json`)                                                             // gas used by execution: 8066312, in header: 7724312
	bt.skipLoad(`^for_amsterdam/prague/eip2537_bls_12_381_precompiles/test_valid_multi_inf.json`)                                                               // gas used by execution: 15713598, in header: 15662298
	bt.skipLoad(`^for_amsterdam/prague/eip2935_historical_block_hashes_from_state/test_block_hashes_call_opcodes.json`)                                         // gas used by execution: 52975, in header: 37568
	bt.skipLoad(`^for_amsterdam/prague/eip2935_historical_block_hashes_from_state/test_block_hashes_history.json`)                                              // gas used by execution: 52447, in header: 37568
	bt.skipLoad(`^for_amsterdam/prague/eip6110_deposits/test_deposit.json`)                                                                                     // gas used by execution: 11790209, in header: 11619209
	bt.skipLoad(`^for_amsterdam/prague/eip7002_el_triggerable_withdrawals/test_withdrawal_requests.json`)                                                       // gas used by execution: 2630328, in header: 2479488
	bt.skipLoad(`^for_amsterdam/prague/eip7251_consolidations/test_consolidation_requests.json`)                                                                // gas used by execution: 1051228, in header: 1164608
	bt.skipLoad(`^for_amsterdam/prague/eip7623_increase_calldata_cost/test_transaction_validity_type_0.json`)                                                   // gas used by execution: 255771, in header: 255770
	bt.skipLoad(`^for_amsterdam/prague/eip7623_increase_calldata_cost/test_transaction_validity_type_4.json`)                                                   // receiptHash mismatch: 81f263df0b6a4e79635cd66fbc81bb1ce4a428dbbf4e645d74dd4530cb873369 != 49cdf52110dcb2abf6aacd1717bd0645f7faa3e51c00548f821a3c54948a2e61, headerNum=1, fd7a1d16abfa1617754f204b41f0e1ec6e5a87702aa8c2a4d8c04316964d02fb
	bt.skipLoad(`^for_amsterdam/prague/eip7685_general_purpose_el_requests/test_valid_multi_type_request_from_same_tx.json`)                                    // gas used by execution: 368367, in header: 488384
	bt.skipLoad(`^for_amsterdam/prague/eip7685_general_purpose_el_requests/test_valid_multi_type_requests.json`)                                                // gas used by execution: 344082, in header: 375680
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_account_warming.json`)                                                                          // gas used by execution: 83922, in header: 316980
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_address_from_set_code.json`)                                                                    // gas used by execution: 68105, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_authorization_reusing_nonce.json`)                                                              // gas used by execution: 67000, in header: 158490
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_into_chain_delegating_set_code.json`)                                                      // gas used by execution: 9847066, in header: 9534492
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_into_self_delegating_set_code.json`)                                                       // gas used by execution: 9846676, in header: 9690389
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_pointer_to_created_from_create_after_oog_call_again.json`)                                 // gas used by execution: 790994, in header: 473869
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_to_precompile_in_pointer_context.json`)                                                    // gas used by execution: 84090, in header: 64570
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_contract_storage_to_pointer_with_storage.json`)                                                 // gas used by execution: 61675, in header: 44175
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_creating_delegation_designation_contract.json`)                                                 // gas used by execution: 987413, in header: 857620
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_creating_tx_to_contract_creator.json`)                                                          // gas used by execution: 162261, in header: 414422
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegate_call_targets.json`)                                                                    // gas used by execution: 67826, in header: 75136
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegated_eoa_can_send_creating_tx.json`)                                                       // gas used by execution: 215476, in header: 432032
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing.json`)                                                                      // gas used by execution: 97270, in header: 102138
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing_and_set.json`)                                                              // gas used by execution: 95726, in header: 91572
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing_failing_tx.json`)                                                           // gas used by execution: 500000, in header: 341510
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing_tx_to.json`)                                                                // gas used by execution: 46000, in header: 28500
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_replacement_call_previous_contract.json`)                                            // gas used by execution: 70761, in header: 64570
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_deploying_delegation_designation_contract.json`)                                                // gas used by execution: 500000, in header: 368512
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_double_auth.json`)                                                                              // gas used by execution: 95826, in header: 91572
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_eoa_init_as_pointer.json`)                                                                      // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_eoa_tx_after_set_code.json`)                                                                    // gas used by execution: 68112, in header: 64570
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_ext_code_on_chain_delegating_set_code.json`)                                                    // gas used by execution: 228987, in header: 448468
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_ext_code_on_self_delegating_set_code.json`)                                                     // gas used by execution: 115045, in header: 271194
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_ext_code_on_self_set_code.json`)                                                                // gas used by execution: 115045, in header: 271194
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_ext_code_on_set_code.json`)                                                                     // gas used by execution: 115045, in header: 271194
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_many_delegations.json`)                                                                         // gas used by execution: 2543106, in header: 15886568
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_nonce_overflow_after_first_authorization.json`)                                                 // gas used by execution: 117940, in header: 260628
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_nonce_validity.json`)                                                                           // gas used by execution: 70431, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_measurements.json`)                                                                     // gas used by execution: 72545, in header: 75136
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_normal.json`)                                                                           // gas used by execution: 94124, in header: 64570
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reentry.json`)                                                                          // gas used by execution: 316341, in header: 477818
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_resets_an_empty_code_account_with_storage.json`)                                        // gas used by execution: 294530, in header: 427336
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reverts.json`)                                                                          // gas used by execution: 138032, in header: 177274
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_pointer.json`)                                                                       // gas used by execution: 1000000, in header: 683020
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_precompile.json`)                                                                    // gas used by execution: 80167, in header: 64570
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_static.json`)                                                                        // gas used by execution: 1073028, in header: 1038428
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_static_reentry.json`)                                                                // gas used by execution: 191847, in header: 140147
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_reset_code.json`)                                                                               // gas used by execution: 136224, in header: 129140
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_self_code_on_set_code.json`)                                                                    // gas used by execution: 92439, in header: 233626
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_self_set_code_cost.json`)                                                                       // gas used by execution: 68361, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_self_sponsored_set_code.json`)                                                                  // gas used by execution: 10000000, in header: 9841510
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_address_and_authority_warm_state.json`)                                                // gas used by execution: 115226, in header: 271194
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_address_and_authority_warm_state_call_types.json`)                                     // gas used by execution: 93152, in header: 233626
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_all_invalid_authorization_tuples.json`)                                                // gas used by execution: 271000, in header: 1584900
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_call_set_code.json`)                                                                   // gas used by execution: 140035, in header: 429684
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_max_depth_call_stack.json`)                                                            // gas used by execution: 292620, in header: 257711
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_multiple_first_valid_authorization_tuples_same_signer.json`)                           // gas used by execution: 293106, in header: 1622468
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_multiple_valid_authorization_tuples_first_invalid_same_signer.json`)                   // gas used by execution: 293106, in header: 1622468
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_multiple_valid_authorization_tuples_same_signer_increasing_nonce.json`)                // gas used by execution: 293106, in header: 439076
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_multiple_valid_authorization_tuples_same_signer_increasing_nonce_self_sponsored.json`) // gas used by execution: 293106, in header: 307588
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_account_deployed_in_same_tx.json`)                                                  // gas used by execution: 190448, in header: 484862
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_contract_creator.json`)                                                             // gas used by execution: 100510, in header: 328720
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_log.json`)                                                                          // gas used by execution: 46649, in header: 29149
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_non_empty_storage_non_zero_nonce.json`)                                             // gas used by execution: 51012, in header: 33512
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_precompile.json`)                                                                   // gas used by execution: 77229, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_self_caller.json`)                                                                  // gas used by execution: 112684, in header: 271194
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_self_destruct.json`)                                                                // gas used by execution: 73109, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_self_destructing_account_deployed_in_same_tx.json`)                                 // gas used by execution: 205010, in header: 509516
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_sstore.json`)                                                                       // gas used by execution: 500000, in header: 341510
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_sstore_then_sload.json`)                                                            // gas used by execution: 138318, in header: 260628
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_system_contract.json`)                                                              // gas used by execution: 141534, in header: 139706
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_tstore_available_at_correct_address.json`)                                          // gas used by execution: 53664, in header: 158490
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_tstore_reentry.json`)                                                               // gas used by execution: 68820, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_using_chain_specific_id.json`)                                                         // gas used by execution: 68106, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_using_valid_synthetic_signatures.json`)                                                // gas used by execution: 68106, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_signature_s_out_of_range.json`)                                                                 // gas used by execution: 68106, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_static_to_pointer.json`)                                                                        // gas used by execution: 1073128, in header: 1038528
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_tx_into_chain_delegating_set_code.json`)                                                        // gas used by execution: 10000000, in header: 9683020
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_tx_into_self_delegating_set_code.json`)                                                         // gas used by execution: 10000000, in header: 9841510
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_valid_tx_invalid_auth_signature.json`)                                                          // gas used by execution: 68106, in header: 196058
	bt.skipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_valid_tx_invalid_chain_id.json`)                                                                // gas used by execution: 72931, in header: 196058
	bt.skipLoad(`^for_amsterdam/shanghai/eip3651_warm_coinbase/test_warm_coinbase_call_out_of_gas.json`)                                                        // gas used by execution: 45846, in header: 37568
	bt.skipLoad(`^for_amsterdam/shanghai/eip3651_warm_coinbase/test_warm_coinbase_gas_usage.json`)                                                              // gas used by execution: 43242, in header: 37568
	bt.skipLoad(`^for_amsterdam/shanghai/eip3855_push0/test_push0_contract_during_call_contexts.json`)                                                          // gas used by execution: 67973, in header: 75136
	bt.skipLoad(`^for_amsterdam/shanghai/eip3855_push0/test_push0_contracts.json`)                                                                              // gas used by execution: 43117, in header: 37568
	bt.skipLoad(`^for_amsterdam/shanghai/eip3860_initcode/test_contract_creating_tx.json`)                                                                      // max initcode size exceeded: code size 49153 limit 49152
	bt.skipLoad(`^for_amsterdam/shanghai/eip4895_withdrawals/test_balance_within_block.json`)                                                                   // gas used by execution: 46076, in header: 37568
	bt.skipLoad(`^for_amsterdam/shanghai/eip4895_withdrawals/test_newly_created_contract.json`)                                                                 // gas used by execution: 53279, in header: 132662
	bt.skipLoad(`^for_amsterdam/shanghai/eip4895_withdrawals/test_no_evm_execution.json`)                                                                       // gas used by execution: 86210, in header: 75136
	bt.skipLoad(`^for_amsterdam/shanghai/eip4895_withdrawals/test_use_value_in_contract.json`)                                                                  // gas used by execution: 52422, in header: 37568
	bt.skipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_initcode_selfdestruct_to_self.json`)                                          // gas used by execution: 58040, in header: 131488
	bt.skipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_selfdestruct_state_access_boundary.json`)                                     // gas used by execution: 65197, in header: 157316
	bt.skipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_selfdestruct_to_precompile_state_access_boundary.json`)                       // gas used by execution: 61183, in header: 135010
	bt.skipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_selfdestruct_to_self.json`)                                                   // gas used by execution: 58582, in header: 133836
	bt.skipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_selfdestruct_to_system_contract.json`)                                        // gas used by execution: 64183, in header: 152620
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7708_eth_transfer_logs/test_burn_log_at_fork_transition.json`)                                      // gas used by execution: 58025, in header: 131488
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_fork_transition.json`)                           // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 100000000
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_via_create_fork_transition.json`)                // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 100000000
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_with_max_initcode_fork_transition.json`)         // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 100000000
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_fork_transition.json`)                       // max initcode size exceeded: code size 65536 limit 49152
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_via_create_fork_transition.json`)            // gas used by execution: 16777216, in header: 676630
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip7954_increase_max_contract_size/test_parent_max_code_size_across_fork.json`)                        // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 100000000
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip8037_state_creation_gas_cost_increase/test_reservoir_available_after_transition.json`)              // gas limit too high: address 0x1AD9bc24818784172FF393bb6F89F094D4d2Ca29, gas limit 16814784
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip8037_state_creation_gas_cost_increase/test_sstore_state_gas_at_transition.json`)                    // gas used by execution: 43106, in header: 37568
	bt.skipLoad(`^for_bpo2toamsterdamattime15k/amsterdam/eip8037_state_creation_gas_cost_increase/test_tx_gas_above_cap_at_transition.json`)                    // gas limit too high: address 0xDD616a20f3b01FC95e6B1701D8a07331D06DD897, gas limit 16777217

	// static — tested in state test format by TestState
	bt.skipLoad(`^for_amsterdam/static/state_tests/`)

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
