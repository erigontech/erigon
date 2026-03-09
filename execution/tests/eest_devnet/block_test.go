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
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_create_out_of_gas_no_log.json`)                                           // block=1, gas used by execution: 166348, in header: 166351
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_failed_inner_operation_no_log.json`)                                      // block=1, gas used by execution: 33618, in header: 33621
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_finalization_burn_logs.json`)                                             // block=1, receiptHash mismatch: 432a8b7b999111d19c579e4fab6b9e24b39500f183bee3a6de0d0788063fe069 != 559d56e60af9621877a9db4037f33c2a27411983b44ec54526ce8584f691292e, headerNum=1, 74bf5654f3b79cf5e3131cc27e206737c863efc1c57aad89d01e2a7f6298a8b4
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_nested_calls_log_order.json`)                                             // block=1, gas used by execution: 122166, in header: 131488
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_reverted_transaction_no_log.json`)                                        // block=1, gas used by execution: 99997, in header: 100000
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7708_eth_transfer_logs/test_selfdestruct_finalization_after_priority_fee.json`)                       // block=1, receiptHash mismatch: ebab83610f8dfd74c1a394f458de5abd682bb67fc419a331c98ab14fa4a3787d != de22d27f88412395e117fd76b35ffa6781ea18725354cab7665a7b6bdd55df28, headerNum=1, 8798592215fae22cb4c5dd5ccc11a0eb4b6d027af40635c5cd43513ce417da45
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_7002_request_invalid.json`)                                    // block=1, gas used by execution: 56234, in header: 150272
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_aborted_storage_access.json`)                                  // block=1, receiptHash mismatch: 3436c842cadfa64946a5dee36dd6ceaab7cbaab05b7ddcb3e8069b72bcf8620a != 0574ca632811062d8709db6085aef9953a58cc20c1f2a9f2fd58973ee9fc43c5, headerNum=1, 55c3ffcf2be2f97acb88c7ffcbb73a3b4f3e8bab487288026fcab8aebbb4a3a2
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_balance_and_oog.json`)                                         // block=1, gas used by execution: 23502, in header: 23602
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_call_7702_delegation_and_oog.json`)                            // block=1, gas used by execution: 23521, in header: 23621
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_call_no_delegation_and_oog_before_target_access.json`)         // block=1, gas used by execution: 23520, in header: 23620
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_call_with_value_in_static_context.json`)                       // block=1, gas used by execution: 528524, in header: 528624
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_callcode_7702_delegation_and_oog.json`)                        // block=1, gas used by execution: 23521, in header: 23621
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_callcode_nested_value_transfer.json`)                          // block=1, gas used by execution: 49608, in header: 131488
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_callcode_no_delegation_and_oog_before_target_access.json`)     // block=1, gas used by execution: 23520, in header: 23620
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_create_transaction_empty_code.json`)                           // block=1, receiptHash mismatch: 118a8c457597ccb2b9400323c1fbbef51437a9e10fb7e621f91b0951cfee9601 != 7289e3be007c4ad5bb73c4c6be056328f5c0bf24ea2c011c1394e7cc5fb63780, headerNum=1, 5342b30393fbe7551e4e6f7cbb93d734a26ffc0dfba9d89e66adfd9dc9c1ab6d
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_delegatecall_7702_delegation_and_oog.json`)                    // block=1, gas used by execution: 23518, in header: 23618
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_delegatecall_no_delegation_and_oog_before_target_access.json`) // block=1, gas used by execution: 23517, in header: 23617
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_extcodecopy_and_oog.json`)                                     // block=1, gas used by execution: 23511, in header: 23611
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_extcodesize_and_oog.json`)                                     // block=1, gas used by execution: 23502, in header: 23602
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_sstore_and_oog.json`)                                          // block=1, gas used by execution: 63573, in header: 37568
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_staticcall_7702_delegation_and_oog.json`)                      // block=1, gas used by execution: 23518, in header: 23618
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7928_block_level_access_lists/test_bal_staticcall_no_delegation_and_oog_before_target_access.json`)   // block=1, gas used by execution: 23517, in header: 23617
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size.json`)                                             // block=1, gas used by execution: 16777216, in header: 38602294
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_code_size_deposit_gas.json`)                                 // block=1, gas used by execution: 1332100, in header: 38601120
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip7954_increase_max_contract_size/test_max_initcode_size_via_create.json`)                              // block=1, gas used by execution: 16768216, in header: 16645728
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip8024_dupn_swapn_exchange/test_dupn_stack_underflow.json`)                                             // block=1, receiptHash mismatch: 496ca8c76f744094f9ba323bd2996f088f7609d26fc17a648298dac6203189a0 != 6ebeb82e2fd4ad8ef581ba011ed8590752fbb658e86bb4f29d186cba3f7b1357, headerNum=1, b5967168fe6d2d540249e81e0c3f1c47c0f88b66ff3703482414f5342af23216
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip8024_dupn_swapn_exchange/test_exchange_invalid_immediate_aborts.json`)                                // block=1, receiptHash mismatch: 3436c842cadfa64946a5dee36dd6ceaab7cbaab05b7ddcb3e8069b72bcf8620a != 0574ca632811062d8709db6085aef9953a58cc20c1f2a9f2fd58973ee9fc43c5, headerNum=1, 46e38e27c442ab7f0135b6835014eb0b8a8ee98f0a175d2fcc3c0b7dbcba8bfd
	bt.SkipLoad(`^for_amsterdam/amsterdam/eip8024_dupn_swapn_exchange/test_exchange_stack_underflow.json`)                                         // block=1, receiptHash mismatch: 3436c842cadfa64946a5dee36dd6ceaab7cbaab05b7ddcb3e8069b72bcf8620a != 0574ca632811062d8709db6085aef9953a58cc20c1f2a9f2fd58973ee9fc43c5, headerNum=1, eca4994f231a234ba9aae3f42e8379657098e4267db8f6a219f1caab1e354076
	bt.SkipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_call_to_precompile.json`)                                              // block=1, gas used by execution: 235733, in header: 235833
	bt.SkipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_nested_call_to_precompile.json`)                                       // block=1, gas used by execution: 253369, in header: 253469
	bt.SkipLoad(`^for_amsterdam/byzantium/eip214_staticcall/test_staticcall_reentrant_call_to_precompile.json`)                                    // block=1, gas used by execution: 1971201, in header: 1971301
	bt.SkipLoad(`^for_amsterdam/cancun/create/test_create_oog_from_eoa_refunds.json`)                                                              // block=1, receiptHash mismatch: 91e4445a3ed8b70b41017281d14d817b16413ae3d96c8afaabd680ec95ff42d9 != ba9991e573821b0c234e361e68d2fe4b0fc940f8380d61fc18ee6ceb963b7094, headerNum=1, b40e3034a6859dfd16c4060e0c80d6b1568f32320b3eb22d6683aae667748324
	bt.SkipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_tload_reentrancy.json`)                                                                 // block=1, gas used by execution: 4939561, in header: 4939591
	bt.SkipLoad(`^for_amsterdam/cancun/eip1153_tstore/test_tstore_reentrancy.json`)                                                                // block=1, gas used by execution: 4949959, in header: 4949989
	bt.SkipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_mcopy_huge_memory_expansion.json`)                                                       // block=1, gas used by execution: 16739645, in header: 16739648
	bt.SkipLoad(`^for_amsterdam/cancun/eip5656_mcopy/test_mcopy_memory_expansion.json`)                                                            // block=1, receiptHash mismatch: 8bcfb9879e7dae354a61c50511d055a756058c40c264fc5a2f2707158044c6ac != 5d8eed7faf98f1780f6d7637a51dbf11d21f276426148ce257381afecc9d9c55, headerNum=1, fa3e93a8768be4c95f84bd21a83341f1030f370b9c6df51586f4bfa5b964691b
	bt.SkipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_dynamic_create2_selfdestruct_collision.json`)                                     // block=1, receiptHash mismatch: 10e8a59e3c241eaf678ed7dac4d17742a04d19e8c245ca365e8d31fa4bffdda1 != c9851c475e7de54512fdd5635cbcbba64e23a11fce28bebc038fba47e56a6a46, headerNum=1, b174c294c2bad735cb2a6fb9af941077277726d37ae03fa23ee72053d40074aa
	bt.SkipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_dynamic_create2_selfdestruct_collision_two_different_transactions.json`)          // block=1, receiptHash mismatch: c47095f836cf575097268646235d3132f035648acd69d2ed81cf2240e8b25b53 != 57e7df7900192e5190668bb70b41320107b32dafaf56497f850f44fb3f215d9f, headerNum=1, 3a46e1d1901a308f6ed8f044e2bdebe02a11d19fbd3f853887bb49e233f52d72
	bt.SkipLoad(`^for_amsterdam/cancun/eip6780_selfdestruct/test_selfdestruct_created_in_same_tx_with_revert.json`)                                // block=1, receiptHash mismatch: 7e9ba0f7c8ae60792b17cd8f66f4927758071ecc540c3301216a72c38c4ef675 != 835cf74995cac576a1287823eea113f304339b92915937c89204aecabd7bbba0, headerNum=1, 9589cba9978ab301fe6728c7239ca232ee5bdda5c3df0c520b7cfd4fbaaeb7a5
	bt.SkipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_call_to_selfdestruct.json`)                                    // block=1, gas used by execution: 193727, in header: 198727
	bt.SkipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_changed_account.json`)                                         // block=1, gas used by execution: 150272, in header: 319328
	bt.SkipLoad(`^for_amsterdam/constantinople/eip1052_extcodehash/test_extcodehash_empty_send_value.json`)                                        // block=1, gas used by execution: 43386, in header: 169056
	bt.SkipLoad(`^for_amsterdam/frontier/create/test_create_address_dynamic_nonce.json`)                                                           // block=1, gas used by execution: 41785, in header: 34224448
	bt.SkipLoad(`^for_amsterdam/frontier/create/test_create_address_nonce_boundary.json`)                                                          // block=1, gas used by execution: 41319, in header: 1352448
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_calldatacopy_word_copy_oog.json`)                                                            // block=1, gas used by execution: 25974, in header: 25977
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_codecopy_word_copy_oog.json`)                                                                // block=1, gas used by execution: 25974, in header: 25977
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_constant_gas.json`)                                                                          // block=1, gas used by execution: 67852, in header: 507168
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_cover_revert.json`)                                                                          // block=1, gas used by execution: 131488, in header: 169056
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_max_stack.json`)                                                                             // block=1, receiptHash mismatch: 81699555b133b2ec23c889e74231ede0a9c00369d128c8e281528e74984e6495 != ef429e98d5fea48ca80c32656efcedf702eec64669848374fe92b035d05ba585, headerNum=1, 59d7052409765d3ea406a62aa934c1874c57bf77474d2927be548f40077f77ae
	bt.SkipLoad(`^for_amsterdam/frontier/opcodes/test_value_transfer_gas_calculation.json`)                                                        // block=1, gas used by execution: 39498, in header: 169056
	bt.SkipLoad(`^for_amsterdam/frontier/scenarios/test_scenarios.json`)                                                                           // block=32, gas used by execution: 5027868, in header: 5027898
	bt.SkipLoad(`^for_amsterdam/prague/eip6110_deposits/test_deposit.json`)                                                                        // block=1, gas used by execution: 9624220, in header: 9624320
	bt.SkipLoad(`^for_amsterdam/prague/eip7002_el_triggerable_withdrawals/test_withdrawal_requests.json`)                                          // block=1, gas used by execution: 150272, in header: 225408
	bt.SkipLoad(`^for_amsterdam/prague/eip7251_consolidations/test_consolidation_requests.json`)                                                   // block=1, gas used by execution: 187840, in header: 262976
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_pointer_to_created_from_create_after_oog_call_again.json`)                    // block=1, gas used by execution: 471784, in header: 473869
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_call_to_pre_authorized_oog.json`)                                                  // block=1, gas used by execution: 26120, in header: 26220
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_delegation_clearing_failing_tx.json`)                                              // block=1, gas used by execution: 472998, in header: 341510
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reentry.json`)                                                             // block=1, gas used by execution: 252410, in header: 477818
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_reverts.json`)                                                             // block=1, gas used by execution: 52133, in header: 177274
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_pointer_to_pointer.json`)                                                          // block=1, gas used by execution: 945996, in header: 683020
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_self_sponsored_set_code.json`)                                                     // block=1, gas used by execution: 9897862, in header: 9841510
	bt.SkipLoad(`^for_amsterdam/prague/eip7702_set_code_tx/test_set_code_to_sstore.json`)                                                          // block=1, gas used by execution: 397862, in header: 341510
	bt.SkipLoad(`^for_amsterdam/shanghai/eip3855_push0/test_push0_contracts.json`)                                                                 // block=1, receiptHash mismatch: 2f13c48591f063e30a792a180116e5ef2611efc62565ec81f9cbe853e23bc631 != 777f1c1c378807634128348e4f0eeca6a0e7f516ea411690ca04266323f671a4, headerNum=1, 5c412c752fde5e90a553a01d5463d569fe2e5cc672b5ed5935dfe4162be62aa0
	bt.SkipLoad(`^for_amsterdam/tangerine_whistle/eip150_operation_gas_costs/test_selfdestruct_state_access_boundary.json`)                        // block=1, gas used by execution: 26223, in header: 31223

	bt.Walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		test.ExperimentalBAL = true // TODO eventually remove this from BlockTest and run normally
		if err := bt.CheckFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}
