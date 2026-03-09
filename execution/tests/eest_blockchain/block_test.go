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

package eest_blockchain_test

import (
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/tests/testutil"
)

func TestExecutionSpecBlockchain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testutil.TestMatcher)
	dir := filepath.Join("..", "execution-spec-tests", "blockchain_tests")

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
