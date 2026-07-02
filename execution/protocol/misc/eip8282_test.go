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

package misc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// noopSyscall is a no-op system call that satisfies the rules.SystemCall signature.
func noopSyscall(_ accounts.Address, _ []byte) ([]byte, error) { return nil, nil }

// TestSystemContractsIncludeBuilderContracts verifies that an Amsterdam-active
// chain config includes both EIP-8282 builder contracts in its system contract map.
//
// If either contract is missing from SystemContracts(), the block execution
// pipeline will not drain the builder deposit/exit queues, causing consensus
// divergence with other clients.
func TestSystemContractsIncludeBuilderContracts(t *testing.T) {
	t.Parallel()

	// AllProtocolChanges has Amsterdam active at time 0.
	contracts := chain.AllProtocolChanges.SystemContracts(0)

	for _, tc := range []struct {
		name string
		key  string
		want accounts.Address
	}{
		{"BuilderDeposit (8282)", "BUILDER_DEPOSIT_CONTRACT_ADDRESS", params.BuilderDepositAddress},
		{"BuilderExit (8282)", "BUILDER_EXIT_CONTRACT_ADDRESS", params.BuilderExitAddress},
	} {
		addr, ok := contracts[tc.key]
		require.True(t, ok, "%s must be present in SystemContracts()", tc.name)
		require.Equal(t, tc.want, addr, "%s address mismatch", tc.name)
	}
}

// TestSystemContractsExcludeBuilderPreAmsterdam verifies that a pre-Amsterdam
// chain config does NOT include the EIP-8282 builder contracts. The builder
// request bus is Amsterdam-only; surfacing them earlier would cause system
// calls to non-existent contracts.
func TestSystemContractsExcludeBuilderPreAmsterdam(t *testing.T) {
	t.Parallel()

	// TestChainOsakaConfig has Osaka active but NOT Amsterdam.
	contracts := chain.TestChainOsakaConfig.SystemContracts(0)

	_, hasDeposit := contracts["BUILDER_DEPOSIT_CONTRACT_ADDRESS"]
	_, hasExit := contracts["BUILDER_EXIT_CONTRACT_ADDRESS"]

	require.False(t, hasDeposit, "BuilderDeposit must NOT appear pre-Amsterdam")
	require.False(t, hasExit, "BuilderExit must NOT appear pre-Amsterdam")
}

// TestDequeueBuilderDepositRequests_EmptyCodeReturnsError verifies that
// DequeueBuilderDepositRequests returns a descriptive error when the builder
// deposit contract has no deployed bytecode. Until the EIP-8282 contract
// bytecodes are finalized and published, this is the expected failure mode.
func TestDequeueBuilderDepositRequests_EmptyCodeReturnsError(t *testing.T) {
	t.Parallel()

	db := testutil.TemporalDB(t)
	tx, domains := testutil.TemporalTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsGetter(tx)))

	// Contract exists with zero-length code.
	statedb.CreateAccount(params.BuilderDepositAddress, true)

	_, err := misc.DequeueBuilderDepositRequests(noopSyscall, statedb, params.BuilderDepositAddress)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Empty Code",
		"must reject syscall to builder deposit contract with empty bytecode")
}

// TestDequeueBuilderExitRequests_EmptyCodeReturnsError verifies the same
// empty-code guard for the builder exit contract.
func TestDequeueBuilderExitRequests_EmptyCodeReturnsError(t *testing.T) {
	t.Parallel()

	db := testutil.TemporalDB(t)
	tx, domains := testutil.TemporalTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsGetter(tx)))

	// Contract exists with zero-length code.
	statedb.CreateAccount(params.BuilderExitAddress, true)

	_, err := misc.DequeueBuilderExitRequests(noopSyscall, statedb, params.BuilderExitAddress)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Empty Code",
		"must reject syscall to builder exit contract with empty bytecode")
}

// TestBuilderContractAddresses verifies the EIP-8282 predeploy addresses
// match the specification, and that the pre-deployed bytecode variables are populated.
func TestBuilderContractAddresses(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		common.HexToAddress("0x00006AE84ed173D4394de5E28F9ED56b28008282"),
		params.BuilderDepositAddress.Value(),
		"BuilderDepositAddress must match EIP-8282 spec")

	require.Equal(t,
		common.HexToAddress("0x000014574A74c805590AFF9499fc7A690f008282"),
		params.BuilderExitAddress.Value(),
		"BuilderExitAddress must match EIP-8282 spec")

	require.NotEmpty(t, misc.BuilderDepositRequestCode,
		"BuilderDepositRequestCode must be populated with official bytecode")
	require.NotEmpty(t, misc.BuilderExitRequestCode,
		"BuilderExitRequestCode must be populated with official bytecode")
}
