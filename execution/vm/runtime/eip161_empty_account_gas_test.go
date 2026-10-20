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

package runtime

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// emptinessForks pairs a fork with the charge a value transfer to an
// EIP-161-empty account owes on it: the flat 25000 before Amsterdam, the
// EIP-8037 state-gas equivalent from Amsterdam on.
var emptinessForks = []struct {
	name       string
	cfg        *chain.Config
	newAcctGas uint64
}{
	{"Osaka", chain.TestChainOsakaConfig, params.CallNewAccountGas},
	{"Amsterdam", chain.AllProtocolChanges, uint64(params.StateGasNewAccount)},
}

// callTo emits a CALL to target forwarding 10000 gas with no calldata.
// CALL pops gas, address, value, argsOffset, argsSize, retOffset, retSize, so
// the pushes run in reverse.
func callTo(target byte, value byte) []byte {
	return []byte{
		byte(vm.PUSH1), 0x00, // retSize
		byte(vm.PUSH1), 0x00, // retOffset
		byte(vm.PUSH1), 0x00, // argsSize
		byte(vm.PUSH1), 0x00, // argsOffset
		byte(vm.PUSH1), value,
		byte(vm.PUSH1), target,
		byte(vm.PUSH2), 0x27, 0x10, // gas
		byte(vm.CALL),
		byte(vm.POP),
	}
}

// runProgram deploys code at a funded contract on the parallel-execution path
// and reports the gas the call consumed along with its return data.
func runProgram(t *testing.T, code []byte, cfg *chain.Config) (uint64, []byte) {
	t.Helper()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	reader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	statedb := state.NewWithVersionMap(reader, state.NewVersionMap(nil))
	statedb.SetNoMaterialize(true)
	defer statedb.Close()

	contract := accounts.InternAddress(common.HexToAddress("0x2000"))
	require.NoError(t, statedb.SetCode(contract, code, tracing.CodeChangeUnspecified))
	require.NoError(t, statedb.SetBalance(contract, *uint256.NewInt(1_000), tracing.BalanceChangeUnspecified))

	const gasLimit = uint64(2_000_000)
	ret, gasRemaining, err := Call(contract, nil, &Config{
		ChainConfig: cfg,
		GasLimit:    gasLimit,
		State:       statedb,
	})
	require.NoError(t, err)
	return gasLimit - gasRemaining.Total(), ret
}

// TestCallChargesNewAccount pins the
// new-account charge against the emptiness predicate that gates it.
//
// A zero-value CALL to a precompile materializes its account (evm.call skips
// the EIP-161 zero-value short-circuit for precompiles), but the account stays
// empty by EIP-161 — zero balance, zero nonce, empty code — so a later
// value-bearing CALL to it still creates state and still owes the charge.
// Reading createObject's SelfDestructPath=false marker as a self-destruct made
// Empty() report false and dropped it, which is what let a preceding zero-value
// call make the whole transaction cheaper.
func TestCallChargesNewAccount(t *testing.T) {
	t.Parallel()

	for _, fork := range emptinessForks {
		t.Run(fork.name, func(t *testing.T) {
			t.Parallel()
			for _, target := range []byte{0x01, 0x02, 0x03, 0x04, 0x42} {
				t.Run(common.Bytes2Hex([]byte{target}), func(t *testing.T) {
					t.Parallel()

					valueCallOnly := append(callTo(target, 0x01), byte(vm.STOP))
					zeroThenValueCall := append(append(callTo(target, 0x00), callTo(target, 0x01)...), byte(vm.STOP))

					control, _ := runProgram(t, valueCallOnly, fork.cfg)
					warmed, _ := runProgram(t, zeroThenValueCall, fork.cfg)

					require.GreaterOrEqual(t, control, fork.newAcctGas,
						"a value transfer to an absent account must be charged account creation")
					require.GreaterOrEqual(t, warmed, fork.newAcctGas,
						"materializing the account first must not waive the account-creation charge")
					require.Greater(t, warmed, control,
						"adding a zero-value call must not make the transaction cheaper")
				})
			}
		})
	}
}

// TestExtCodeHashEmptyAccount pins EIP-1052: EXTCODEHASH of an
// empty account is 0. A materializing zero-value CALL must not change that —
// judging the account non-empty made EXTCODEHASH fall through to the code hash
// and return the hash of empty code instead of zero.
func TestExtCodeHashEmptyAccount(t *testing.T) {
	t.Parallel()

	for _, fork := range emptinessForks {
		t.Run(fork.name, func(t *testing.T) {
			t.Parallel()

			code := append(callTo(0x02, 0x00),
				byte(vm.PUSH1), 0x02, byte(vm.EXTCODEHASH),
				byte(vm.PUSH1), 0x00, byte(vm.MSTORE),
				byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN))

			_, ret := runProgram(t, code, fork.cfg)
			require.Equal(t, make([]byte, 32), ret,
				"EXTCODEHASH of an empty account is 0, even after a call materialized it")
		})
	}
}

// TestSelfdestructChargesNewAccount pins the
// SELFDESTRUCT beneficiary-creation charge against the same predicate: sending
// a non-zero balance to an EIP-161-empty beneficiary creates state and owes the
// charge, whether or not an earlier call materialized the account.
func TestSelfdestructChargesNewAccount(t *testing.T) {
	t.Parallel()

	for _, fork := range emptinessForks {
		t.Run(fork.name, func(t *testing.T) {
			t.Parallel()

			destructOnly := []byte{byte(vm.PUSH1), 0x02, byte(vm.SELFDESTRUCT)}
			callThenDestruct := append(callTo(0x02, 0x00), byte(vm.PUSH1), 0x02, byte(vm.SELFDESTRUCT))

			control, _ := runProgram(t, destructOnly, fork.cfg)
			warmed, _ := runProgram(t, callThenDestruct, fork.cfg)

			require.GreaterOrEqual(t, control, params.CreateBySelfdestructGas,
				"SELFDESTRUCT to an empty beneficiary must be charged account creation")
			require.GreaterOrEqual(t, warmed, params.CreateBySelfdestructGas,
				"materializing the beneficiary first must not waive the charge")
			require.Greater(t, warmed, control,
				"adding a zero-value call must not make the transaction cheaper")
		})
	}
}
