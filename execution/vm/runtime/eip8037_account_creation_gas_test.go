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

// gasForProgram deploys code at a funded contract on the parallel-execution
// path and reports the gas the call consumed.
func gasForProgram(t *testing.T, code []byte) uint64 {
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
	_, gasRemaining, err := Call(contract, nil, &Config{
		ChainConfig: chain.AllProtocolChanges,
		GasLimit:    gasLimit,
		State:       statedb,
	})
	require.NoError(t, err)
	return gasLimit - gasRemaining.Total()
}

// TestValueCallAfterZeroValueCallStillChargesAccountCreation pins the EIP-8037
// account-creation charge against the emptiness predicate that gates it.
//
// A zero-value CALL to a precompile materializes its account (evm.call skips
// the EIP-161 zero-value short-circuit for precompiles), but the account stays
// empty by EIP-161 — zero balance, zero nonce, empty code — so a later
// value-bearing CALL to it still creates state and still owes
// StateGasNewAccount. Reading createObject's SelfDestructPath=false marker as
// a self-destruct made Empty() report false and dropped that charge, which is
// what let a preceding zero-value call make the whole transaction cheaper.
func TestValueCallAfterZeroValueCallStillChargesAccountCreation(t *testing.T) {
	t.Parallel()

	for _, target := range []byte{0x01, 0x02, 0x03, 0x04, 0x42} {
		t.Run(common.Bytes2Hex([]byte{target}), func(t *testing.T) {
			t.Parallel()

			valueCallOnly := append(callTo(target, 0x01), byte(vm.STOP))
			zeroThenValueCall := append(append(callTo(target, 0x00), callTo(target, 0x01)...), byte(vm.STOP))

			control := gasForProgram(t, valueCallOnly)
			warmed := gasForProgram(t, zeroThenValueCall)

			require.GreaterOrEqual(t, control, uint64(params.StateGasNewAccount),
				"a value transfer to an absent account must be charged account creation")
			require.GreaterOrEqual(t, warmed, uint64(params.StateGasNewAccount),
				"materializing the account first must not waive the account-creation charge")
			require.Greater(t, warmed, control,
				"adding a zero-value call must not make the transaction cheaper")
		})
	}
}
