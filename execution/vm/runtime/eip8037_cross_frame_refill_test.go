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
	"slices"
	"testing"

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

const (
	setterAddr   = 0x1000
	clearerAddr  = 0x1001
	reverterAddr = 0x1002
	halterAddr   = 0x1003
	callerAddr   = 0x2000
)

// delegateCallTo emits a DELEGATECALL to target forwarding all gas, so the
// callee writes the caller's storage. The operands are pushed in reverse: the
// opcode pops gas, address, argsOffset, argsSize, retOffset, retSize.
func delegateCallTo(target uint16) []byte {
	return []byte{
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, // retSize, retOffset
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, // argsSize, argsOffset
		byte(vm.PUSH2), byte(target >> 8), byte(target),
		byte(vm.GAS), byte(vm.DELEGATECALL), byte(vm.POP),
	}
}

func sstore(slot, value byte) []byte {
	return []byte{byte(vm.PUSH1), value, byte(vm.PUSH1), slot, byte(vm.SSTORE)}
}

// deployStateGasContracts installs the setter/clearer pair, the two setters that
// fail after spilling, plus a caller running callerCode, and returns the caller
// and the state it was deployed into.
func deployStateGasContracts(t *testing.T, callerCode []byte) (accounts.Address, *state.IntraBlockState) {
	t.Helper()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	reader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	statedb := state.NewWithVersionMap(reader, state.NewVersionMap(nil))
	statedb.SetNoMaterialize(true)
	t.Cleanup(statedb.Close)

	deploy := func(at uint16, code []byte) accounts.Address {
		addr := accounts.InternAddress(common.BytesToAddress([]byte{byte(at >> 8), byte(at)}))
		require.NoError(t, statedb.SetCode(addr, code, tracing.CodeChangeUnspecified))
		return addr
	}
	stop := []byte{byte(vm.STOP)}
	deploy(setterAddr, slices.Concat(sstore(1, 1), stop))
	deploy(clearerAddr, slices.Concat(sstore(1, 0), stop))
	revert := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	deploy(reverterAddr, slices.Concat(sstore(1, 1), revert))
	deploy(halterAddr, slices.Concat(sstore(1, 1), []byte{byte(vm.INVALID)}))
	return deploy(callerAddr, callerCode), statedb
}

// TestStateGasReturnsToGasLeftAcrossFrames pins EIP-8037's promise that state
// gas is never stranded in the reservoir. The first child allocates a slot and
// spills from its gas_left; the second clears the same slot, but has no spill
// of its own, so its refill lands in the reservoir. The merge must absorb it,
// leaving the reservoir at its start-of-transaction value.
func TestStateGasReturnsToGasLeftAcrossFrames(t *testing.T) {
	t.Parallel()

	stop := []byte{byte(vm.STOP)}
	callSiblings := slices.Concat(delegateCallTo(setterAddr), delegateCallTo(clearerAddr), stop)
	// Spilling for a second slot in between leaves this frame more spill than
	// the sibling's refill can cover.
	spillInBetween := slices.Concat(delegateCallTo(setterAddr), sstore(2, 1),
		delegateCallTo(clearerAddr), stop)

	for _, tc := range []struct {
		name                                 string
		caller                               []byte
		gasLimit, wantReservoir, wantGasLeft uint64
	}{
		{"reservoir equals spill", callSiblings, 2_000_000, 0, 1_981_750},
		{"spill under reservoir", callSiblings, params.MaxTxnGasLimit + 50_000, 50_000, 16_758_966},
		{"spill over reservoir", spillInBetween, 2_000_000, 0, 1_871_724},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caller, statedb := deployStateGasContracts(t, tc.caller)

			_, gasRemaining, err := Call(caller, nil, &Config{
				ChainConfig: chain.AllProtocolChanges,
				GasLimit:    tc.gasLimit,
				State:       statedb,
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantReservoir, gasRemaining.State)
			// The absorbed gas must land in gas_left: the reservoir assertion
			// above also holds for a merge that drops it on the floor.
			require.Equal(t, tc.wantGasLeft, gasRemaining.Execution)
		})
	}
}

// A merge moves gas_left, so a tracer following OnGasChange must be told; an
// unannounced increase desynchronises every gas-tracking tracer. It is not a
// leftover refund, so a tracer counting those must not see one more per call.
func TestStateGasMergeIsAnnouncedToTracer(t *testing.T) {
	t.Parallel()

	gains := map[tracing.GasChangeReason][]uint64{}
	hooks := &tracing.Hooks{
		OnGasChange: func(old, newGas uint64, reason tracing.GasChangeReason) {
			if newGas > old {
				gains[reason] = append(gains[reason], newGas-old)
			}
		},
	}

	stop := []byte{byte(vm.STOP)}
	caller, statedb := deployStateGasContracts(t,
		slices.Concat(delegateCallTo(setterAddr), delegateCallTo(clearerAddr), stop))

	_, _, err := Call(caller, nil, &Config{
		ChainConfig: chain.AllProtocolChanges,
		GasLimit:    2_000_000,
		State:       statedb,
		EVMConfig:   vm.Config{Tracer: hooks},
	})
	require.NoError(t, err)
	require.Contains(t, gains[tracing.GasChangeCallStateGasReturned], uint64(97_920),
		"the reservoir->gas_left move must be reported")
	require.Len(t, gains[tracing.GasChangeCallLeftOverRefunded], 2,
		"one leftover refund per child call, and none for the state-gas merge")
}

// A failing child hands back its entry reservoir and its spill with it, so the
// parent must not merge that spill: doing so would credit gas_left twice for
// state creation the child rolled back.
func TestFailingChildStateGasIsNotMerged(t *testing.T) {
	t.Parallel()

	stop := []byte{byte(vm.STOP)}
	for _, tc := range []struct {
		name                       string
		child                      uint16
		wantReservoir, wantGasLeft uint64
	}{
		{"revert", reverterAddr, 50_000, 16_762_085},
		{"exceptional halt", halterAddr, 50_000, 262_094},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			caller, statedb := deployStateGasContracts(t, slices.Concat(delegateCallTo(tc.child), stop))

			_, gasRemaining, err := Call(caller, nil, &Config{
				ChainConfig: chain.AllProtocolChanges,
				GasLimit:    params.MaxTxnGasLimit + 50_000,
				State:       statedb,
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantReservoir, gasRemaining.State)
			require.Equal(t, tc.wantGasLeft, gasRemaining.Execution)
		})
	}
}
