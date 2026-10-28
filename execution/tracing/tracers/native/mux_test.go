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

package native

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMuxForwardsTxEndV2(t *testing.T) {
	receipt := &types.Receipt{GasUsed: 100}
	usage := mdgas.TxnGasUsage{BlockExecutionGasUsed: 40, BlockStateGasUsed: 70, GasRefund: 10}
	var calls int
	var legacyCalls int
	mux := newTestMuxTracer([]string{"v2", "v1", "nil"}, []*tracers.Tracer{
		{Hooks: &tracing.Hooks{
			OnTxEndV2: func(gotReceipt *types.Receipt, txnGasUsage mdgas.TxnGasUsage, err error) {
				require.Same(t, receipt, gotReceipt)
				require.Equal(t, usage, txnGasUsage)
				require.NoError(t, err)
				calls++
			},
			OnTxEnd: func(*types.Receipt, error) { t.Fatal("V2 must take precedence") },
		}},
		{Hooks: &tracing.Hooks{OnTxEnd: func(gotReceipt *types.Receipt, err error) {
			require.Same(t, receipt, gotReceipt)
			require.NoError(t, err)
			legacyCalls++
		}}},
		{},
	})
	mux.EmitTxEnd(receipt, usage, nil)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, legacyCalls)
}

func TestMuxForwardsFrameV2(t *testing.T) {
	entry := []mdgas.MdGas{{Execution: 100, State: 200}, {Execution: 30, State: 50}}
	usage := []mdgas.MdGasUsage{{Execution: 3, State: 12, StateSpill: 2}, {Execution: 20, State: -10}}
	var entered []mdgas.MdGas
	var exited []mdgas.MdGasUsage
	var legacyEntry []uint64
	var legacyUsage []uint64
	children := []*tracers.Tracer{
		{Hooks: &tracing.Hooks{
			OnEnterV2: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, gas mdgas.MdGas, _ uint256.Int, _ []byte) {
				entered = append(entered, gas)
			},
			OnExitV2: func(_ int, _ []byte, gasUsed mdgas.MdGasUsage, _ error, _ bool) {
				exited = append(exited, gasUsed)
			},
			OnEnter: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
				t.Fatal("V2 must take precedence")
			},
			OnExit: func(_ int, _ []byte, _ uint64, _ error, _ bool) {
				t.Fatal("V2 must take precedence")
			},
		}},
		{Hooks: &tracing.Hooks{
			OnEnter: func(_ int, _ byte, _, _ accounts.Address, _ bool, _ []byte, gas uint64, _ uint256.Int, _ []byte) {
				legacyEntry = append(legacyEntry, gas)
			},
			OnExit: func(_ int, _ []byte, gasUsed uint64, _ error, _ bool) {
				legacyUsage = append(legacyUsage, gasUsed)
			},
		}},
		{},
		{Hooks: &tracing.Hooks{}},
	}
	mux := newTestMuxTracer([]string{"v2", "v1", "nil", "empty"}, children)
	mux.EmitEnter(0, 0xf1, accounts.ZeroAddress, accounts.ZeroAddress, false, nil, entry[0], uint256.Int{}, nil)
	mux.EmitEnter(1, 0xf1, accounts.ZeroAddress, accounts.ZeroAddress, false, nil, entry[1], uint256.Int{}, nil)
	mux.EmitExit(1, nil, usage[0], nil, false)
	mux.EmitExit(0, nil, usage[1], nil, false)
	require.Equal(t, entry, entered)
	require.Equal(t, usage, exited)
	require.Equal(t, []uint64{100, 30}, legacyEntry)
	require.Equal(t, []uint64{3, 20}, legacyUsage)
}

func TestMuxForwardsOpcodeV2(t *testing.T) {
	gas := mdgas.MdGas{Execution: 100, State: 200}
	cost := mdgas.MdGasCost{Execution: 10, State: 50}
	var receivedGas []mdgas.MdGas
	var receivedCost []mdgas.MdGasCost
	var legacy [][2]uint64
	children := []*tracers.Tracer{
		{Hooks: &tracing.Hooks{
			OnOpcodeV2: func(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, _ tracing.OpContext, rData []byte, depth int, err error) {
				require.Equal(t, uint64(42), pc)
				require.Equal(t, byte(0x55), op)
				require.Equal(t, []byte{1}, rData)
				require.Equal(t, 2, depth)
				require.NoError(t, err)
				receivedGas = append(receivedGas, gas)
				receivedCost = append(receivedCost, cost)
			},
			OnOpcode: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
				t.Fatal("V2 must take precedence")
			},
		}},
		{Hooks: &tracing.Hooks{OnOpcode: func(_ uint64, _ byte, gas, cost uint64, _ tracing.OpContext, _ []byte, _ int, _ error) {
			legacy = append(legacy, [2]uint64{gas, cost})
		}}},
		{},
		{Hooks: &tracing.Hooks{}},
	}
	mux := newTestMuxTracer([]string{"v2", "v1", "nil", "empty"}, children)
	mux.EmitOpcode(42, 0x55, gas, cost, nil, []byte{1}, 2, nil)
	require.Equal(t, []mdgas.MdGas{gas}, receivedGas)
	require.Equal(t, []mdgas.MdGasCost{cost}, receivedCost)
	require.Equal(t, [][2]uint64{{100, 10}}, legacy)
}

func TestMuxForwardsFaultV2(t *testing.T) {
	gas := mdgas.MdGas{Execution: 100, State: 200}
	cost := mdgas.MdGasCost{Execution: 10, State: 50}
	fault := errors.New("opcode failed")
	var receivedGas []mdgas.MdGas
	var receivedCost []mdgas.MdGasCost
	var legacy [][2]uint64
	children := []*tracers.Tracer{
		{Hooks: &tracing.Hooks{
			OnFaultV2: func(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, _ tracing.OpContext, depth int, err error) {
				require.Equal(t, uint64(42), pc)
				require.Equal(t, byte(0x55), op)
				require.Equal(t, 2, depth)
				require.ErrorIs(t, err, fault)
				receivedGas = append(receivedGas, gas)
				receivedCost = append(receivedCost, cost)
			},
			OnFault: func(_ uint64, _ byte, _, _ uint64, _ tracing.OpContext, _ int, _ error) {
				t.Fatal("V2 must take precedence")
			},
		}},
		{Hooks: &tracing.Hooks{OnFault: func(_ uint64, _ byte, gas, cost uint64, _ tracing.OpContext, _ int, _ error) {
			legacy = append(legacy, [2]uint64{gas, cost})
		}}},
		{},
		{Hooks: &tracing.Hooks{}},
	}
	mux := newTestMuxTracer([]string{"v2", "v1", "nil", "empty"}, children)
	mux.EmitFault(42, 0x55, gas, cost, nil, 2, fault)
	require.Equal(t, []mdgas.MdGas{gas}, receivedGas)
	require.Equal(t, []mdgas.MdGasCost{cost}, receivedCost)
	require.Equal(t, [][2]uint64{{100, 10}}, legacy)
}

func TestMuxForwardsGasChangeV2(t *testing.T) {
	old := mdgas.MdGas{Execution: 100, State: 200}
	new := mdgas.MdGas{Execution: 90, State: 150}
	var received [][2]mdgas.MdGas
	var reasons []tracing.GasChangeReason
	var legacy [][2]uint64
	children := []*tracers.Tracer{
		{Hooks: &tracing.Hooks{
			OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
				received = append(received, [2]mdgas.MdGas{old, new})
				reasons = append(reasons, reason)
			},
			OnGasChange: func(_, _ uint64, _ tracing.GasChangeReason) { t.Fatal("V2 must take precedence") },
		}},
		{Hooks: &tracing.Hooks{OnGasChange: func(old, new uint64, _ tracing.GasChangeReason) {
			legacy = append(legacy, [2]uint64{old, new})
		}}},
		{},
	}
	mux := newTestMuxTracer([]string{"v2", "v1", "nil"}, children)
	mux.EmitGasChange(old, new, tracing.GasChangeCallOpCode)
	require.Equal(t, [][2]mdgas.MdGas{{old, new}}, received)
	require.Equal(t, []tracing.GasChangeReason{tracing.GasChangeCallOpCode}, reasons)
	require.Equal(t, [][2]uint64{{100, 90}}, legacy)
	mux.EmitGasChange(old, new, tracing.GasChangeTxIntrinsicGas)
	require.Equal(t, [2]uint64{100, 90}, legacy[1])
}

// newTestMuxTracer is a test helper that constructs a muxTracer from
// pre-built child tracers, bypassing the JSON-based registry lookup
// used by newMuxTracer. This mirrors geth's NewMuxTracer constructor
// which accepts names and tracer slices directly.
func newTestMuxTracer(names []string, children []*tracers.Tracer) *tracers.Tracer {
	return (&muxTracer{names: names, tracers: children}).tracer()
}

// TestMuxForwardsV2StateHooks verifies that the mux tracer fans out the V2
// variants of state-change hooks to child tracers.
//
// A child tracer that only implements OnCodeChangeV2 / OnNonceChangeV2 must
// still receive events when wrapped behind the mux. The mux must also fall
// back to the V1 hook when a child only implements V1, mirroring the
// precedence used in execution/state/state_object.go.
func TestMuxForwardsV2StateHooks(t *testing.T) {
	var (
		codeV2Calls  int
		nonceV2Calls int
		codeV1Calls  int
		nonceV1Calls int
	)

	v2Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnCodeChangeV2: func(_ accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte, _ tracing.CodeChangeReason) {
				codeV2Calls++
			},
			OnNonceChangeV2: func(_ accounts.Address, _, _ uint64, _ tracing.NonceChangeReason) {
				nonceV2Calls++
			},
		},
	}

	v1Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnCodeChange: func(_ accounts.Address, _ accounts.CodeHash, _ []byte, _ accounts.CodeHash, _ []byte) {
				codeV1Calls++
			},
			OnNonceChange: func(_ accounts.Address, _, _ uint64) {
				nonceV1Calls++
			},
		},
	}

	mux := newTestMuxTracer([]string{"v2", "v1"}, []*tracers.Tracer{v2Child, v1Child})

	// The mux must expose V2 hooks so the state processor can invoke them.
	require.NotNil(t, mux.OnCodeChangeV2, "mux does not expose OnCodeChangeV2; V2-only child tracers will miss code changes")
	require.NotNil(t, mux.OnNonceChangeV2, "mux does not expose OnNonceChangeV2; V2-only child tracers will miss nonce changes")

	// Fire the V2 hooks on the mux.
	mux.OnCodeChangeV2(accounts.Address{}, accounts.CodeHash{}, nil, accounts.CodeHash{}, nil, tracing.CodeChangeContractCreation)
	mux.OnNonceChangeV2(accounts.Address{}, 0, 1, tracing.NonceChangeEoACall)

	// V2 child must have received the V2 call.
	require.Equal(t, 1, codeV2Calls, "V2 child OnCodeChangeV2 call count")
	require.Equal(t, 1, nonceV2Calls, "V2 child OnNonceChangeV2 call count")

	// V1 child must have received the fallback from V2 → V1.
	require.Equal(t, 1, codeV1Calls, "V1 child OnCodeChange call count (mux should fall back from V2 to V1)")
	require.Equal(t, 1, nonceV1Calls, "V1 child OnNonceChange call count (mux should fall back from V2 to V1)")
}

// TestMuxForwardsSystemCallV2 verifies that the mux tracer fans out the
// OnSystemCallStartV2 hook, falling back to OnSystemCallStart for children
// that only implement the V1 variant.
func TestMuxForwardsSystemCallV2(t *testing.T) {
	var (
		sysV2Calls int
		sysV1Calls int
	)

	v2Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnSystemCallStartV2: func(_ *tracing.VMContext) {
				sysV2Calls++
			},
		},
	}

	v1Child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnSystemCallStart: func() {
				sysV1Calls++
			},
		},
	}

	mux := newTestMuxTracer([]string{"v2", "v1"}, []*tracers.Tracer{v2Child, v1Child})

	require.NotNil(t, mux.OnSystemCallStartV2, "mux does not expose OnSystemCallStartV2")

	mux.OnSystemCallStartV2(nil)

	require.Equal(t, 1, sysV2Calls, "V2 child OnSystemCallStartV2 call count")
	require.Equal(t, 1, sysV1Calls, "V1 child OnSystemCallStart call count (mux should fall back from V2 to V1)")
}

// TestMuxForwardsSystemCallEnd verifies that the mux tracer fans out
// OnSystemCallEnd to all children that register it.
func TestMuxForwardsSystemCallEnd(t *testing.T) {
	var endCalls int

	child := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnSystemCallEnd: func() {
				endCalls++
			},
		},
	}

	mux := newTestMuxTracer([]string{"child"}, []*tracers.Tracer{child})

	require.NotNil(t, mux.OnSystemCallEnd, "mux does not expose OnSystemCallEnd")

	mux.OnSystemCallEnd()

	require.Equal(t, 1, endCalls, "child OnSystemCallEnd call count")
}

// TestMuxEmptyChildHookFunctionsDoNotPanic verifies that the mux gracefully
// skips children whose individual hook function fields are nil (i.e., a tracer
// that exists but doesn't implement every hook), without panicking.
func TestMuxEmptyChildHookFunctionsDoNotPanic(t *testing.T) {
	// A child with a non-nil Hooks struct but no hook functions set.
	emptyChild := &tracers.Tracer{
		Hooks: &tracing.Hooks{},
	}

	mux := newTestMuxTracer([]string{"empty"}, []*tracers.Tracer{emptyChild})

	// None of these should panic.
	require.NotPanics(t, func() {
		mux.OnCodeChangeV2(accounts.Address{}, accounts.CodeHash{}, nil, accounts.CodeHash{}, nil, tracing.CodeChangeContractCreation)
	}, "OnCodeChangeV2 with nil child hook must not panic")

	require.NotPanics(t, func() {
		mux.OnNonceChangeV2(accounts.Address{}, 0, 1, tracing.NonceChangeEoACall)
	}, "OnNonceChangeV2 with nil child hook must not panic")

	require.NotPanics(t, func() {
		mux.OnSystemCallStartV2(nil)
	}, "OnSystemCallStartV2 with nil child hook must not panic")

	require.NotPanics(t, func() {
		mux.OnSystemCallEnd()
	}, "OnSystemCallEnd with nil child hook must not panic")
}

// TestMuxNilHooksPointerDoesNotPanic verifies that a child tracer with a
// nil *tracing.Hooks pointer (e.g., &tracers.Tracer{}) does not cause a
// panic from promoted-field access in the mux fanout loops.
func TestMuxNilHooksPointerDoesNotPanic(t *testing.T) {
	nilHooksChild := &tracers.Tracer{} // Hooks is nil

	mux := newTestMuxTracer([]string{"nil"}, []*tracers.Tracer{nilHooksChild})

	require.NotPanics(t, func() {
		mux.OnSystemCallStartV2(nil)
	}, "OnSystemCallStartV2 with nil Hooks pointer must not panic")

	require.NotPanics(t, func() {
		mux.OnSystemCallEnd()
	}, "OnSystemCallEnd with nil Hooks pointer must not panic")

	require.NotPanics(t, func() {
		mux.OnCodeChangeV2(accounts.Address{}, accounts.CodeHash{}, nil, accounts.CodeHash{}, nil, tracing.CodeChangeContractCreation)
	}, "OnCodeChangeV2 with nil Hooks pointer must not panic")

	require.NotPanics(t, func() {
		mux.OnNonceChangeV2(accounts.Address{}, 0, 1, tracing.NonceChangeEoACall)
	}, "OnNonceChangeV2 with nil Hooks pointer must not panic")
}
