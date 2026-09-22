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

package vm

import (
	"math"
	"testing"
	"unsafe"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestFrameGasUsageRevert(t *testing.T) {
	for _, typ := range []OpCode{CALLCODE, CREATE} {
		for _, tc := range []struct {
			name      string
			ending    []byte
			execution uint64
		}{
			{name: "revert", ending: []byte{byte(PUSH0), byte(PUSH0), byte(REVERT)}, execution: 12_110},
			{name: "exceptional halt", ending: []byte{byte(INVALID)}, execution: 200_000},
		} {
			t.Run(typ.String()+"/"+tc.name, func(t *testing.T) {
				ibs := state.New(state.NewNoopReader())
				defer ibs.Close()
				evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
				initial := mdgas.MdGas{Execution: 200_000, State: params.StateGasPerStorageSet / 2}
				code := append([]byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE)}, tc.ending...)
				var remaining mdgas.MdGas
				var used mdgas.MdGasUsage
				var err error
				if typ == CREATE {
					_, _, remaining, used, err = evm.Create(accounts.ZeroAddress, code, initial, uint256.Int{}, nil, false)
				} else {
					address := accounts.InternAddress(common.HexToAddress("0x1000"))
					require.NoError(t, ibs.SetCode(address, code, tracing.CodeChangeUnspecified))
					_, remaining, used, err = evm.CallCode(accounts.ZeroAddress, address, nil, initial, uint256.Int{})
				}
				require.Error(t, err)
				require.Equal(t, mdgas.MdGas{Execution: initial.Execution - tc.execution, State: initial.State}, remaining)
				require.Equal(t, mdgas.MdGasUsage{Execution: tc.execution}, used)
			})
		}
	}
}

// TestDeriveFrameExecutionGasUsed covers the EIP-8037 cases where the formula
// Execution = (inputTotal − gasRemainingTotal) − stateGasUsed must hold,
// including the refund-heavy case where stateGas grows above the input
// because inline state-gas refunds (refillStateGas) credit the
// frame's local reservoir.
func TestDeriveFrameExecutionGasUsed(t *testing.T) {
	t.Parallel()

	const sgps = 64 * 1530 // params.StateGasPerStorageSet

	cases := []struct {
		name              string
		inputTotal        uint64
		gasRemainingTotal uint64
		stateGasUsed      int64
		want              uint64
	}{
		{
			// Plain frame: 50 execution ops, 30 state charge from a 100-unit
			// reservoir, no spillover.
			//   input = 1000 R + 100 S = 1100
			//   leftover = 950 R + 70 S = 1020
			//   stateGasUsed = 30
			name:              "charges_only_no_spillover",
			inputTotal:        1100,
			gasRemainingTotal: 1020,
			stateGasUsed:      30,
			want:              50,
		},
		{
			// Spillover: 50 execution ops + state charge 200 against a 100-unit
			// reservoir, 100 spills into execution gas.
			//   input = 1000 R + 100 S = 1100
			//   leftover = 850 R + 0 S = 850
			//   stateGasUsed = 200 (full charge, regardless of spillover)
			// Want 50 (ExecutionUsedDirect only — the spilled portion is
			// already represented in stateGasUsed).
			name:              "with_spillover",
			inputTotal:        1100,
			gasRemainingTotal: 850,
			stateGasUsed:      200,
			want:              50,
		},
		{
			// Refunds exceed the frame's own charges — typical for a
			// DELEGATECALL/CALLCODE callee that clears a slot an ancestor
			// set. The refund (sgps) credits the local reservoir directly,
			// pushing gasRemainingTotal above inputTotal.
			//   input = 100 R + 50 S = 150
			//   refund sgps grows leftover state to 50 + sgps
			//   10 execution ops → leftover execution = 90
			//   stateGasUsed = -sgps
			// Want 10 (the execution ops). A guarded uint64 subtraction would
			// see gasRemainingTotal > inputTotal and (wrongly) return 0.
			name:              "refunds_exceed_charges_intermediate_frame",
			inputTotal:        150,
			gasRemainingTotal: 90 + 50 + sgps,
			stateGasUsed:      -sgps,
			want:              10,
		},
		{
			// Pure refund frame, no execution work. Verifies signed cancellation
			// across the entire delta.
			name:              "refunds_only_no_execution_ops",
			inputTotal:        150,
			gasRemainingTotal: 150 + sgps,
			stateGasUsed:      -sgps,
			want:              0,
		},
		{
			// Gas totals are unsigned reservoir balances and are free to
			// occupy the full uint64 range. The helper must stay correct
			// when those totals sit above 2^63 — a naive int64 promotion
			// would flip the sign and break the arithmetic.
			name:              "huge_gas_totals_uint64_safe",
			inputTotal:        math.MaxUint64,
			gasRemainingTotal: math.MaxUint64 - 12345,
			stateGasUsed:      300,
			want:              12345 - 300,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := deriveFrameExecutionGasUsed(tc.inputTotal, tc.gasRemainingTotal, tc.stateGasUsed)
			if got != tc.want {
				t.Fatalf("deriveFrameExecutionGasUsed(input=%d, leftover=%d, state=%d) = %d, want %d",
					tc.inputTotal, tc.gasRemainingTotal, tc.stateGasUsed, got, tc.want)
			}
		})
	}
}

// TestEVMFitsItsSizeClass keeps EVM inside the allocation size class its field
// order was chosen for. The bound is one-sided: shrinking EVM is free, growing
// it past the class is what costs a size class per allocation.
func TestEVMFitsItsSizeClass(t *testing.T) {
	t.Parallel()

	if got := unsafe.Sizeof(EVM{}); got > evmSizeClass {
		t.Fatalf("sizeof(EVM) = %d, above the %d-byte size class: pack the new field into "+
			"existing padding, or raise evmSizeClass knowing every EVM allocation grows", got, evmSizeClass)
	}
}

func TestZeroUnpricedBaseFee(t *testing.T) {
	for _, tc := range []struct {
		name       string
		noBaseFee  bool
		gasPrice   uint64
		wantZeroed bool
	}{
		{name: "unpriced call skipping the fee checks", noBaseFee: true, gasPrice: 0, wantZeroed: true},
		{name: "priced call skipping the fee checks", noBaseFee: true, gasPrice: 3, wantZeroed: false},
		{name: "unpriced call under the fee checks", noBaseFee: false, gasPrice: 0, wantZeroed: false},
		{name: "priced call under the fee checks", noBaseFee: false, gasPrice: 3, wantZeroed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockCtx := evmtypes.BlockContext{BaseFee: *uint256.NewInt(7)}
			txCtx := evmtypes.TxContext{GasPrice: *uint256.NewInt(tc.gasPrice)}

			got := ZeroUnpricedBaseFee(blockCtx, txCtx, Config{NoBaseFee: tc.noBaseFee})

			want := uint256.NewInt(7)
			if tc.wantZeroed {
				want = uint256.NewInt(0)
			}
			require.Equal(t, want, &got.BaseFee)
		})
	}
}
