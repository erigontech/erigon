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

import "testing"

// TestDeriveFrameRegularGasUsed covers the EIP-8037 cases where the formula
// Regular = (inputTotal − gasRemainingTotal) − stateGasUsed must hold,
// including the refund-heavy case where stateGas grows above the input
// because inline state-gas refunds (creditStateGasRefund) credit the
// frame's local reservoir.
func TestDeriveFrameRegularGasUsed(t *testing.T) {
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
			// Plain frame: 50 regular ops, 30 state charge from a 100-unit
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
			// Spillover: 50 regular ops + state charge 200 against a 100-unit
			// reservoir, 100 spills into regular gas.
			//   input = 1000 R + 100 S = 1100
			//   leftover = 850 R + 0 S = 850
			//   stateGasUsed = 200 (full charge, regardless of spillover)
			// Want 50 (RegularUsedDirect only — the spilled portion is
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
			//   10 regular ops → leftover regular = 90
			//   stateGasUsed = -sgps
			// Want 10 (the regular ops). A guarded uint64 subtraction would
			// see gasRemainingTotal > inputTotal and (wrongly) return 0.
			name:              "refunds_exceed_charges_intermediate_frame",
			inputTotal:        150,
			gasRemainingTotal: 90 + 50 + sgps,
			stateGasUsed:      -sgps,
			want:              10,
		},
		{
			// Pure refund frame, no regular work. Verifies signed cancellation
			// across the entire delta.
			name:              "refunds_only_no_regular_ops",
			inputTotal:        150,
			gasRemainingTotal: 150 + sgps,
			stateGasUsed:      -sgps,
			want:              0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := deriveFrameRegularGasUsed(tc.inputTotal, tc.gasRemainingTotal, tc.stateGasUsed)
			if got != tc.want {
				t.Fatalf("deriveFrameRegularGasUsed(input=%d, leftover=%d, state=%d) = %d, want %d",
					tc.inputTotal, tc.gasRemainingTotal, tc.stateGasUsed, got, tc.want)
			}
		})
	}
}
