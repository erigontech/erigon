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

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// TestInclusionContributions pins the EIP-8037 inclusion-contribution
// formula for representative tx shapes. The formula derives the per-dim
// values that callers feed to CheckBlockGasInclusion:
//
//	pre-Amsterdam: (gas, 0)
//	Amsterdam:     regular = max(min(TX_MAX, gas - intrinsic.state), intrinsic.FloorGasCost)
//	               state   = gas - intrinsic.regular
//
// Cross-dimension subtraction lets a tx whose declared gas_limit must
// cover BOTH dimensions (e.g. a creation paying intrinsic.state up front
// or an EIP-7702 tx with per-auth state cost) still fit in either
// reservoir; the FloorGasCost lower bound on regular mirrors block
// accounting's `max(combined.Regular, FloorGasCost)`.
func TestInclusionContributions(t *testing.T) {
	t.Run("pre-Amsterdam returns (gas, 0) regardless of intrinsic", func(t *testing.T) {
		// Pre-Amsterdam only exercises the regular dim. State should be 0
		// even when the intrinsic computation surfaces a non-zero StateGas
		// (which it shouldn't pre-Amsterdam, but the formula must not
		// leak it).
		regular, state := InclusionContributions(
			100_000,
			mdgas.IntrinsicGasCalcResult{RegularGas: 21000, StateGas: 25000, FloorGasCost: 21000},
			false, // isAmsterdam
		)
		require.Equal(t, uint64(100_000), regular)
		require.Equal(t, uint64(0), state)
	})

	t.Run("Amsterdam vanilla transfer (intrinsic.state == 0)", func(t *testing.T) {
		// Vanilla transfer: intrinsic.regular = TxGas, intrinsic.state = 0,
		// FloorGasCost = TxGas. Both contributions collapse to gas - 0 = gas
		// minus the matching intrinsic (regular check subtracts state=0,
		// state check subtracts regular=21000).
		gas := uint64(50_000)
		regular, state := InclusionContributions(
			gas,
			mdgas.IntrinsicGasCalcResult{RegularGas: params.TxGas, StateGas: 0, FloorGasCost: params.TxGas},
			true,
		)
		// regular = max(min(TX_MAX, 50000 - 0), 21000) = 50000
		require.Equal(t, uint64(50_000), regular)
		// state = 50000 - 21000 = 29000
		require.Equal(t, uint64(29_000), state)
	})

	t.Run("Amsterdam contract creation (intrinsic.state > 0)", func(t *testing.T) {
		// Creation: intrinsic.regular = TxGas + CreateGasEIP8037,
		// intrinsic.state = StateGasNewAccount. A tx with gas_limit large
		// enough to cover both dimensions should fit; the regular check
		// strips intrinsic.state (which only the creation path pays) so the
		// resulting regular contribution doesn't double-count it.
		intrinsicRegular := params.TxGas + params.CreateGasEIP8037
		intrinsicState := uint64(params.StateGasNewAccount)
		floor := params.TxGas
		// Pick gas well above (intrinsicState + floor) so the cross-dim
		// subtraction lands above the floor (otherwise the floor case
		// dominates and the cross-dim assertion below is meaningless).
		gas := intrinsicState + floor + 50_000
		regular, state := InclusionContributions(
			gas,
			mdgas.IntrinsicGasCalcResult{RegularGas: intrinsicRegular, StateGas: intrinsicState, FloorGasCost: floor},
			true,
		)
		// regular = max(min(TX_MAX, gas - intrinsicState), floor) with floor losing
		require.Equal(t, gas-intrinsicState, regular)
		// state = gas - intrinsicRegular
		require.Equal(t, gas-intrinsicRegular, state)
		require.Greater(t, regular, floor, "sanity: regular should exceed floor at this gas value")
	})

	t.Run("Amsterdam EIP-7702 tx with multiple authorizations", func(t *testing.T) {
		// EIP-7702 tx with N authorizations: intrinsic.state accumulates
		// N * StateGasNewAccountAndAuth, intrinsic.regular accumulates
		// N * PerAuthBaseCostEIP8037 on top of TxGas. The cross-dimension
		// subtraction matters here: the state check must subtract
		// intrinsic.regular (which includes the per-auth base cost) so a
		// well-sized 7702 tx isn't over-rejected.
		nAuths := uint64(3)
		intrinsicRegular := params.TxGas + nAuths*params.PerAuthBaseCostEIP8037
		intrinsicState := nAuths * uint64(params.StateGasNewAccountAndAuth)
		floor := params.TxGas
		// Same sizing rationale as the creation case: keep gas above
		// (intrinsicState + floor) so neither floor nor TX_MAX dominates.
		gas := intrinsicState + floor + 100_000
		regular, state := InclusionContributions(
			gas,
			mdgas.IntrinsicGasCalcResult{RegularGas: intrinsicRegular, StateGas: intrinsicState, FloorGasCost: floor},
			true,
		)
		require.Equal(t, gas-intrinsicState, regular)
		require.Equal(t, gas-intrinsicRegular, state)
	})

	t.Run("Amsterdam: TX_MAX cap clamps regular contribution", func(t *testing.T) {
		// A tx with gas_limit above MaxTxnGasLimit on the regular dim
		// (after subtracting intrinsic.state) must be capped at TX_MAX.
		// The state contribution is unaffected by the cap.
		gas := params.MaxTxnGasLimit + 1_000_000
		regular, state := InclusionContributions(
			gas,
			mdgas.IntrinsicGasCalcResult{RegularGas: params.TxGas, StateGas: 0, FloorGasCost: params.TxGas},
			true,
		)
		require.Equal(t, params.MaxTxnGasLimit, regular)
		require.Equal(t, gas-params.TxGas, state)
	})

	t.Run("Amsterdam: FloorGasCost lower bound on regular", func(t *testing.T) {
		// When intrinsic.state is large enough that gas - intrinsic.state
		// drops below FloorGasCost, the regular contribution must be
		// raised to FloorGasCost — mirroring block accounting's
		// `max(combined.Regular, FloorGasCost)`. Without this, the
		// inclusion check would under-budget vs realized consumption.
		floor := uint64(40_000)
		intrinsicState := uint64(60_000) // forces gas - state = 30000 < floor
		gas := uint64(90_000)
		regular, state := InclusionContributions(
			gas,
			mdgas.IntrinsicGasCalcResult{RegularGas: params.TxGas, StateGas: intrinsicState, FloorGasCost: floor},
			true,
		)
		require.Equal(t, floor, regular)
		require.Equal(t, gas-params.TxGas, state)
	})

	t.Run("Amsterdam: gas == intrinsic.state — regular floored, not negative", func(t *testing.T) {
		// Edge case: gas exactly equals intrinsic.state. The naive
		// subtraction is 0, then floor kicks in. Verifies no underflow.
		intrinsicState := uint64(50_000)
		floor := uint64(25_000)
		regular, state := InclusionContributions(
			intrinsicState,
			mdgas.IntrinsicGasCalcResult{RegularGas: params.TxGas, StateGas: intrinsicState, FloorGasCost: floor},
			true,
		)
		require.Equal(t, floor, regular)
		require.Equal(t, intrinsicState-params.TxGas, state)
	})

	t.Run("Amsterdam: gas < intrinsic.state — regular stays 0 then floored", func(t *testing.T) {
		// Such a tx would fail the intrinsic check upstream, but the
		// helper must not underflow. Regular falls back to 0, then floor.
		regular, state := InclusionContributions(
			10_000,
			mdgas.IntrinsicGasCalcResult{RegularGas: 5_000, StateGas: 50_000, FloorGasCost: 21_000},
			true,
		)
		require.Equal(t, uint64(21_000), regular, "floor applied even when gas < intrinsic.state")
		require.Equal(t, uint64(5_000), state, "state = 10000 - 5000")
	})

	t.Run("Amsterdam: gas < intrinsic.regular — state stays 0", func(t *testing.T) {
		// Likewise, a gas value below intrinsic.regular must not underflow
		// the state contribution.
		regular, state := InclusionContributions(
			10_000,
			mdgas.IntrinsicGasCalcResult{RegularGas: 50_000, StateGas: 0, FloorGasCost: 5_000},
			true,
		)
		require.Equal(t, uint64(10_000), regular)
		require.Equal(t, uint64(0), state, "no underflow when gas < intrinsic.regular")
	})
}
