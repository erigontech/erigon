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

	"github.com/erigontech/erigon/execution/protocol/params"
)

// TestInclusionContributions pins the EIP-8037 inclusion-contribution formula
// that callers feed to CheckBlockGasInclusion:
//
//	pre-Amsterdam: (gas, 0)
//	Amsterdam:     execution = min(MaxTxnGasLimit, tx.gas), state = tx.gas
//
// The full gas_limit is reserved in both dimensions (EIP-8037 check_transaction
// checks min(TX_MAX_GAS_LIMIT, tx.gas) against the execution reservoir and tx.gas
// against the state reservoir); no intrinsic gas is subtracted.
func TestInclusionContributions(t *testing.T) {
	t.Run("pre-Amsterdam returns (gas, 0)", func(t *testing.T) {
		execution, state := InclusionContributions(100_000, false, false)
		require.Equal(t, uint64(100_000), execution)
		require.Equal(t, uint64(0), state)
	})

	t.Run("Amsterdam reserves the full gas_limit in both dimensions", func(t *testing.T) {
		execution, state := InclusionContributions(50_000, true, false)
		require.Equal(t, uint64(50_000), execution)
		require.Equal(t, uint64(50_000), state)
	})

	t.Run("Amsterdam caps execution at MaxTxnGasLimit, leaves state uncapped", func(t *testing.T) {
		gas := params.MaxTxnGasLimit + 1_000_000
		execution, state := InclusionContributions(gas, true, false)
		require.Equal(t, params.MaxTxnGasLimit, execution)
		require.Equal(t, gas, state)
	})

	t.Run("Amsterdam without the execution gas cap reserves the full gas_limit in both dimensions", func(t *testing.T) {
		gas := params.MaxTxnGasLimit + 1_000_000
		execution, state := InclusionContributions(gas, true, true)
		require.Equal(t, gas, execution)
		require.Equal(t, gas, state)
	})

	t.Run("pre-Amsterdam ignores skipping the execution gas cap", func(t *testing.T) {
		execution, state := InclusionContributions(100_000, false, true)
		require.Equal(t, uint64(100_000), execution)
		require.Equal(t, uint64(0), state)
	})
}

func TestMaxInclusionGas(t *testing.T) {
	limit := params.MaxTxnGasLimit
	tests := []struct {
		name               string
		amsterdam, skipCap bool
		execution, state   uint64
		want               uint64
	}{
		{"pre-Amsterdam", false, false, 10_000_000, 1_000, 10_000_000},
		{"cap on, execution holds a cap", true, false, limit, 30_000_000, 30_000_000},
		{"cap on, execution below a cap", true, false, limit - 1, 30_000_000, limit - 1},
		{"cap on, state binds", true, false, 30_000_000, 2_000_000, 2_000_000},
		{"cap off", true, true, 29_988_000, 30_000_000, 29_988_000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gp := NewBlockGasPool(tc.execution, tc.state, 0)
			got := MaxInclusionGas(gp, tc.amsterdam, tc.skipCap)
			require.Equal(t, tc.want, got)
			// The largest admitted gas limit: one more gas no longer fits.
			e, s := InclusionContributions(got, tc.amsterdam, tc.skipCap)
			require.NoError(t, CheckBlockGasInclusion(gp, e, s, 0))
			e, s = InclusionContributions(got+1, tc.amsterdam, tc.skipCap)
			require.ErrorIs(t, CheckBlockGasInclusion(gp, e, s, 0), ErrGasLimitReached)
		})
	}
}
