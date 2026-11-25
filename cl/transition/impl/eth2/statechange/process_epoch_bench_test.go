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

package statechange

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func BenchmarkEpochProcessing(b *testing.B) {
	for _, validators := range []int{200_000, 2_400_000} {
		for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
			for _, pattern := range effectiveBalanceWritePatterns {
				b.Run(fmt.Sprintf("%dk/%s/%s", validators/1000, version, pattern.name), func(b *testing.B) {
					benchmarkEpochProcessing(b, validators, version, pattern.indexes(validators))
				})
			}
		}
	}
}

// Validators at these indexes drop below the hysteresis band, so each epoch
// boundary rewrites their effective balance.
var effectiveBalanceWritePatterns = []struct {
	name    string
	indexes func(validators int) []int
}{
	{"stride32", func(validators int) []int {
		var indexes []int
		for i := 0; i < validators; i += 32 {
			indexes = append(indexes, i)
		}
		return indexes
	}},
	// Mainnet writes about 29 validator records per epoch boundary, in a few
	// clusters of consecutive indexes.
	{"mainnet", func(validators int) []int {
		var indexes []int
		for cluster, size := range []int{4, 4, 4, 4, 3, 3, 3, 4} {
			first := cluster*validators/8/256*256 + 10
			for i := range size {
				indexes = append(indexes, first+i)
			}
		}
		return indexes
	}},
}

func benchmarkEpochProcessing(b *testing.B, validators int, version clparams.StateVersion, writes []int) {
	base := newRewardsTestState(b, validators)
	upgradeTo(b, base, version)
	cfg := base.BeaconConfig()
	for _, i := range writes {
		require.NoError(b, base.SetValidatorBalance(i, cfg.MinActivationBalance-2*cfg.EffectiveBalanceIncrement))
	}
	_, err := base.HashSSZ()
	require.NoError(b, err)
	requireEffectiveBalanceWrites(b, base, validators, len(writes))
	eligible := state.EligibleValidatorsIndicies(base)
	flags := GetUnslashedIndiciesSet(base.BeaconConfig(), state.PreviousEpoch(base), base.ValidatorSet(), base.PreviousEpochParticipation())

	b.Run("RewardsAndPenalties", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			working, err := base.Copy()
			require.NoError(b, err)
			_, err = working.HashSSZ()
			require.NoError(b, err)
			b.StartTimer()
			require.NoError(b, ProcessRewardsAndPenalties(working, eligible, flags))
		}
	})

	b.Run("EffectiveBalanceUpdatesAndHashSSZ", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			working, err := base.Copy()
			require.NoError(b, err)
			_, err = working.HashSSZ()
			require.NoError(b, err)
			b.StartTimer()
			require.NoError(b, ProcessEffectiveBalanceUpdates(working))
			_, err = working.HashSSZ()
			require.NoError(b, err)
		}
	})

	b.Run("ProcessEpochAndHashSSZ", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			working, err := base.Copy()
			require.NoError(b, err)
			cfg := working.BeaconConfig()
			require.NoError(b, working.SetSlot(3*cfg.SlotsPerEpoch))
			working.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: state.PreviousEpoch(working)})
			require.NoError(b, working.InitBeaconState())
			_, err = working.HashSSZ()
			require.NoError(b, err)
			b.StartTimer()
			require.NoError(b, ProcessEpoch(working))
			_, err = working.HashSSZ()
			require.NoError(b, err)
		}
	})
}

func requireEffectiveBalanceWrites(b *testing.B, base *state.CachingBeaconState, validators, want int) {
	b.Helper()
	working, err := base.Copy()
	require.NoError(b, err)
	require.NoError(b, ProcessEffectiveBalanceUpdates(working))
	written := 0
	for i := range validators {
		before, err := base.ValidatorEffectiveBalance(i)
		require.NoError(b, err)
		after, err := working.ValidatorEffectiveBalance(i)
		require.NoError(b, err)
		if before != after {
			written++
		}
	}
	require.Equal(b, want, written)
}
