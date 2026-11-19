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
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func BenchmarkEpochProcessing(b *testing.B) {
	base := newRewardsTestState(b, 200_000)
	upgradeTo(b, base, clparams.FuluVersion)
	_, err := base.HashSSZ()
	require.NoError(b, err)
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
}
