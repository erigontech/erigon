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

package state

import (
	"math"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestCopyWithTotalActiveBalanceMatchesRescannedCopy(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := New(&cfg)
	source.SetVersion(clparams.AltairVersion)

	for index, balance := range []uint64{32_000_000_000, 16_000_000_000} {
		validator := activeValidatorWithIndex(uint64(index))
		validator.SetEffectiveBalance(balance)
		require.NoError(t, source.AddValidator(validator, balance))
	}
	inactive := activeValidatorWithIndex(2)
	inactive.SetActivationEpoch(math.MaxUint64)
	require.NoError(t, source.AddValidator(inactive, 32_000_000_000))
	require.NoError(t, source.InitBeaconState())

	rescanned, err := source.Copy()
	require.NoError(t, err)
	rescannedTotal := rescanned.GetTotalActiveBalance()
	seeded, err := source.CopyWithTotalActiveBalance(rescannedTotal)
	require.NoError(t, err)
	require.Equal(t, rescannedTotal, seeded.GetTotalActiveBalance())
	require.Equal(t, rescanned.BaseRewardPerIncrement(), seeded.BaseRewardPerIncrement())

	clamped, err := source.CopyWithTotalActiveBalance(cfg.EffectiveBalanceIncrement - 1)
	require.NoError(t, err)
	require.Equal(t, cfg.EffectiveBalanceIncrement, clamped.GetTotalActiveBalance())
}

func TestGloasCopyIntoReusedSpareMatchesFreshDecode(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := gloasCopyTestState(t, &cfg, 86, 1)
	spare := gloasCopyTestState(t, &cfg, 22, 1_000)

	sourceRoot, err := source.HashSSZ()
	require.NoError(t, err)
	_, err = spare.HashSSZ()
	require.NoError(t, err)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	fresh := New(&cfg)
	require.NoError(t, fresh.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	freshRoot, err := fresh.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, source.CopyInto(spare))
	spareRoot, err := spare.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, spareRoot)
	require.Equal(t, freshRoot, spareRoot)

	require.NoError(t, spare.AddValidator(activeValidatorWithIndex(2_000), 32_000_000_000))
	spare.SetEpochParticipationForValidatorIndex(true, 0, 3)
	mutatedSpareRoot, err := spare.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, spareRoot, mutatedSpareRoot)
	unchangedSourceRoot, err := source.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, unchangedSourceRoot)
}

func gloasCopyTestState(t *testing.T, cfg *clparams.BeaconChainConfig, count int, firstIndex uint64) *CachingBeaconState {
	t.Helper()
	beaconState := New(cfg)
	beaconState.SetVersion(clparams.GloasVersion)
	for index := range count {
		require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(firstIndex+uint64(index)), 32_000_000_000))
	}
	beaconState.SetInactivityScores(make([]uint64, count))
	previousParticipation := solid.NewParticipationBitList(count, int(cfg.ValidatorRegistryLimit))
	currentParticipation := solid.NewParticipationBitList(count, int(cfg.ValidatorRegistryLimit))
	for index := range count {
		previousParticipation.Set(index, byte(index%8))
		currentParticipation.Set(index, byte((index+1)%8))
	}
	beaconState.SetPreviousEpochParticipation(previousParticipation)
	beaconState.SetCurrentEpochParticipation(currentParticipation)
	require.NoError(t, beaconState.InitBeaconState())
	return beaconState
}
