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
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

func TestProcessRewardsAndPenaltiesWritesBalancesSeriallyInEligibleOrder(t *testing.T) {
	s := newRewardsTestState(t, 4096)
	eligible := state.EligibleValidatorsIndicies(s)
	require.Less(t, len(eligible), s.ValidatorLength())
	flags := GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())

	var mu sync.Mutex
	written := make([]uint64, 0, len(eligible))
	s.SetEvents(raw.Events{
		OnNewValidatorBalance: func(index int, _ uint64) error {
			runtime.Gosched()
			mu.Lock()
			written = append(written, uint64(index))
			mu.Unlock()
			return nil
		},
	})

	require.NoError(t, ProcessRewardsAndPenalties(s, eligible, flags))
	require.Equal(t, eligible, written)
}

func TestProcessRewardsAndPenaltiesComputeErrorWritesNoBalance(t *testing.T) {
	s := newRewardsTestState(t, 4096)
	eligible := state.EligibleValidatorsIndicies(s)
	flags := GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	valid := uint64(1)
	require.Contains(t, eligible, valid)

	writes := 0
	s.SetEvents(raw.Events{
		OnNewValidatorBalance: func(_ int, _ uint64) error {
			writes++
			return nil
		},
	})
	before, err := s.ValidatorBalance(int(valid))
	require.NoError(t, err)
	require.Error(t, ProcessRewardsAndPenalties(s, []uint64{valid, uint64(s.ValidatorLength())}, flags))
	after, err := s.ValidatorBalance(int(valid))
	require.NoError(t, err)
	require.Zero(t, writes)
	require.Equal(t, before, after)
}

// The spec applies each flag's and the inactivity rewards and penalties in turn
// and clamps the balance at zero after each penalty, so a reward that follows a
// clamped penalty is not absorbed by it.
func TestProcessRewardsAndPenaltiesClampsEachDeltaSet(t *testing.T) {
	s := newRewardsTestState(t, 4096)
	cfg := s.BeaconConfig()
	const index = 2
	require.NoError(t, s.SetValidatorBalance(index, 1))
	eligible := state.EligibleValidatorsIndicies(s)
	require.Contains(t, eligible, uint64(index))
	flags := GetUnslashedIndiciesSet(cfg, state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	require.False(t, flags[cfg.TimelySourceFlagIndex][index])
	require.True(t, flags[cfg.TimelyTargetFlagIndex][index])
	require.False(t, flags[cfg.TimelyHeadFlagIndex][index])
	require.False(t, state.InactivityLeaking(s))

	baseReward, err := s.BaseReward(index)
	require.NoError(t, err)
	totals, err := getFlagsTotalBalances(s, flags)
	require.NoError(t, err)
	weights := cfg.ParticipationWeights()
	inc := cfg.EffectiveBalanceIncrement
	sourcePenalty := baseReward * weights[cfg.TimelySourceFlagIndex] / cfg.WeightDenominator
	targetReward := baseReward * weights[cfg.TimelyTargetFlagIndex] * (totals[cfg.TimelyTargetFlagIndex] / inc) /
		(s.GetTotalActiveBalance() / inc * cfg.WeightDenominator)
	require.Positive(t, targetReward)
	require.Greater(t, sourcePenalty, 1+targetReward)

	require.NoError(t, ProcessRewardsAndPenalties(s, eligible, flags))
	balance, err := s.ValidatorBalance(index)
	require.NoError(t, err)
	require.Equal(t, targetReward, balance)
}

func newRewardsTestState(t testing.TB, validatorCount int) *state.CachingBeaconState {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)
	require.NoError(t, utils.DecodeSSZSnappy(s, startingEffectiveBalancesState, int(clparams.BellatrixVersion)))
	for i := s.ValidatorLength(); i < validatorCount; i++ {
		var publicKey [48]byte
		binary.LittleEndian.PutUint64(publicKey[:8], uint64(i+1))
		validator := solid.NewValidatorFromParameters(
			publicKey,
			[32]byte{},
			cfg.MinActivationBalance,
			i%17 == 0,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		)
		require.NoError(t, s.AddValidator(validator, cfg.MinActivationBalance+uint64(i%3)*cfg.EffectiveBalanceIncrement))
		s.AddPreviousEpochParticipationFlags(0)
		s.AddCurrentEpochParticipationFlags(0)
		s.AddInactivityScore(0)
	}
	require.NoError(t, s.SetSlot(2*cfg.SlotsPerEpoch))

	allFlags := cltypes.ParticipationFlags(0)
	for flagIndex := range cfg.ParticipationWeights() {
		allFlags = allFlags.Add(flagIndex)
	}
	for i := range validatorCount {
		var flags cltypes.ParticipationFlags
		switch i % 4 {
		case 0:
			flags = allFlags
		case 1:
			flags = flags.Add(int(cfg.TimelySourceFlagIndex))
		case 2:
			flags = flags.Add(int(cfg.TimelyTargetFlagIndex))
		}
		require.NoError(t, s.SetEffectiveBalanceForValidatorAtIndex(i, cfg.MinActivationBalance))
		require.NoError(t, s.SetValidatorBalance(i, cfg.MinActivationBalance+uint64(i%3)*cfg.EffectiveBalanceIncrement))
		activationEpoch := uint64(0)
		exitEpoch := cfg.FarFutureEpoch
		withdrawableEpoch := cfg.FarFutureEpoch
		if i%13 == 0 {
			activationEpoch = state.Epoch(s) + 1
		} else if i%19 == 0 {
			exitEpoch = 0
			withdrawableEpoch = 0
		}
		require.NoError(t, s.SetActivationEpochForValidatorAtIndex(i, activationEpoch))
		require.NoError(t, s.SetExitEpochForValidatorAtIndex(i, exitEpoch))
		require.NoError(t, s.SetWithdrawableEpochForValidatorAtIndex(i, withdrawableEpoch))
		require.NoError(t, s.SetValidatorSlashed(i, i%17 == 0))
		s.SetEpochParticipationForValidatorIndex(false, i, flags)
		if i%4 == 3 {
			require.NoError(t, s.SetValidatorInactivityScore(i, uint64(i%11+1)))
		}
	}
	require.NoError(t, s.InitBeaconState())
	return s
}
