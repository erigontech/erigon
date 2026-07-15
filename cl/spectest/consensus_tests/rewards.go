// Copyright 2024 The Erigon Authors
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

package consensus_tests

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
)

type RewardsCore struct{}

type rewardDeltas struct {
	rewards   []uint64
	penalties []uint64
}

func (d *rewardDeltas) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < 8 {
		return fmt.Errorf("reward deltas are too short: %d", len(buf))
	}
	rewardsOffset := int(binary.LittleEndian.Uint32(buf))
	penaltiesOffset := int(binary.LittleEndian.Uint32(buf[4:]))
	if rewardsOffset != 8 || penaltiesOffset < rewardsOffset || penaltiesOffset > len(buf) {
		return fmt.Errorf("invalid reward delta offsets: %d, %d", rewardsOffset, penaltiesOffset)
	}
	rewardsBytes, penaltiesBytes := buf[rewardsOffset:penaltiesOffset], buf[penaltiesOffset:]
	if len(rewardsBytes)%8 != 0 || len(penaltiesBytes)%8 != 0 {
		return fmt.Errorf("invalid reward delta lengths: %d, %d", len(rewardsBytes), len(penaltiesBytes))
	}
	d.rewards = decodeUint64s(rewardsBytes)
	d.penalties = decodeUint64s(penaltiesBytes)
	return nil
}

func decodeUint64s(buf []byte) []uint64 {
	values := make([]uint64, len(buf)/8)
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(buf[i*8:])
	}
	return values
}

func (b *RewardsCore) Run(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	if err != nil {
		return err
	}
	validatorCount := preState.ValidatorLength()
	eligible := state.EligibleValidatorsIndicies(preState)
	participation := statechange.GetUnslashedIndiciesSet(
		preState.BeaconConfig(),
		state.PreviousEpoch(preState),
		preState.ValidatorSet(),
		preState.PreviousEpochParticipation(),
	)
	weights := preState.BeaconConfig().ParticipationWeights()
	activeIncrements := preState.GetTotalActiveBalance() / preState.BeaconConfig().EffectiveBalanceIncrement
	if activeIncrements == 0 {
		return fmt.Errorf("active balance has no effective balance increments")
	}

	participatingIncrements := make([]uint64, len(weights))
	for flagIndex := range weights {
		for validatorIndex := range validatorCount {
			if !participation[flagIndex][validatorIndex] {
				continue
			}
			effectiveBalance, err := preState.ValidatorEffectiveBalance(validatorIndex)
			if err != nil {
				return err
			}
			participatingIncrements[flagIndex] += effectiveBalance / preState.BeaconConfig().EffectiveBalanceIncrement
		}
	}

	for flagIndex, name := range []string{"source_deltas.ssz_snappy", "target_deltas.ssz_snappy", "head_deltas.ssz_snappy"} {
		have, err := flagRewardDeltas(preState, validatorCount, eligible, participation[flagIndex], weights[flagIndex], participatingIncrements[flagIndex], activeIncrements, flagIndex)
		if err != nil {
			return err
		}
		if err := compareRewardDeltas(root, c, name, have); err != nil {
			return err
		}
	}

	have, err := inactivityRewardDeltas(preState, validatorCount, eligible, participation[preState.BeaconConfig().TimelyTargetFlagIndex])
	if err != nil {
		return err
	}
	return compareRewardDeltas(root, c, "inactivity_penalty_deltas.ssz_snappy", have)
}

func flagRewardDeltas(
	beaconState *state.CachingBeaconState,
	validatorCount int,
	eligible []uint64,
	participating []bool,
	weight uint64,
	participatingIncrements uint64,
	activeIncrements uint64,
	flagIndex int,
) (*rewardDeltas, error) {
	deltas := &rewardDeltas{rewards: make([]uint64, validatorCount), penalties: make([]uint64, validatorCount)}
	denominator := activeIncrements * beaconState.BeaconConfig().WeightDenominator
	for _, validatorIndex := range eligible {
		baseReward, err := beaconState.BaseReward(validatorIndex)
		if err != nil {
			return nil, err
		}
		if participating[validatorIndex] {
			if !state.InactivityLeaking(beaconState) {
				deltas.rewards[validatorIndex] = baseReward * weight * participatingIncrements / denominator
			}
		} else if flagIndex != int(beaconState.BeaconConfig().TimelyHeadFlagIndex) {
			deltas.penalties[validatorIndex] = baseReward * weight / beaconState.BeaconConfig().WeightDenominator
		}
	}
	return deltas, nil
}

func inactivityRewardDeltas(beaconState *state.CachingBeaconState, validatorCount int, eligible []uint64, timelyTarget []bool) (*rewardDeltas, error) {
	deltas := &rewardDeltas{rewards: make([]uint64, validatorCount), penalties: make([]uint64, validatorCount)}
	denominator := beaconState.BeaconConfig().InactivityScoreBias * beaconState.BeaconConfig().GetPenaltyQuotient(beaconState.Version())
	if denominator == 0 {
		return nil, fmt.Errorf("inactivity penalty denominator is zero")
	}
	for _, validatorIndex := range eligible {
		if timelyTarget[validatorIndex] {
			continue
		}
		effectiveBalance, err := beaconState.ValidatorEffectiveBalance(int(validatorIndex))
		if err != nil {
			return nil, err
		}
		inactivityScore, err := beaconState.ValidatorInactivityScore(int(validatorIndex))
		if err != nil {
			return nil, err
		}
		deltas.penalties[validatorIndex] = effectiveBalance * inactivityScore / denominator
	}
	return deltas, nil
}

func compareRewardDeltas(root fs.FS, c spectest.TestCase, name string, have *rewardDeltas) error {
	want := &rewardDeltas{}
	if err := spectest.ReadSsz(root, c.Version(), name, want); err != nil {
		return err
	}
	if !slices.Equal(want.rewards, have.rewards) || !slices.Equal(want.penalties, have.penalties) {
		return fmt.Errorf("%s mismatch", name)
	}
	return nil
}
