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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/common/clonable"
)

type RewardsCore struct{}

type rewardDeltas struct {
	rewards   []uint64
	penalties []uint64
}

func (*rewardDeltas) Clone() clonable.Clonable { return &rewardDeltas{} }

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
	if preState.Version() == clparams.Phase0Version {
		return runPhase0Rewards(root, c, preState)
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

func runPhase0Rewards(root fs.FS, c spectest.TestCase, beaconState *state.CachingBeaconState) error {
	validatorCount := beaconState.ValidatorLength()
	eligible := state.EligibleValidatorsIndicies(beaconState)
	matching := make([][]bool, 3)
	for flagIndex := range matching {
		matching[flagIndex] = make([]bool, validatorCount)
	}
	for validatorIndex := range validatorCount {
		var values [3]bool
		var err error
		values[0], err = beaconState.ValidatorIsPreviousMatchingSourceAttester(validatorIndex)
		if err != nil {
			return err
		}
		values[1], err = beaconState.ValidatorIsPreviousMatchingTargetAttester(validatorIndex)
		if err != nil {
			return err
		}
		values[2], err = beaconState.ValidatorIsPreviousMatchingHeadAttester(validatorIndex)
		if err != nil {
			return err
		}
		validator, err := beaconState.ValidatorForValidatorIndex(validatorIndex)
		if err != nil {
			return err
		}
		if validator.Slashed() {
			continue
		}
		for flagIndex := range values {
			matching[flagIndex][validatorIndex] = values[flagIndex]
		}
	}

	for flagIndex, name := range []string{"source_deltas.ssz_snappy", "target_deltas.ssz_snappy", "head_deltas.ssz_snappy"} {
		have, err := phase0ComponentRewardDeltas(beaconState, eligible, matching[flagIndex])
		if err != nil {
			return err
		}
		if err := compareRewardDeltas(root, c, name, have); err != nil {
			return err
		}
	}

	have, err := phase0InclusionDelayRewardDeltas(beaconState, matching[0])
	if err != nil {
		return err
	}
	if err := compareRewardDeltas(root, c, "inclusion_delay_deltas.ssz_snappy", have); err != nil {
		return err
	}
	have, err = phase0InactivityRewardDeltas(beaconState, eligible, matching[1])
	if err != nil {
		return err
	}
	return compareRewardDeltas(root, c, "inactivity_penalty_deltas.ssz_snappy", have)
}

func phase0ComponentRewardDeltas(beaconState *state.CachingBeaconState, eligible []uint64, matching []bool) (*rewardDeltas, error) {
	validatorCount := beaconState.ValidatorLength()
	deltas := &rewardDeltas{rewards: make([]uint64, validatorCount), penalties: make([]uint64, validatorCount)}
	increment := beaconState.BeaconConfig().EffectiveBalanceIncrement
	attestingBalance := increment
	var sum uint64
	for validatorIndex, isMatching := range matching {
		if !isMatching {
			continue
		}
		effectiveBalance, err := beaconState.ValidatorEffectiveBalance(validatorIndex)
		if err != nil {
			return nil, err
		}
		sum += effectiveBalance
	}
	attestingBalance = max(attestingBalance, sum)
	activeIncrements := beaconState.GetTotalActiveBalance() / increment
	if activeIncrements == 0 {
		return nil, fmt.Errorf("active balance has no effective balance increments")
	}
	for _, validatorIndex := range eligible {
		baseReward, err := beaconState.BaseReward(validatorIndex)
		if err != nil {
			return nil, err
		}
		if matching[validatorIndex] {
			if state.InactivityLeaking(beaconState) {
				deltas.rewards[validatorIndex] = baseReward
			} else {
				deltas.rewards[validatorIndex] = baseReward * (attestingBalance / increment) / activeIncrements
			}
		} else {
			deltas.penalties[validatorIndex] = baseReward
		}
	}
	return deltas, nil
}

func phase0InclusionDelayRewardDeltas(beaconState *state.CachingBeaconState, matchingSource []bool) (*rewardDeltas, error) {
	deltas := &rewardDeltas{rewards: make([]uint64, beaconState.ValidatorLength()), penalties: make([]uint64, beaconState.ValidatorLength())}
	for validatorIndex, isMatching := range matchingSource {
		if !isMatching {
			continue
		}
		attestation, err := beaconState.ValidatorMinPreviousInclusionDelayAttestation(validatorIndex)
		if err != nil {
			return nil, err
		}
		if attestation == nil || attestation.InclusionDelay == 0 || attestation.ProposerIndex >= uint64(beaconState.ValidatorLength()) {
			return nil, fmt.Errorf("invalid inclusion delay attestation for validator %d", validatorIndex)
		}
		baseReward, err := beaconState.BaseReward(uint64(validatorIndex))
		if err != nil {
			return nil, err
		}
		proposerReward := baseReward / beaconState.BeaconConfig().ProposerRewardQuotient
		deltas.rewards[attestation.ProposerIndex] += proposerReward
		deltas.rewards[validatorIndex] += (baseReward - proposerReward) / attestation.InclusionDelay
	}
	return deltas, nil
}

func phase0InactivityRewardDeltas(beaconState *state.CachingBeaconState, eligible []uint64, matchingTarget []bool) (*rewardDeltas, error) {
	deltas := &rewardDeltas{rewards: make([]uint64, beaconState.ValidatorLength()), penalties: make([]uint64, beaconState.ValidatorLength())}
	if !state.InactivityLeaking(beaconState) {
		return deltas, nil
	}
	for _, validatorIndex := range eligible {
		baseReward, err := beaconState.BaseReward(validatorIndex)
		if err != nil {
			return nil, err
		}
		proposerReward := baseReward / beaconState.BeaconConfig().ProposerRewardQuotient
		deltas.penalties[validatorIndex] = beaconState.BeaconConfig().BaseRewardsPerEpoch*baseReward - proposerReward
		validator, err := beaconState.ValidatorForValidatorIndex(int(validatorIndex))
		if err != nil {
			return nil, err
		}
		if validator.Slashed() || !matchingTarget[validatorIndex] {
			deltas.penalties[validatorIndex] += validator.EffectiveBalance() * state.FinalityDelay(beaconState) / beaconState.BeaconConfig().InactivityPenaltyQuotient
		}
	}
	return deltas, nil
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
