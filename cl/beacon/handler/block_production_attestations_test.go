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

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func flagRewards(flagIndex uint8, numerator uint64, validators ...uint64) []attesterFlagReward {
	rewards := make([]attesterFlagReward, 0, len(validators))
	for _, v := range validators {
		rewards = append(rewards, attesterFlagReward{
			attesterFlag: attesterFlag{validatorIndex: v, flagIndex: flagIndex, currentEpoch: true},
			numerator:    numerator,
		})
	}
	return rewards
}

func newTestCandidate(slot uint64, rewards ...[]attesterFlagReward) attestationCandidate {
	candidate := attestationCandidate{attestation: &solid.Attestation{Data: &solid.AttestationData{Slot: slot}}}
	for _, r := range rewards {
		candidate.rewards = append(candidate.rewards, r...)
	}
	return candidate
}

func selectedSlots(atts []*solid.Attestation) []uint64 {
	slots := make([]uint64, 0, len(atts))
	for _, att := range atts {
		slots = append(slots, att.Data.Slot)
	}
	return slots
}

func TestSelectAttestationsSkipsVotesAlreadyCovered(t *testing.T) {
	wide := newTestCandidate(1, flagRewards(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
	overlapping := newTestCandidate(2, flagRewards(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8))
	disjoint := newTestCandidate(3, flagRewards(0, 10, 10, 11))

	selected := selectAttestations([]attestationCandidate{wide, overlapping, disjoint}, 1, 2)

	require.Equal(t, []uint64{1, 3}, selectedSlots(selected))
}

func TestSelectAttestationsDropsCandidatesWithoutNewReward(t *testing.T) {
	wide := newTestCandidate(1, flagRewards(0, 10, 0, 1, 2, 3))
	subset := newTestCandidate(2, flagRewards(0, 10, 0, 1))

	selected := selectAttestations([]attestationCandidate{wide, subset}, 1, 8)

	require.Equal(t, []uint64{1}, selectedSlots(selected))
}

func TestSelectAttestationsCountsNewFlagsOfCoveredValidators(t *testing.T) {
	sourceAndTarget := newTestCandidate(1, flagRewards(0, 14, 0, 1, 2), flagRewards(1, 26, 0, 1, 2))
	headOnlySameValidators := newTestCandidate(2, flagRewards(0, 14, 0, 1, 2), flagRewards(1, 26, 0, 1, 2), flagRewards(2, 14, 0, 1, 2))
	otherValidators := newTestCandidate(3, flagRewards(0, 14, 7))

	selected := selectAttestations([]attestationCandidate{sourceAndTarget, headOnlySameValidators, otherValidators}, 1, 2)

	require.Equal(t, []uint64{2, 3}, selectedSlots(selected))
}
