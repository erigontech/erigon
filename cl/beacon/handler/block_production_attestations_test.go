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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

var testParticipationWeights = []uint64{14, 26, 14}

func newTestCandidate(slot uint64, currentEpoch bool, flags uint8, validators ...uint64) attestationCandidate {
	candidate := attestationCandidate{
		attestation:  &solid.Attestation{Data: &solid.AttestationData{Slot: slot}},
		currentEpoch: currentEpoch,
	}
	for _, v := range validators {
		candidate.attesters = append(candidate.attesters, v)
		candidate.baseRewards = append(candidate.baseRewards, 1)
		candidate.newFlags = append(candidate.newFlags, flags)
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

const (
	sourceFlag = 1 << 0
	allFlags   = 0b111
)

func TestSelectAttestationsSkipsVotesAlreadyCovered(t *testing.T) {
	wide := newTestCandidate(1, true, sourceFlag, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	overlapping := newTestCandidate(2, true, sourceFlag, 0, 1, 2, 3, 4, 5, 6, 7, 8)
	disjoint := newTestCandidate(3, true, sourceFlag, 10, 11)

	selected := selectAttestations([]attestationCandidate{wide, overlapping, disjoint}, testParticipationWeights, 1, 2)

	require.Equal(t, []uint64{1, 3}, selectedSlots(selected))
}

func TestSelectAttestationsDropsCandidatesWithoutNewReward(t *testing.T) {
	wide := newTestCandidate(1, true, sourceFlag, 0, 1, 2, 3)
	subset := newTestCandidate(2, true, sourceFlag, 0, 1)

	selected := selectAttestations([]attestationCandidate{wide, subset}, testParticipationWeights, 1, 8)

	require.Equal(t, []uint64{1}, selectedSlots(selected))
}

func TestSelectAttestationsCountsNewFlagsOfCoveredValidators(t *testing.T) {
	// sourceAndTarget wins first; afterwards only the head flags of allFlagsSameValidators
	// are new, and they are still worth more than otherValidators.
	sourceAndTarget := newTestCandidate(1, true, 0b011, 0, 1, 2, 3, 4, 5)
	allFlagsSameValidators := newTestCandidate(2, true, allFlags, 0, 1, 2)
	otherValidators := newTestCandidate(3, true, sourceFlag, 7)

	selected := selectAttestations([]attestationCandidate{sourceAndTarget, allFlagsSameValidators, otherValidators}, testParticipationWeights, 1, 2)

	require.Equal(t, []uint64{1, 2}, selectedSlots(selected))
}

func TestSelectAttestationsTracksEpochsSeparately(t *testing.T) {
	previousEpoch := newTestCandidate(1, false, allFlags, 0, 1)
	currentEpoch := newTestCandidate(2, true, allFlags, 0, 1)

	selected := selectAttestations([]attestationCandidate{previousEpoch, currentEpoch}, testParticipationWeights, 1, 8)

	require.ElementsMatch(t, []uint64{1, 2}, selectedSlots(selected))
}

func TestSelectAttestationsDropsCandidatesWorthLessThanOneGwei(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	worthless := newTestCandidate(1, true, sourceFlag, 1)
	valuable := newTestCandidate(2, true, sourceFlag, 2)
	valuable.baseRewards[0] = proposerRewardDenominator(&cfg)

	selected := selectAttestations([]attestationCandidate{worthless, valuable}, cfg.ParticipationWeights(), proposerRewardDenominator(&cfg), 8)

	require.Equal(t, []uint64{2}, selectedSlots(selected))
}

func TestSelectAttestationsBreaksTiesIndependentlyOfInputOrder(t *testing.T) {
	first := newTestCandidate(1, true, sourceFlag, 1)
	first.attestation.Signature = [96]byte{2}
	second := newTestCandidate(2, true, sourceFlag, 2)
	second.attestation.Signature = [96]byte{1}

	forward := selectAttestations([]attestationCandidate{first, second}, testParticipationWeights, 1, 1)
	backward := selectAttestations([]attestationCandidate{second, first}, testParticipationWeights, 1, 1)

	require.Equal(t, selectedSlots(forward), selectedSlots(backward))
}

func TestSelectAttestationsKeepsEpochFlagsApartForAnyFlagCount(t *testing.T) {
	weights := []uint64{1, 1, 1, 1, 1}
	previousEpochFlag4 := newTestCandidate(1, false, 1<<4, 0)
	currentEpochFlag0 := newTestCandidate(2, true, 1<<0, 0)

	selected := selectAttestations([]attestationCandidate{previousEpochFlag4, currentEpochFlag0}, weights, 1, 8)

	require.ElementsMatch(t, []uint64{1, 2}, selectedSlots(selected))
}
