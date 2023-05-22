package transition

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/metrics/methelp"
	"golang.org/x/exp/slices"
)

func ProcessAttestations(s *state2.BeaconState, attestations *cltypes.AttestationList, fullValidation bool) error {
	attestingIndiciesSet := make([][]uint64, attestations.Len())
	h := methelp.NewHistTimer("beacon_process_attestations")
	baseRewardPerIncrement := s.BaseRewardPerIncrement()

	c := h.Tag("attestation_step", "process")
	var err error
	attestations.ForEach(func(a *solid.Attestation, idx, total int) bool {
		if attestingIndiciesSet[idx], err = processAttestation(s, a, baseRewardPerIncrement); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	var valid bool
	c.PutSince()
	if fullValidation {
		c = h.Tag("attestation_step", "validate")
		valid, err = verifyAttestations(s, attestations, attestingIndiciesSet)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessAttestation: wrong bls data")
		}
		c.PutSince()
	}

	return nil
}

func processAttestationPostAltair(s *state2.BeaconState, attestation *solid.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.AttestantionData()
	currentEpoch := state2.Epoch(s.BeaconState)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	h := methelp.NewHistTimer("beacon_process_attestation_post_altair")

	c := h.Tag("step", "get_participation_flag")
	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(attestation.AttestantionData(), stateSlot-data.Slot())
	if err != nil {
		return nil, err
	}
	c.PutSince()

	c = h.Tag("step", "get_attesting_indices")

	attestingIndicies, err := s.GetAttestingIndicies(attestation.AttestantionData(), attestation.AggregationBits(), true)
	if err != nil {
		return nil, err
	}

	c.PutSince()

	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target().Epoch() == currentEpoch

	c = h.Tag("step", "update_attestation")
	for _, attesterIndex := range attestingIndicies {
		val, err := s.ValidatorEffectiveBalance(int(attesterIndex))
		if err != nil {
			return nil, err
		}

		baseReward := (val / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			flagParticipation := s.EpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex))
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || flagParticipation.HasFlag(flagIndex) {
				continue
			}
			s.SetEpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex), flagParticipation.Add(flagIndex))
			proposerRewardNumerator += baseReward * weight
		}
	}
	c.PutSince()
	// Reward proposer
	c = h.Tag("step", "get_proposer_index")
	proposer, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	c.PutSince()
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return attestingIndicies, state2.IncreaseBalance(s.BeaconState, proposer, reward)
}

// processAttestationsPhase0 implements the rules for phase0 processing.
func processAttestationPhase0(s *state2.BeaconState, attestation *solid.Attestation) ([]uint64, error) {
	data := attestation.AttestantionData()
	committee, err := s.GetBeaconCommitee(data.Slot(), data.ValidatorIndex())
	if err != nil {
		return nil, err
	}

	if len(committee) != utils.GetBitlistLength(attestation.AggregationBits()) {
		return nil, fmt.Errorf("processAttestationPhase0: mismatching aggregation bits size")
	}
	// Cached so it is performant.
	proposerIndex, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	// Create the attestation to add to pending attestations
	pendingAttestation := &cltypes.PendingAttestation{
		Data:            data,
		AggregationBits: attestation.AggregationBits(),
		InclusionDelay:  s.Slot() - data.Slot(),
		ProposerIndex:   proposerIndex,
	}
	isCurrentAttestation := data.Target().Epoch() == state2.Epoch(s.BeaconState)
	// Depending of what slot we are on we put in either the current justified or previous justified.
	if isCurrentAttestation {
		if !data.Source().Equal(s.CurrentJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddCurrentEpochAtteastation(pendingAttestation)
	} else {
		if !data.Source().Equal(s.PreviousJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddPreviousEpochAttestation(pendingAttestation)
	}
	// Not required by specs but needed if we want performant epoch transition.
	indicies, err := s.GetAttestingIndicies(attestation.AttestantionData(), attestation.AggregationBits(), true)
	if err != nil {
		return nil, err
	}
	epochRoot, err := state2.GetBlockRoot(s.BeaconState, attestation.AttestantionData().Target().Epoch())
	if err != nil {
		return nil, err
	}
	slotRoot, err := s.GetBlockRootAtSlot(attestation.AttestantionData().Slot())
	if err != nil {
		return nil, err
	}
	// Basically we flag all validators we are currently attesting. will be important for rewards/finalization processing.
	for _, index := range indicies {
		validator, err := s.ValidatorForValidatorIndex(int(index))
		if err != nil {
			return nil, err
		}
		// NOTE: does not affect state root.
		// We need to set it to currents or previouses depending on which attestation we process.
		if isCurrentAttestation {
			if validator.MinCurrentInclusionDelayAttestation == nil || validator.MinCurrentInclusionDelayAttestation.InclusionDelay > pendingAttestation.InclusionDelay {
				validator.MinCurrentInclusionDelayAttestation = pendingAttestation
			}
			validator.IsCurrentMatchingSourceAttester = true
			if attestation.AttestantionData().Target().BlockRoot() == epochRoot {
				validator.IsCurrentMatchingTargetAttester = true
			} else {
				continue
			}
			if attestation.AttestantionData().BeaconBlockRoot() == slotRoot {
				validator.IsCurrentMatchingHeadAttester = true
			}
		} else {
			if validator.MinPreviousInclusionDelayAttestation == nil || validator.MinPreviousInclusionDelayAttestation.InclusionDelay > pendingAttestation.InclusionDelay {
				validator.MinPreviousInclusionDelayAttestation = pendingAttestation
			}
			validator.IsPreviousMatchingSourceAttester = true
			if attestation.AttestantionData().Target().BlockRoot() != epochRoot {
				continue
			}
			validator.IsPreviousMatchingTargetAttester = true
			if attestation.AttestantionData().BeaconBlockRoot() == slotRoot {
				validator.IsPreviousMatchingHeadAttester = true
			}
		}
	}
	return indicies, nil
}

// ProcessAttestation takes an attestation and process it.
func processAttestation(s *state2.BeaconState, attestation *solid.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.AttestantionData()
	currentEpoch := state2.Epoch(s.BeaconState)
	previousEpoch := state2.PreviousEpoch(s.BeaconState)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()
	// Prelimary checks.
	if (data.Target().Epoch() != currentEpoch && data.Target().Epoch() != previousEpoch) || data.Target().Epoch() != state2.GetEpochAtSlot(s.BeaconConfig(), data.Slot()) {
		return nil, errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot()+beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot()+beaconConfig.SlotsPerEpoch {
		return nil, errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.ValidatorIndex() >= s.CommitteeCount(data.Target().Epoch()) {
		return nil, errors.New("ProcessAttestation: attester index out of range")
	}
	// check if we need to use rules for phase0 or post-altair.
	if s.Version() == clparams.Phase0Version {
		return processAttestationPhase0(s, attestation)
	}
	return processAttestationPostAltair(s, attestation, baseRewardPerIncrement)
}

func verifyAttestations(s *state2.BeaconState, attestations *cltypes.AttestationList, attestingIndicies [][]uint64) (bool, error) {
	var err error
	valid := true
	attestations.ForEach(func(a *solid.Attestation, idx, total int) bool {
		indexedAttestation := state2.GetIndexedAttestation(a, attestingIndicies[idx])
		valid, err = state2.IsValidIndexedAttestation(s.BeaconState, indexedAttestation)
		if err != nil {
			return false
		}
		if !valid {
			return false
		}
		return true
	})

	return valid, err
}
