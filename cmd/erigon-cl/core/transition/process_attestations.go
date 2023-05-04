package transition

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"golang.org/x/exp/slices"
)

func ProcessAttestations(s *state.BeaconState, attestations []*cltypes.Attestation, fullValidation bool) error {
	var err error
	attestingIndiciesSet := make([][]uint64, len(attestations))

	baseRewardPerIncrement := s.BaseRewardPerIncrement()

	for i, attestation := range attestations {
		if attestingIndiciesSet[i], err = processAttestation(s, attestation, baseRewardPerIncrement); err != nil {
			return err
		}
	}
	if fullValidation {
		valid, err := verifyAttestations(s, attestations, attestingIndiciesSet)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessAttestation: wrong bls data")
		}
	}

	return nil
}

func processAttestationPostAltair(s *state.BeaconState, attestation *cltypes.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.Data
	currentEpoch := state.Epoch(s.BeaconState)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(attestation.Data, stateSlot-data.Slot)
	if err != nil {
		return nil, err
	}

	attestingIndicies, err := s.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, true)
	if err != nil {
		return nil, err
	}
	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target.Epoch == currentEpoch

	validators := s.Validators()
	for _, attesterIndex := range attestingIndicies {
		baseReward := (validators[attesterIndex].EffectiveBalance / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			flagParticipation := s.EpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex))
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || flagParticipation.HasFlag(flagIndex) {
				continue
			}
			s.SetEpochParticipationForValidatorIndex(isCurrentEpoch, int(attesterIndex), flagParticipation.Add(flagIndex))
			proposerRewardNumerator += baseReward * weight
		}
	}
	// Reward proposer
	proposer, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return attestingIndicies, state.IncreaseBalance(s.BeaconState, proposer, reward)
}

// processAttestationsPhase0 implements the rules for phase0 processing.
func processAttestationPhase0(s *state.BeaconState, attestation *cltypes.Attestation) ([]uint64, error) {
	data := attestation.Data
	committee, err := s.GetBeaconCommitee(data.Slot, data.Index)
	if err != nil {
		return nil, err
	}

	if len(committee) != utils.GetBitlistLength(attestation.AggregationBits) {
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
		AggregationBits: attestation.AggregationBits,
		InclusionDelay:  s.Slot() - data.Slot,
		ProposerIndex:   proposerIndex,
	}
	isCurrentAttestation := data.Target.Epoch == state.Epoch(s.BeaconState)
	// Depending of what slot we are on we put in either the current justified or previous justified.
	if isCurrentAttestation {
		if !data.Source.Equal(s.CurrentJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddCurrentEpochAtteastation(pendingAttestation)
	} else {
		if !data.Source.Equal(s.PreviousJustifiedCheckpoint()) {
			return nil, fmt.Errorf("processAttestationPhase0: mismatching sources")
		}
		s.AddPreviousEpochAttestation(pendingAttestation)
	}
	// Not required by specs but needed if we want performant epoch transition.
	indicies, err := s.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, true)
	if err != nil {
		return nil, err
	}
	epochRoot, err := state.GetBlockRoot(s.BeaconState, attestation.Data.Target.Epoch)
	if err != nil {
		return nil, err
	}
	slotRoot, err := s.GetBlockRootAtSlot(attestation.Data.Slot)
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
			if attestation.Data.Target.Root == epochRoot {
				validator.IsCurrentMatchingTargetAttester = true
			} else {
				continue
			}
			if attestation.Data.BeaconBlockHash == slotRoot {
				validator.IsCurrentMatchingHeadAttester = true
			}
		} else {
			if validator.MinPreviousInclusionDelayAttestation == nil || validator.MinPreviousInclusionDelayAttestation.InclusionDelay > pendingAttestation.InclusionDelay {
				validator.MinPreviousInclusionDelayAttestation = pendingAttestation
			}
			validator.IsPreviousMatchingSourceAttester = true
			if attestation.Data.Target.Root != epochRoot {
				continue
			}
			validator.IsPreviousMatchingTargetAttester = true
			if attestation.Data.BeaconBlockHash == slotRoot {
				validator.IsPreviousMatchingHeadAttester = true
			}
		}
	}
	return indicies, nil
}

// ProcessAttestation takes an attestation and process it.
func processAttestation(s *state.BeaconState, attestation *cltypes.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.Data
	currentEpoch := state.Epoch(s.BeaconState)
	previousEpoch := state.PreviousEpoch(s.BeaconState)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()
	// Prelimary checks.
	if (data.Target.Epoch != currentEpoch && data.Target.Epoch != previousEpoch) || data.Target.Epoch != state.GetEpochAtSlot(s.BeaconConfig(), data.Slot) {
		return nil, errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot+beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot+beaconConfig.SlotsPerEpoch {
		return nil, errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.Index >= s.CommitteeCount(data.Target.Epoch) {
		return nil, errors.New("ProcessAttestation: attester index out of range")
	}
	// check if we need to use rules for phase0 or post-altair.
	if s.Version() == clparams.Phase0Version {
		return processAttestationPhase0(s, attestation)
	}
	return processAttestationPostAltair(s, attestation, baseRewardPerIncrement)
}

type verifyAttestationWorkersResult struct {
	success bool
	err     error
}

func verifyAttestationWorker(s *state.BeaconState, attestation *cltypes.Attestation, attestingIndicies []uint64, resultCh chan verifyAttestationWorkersResult) {
	indexedAttestation := state.GetIndexedAttestation(attestation, attestingIndicies)
	success, err := state.IsValidIndexedAttestation(s.BeaconState, indexedAttestation)
	resultCh <- verifyAttestationWorkersResult{success: success, err: err}
}

func verifyAttestations(state *state.BeaconState, attestations []*cltypes.Attestation, attestingIndicies [][]uint64) (bool, error) {
	resultCh := make(chan verifyAttestationWorkersResult, len(attestations))

	for i, attestation := range attestations {
		go verifyAttestationWorker(state, attestation, attestingIndicies[i], resultCh)
	}
	for i := 0; i < len(attestations); i++ {
		result := <-resultCh
		if result.err != nil {
			return false, result.err
		}
		if !result.success {
			return false, nil
		}
	}
	close(resultCh)
	return true, nil
}
