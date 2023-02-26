package transition

import (
	"errors"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"golang.org/x/exp/slices"
)

func (s *StateTransistor) ProcessAttestations(attestations []*cltypes.Attestation) error {
	var err error
	attestingIndiciesSet := make([][]uint64, len(attestations))

	baseRewardPerIncrement := s.state.BaseRewardPerIncrement()

	for i, attestation := range attestations {
		if attestingIndiciesSet[i], err = s.processAttestation(attestation, baseRewardPerIncrement); err != nil {
			return err
		}
	}
	valid, err := s.verifyAttestations(attestations, attestingIndiciesSet)
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("ProcessAttestation: wrong bls data")
	}
	return nil
}

// ProcessAttestation takes an attestation and process it.
func (s *StateTransistor) processAttestation(attestation *cltypes.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.Data
	currentEpoch := s.state.Epoch()
	previousEpoch := s.state.PreviousEpoch()
	stateSlot := s.state.Slot()
	if (data.Target.Epoch != currentEpoch && data.Target.Epoch != previousEpoch) || data.Target.Epoch != s.state.GetEpochAtSlot(data.Slot) {
		return nil, errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot+s.beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot+s.beaconConfig.SlotsPerEpoch {
		return nil, errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.Index >= s.state.CommitteeCount(data.Target.Epoch) {
		return nil, errors.New("ProcessAttestation: attester index out of range")
	}
	participationFlagsIndicies, err := s.state.GetAttestationParticipationFlagIndicies(attestation.Data, stateSlot-data.Slot)
	if err != nil {
		return nil, err
	}

	attestingIndicies, err := s.state.GetAttestingIndicies(attestation.Data, attestation.AggregationBits)
	if err != nil {
		return nil, err
	}
	var proposerRewardNumerator uint64

	var epochParticipation cltypes.ParticipationFlagsList
	if data.Target.Epoch == currentEpoch {
		epochParticipation = s.state.CurrentEpochParticipation()
	} else {
		epochParticipation = s.state.PreviousEpochParticipation()
	}
	validators := s.state.Validators()

	for _, attesterIndex := range attestingIndicies {
		baseReward := (validators[attesterIndex].EffectiveBalance / s.beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range s.beaconConfig.ParticipationWeights() {
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || epochParticipation[attesterIndex].HasFlag(flagIndex) {
				continue
			}
			epochParticipation[attesterIndex] = epochParticipation[attesterIndex].Add(flagIndex)
			proposerRewardNumerator += baseReward * weight
		}
	}
	// Reward proposer
	proposer, err := s.state.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	// Set participation
	if data.Target.Epoch == currentEpoch {
		s.state.SetCurrentEpochParticipation(epochParticipation)
	} else {
		s.state.SetPreviousEpochParticipation(epochParticipation)
	}
	proposerRewardDenominator := (s.beaconConfig.WeightDenominator - s.beaconConfig.ProposerWeight) * s.beaconConfig.WeightDenominator / s.beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return attestingIndicies, s.state.IncreaseBalance(proposer, reward)
}

type verifyAttestationWorkersResult struct {
	success bool
	err     error
}

func (s *StateTransistor) verifyAttestationWorker(state *state.BeaconState, attestation *cltypes.Attestation, attestingIndicies []uint64, resultCh chan verifyAttestationWorkersResult) {
	indexedAttestation, err := state.GetIndexedAttestation(attestation, attestingIndicies)
	if err != nil {
		resultCh <- verifyAttestationWorkersResult{err: err}
		return
	}
	success, err := s.isValidIndexedAttestation(indexedAttestation)
	resultCh <- verifyAttestationWorkersResult{success: success, err: err}
}

func (s *StateTransistor) verifyAttestations(attestations []*cltypes.Attestation, attestingIndicies [][]uint64) (bool, error) {
	if s.noValidate {
		return true, nil
	}
	resultCh := make(chan verifyAttestationWorkersResult, len(attestations))

	for i, attestation := range attestations {
		go s.verifyAttestationWorker(s.state, attestation, attestingIndicies[i], resultCh)
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
