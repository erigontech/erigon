package transition

import (
	"errors"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"golang.org/x/exp/slices"
)

type attestingIndiciesWorkersResult struct {
	indicies []uint64
	index    int
	err      error
}

func attestingIndiciesWorker(state *state.BeaconState, index int, attestation *cltypes.Attestation, resultCh chan attestingIndiciesWorkersResult) {
	attestingIndicies, err := state.GetAttestingIndicies(attestation.Data, attestation.AggregationBits)
	resultCh <- attestingIndiciesWorkersResult{
		indicies: attestingIndicies,
		index:    index,
		err:      err,
	}
}

// Takes attestations and concurently returns a batch of the indicies.
func (s *StateTransistor) getAttestingIndiciesBatch(attestations []*cltypes.Attestation) ([][]uint64, error) {
	attestingIndiciesSets := make([][]uint64, len(attestations))
	resultCh := make(chan attestingIndiciesWorkersResult, len(attestations))
	for i, attestation := range attestations {
		go attestingIndiciesWorker(s.state, i, attestation, resultCh)
	}
	for i := 0; i < len(attestations); i++ {
		result := <-resultCh
		if result.err != nil {
			return nil, result.err
		}
		attestingIndiciesSets[result.index] = result.indicies
	}
	close(resultCh)
	return attestingIndiciesSets, nil
}

func (s *StateTransistor) ProcessAttestations(attestations []*cltypes.Attestation) error {
	attestingIndiciesSet, err := s.getAttestingIndiciesBatch(attestations)
	if err != nil {
		return err
	}
	for i, attestation := range attestations {
		if err := s.processAttestation(attestation, attestingIndiciesSet[i]); err != nil {
			return err
		}
	}
	return nil
}

// ProcessAttestation takes an attestation and process it.
func (s *StateTransistor) processAttestation(attestation *cltypes.Attestation, attestingIndicies []uint64) error {
	participationFlagWeights := []uint64{
		s.beaconConfig.TimelySourceWeight,
		s.beaconConfig.TimelyTargetWeight,
		s.beaconConfig.TimelyHeadWeight,
	}

	totalActiveBalance, err := s.state.GetTotalActiveBalance()
	if err != nil {
		return err
	}
	data := attestation.Data
	currentEpoch := s.state.Epoch()
	previousEpoch := s.state.PreviousEpoch()
	stateSlot := s.state.Slot()
	if (data.Target.Epoch != currentEpoch && data.Target.Epoch != previousEpoch) || data.Target.Epoch != s.state.GetEpochAtSlot(data.Slot) {
		return errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot+s.beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot+s.beaconConfig.SlotsPerEpoch {
		return errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.Index >= s.state.CommitteeCount(data.Target.Epoch) {
		return errors.New("ProcessAttestation: attester index out of range")
	}
	participationFlagsIndicies, err := s.state.GetAttestationParticipationFlagIndicies(attestation.Data, stateSlot-data.Slot)
	if err != nil {
		return err
	}
	var proposerRewardNumerator uint64

	valid, err := s.verifyAttestation(attestation, attestingIndicies)
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("ProcessAttestation: wrong bls data")
	}
	var epochParticipation cltypes.ParticipationFlagsList
	if data.Target.Epoch == currentEpoch {
		epochParticipation = s.state.CurrentEpochParticipation()
	} else {
		epochParticipation = s.state.PreviousEpochParticipation()
	}

	for _, attesterIndex := range attestingIndicies {
		for flagIndex, weight := range participationFlagWeights {
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || epochParticipation[attesterIndex].HasFlag(flagIndex) {
				continue
			}
			epochParticipation[attesterIndex] = epochParticipation[attesterIndex].Add(flagIndex)
			baseReward, err := s.state.BaseReward(totalActiveBalance, attesterIndex)
			if err != nil {
				return err
			}
			proposerRewardNumerator += baseReward * weight
		}
	}
	// Reward proposer
	proposer, err := s.state.GetBeaconProposerIndex()
	if err != nil {
		return err
	}
	// Set participation
	if data.Target.Epoch == currentEpoch {
		s.state.SetCurrentEpochParticipation(epochParticipation)
	} else {
		s.state.SetPreviousEpochParticipation(epochParticipation)
	}
	proposerRewardDenominator := (s.beaconConfig.WeightDenominator - s.beaconConfig.ProposerWeight) * s.beaconConfig.WeightDenominator / s.beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return s.state.IncreaseBalance(int(proposer), reward)
}

func (s *StateTransistor) verifyAttestation(attestation *cltypes.Attestation, attestingIndicies []uint64) (bool, error) {
	if s.noValidate {
		return true, nil
	}
	indexedAttestation, err := s.state.GetIndexedAttestation(attestation, attestingIndicies)
	if err != nil {
		return false, err
	}
	return isValidIndexedAttestation(s.state, indexedAttestation)
}
