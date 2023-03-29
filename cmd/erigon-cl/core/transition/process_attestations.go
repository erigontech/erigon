package transition

import (
	"errors"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"golang.org/x/exp/slices"
)

func ProcessAttestations(state *state.BeaconState, attestations []*cltypes.Attestation, fullValidation bool) error {
	var err error
	attestingIndiciesSet := make([][]uint64, len(attestations))

	baseRewardPerIncrement := state.BaseRewardPerIncrement()

	for i, attestation := range attestations {
		if attestingIndiciesSet[i], err = processAttestation(state, attestation, baseRewardPerIncrement); err != nil {
			return err
		}
	}
	if fullValidation {
		valid, err := verifyAttestations(state, attestations, attestingIndiciesSet)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessAttestation: wrong bls data")
		}
	}

	return nil
}

// ProcessAttestation takes an attestation and process it.
func processAttestation(state *state.BeaconState, attestation *cltypes.Attestation, baseRewardPerIncrement uint64) ([]uint64, error) {
	data := attestation.Data
	currentEpoch := state.Epoch()
	previousEpoch := state.PreviousEpoch()
	stateSlot := state.Slot()
	beaconConfig := state.BeaconConfig()
	if (data.Target.Epoch != currentEpoch && data.Target.Epoch != previousEpoch) || data.Target.Epoch != state.GetEpochAtSlot(data.Slot) {
		return nil, errors.New("ProcessAttestation: attestation with invalid epoch")
	}
	if data.Slot+beaconConfig.MinAttestationInclusionDelay > stateSlot || stateSlot > data.Slot+beaconConfig.SlotsPerEpoch {
		return nil, errors.New("ProcessAttestation: attestation slot not in range")
	}
	if data.Index >= state.CommitteeCount(data.Target.Epoch) {
		return nil, errors.New("ProcessAttestation: attester index out of range")
	}
	participationFlagsIndicies, err := state.GetAttestationParticipationFlagIndicies(attestation.Data, stateSlot-data.Slot)
	if err != nil {
		return nil, err
	}

	attestingIndicies, err := state.GetAttestingIndicies(attestation.Data, attestation.AggregationBits)
	if err != nil {
		return nil, err
	}
	var proposerRewardNumerator uint64

	var epochParticipation cltypes.ParticipationFlagsList
	if data.Target.Epoch == currentEpoch {
		epochParticipation = state.CurrentEpochParticipation()
	} else {
		epochParticipation = state.PreviousEpochParticipation()
	}
	validators := state.Validators()

	for _, attesterIndex := range attestingIndicies {
		baseReward := (validators[attesterIndex].EffectiveBalance / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) || epochParticipation[attesterIndex].HasFlag(flagIndex) {
				continue
			}
			epochParticipation[attesterIndex] = epochParticipation[attesterIndex].Add(flagIndex)
			proposerRewardNumerator += baseReward * weight
		}
	}
	// Reward proposer
	proposer, err := state.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	// Set participation
	if data.Target.Epoch == currentEpoch {
		state.SetCurrentEpochParticipation(epochParticipation)
	} else {
		state.SetPreviousEpochParticipation(epochParticipation)
	}
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return attestingIndicies, state.IncreaseBalance(proposer, reward)
}

type verifyAttestationWorkersResult struct {
	success bool
	err     error
}

func verifyAttestationWorker(state *state.BeaconState, attestation *cltypes.Attestation, attestingIndicies []uint64, resultCh chan verifyAttestationWorkersResult) {
	indexedAttestation, err := state.GetIndexedAttestation(attestation, attestingIndicies)
	if err != nil {
		resultCh <- verifyAttestationWorkersResult{err: err}
		return
	}
	success, err := isValidIndexedAttestation(state, indexedAttestation)
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
