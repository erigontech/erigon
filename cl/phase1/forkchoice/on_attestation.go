package forkchoice

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var (
	ErrIgnore = fmt.Errorf("ignore")
)

// OnAttestation processes incoming attestations.
func (f *ForkChoiceStore) OnAttestation(
	attestation *solid.Attestation,
	fromBlock bool,
	insert bool,
) error {
	if !f.synced.Load() {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headHash = libcommon.Hash{}
	data := attestation.AttestantionData()
	if err := f.ValidateOnAttestation(attestation); err != nil {
		return err
	}
	currentEpoch := f.computeEpochAtSlot(f.Slot())

	if f.Slot() < attestation.AttestantionData().Slot()+1 || data.Target().Epoch() > currentEpoch {
		return nil
	}

	if !fromBlock {
		if err := f.validateTargetEpochAgainstCurrentTime(attestation); err != nil {
			return err
		}
	}
	headState := f.syncedDataManager.HeadState()
	var attestationIndicies []uint64
	var err error
	target := data.Target()

	if headState == nil {
		attestationIndicies, err = f.verifyAttestationWithCheckpointState(
			target,
			attestation,
			fromBlock,
		)
	} else {
		attestationIndicies, err = f.verifyAttestationWithState(headState, attestation, fromBlock)
	}
	if err != nil {
		return err
	}

	// Lastly update latest messages.
	f.processAttestingIndicies(attestation, attestationIndicies)

	return nil
}

func (f *ForkChoiceStore) ProcessAttestingIndicies(
	attestation *solid.Attestation,
	attestionIndicies []uint64,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.processAttestingIndicies(attestation, attestionIndicies)
}

func (f *ForkChoiceStore) verifyAttestationWithCheckpointState(
	target solid.Checkpoint,
	attestation *solid.Attestation,
	fromBlock bool,
) (attestationIndicies []uint64, err error) {
	data := attestation.AttestantionData()
	targetState, err := f.getCheckpointState(target)
	if err != nil {
		return nil, err
	}
	// Verify attestation signature.
	if targetState == nil {
		return nil, fmt.Errorf("target state does not exist")
	}
	// Now we need to find the attesting indicies.
	attestationIndicies, err = targetState.getAttestingIndicies(
		&data,
		attestation.AggregationBits(),
	)
	if err != nil {
		return nil, err
	}
	if !fromBlock {
		indexedAttestation := state.GetIndexedAttestation(attestation, attestationIndicies)
		if err != nil {
			return nil, err
		}

		valid, err := targetState.isValidIndexedAttestation(indexedAttestation)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, fmt.Errorf("invalid attestation")
		}
	}
	return attestationIndicies, nil
}

func (f *ForkChoiceStore) verifyAttestationWithState(
	s *state.CachingBeaconState,
	attestation *solid.Attestation,
	fromBlock bool,
) (attestationIndicies []uint64, err error) {
	data := attestation.AttestantionData()
	if err != nil {
		return nil, err
	}

	attestationIndicies, err = s.GetAttestingIndicies(data, attestation.AggregationBits(), true)
	if err != nil {
		return nil, err
	}
	if !fromBlock {
		indexedAttestation := state.GetIndexedAttestation(attestation, attestationIndicies)
		if err != nil {
			return nil, err
		}
		valid, err := state.IsValidIndexedAttestation(s, indexedAttestation)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, fmt.Errorf("invalid attestation")
		}
	}
	return attestationIndicies, nil
}

func (f *ForkChoiceStore) setLatestMessage(index uint64, message LatestMessage) {
	if index >= uint64(len(f.latestMessages)) {
		if index >= uint64(cap(f.latestMessages)) {
			tmp := make([]LatestMessage, index+1, index*2)
			copy(tmp, f.latestMessages)
			f.latestMessages = tmp
		}
		f.latestMessages = f.latestMessages[:index+1]
	}
	f.latestMessages[index] = message
}

func (f *ForkChoiceStore) getLatestMessage(validatorIndex uint64) (LatestMessage, bool) {
	if validatorIndex >= uint64(len(f.latestMessages)) ||
		f.latestMessages[validatorIndex] == (LatestMessage{}) {
		return LatestMessage{}, false
	}
	return f.latestMessages[validatorIndex], true
}

func (f *ForkChoiceStore) isUnequivocating(validatorIndex uint64) bool {
	// f.equivocatingIndicies is a bitlist
	index := int(validatorIndex) / 8
	if index >= len(f.equivocatingIndicies) {
		return false
	}
	subIndex := int(validatorIndex) % 8
	return f.equivocatingIndicies[index]&(1<<uint(subIndex)) != 0
}

func (f *ForkChoiceStore) setUnequivocating(validatorIndex uint64) {
	index := int(validatorIndex) / 8
	if index >= len(f.equivocatingIndicies) {
		if index >= cap(f.equivocatingIndicies) {
			tmp := make([]byte, index+1, index*2)
			copy(tmp, f.equivocatingIndicies)
			f.equivocatingIndicies = tmp
		}
		f.equivocatingIndicies = f.equivocatingIndicies[:index+1]
	}
	subIndex := int(validatorIndex) % 8
	f.equivocatingIndicies[index] |= 1 << uint(subIndex)
}

func (f *ForkChoiceStore) processAttestingIndicies(
	attestation *solid.Attestation,
	indicies []uint64,
) {
	beaconBlockRoot := attestation.AttestantionData().BeaconBlockRoot()
	target := attestation.AttestantionData().Target()

	for _, index := range indicies {
		if f.isUnequivocating(index) {
			continue
		}
		validatorMessage, has := f.getLatestMessage(index)
		if !has || target.Epoch() > validatorMessage.Epoch {
			f.setLatestMessage(index, LatestMessage{
				Epoch: target.Epoch(),
				Root:  beaconBlockRoot,
			})
		}
	}
}

func (f *ForkChoiceStore) ValidateOnAttestation(attestation *solid.Attestation) error {
	target := attestation.AttestantionData().Target()

	if target.Epoch() != f.computeEpochAtSlot(attestation.AttestantionData().Slot()) {
		return fmt.Errorf("mismatching target epoch with slot data")
	}
	if _, has := f.forkGraph.GetHeader(target.BlockRoot()); !has {
		return fmt.Errorf("target root is missing")
	}
	if blockHeader, has := f.forkGraph.GetHeader(attestation.AttestantionData().BeaconBlockRoot()); !has ||
		blockHeader.Slot > attestation.AttestantionData().Slot() {
		return fmt.Errorf("bad attestation data")
	}
	// LMD vote must be consistent with FFG vote target
	targetSlot := f.computeStartSlotAtEpoch(target.Epoch())
	ancestorRoot := f.Ancestor(attestation.AttestantionData().BeaconBlockRoot(), targetSlot)
	if ancestorRoot == (libcommon.Hash{}) {
		return fmt.Errorf("could not retrieve ancestor")
	}
	if ancestorRoot != target.BlockRoot() {
		return fmt.Errorf("ancestor root mismatches with target")
	}

	return nil
}

func (f *ForkChoiceStore) validateTargetEpochAgainstCurrentTime(
	attestation *solid.Attestation,
) error {
	target := attestation.AttestantionData().Target()
	// Attestations must be from the current or previous epoch
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	// Use GENESIS_EPOCH for previous when genesis to avoid underflow
	previousEpoch := currentEpoch - 1
	if currentEpoch <= f.beaconCfg.GenesisEpoch {
		previousEpoch = f.beaconCfg.GenesisEpoch
	}
	if target.Epoch() == currentEpoch || target.Epoch() == previousEpoch {
		return nil
	}
	return fmt.Errorf("verification of attestation against current time failed")
}
