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

package forkchoice

import (
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

var (
	ErrIgnore = errors.New("ignore")
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
	f.headHash = common.Hash{}
	data := attestation.Data
	if err := f.ValidateOnAttestation(attestation); err != nil {
		return err
	}
	currentEpoch := f.computeEpochAtSlot(f.Slot())

	if f.Slot() < data.Slot+1 || data.Target.Epoch > currentEpoch {
		return nil
	}

	if !fromBlock {
		if err := f.validateTargetEpochAgainstCurrentTime(attestation); err != nil {
			return err
		}
	}
	var attestationIndicies []uint64
	var err error
	target := data.Target

	if f.syncedDataManager.Syncing() {
		attestationIndicies, err = f.verifyAttestationWithCheckpointState(
			target,
			attestation,
			fromBlock,
		)
	} else {
		if err := f.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
			attestationIndicies, err = f.verifyAttestationWithState(headState, attestation, fromBlock)
			return err
		}); err != nil {
			return err
		}
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
	targetState, err := f.getCheckpointState(target)
	if err != nil {
		return nil, err
	}
	// Verify attestation signature.
	if targetState == nil {
		return nil, errors.New("target state does not exist")
	}
	// Now we need to find the attesting indicies.
	attestationIndicies, err = targetState.getAttestingIndicies(
		attestation,
		attestation.AggregationBits.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	if !fromBlock {
		indexedAttestation := state.GetIndexedAttestation(attestation, attestationIndicies)
		valid, err := targetState.isValidIndexedAttestation(indexedAttestation)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, errors.New("invalid attestation")
		}
	}
	return attestationIndicies, nil
}

func (f *ForkChoiceStore) verifyAttestationWithState(
	s *state.CachingBeaconState,
	attestation *solid.Attestation,
	fromBlock bool,
) (attestationIndicies []uint64, err error) {
	attestationIndicies, err = s.GetAttestingIndicies(attestation, true)
	if err != nil {
		return nil, err
	}
	if !fromBlock {
		indexedAttestation := state.GetIndexedAttestation(attestation, attestationIndicies)
		valid, err := state.IsValidIndexedAttestation(s, indexedAttestation)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, errors.New("invalid attestation")
		}
	}
	return attestationIndicies, nil
}

func (f *ForkChoiceStore) setLatestMessage(index uint64, message LatestMessage) {
	f.latestMessages.set(int(index), message)
}

func (f *ForkChoiceStore) getLatestMessage(validatorIndex uint64) (LatestMessage, bool) {
	return f.latestMessages.get(int(validatorIndex))
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
	beaconBlockRoot := attestation.Data.BeaconBlockRoot
	target := attestation.Data.Target

	for _, index := range indicies {
		if f.isUnequivocating(index) {
			continue
		}
		validatorMessage, has := f.getLatestMessage(index)
		if !has || target.Epoch > validatorMessage.Epoch {
			f.setLatestMessage(index, LatestMessage{
				Epoch: target.Epoch,
				Root:  beaconBlockRoot,
			})
		}
	}
}

func (f *ForkChoiceStore) ValidateOnAttestation(attestation *solid.Attestation) error {
	target := attestation.Data.Target

	if target.Epoch != f.computeEpochAtSlot(attestation.Data.Slot) {
		return errors.New("mismatching target epoch with slot data")
	}
	if _, has := f.forkGraph.GetHeader(target.Root); !has {
		return errors.New("target root is missing")
	}
	if blockHeader, has := f.forkGraph.GetHeader(attestation.Data.BeaconBlockRoot); !has ||
		blockHeader.Slot > attestation.Data.Slot {
		return errors.New("bad attestation data")
	}
	// LMD vote must be consistent with FFG vote target
	targetSlot := f.computeStartSlotAtEpoch(target.Epoch)
	ancestorRoot := f.Ancestor(attestation.Data.BeaconBlockRoot, targetSlot)
	if ancestorRoot == (common.Hash{}) {
		return errors.New("could not retrieve ancestor")
	}
	if ancestorRoot != target.Root {
		return errors.New("ancestor root mismatches with target")
	}

	return nil
}

func (f *ForkChoiceStore) validateTargetEpochAgainstCurrentTime(
	attestation *solid.Attestation,
) error {
	target := attestation.Data.Target
	// Attestations must be from the current or previous epoch
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	// Use GENESIS_EPOCH for previous when genesis to avoid underflow
	previousEpoch := currentEpoch - 1
	if currentEpoch <= f.beaconCfg.GenesisEpoch {
		previousEpoch = f.beaconCfg.GenesisEpoch
	}
	if target.Epoch == currentEpoch || target.Epoch == previousEpoch {
		return nil
	}
	return errors.New("verification of attestation against current time failed")
}
