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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

var ErrIgnore = errors.New("ignore")

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
	return f.onAttestationLocked(attestation, fromBlock, nil, common.Hash{})
}

// onAttestationLocked reads the committees from blockState, the post-state of the block at blockRoot, when it
// has the target's shuffling.
func (f *ForkChoiceStore) onAttestationLocked(
	attestation *solid.Attestation,
	fromBlock bool,
	blockState *state.CachingBeaconState,
	blockRoot common.Hash,
) error {
	f.headHash = common.Hash{} // reset current head hash to force recomputation on next GetHead
	f.headPayloadStatus = cltypes.PayloadStatusPending
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

	useCheckpointState := f.syncedDataManager.Syncing()
	if blockState != nil && f.HeadStateHasTargetShuffling(blockState, blockRoot, target) {
		useCheckpointState = false
		attestationIndicies, err = f.verifyAttestationWithState(blockState, attestation, fromBlock)
	} else if !useCheckpointState {
		if err := f.syncedDataManager.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, headRoot common.Hash, _ uint64) error {
			if !f.HeadStateHasTargetShuffling(headState, headRoot, target) {
				useCheckpointState = true
				return nil
			}
			attestationIndicies, err = f.verifyAttestationWithState(headState, attestation, fromBlock)
			return err
		}); err != nil {
			return err
		}
	}
	if useCheckpointState {
		attestationIndicies, err = f.verifyAttestationWithCheckpointState(
			target,
			attestation,
			fromBlock,
		)
	}
	if err != nil {
		return err
	}

	// Lastly update latest messages.
	f.updateLatestMessages(attestation, attestationIndicies)

	return nil
}

// HeadStateHasTargetShuffling reports whether the head state computes the target's committees.
// The one-epoch window preserves the target's final seed mix; the dependent block then identifies the same committees.
func (f *ForkChoiceStore) HeadStateHasTargetShuffling(headState *state.CachingBeaconState, headRoot common.Hash, target solid.Checkpoint) bool {
	headEpoch := state.Epoch(headState)
	if target.Epoch > headEpoch+1 || target.Epoch+1 < headEpoch {
		return false
	}
	dependentSlot := computeShufflingDependentSlot(target.Epoch, f.beaconCfg.MinSeedLookahead, f.beaconCfg.SlotsPerEpoch)
	targetDependentRoot := f.Ancestor(target.Root, dependentSlot).Root
	if targetDependentRoot == (common.Hash{}) {
		return false
	}
	return targetDependentRoot == f.Ancestor(headRoot, dependentSlot).Root
}

// TargetBeaconCommittee returns a committee from the target checkpoint state.
// claimBuild is called only by the caller that starts an uncached build, and a false result refuses the build.
func (f *ForkChoiceStore) TargetBeaconCommittee(target solid.Checkpoint, slot, committeeIndex uint64, claimBuild func() bool) ([]uint64, uint64, error) {
	targetState, err := f.targetCheckpointState(target, slot, claimBuild)
	if err != nil {
		return nil, 0, err
	}
	if targetState == nil {
		return nil, 0, errors.New("target state does not exist")
	}
	committee, committeesPerSlot := targetState.beaconCommittee(slot, committeeIndex)
	return committee, committeesPerSlot, nil
}

type targetStateBuild struct {
	done  chan struct{}
	state *checkpointState
	err   error
}

func (f *ForkChoiceStore) targetCheckpointState(target solid.Checkpoint, slot uint64, claimBuild func() bool) (*checkpointState, error) {
	if cached, ok := f.checkpointStates.Load(target); ok {
		return cached.(*checkpointState), nil
	}
	f.targetStateBuildsMu.Lock()
	if build, ok := f.targetStateBuilds[target]; ok {
		f.targetStateBuildsMu.Unlock()
		<-build.done
		return build.state, build.err
	}
	if cached, ok := f.checkpointStates.Load(target); ok {
		f.targetStateBuildsMu.Unlock()
		return cached.(*checkpointState), nil
	}
	if len(f.targetStateBuilds) >= maxConcurrentTargetCommitteeBuilds {
		f.targetStateBuildsMu.Unlock()
		return nil, errors.New("too many target committee builds")
	}
	if claimBuild != nil && !claimBuild() {
		f.targetStateBuildsMu.Unlock()
		return nil, errors.New("target state build already claimed")
	}
	if f.targetStateBuilds == nil {
		f.targetStateBuilds = make(map[solid.Checkpoint]*targetStateBuild)
	}
	build := &targetStateBuild{done: make(chan struct{})}
	f.targetStateBuilds[target] = build
	f.targetStateBuildsMu.Unlock()

	defer func() {
		f.targetStateBuildsMu.Lock()
		delete(f.targetStateBuilds, target)
		f.targetStateBuildsMu.Unlock()
		close(build.done)
	}()
	build.state, build.err = f.buildTargetCheckpointState(target, slot)
	return build.state, build.err
}

func (f *ForkChoiceStore) buildTargetCheckpointState(target solid.Checkpoint, slot uint64) (*checkpointState, error) {
	header, ok := f.forkGraph.GetHeader(target.Root)
	if !ok {
		return nil, errors.New("target checkpoint block does not exist")
	}
	if header.Slot+maxTargetCheckpointBlockAge*f.beaconCfg.SlotsPerEpoch < slot {
		return nil, errors.New("target checkpoint block is too old")
	}
	if finalized, ok := f.finalizedCheckpoint.Load().(solid.Checkpoint); ok &&
		f.Ancestor(target.Root, finalized.Epoch*f.beaconCfg.SlotsPerEpoch).Root != finalized.Root {
		return nil, errors.New("target checkpoint does not descend from finalized checkpoint")
	}
	return f.getCheckpointState(target)
}

func (f *ForkChoiceStore) ProcessAttestingIndicies(
	attestation *solid.Attestation,
	attestionIndicies []uint64,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateLatestMessages(attestation, attestionIndicies)
}

func (f *ForkChoiceStore) verifyAttestationWithCheckpointState(
	target solid.Checkpoint,
	attestation *solid.Attestation,
	fromBlock bool,
) (attestationIndicies []uint64, err error) {
	targetState, err := f.getCheckpointStateLocked(target)
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
	f.gloasWeightTree.markDirty(index)
}

func (f *ForkChoiceStore) trackGloasWeights() bool {
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	return f.beaconCfg.GetCurrentStateVersion(currentEpoch) >= clparams.GloasVersion
}

func (f *ForkChoiceStore) getLatestMessage(validatorIndex uint64) (LatestMessage, bool) {
	return f.latestMessages.get(int(validatorIndex))
}

func (f *ForkChoiceStore) isUnequivocating(validatorIndex uint64) bool {
	// f.equivocatingIndicies is a bitlist
	index := validatorIndex / 8
	if index >= uint64(len(f.equivocatingIndicies)) {
		return false
	}
	subIndex := validatorIndex % 8
	return f.equivocatingIndicies[index]&(1<<subIndex) != 0
}

func (f *ForkChoiceStore) setUnequivocating(validatorIndex uint64) {
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending
	index := int(validatorIndex) / 8
	if index >= len(f.equivocatingIndicies) {
		if index < cap(f.equivocatingIndicies) {
			f.equivocatingIndicies = f.equivocatingIndicies[:index+1]
		} else {
			nextCap := cap(f.equivocatingIndicies)
			if nextCap == 0 {
				nextCap = 1
			}
			for nextCap <= index {
				nextCap *= 2
			}
			next := make([]byte, index+1, nextCap)
			copy(next, f.equivocatingIndicies)
			f.equivocatingIndicies = next
		}
	}
	subIndex := int(validatorIndex) % 8
	f.equivocatingIndicies[index] |= 1 << uint(subIndex)
	f.gloasWeightTree.markDirty(validatorIndex)
}

func (f *ForkChoiceStore) updateLatestMessages(
	attestation *solid.Attestation,
	indicies []uint64,
) {
	if f.trackGloasWeights() {
		f.updateLatestMessagesGloas(attestation, indicies)
	} else {
		f.updateLatestMessagesPreGloas(attestation, indicies)
	}
}

func (f *ForkChoiceStore) updateLatestMessagesPreGloas(
	attestation *solid.Attestation,
	indicies []uint64,
) {
	slot := attestation.Data.Slot
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
				Slot:  slot, // Set slot for GLOAS compatibility at fork boundary
			})
		}
	}
}

// updateLatestMessagesGloas updates latest messages using slot-based comparison
// and tracks payload_present from the attestation index.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) updateLatestMessagesGloas(
	attestation *solid.Attestation,
	indicies []uint64,
) {
	slot := attestation.Data.Slot
	beaconBlockRoot := attestation.Data.BeaconBlockRoot
	payloadPresent := attestation.Data.CommitteeIndex == 1
	updated := false

	for _, index := range indicies {
		if f.isUnequivocating(index) {
			continue
		}
		validatorMessage, has := f.getLatestMessage(index)
		if !has || slot > validatorMessage.Slot {
			f.setLatestMessage(index, LatestMessage{
				Slot:           slot,
				Root:           beaconBlockRoot,
				PayloadPresent: payloadPresent,
			})
			updated = true
		}
	}
	if updated {
		f.headHash = common.Hash{}
		f.headPayloadStatus = cltypes.PayloadStatusPending
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
	blockHeader, has := f.forkGraph.GetHeader(attestation.Data.BeaconBlockRoot)
	if !has || blockHeader.Slot > attestation.Data.Slot {
		return errors.New("bad attestation data")
	}

	// [New in Gloas:EIP7732] Validate attestation index
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if f.beaconCfg.GetCurrentStateVersion(currentEpoch) >= clparams.GloasVersion {
		// index must be 0 or 1
		if attestation.Data.CommitteeIndex != 0 && attestation.Data.CommitteeIndex != 1 {
			return errors.New("attestation index must be 0 or 1")
		}
		// if block_slot == attestation_slot, index must be 0
		if blockHeader.Slot == attestation.Data.Slot && attestation.Data.CommitteeIndex != 0 {
			return errors.New("attestation index must be 0 when block_slot equals attestation_slot")
		}
		if attestation.Data.CommitteeIndex == 1 && !f.isPayloadAvailable(attestation.Data.BeaconBlockRoot) {
			return errors.New("attestation with index 1 requires an imported payload")
		}
	}

	// LMD vote must be consistent with FFG vote target
	targetSlot := f.computeStartSlotAtEpoch(target.Epoch)
	ancestorNode := f.Ancestor(attestation.Data.BeaconBlockRoot, targetSlot)
	if ancestorNode.Root == (common.Hash{}) {
		return errors.New("could not retrieve ancestor")
	}
	if ancestorNode.Root != target.Root {
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
