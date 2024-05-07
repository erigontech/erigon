package forkchoice

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/network/subnets"
	"github.com/ledgerwatch/erigon/cl/utils"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

const (
	maxAttestationJobLifetime = 30 * time.Minute
	maxBlockJobLifetime       = 36 * time.Second // 3 mainnet slots
)

var (
	ErrIgnore = fmt.Errorf("ignore")
)

// OnAttestation processes incoming attestations.
func (f *ForkChoiceStore) OnAttestation(attestation *solid.Attestation, fromBlock bool, insert bool) error {
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
	target := data.Target()

	targetState, err := f.getCheckpointState(target)
	if err != nil {
		return nil
	}
	// Verify attestation signature.
	if targetState == nil {
		return fmt.Errorf("target state does not exist")
	}
	// Now we need to find the attesting indicies.
	attestationIndicies, err := targetState.getAttestingIndicies(&data, attestation.AggregationBits())
	if err != nil {
		return err
	}
	if !fromBlock {
		indexedAttestation := state.GetIndexedAttestation(attestation, attestationIndicies)
		if err != nil {
			return err
		}

		valid, err := targetState.isValidIndexedAttestation(indexedAttestation)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid attestation")
		}
	}
	// Lastly update latest messages.
	f.processAttestingIndicies(attestation, attestationIndicies)
	if !fromBlock && insert {
		// Add to the pool when verified.
		f.operationsPool.AttestationsPool.Insert(attestation.Signature(), attestation)
	}
	return nil
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
	if validatorIndex >= uint64(len(f.latestMessages)) || f.latestMessages[validatorIndex] == (LatestMessage{}) {
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

func (f *ForkChoiceStore) processAttestingIndicies(attestation *solid.Attestation, indicies []uint64) {
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
	if blockHeader, has := f.forkGraph.GetHeader(attestation.AttestantionData().BeaconBlockRoot()); !has || blockHeader.Slot > attestation.AttestantionData().Slot() {
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

func (f *ForkChoiceStore) validateTargetEpochAgainstCurrentTime(attestation *solid.Attestation) error {
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

func (f *ForkChoiceStore) OnCheckReceivedAttestation(topic string, att *solid.Attestation) error {
	var (
		root           = att.AttestantionData().BeaconBlockRoot()
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
		epoch          = att.AttestantionData().Target().Epoch()
		bits           = att.AggregationBits()
	)
	// [REJECT] The committee index is within the expected range
	committeeCount := subnets.ComputeCommitteeCountPerSlot(f.syncedDataManager.HeadState(), slot, f.beaconCfg.SlotsPerEpoch)
	if committeeIndex >= committeeCount {
		return fmt.Errorf("committee index out of range")
	}
	// [REJECT] The attestation is for the correct subnet -- i.e. compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, index) == subnet_id
	subnetId := subnets.ComputeSubnetForAttestation(committeeCount, slot, committeeIndex, f.beaconCfg.SlotsPerEpoch, f.netCfg.AttestationSubnetCount)
	topicSubnetId, err := gossip.SubnetIdFromTopicBeaconAttestation(topic)
	if err != nil {
		return err
	}
	if subnetId != topicSubnetId {
		return fmt.Errorf("wrong subnet")
	}
	// [IGNORE] attestation.data.slot is within the last ATTESTATION_PROPAGATION_SLOT_RANGE slots (within a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) --
	// i.e. attestation.data.slot + ATTESTATION_PROPAGATION_SLOT_RANGE >= current_slot >= attestation.data.slot (a client MAY queue future attestations for processing at the appropriate slot).
	currentSlot := utils.GetCurrentSlot(f.genesisTime, f.beaconCfg.SecondsPerSlot)
	if currentSlot < slot || currentSlot > slot+f.netCfg.AttestationPropagationSlotRange {
		return fmt.Errorf("not in propagation range %w", ErrIgnore)
	}
	// [REJECT] The attestation's epoch matches its target -- i.e. attestation.data.target.epoch == compute_epoch_at_slot(attestation.data.slot)
	if epoch != slot/f.beaconCfg.SlotsPerEpoch {
		return fmt.Errorf("epoch mismatch")
	}
	//[REJECT] The attestation is unaggregated -- that is, it has exactly one participating validator (len([bit for bit in aggregation_bits if bit]) == 1, i.e. exactly 1 bit is set).
	emptyCount := 0
	onBitIndex := 0
	for i := 0; i < len(bits); i++ {
		if bits[i] == 0 {
			emptyCount++
		} else if bits[i]&(bits[i]-1) != 0 {
			return fmt.Errorf("exactly one bit set")
		} else {
			// find the onBit bit index
			for onBit := uint(0); onBit < 8; onBit++ {
				if bits[i]&(1<<onBit) != 0 {
					onBitIndex = i*8 + int(onBit)
					break
				}
			}
		}
	}
	if emptyCount != len(bits)-1 {
		return fmt.Errorf("exactly one bit set")
	}
	// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, attestation.data.slot, index)).
	if len(bits)*8 < int(committeeCount) {
		return fmt.Errorf("aggregation bits mismatch")
	}

	// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
	committees, err := f.syncedDataManager.HeadState().GetBeaconCommitee(slot, committeeIndex)
	if err != nil {
		return err
	}
	if onBitIndex >= len(committees) {
		return fmt.Errorf("on bit index out of committee range")
	}
	if err := func() error {
		// mark the validator as seen
		vIndex := committees[onBitIndex]
		f.validatorAttSeenLock.Lock()
		defer f.validatorAttSeenLock.Unlock()
		if _, ok := f.validatorAttestationSeen[epoch]; !ok {
			f.validatorAttestationSeen[epoch] = make(map[uint64]struct{})
		}
		if _, ok := f.validatorAttestationSeen[epoch][vIndex]; ok {
			return fmt.Errorf("validator already seen in target epoch %w", ErrIgnore)
		}
		f.validatorAttestationSeen[epoch][vIndex] = struct{}{}
		// always check and delete previous epoch if it exists
		delete(f.validatorAttestationSeen, epoch-1)
		return nil
	}(); err != nil {
		return err
	}

	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).
	if _, ok := f.forkGraph.GetHeader(root); !ok {
		return fmt.Errorf("block not found %w", ErrIgnore)
	}
	// [REJECT] The block being voted for (attestation.data.beacon_block_root) passes validation.

	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	if f.Ancestor(root, f.computeStartSlotAtEpoch(epoch)) != att.AttestantionData().Target().BlockRoot() {
		return fmt.Errorf("invalid target block")
	}
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	if f.Ancestor(root, f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch())) != f.FinalizedCheckpoint().BlockRoot() {
		return fmt.Errorf("invalid finalized checkpoint %w", ErrIgnore)
	}
	return nil
}
