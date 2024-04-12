package forkchoice

import (
	"context"
	"fmt"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/log/v3"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
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

func (f *ForkChoiceStore) verifyAggregateAndProofSignature(state *state.CachingBeaconState, aggregate *cltypes.AggregateAndProof) error {
	slot := aggregate.Aggregate.AttestantionData().Slot()
	publicKey, err := state.ValidatorPublicKey(int(aggregate.AggregatorIndex))
	if err != nil {
		return err
	}
	domain, err := state.GetDomain(state.BeaconConfig().DomainSelectionProof, f.computeEpochAtSlot(slot))
	if err != nil {
		return err
	}
	signingRoot := utils.Sha256(merkle_tree.Uint64Root(slot).Bytes(), domain)
	valid, err := bls.Verify(aggregate.SelectionProof[:], signingRoot[:], publicKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid bls signature on aggregate and proof")
	}
	return nil
}

func (f *ForkChoiceStore) verifyAggregatorSignature(state *state.CachingBeaconState, aggregate *cltypes.SignedAggregateAndProof) error {
	publicKey, err := state.ValidatorPublicKey(int(aggregate.Message.AggregatorIndex))
	if err != nil {
		return err
	}
	domain, err := state.GetDomain(state.BeaconConfig().DomainAggregateAndProof, f.Slot())
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(aggregate.Message, domain)
	if err != nil {
		return err
	}
	valid, err := bls.Verify(aggregate.Signature[:], signingRoot[:], publicKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid bls signature on aggregate and proof")
	}
	return nil
}

func (f *ForkChoiceStore) verifyAggregateMessageSignature(s *state.CachingBeaconState, aggregateAndProof *cltypes.SignedAggregateAndProof, attestingIndicies []uint64) error {
	indexedAttestation := state.GetIndexedAttestation(aggregateAndProof.Message.Aggregate, attestingIndicies)

	valid, err := state.IsValidIndexedAttestation(s, indexedAttestation)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid aggregate signature")
	}
	return nil
}

func (f *ForkChoiceStore) verifySignaturesOnAggregate(s *state.CachingBeaconState, aggregateAndProof *cltypes.SignedAggregateAndProof) error {
	aggregationBits := aggregateAndProof.Message.Aggregate.AggregationBits()
	// [REJECT] The aggregate attestation has participants -- that is, len(get_attesting_indices(state, aggregate)) >= 1.
	attestingIndicies, err := s.GetAttestingIndicies(aggregateAndProof.Message.Aggregate.AttestantionData(), aggregationBits, true)
	if err != nil {
		return err
	}
	if len(attestingIndicies) == 0 {
		return fmt.Errorf("no attesting indicies")
	}
	// [REJECT] The aggregate_and_proof.selection_proof is a valid signature of the aggregate.data.slot by the validator with index aggregate_and_proof.aggregator_index.
	if err := f.verifyAggregateAndProofSignature(s, aggregateAndProof.Message); err != nil {
		return err
	}

	// [REJECT] The aggregator signature, signed_aggregate_and_proof.signature, is valid.
	if err := f.verifyAggregatorSignature(s, aggregateAndProof); err != nil {
		return err
	}

	return f.verifyAggregateMessageSignature(s, aggregateAndProof, attestingIndicies)
}

// OnAggregateAndProof processes incoming aggregate and proofs. it is called when a new aggregate and proof is received either via gossip or from the Beacon API.
func (f *ForkChoiceStore) OnAggregateAndProof(aggregateAndProof *cltypes.SignedAggregateAndProof, _ bool) error {
	headState := f.syncedDataManager.HeadState()
	if headState == nil {
		return nil
	}
	selectionProof := aggregateAndProof.Message.SelectionProof
	aggregateData := aggregateAndProof.Message.Aggregate.AttestantionData()
	target := aggregateAndProof.Message.Aggregate.AttestantionData().Target()
	slot := aggregateAndProof.Message.Aggregate.AttestantionData().Slot()
	committeeIndex := aggregateAndProof.Message.Aggregate.AttestantionData().CommitteeIndex()

	// [IGNORE] aggregate.data.slot is within the last ATTESTATION_PROPAGATION_SLOT_RANGE slots (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. current_slot >= aggregate.data.slot (a client MAY queue future aggregates for processing at the appropriate slot).
	if aggregateData.Slot() > f.Slot() {
		f.scheduleAggregateForLaterProcessing(aggregateAndProof)
		return nil
	}
	// [IGNORE] the epoch of aggregate.data.slot is either the current or previous epoch (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. compute_epoch_at_slot(aggregate.data.slot) in (get_previous_epoch(state), get_current_epoch(state))
	if state.PreviousEpoch(headState) != f.computeEpochAtSlot(slot) && state.Epoch(headState) != f.computeEpochAtSlot(slot) {
		return nil
	}

	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by aggregate.data.beacon_block_root -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	if f.Ancestor(aggregateData.BeaconBlockRoot(), f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch())) != f.FinalizedCheckpoint().BlockRoot() {
		return nil
	}

	// [IGNORE] The block being voted for (aggregate.data.beacon_block_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue aggregates for processing once block is retrieved).
	if _, ok := f.forkGraph.GetHeader(aggregateData.BeaconBlockRoot()); !ok {
		return nil
	}

	// [REJECT] The committee index is within the expected range -- i.e. index < get_committee_count_per_slot(state, aggregate.data.target.epoch).
	committeeCountPerSlot := headState.CommitteeCount(target.Epoch())
	if aggregateData.CommitteeIndex() >= committeeCountPerSlot {
		return fmt.Errorf("invalid committee index in aggregate and proof")
	}
	// [REJECT] The aggregate attestation's epoch matches its target -- i.e. aggregate.data.target.epoch == compute_epoch_at_slot(aggregate.data.slot)
	if aggregateData.Target().Epoch() != f.computeEpochAtSlot(slot) {
		return fmt.Errorf("invalid target epoch in aggregate and proof")
	}
	committee, err := headState.GetBeaconCommitee(slot, committeeIndex)
	if err != nil {
		return err
	}

	// [REJECT] aggregate_and_proof.selection_proof selects the validator as an aggregator for the slot -- i.e. is_aggregator(state, aggregate.data.slot, index, aggregate_and_proof.selection_proof) returns True.
	if !state.IsAggregator(f.beaconCfg, uint64(len(committee)), committeeIndex, selectionProof) {
		log.Warn("receveived aggregate and proof from invalid aggregator")
		return fmt.Errorf("invalid aggregate and proof")
	}
	// [REJECT] The aggregator's validator index is within the committee -- i.e. aggregate_and_proof.aggregator_index in get_beacon_committee(state, aggregate.data.slot, index).
	if !slices.Contains(committee, aggregateAndProof.Message.AggregatorIndex) {
		return fmt.Errorf("committee index not in committee")
	}

	if err := f.verifySignaturesOnAggregate(headState, aggregateAndProof); err != nil {
		return err
	}

	// [REJECT] The aggregate attestation's target block is an ancestor of the block named in the LMD vote -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, aggregate.data.target.epoch) == aggregate.data.target.root
	if f.Ancestor(aggregateData.BeaconBlockRoot(), f.computeStartSlotAtEpoch(target.Epoch())) != target.BlockRoot() {
		return fmt.Errorf("invalid target block")
	}

	// Add to aggregation pool
	if err := f.aggregationPool.AddAttestation(aggregateAndProof.Message.Aggregate); err != nil {
		return errors.WithMessagef(err, "failed to add attestation to pool")
	}

	return nil
}

type aggregateJob struct {
	aggregate *cltypes.SignedAggregateAndProof
	when      time.Time
}

func (f *ForkChoiceStore) scheduleAggregateForLaterProcessing(agg *cltypes.SignedAggregateAndProof) {
	root, err := agg.HashSSZ()
	if err != nil {
		log.Error("failed to hash attestation", "err", err)
		return
	}
	f.aggregatesSet.Store(root, &aggregateJob{
		aggregate: agg,
		when:      time.Now(),
	})
}

type blockJob struct {
	block     *cltypes.SignedBeaconBlock
	blockRoot libcommon.Hash
	when      time.Time
}

func (f *ForkChoiceStore) scheduleBlockForLaterProcessing(block *cltypes.SignedBeaconBlock) {
	root, err := block.HashSSZ()
	if err != nil {
		log.Error("failed to hash block", "err", err)
		return
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		log.Error("failed to hash block root", "err", err)
		return
	}
	f.blocksSet.Store(root, &blockJob{
		block:     block,
		when:      time.Now(),
		blockRoot: blockRoot,
	})
}

func (f *ForkChoiceStore) StartJobsRTT(ctx context.Context) {
	go func() {
		interval := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-interval.C:
				f.aggregatesSet.Range(func(key, value interface{}) bool {
					job := value.(*aggregateJob)
					if time.Since(job.when) > maxAttestationJobLifetime {
						f.aggregatesSet.Delete(key)
						return true
					}

					if job.aggregate.Message.Aggregate.AttestantionData().Slot() <= f.Slot() {
						if err := f.OnAggregateAndProof(job.aggregate, false); err != nil {
							log.Warn("failed to process attestation", "err", err)
						}
						f.aggregatesSet.Delete(key)
					}
					// helps the CPU not to be overloaded
					select {
					case <-ctx.Done():
						return false
					case <-time.After(20 * time.Millisecond):
					}
					return true
				})
				f.blocksSet.Range(func(key, value interface{}) bool {
					job := value.(*blockJob)
					if time.Since(job.when) > maxBlockJobLifetime {
						f.blocksSet.Delete(key)
						return true
					}

					f.mu.Lock()
					if err := f.isDataAvailable(ctx, job.block.Block.Slot, job.blockRoot, job.block.Block.Body.BlobKzgCommitments); err != nil {
						f.mu.Unlock()
						return true
					}
					f.mu.Unlock()

					if err := f.OnBlock(ctx, job.block, true, true, true); err != nil {
						log.Warn("failed to process attestation", "err", err)
					}
					f.blocksSet.Delete(key)

					return true
				})
			}
		}
	}()

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
