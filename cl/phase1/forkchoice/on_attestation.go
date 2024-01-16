package forkchoice

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/cache"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

const maxAttestationJobLifetime = 30 * time.Minute

// OnAttestation processes incoming attestations.
func (f *ForkChoiceStore) OnAttestation(attestation *solid.Attestation, fromBlock bool, insert bool) error {
	if !f.synced.Load() {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headHash = libcommon.Hash{}
	data := attestation.AttestantionData()
	if err := f.validateOnAttestation(attestation, fromBlock); err != nil {
		return err
	}
	// Schedule for later processing.
	if f.Slot() < attestation.AttestantionData().Slot()+1 {
		f.scheduleAttestationForLaterProcessing(attestation, fromBlock)
		return nil
	}
	target := data.Target()
	if cachedIndicies, ok := cache.LoadAttestatingIndicies(&data, attestation.AggregationBits()); ok {
		f.processAttestingIndicies(attestation, cachedIndicies)
		return nil
	}
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
	cache.StoreAttestation(&data, attestation.AggregationBits(), attestationIndicies)
	// Lastly update latest messages.
	f.processAttestingIndicies(attestation, attestationIndicies)
	if !fromBlock && insert {
		// Add to the pool when verified.
		f.operationsPool.AttestationsPool.Insert(attestation.Signature(), attestation)
	}
	return nil
}

func (f *ForkChoiceStore) OnAggregateAndProof(aggregateAndProof *cltypes.SignedAggregateAndProof, test bool) error {
	if !f.synced.Load() {
		return nil
	}
	slot := aggregateAndProof.Message.Aggregate.AttestantionData().Slot()
	selectionProof := aggregateAndProof.Message.SelectionProof
	committeeIndex := aggregateAndProof.Message.Aggregate.AttestantionData().ValidatorIndex()
	epoch := state.GetEpochAtSlot(f.beaconCfg, slot)

	if err := f.validateOnAttestation(aggregateAndProof.Message.Aggregate, false); err != nil {
		return err
	}

	target := aggregateAndProof.Message.Aggregate.AttestantionData().Target()
	targetState, err := f.getCheckpointState(target)
	if err != nil {
		return nil
	}

	activeIndicies := targetState.getActiveIndicies(epoch)
	activeIndiciesLength := uint64(len(activeIndicies))

	count := targetState.committeeCount(epoch, activeIndiciesLength) * f.beaconCfg.SlotsPerEpoch
	start := (activeIndiciesLength * committeeIndex) / count
	end := (activeIndiciesLength * (committeeIndex + 1)) / count
	committeeLength := end - start
	if !state.IsAggregator(f.beaconCfg, committeeLength, slot, committeeIndex, selectionProof) {
		log.Warn("invalid aggregate and proof")
		return fmt.Errorf("invalid aggregate and proof")
	}
	return f.OnAttestation(aggregateAndProof.Message.Aggregate, false, false)
}

type attestationJob struct {
	attestation *solid.Attestation
	insert      bool
	when        time.Time
}

// scheduleAttestationForLaterProcessing scheudules an attestation for later processing
func (f *ForkChoiceStore) scheduleAttestationForLaterProcessing(attestation *solid.Attestation, insert bool) {
	root, err := attestation.HashSSZ()
	if err != nil {
		log.Error("failed to hash attestation", "err", err)
		return
	}
	f.attestationSet.Store(root, &attestationJob{
		attestation: attestation,
		insert:      insert,
		when:        time.Now(),
	})
}

func (f *ForkChoiceStore) StartAttestationsRTT() {
	go func() {
		interval := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-f.ctx.Done():
				return
			case <-interval.C:
				f.attestationSet.Range(func(key, value interface{}) bool {
					job := value.(*attestationJob)
					if time.Since(job.when) > maxAttestationJobLifetime {
						f.attestationSet.Delete(key)
						return true
					}
					if f.Slot() >= job.attestation.AttestantionData().Slot()+1 {
						if err := f.OnAttestation(job.attestation, false, job.insert); err != nil {
							log.Warn("failed to process attestation", "err", err)
						}
						f.attestationSet.Delete(key)
					}
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

func (f *ForkChoiceStore) validateOnAttestation(attestation *solid.Attestation, fromBlock bool) error {
	target := attestation.AttestantionData().Target()

	if !fromBlock {
		if err := f.validateTargetEpochAgainstCurrentTime(attestation); err != nil {
			return err
		}
	}
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
