package forkchoice

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/cache"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// OnAttestation processes incoming attestations.
func (f *ForkChoiceStore) OnAttestation(attestation *solid.Attestation, fromBlock bool) error {
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
	return nil
}

// scheduleAttestationForLaterProcessing scheudules an attestation for later processing
func (f *ForkChoiceStore) scheduleAttestationForLaterProcessing(attestation *solid.Attestation, fromBlock bool) {
	go func() {
		logInterval := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-f.ctx.Done():
				return
			case <-logInterval.C:
				if f.Slot() < attestation.AttestantionData().Slot()+1 {
					continue
				}
				if err := f.OnAttestation(attestation, false); err != nil {
					log.Trace("could not process scheduled attestation", "reason", err)
				}
				return
			}
		}
	}()
}

func (f *ForkChoiceStore) processAttestingIndicies(attestation *solid.Attestation, indicies []uint64) {
	beaconBlockRoot := attestation.AttestantionData().BeaconBlockRoot()
	target := attestation.AttestantionData().Target()

	for _, index := range indicies {
		if _, ok := f.equivocatingIndicies[index]; ok {
			continue
		}
		validatorMessage, has := f.latestMessages[index]
		if !has || target.Epoch() > validatorMessage.Epoch {
			f.latestMessages[index] = &LatestMessage{
				Epoch: target.Epoch(),
				Root:  beaconBlockRoot,
			}
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
	if currentEpoch <= f.forkGraph.Config().GenesisEpoch {
		previousEpoch = f.forkGraph.Config().GenesisEpoch
	}
	if target.Epoch() == currentEpoch || target.Epoch() == previousEpoch {
		return nil
	}
	return fmt.Errorf("verification of attestation against current time failed")
}
