package forkchoice

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/cache"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// OnAttestation processes incoming attestations. TODO(Giulio2002): finish it with forward changesets.
func (f *ForkChoiceStore) OnAttestation(attestation *cltypes.Attestation, fromBlock bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.validateOnAttestation(attestation, fromBlock); err != nil {
		return err
	}
	target := attestation.Data.Target
	if cachedIndicies, ok := cache.LoadAttestatingIndicies(attestation.Data); ok {
		f.processAttestingIndicies(attestation, cachedIndicies)
		return nil
	}
	targetState, err := f.getCheckpointState(*target)
	if err != nil {
		return nil
	}
	// Verify attestation signature.
	if targetState == nil {
		return fmt.Errorf("target state does not exist")
	}
	// Now we need to find the attesting indicies.
	attestationIndicies, err := targetState.getAttestingIndicies(attestation.Data, attestation.AggregationBits)
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
	return nil
}

func (f *ForkChoiceStore) processAttestingIndicies(attestation *cltypes.Attestation, indicies []uint64) {
	beaconBlockRoot := attestation.Data.BeaconBlockHash
	target := attestation.Data.Target

	for _, index := range indicies {
		if _, ok := f.equivocatingIndicies[index]; ok {
			continue
		}
		validatorMessage, has := f.latestMessages[index]
		if !has || target.Epoch > validatorMessage.Epoch {
			f.latestMessages[index] = &LatestMessage{
				Epoch: target.Epoch,
				Root:  beaconBlockRoot,
			}
		}
	}
}

func (f *ForkChoiceStore) validateOnAttestation(attestation *cltypes.Attestation, fromBlock bool) error {
	target := attestation.Data.Target

	if !fromBlock {
		if err := f.validateTargetEpochAgainstCurrentTime(attestation); err != nil {
			return err
		}
	}
	if target.Epoch != f.computeEpochAtSlot(attestation.Data.Slot) {
		return fmt.Errorf("mismatching target epoch with slot data")
	}
	if _, has := f.forkGraph.GetHeader(target.Root); !has {
		return fmt.Errorf("target root is missing")
	}
	if blockHeader, has := f.forkGraph.GetHeader(attestation.Data.BeaconBlockHash); !has || blockHeader.Slot > attestation.Data.Slot {
		return fmt.Errorf("bad attestation data")
	}
	// LMD vote must be consistent with FFG vote target
	targetSlot := f.computeStartSlotAtEpoch(target.Epoch)
	ancestorRoot := f.Ancestor(attestation.Data.BeaconBlockHash, targetSlot)
	if ancestorRoot == (libcommon.Hash{}) {
		return fmt.Errorf("could not retrieve ancestor")
	}
	if ancestorRoot != target.Root {
		return fmt.Errorf("ancestor root mismatches with target")
	}
	if f.Slot() < attestation.Data.Slot+1 {
		return fmt.Errorf("future attestation")
	}
	return nil
}

func (f *ForkChoiceStore) validateTargetEpochAgainstCurrentTime(attestation *cltypes.Attestation) error {
	target := attestation.Data.Target
	// Attestations must be from the current or previous epoch
	currentEpoch := f.computeEpochAtSlot(f.Slot())
	// Use GENESIS_EPOCH for previous when genesis to avoid underflow
	previousEpoch := currentEpoch - 1
	if currentEpoch <= f.forkGraph.Config().GenesisEpoch {
		previousEpoch = f.forkGraph.Config().GenesisEpoch
	}
	if target.Epoch == currentEpoch || target.Epoch == previousEpoch {
		return nil
	}
	return fmt.Errorf("verification of attestation against current time failed")
}
