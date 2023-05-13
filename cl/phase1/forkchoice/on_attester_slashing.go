package forkchoice

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (f *ForkChoiceStore) OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Check if these attestation is even slashable.
	attestation1 := attesterSlashing.Attestation_1
	attestation2 := attesterSlashing.Attestation_2
	if !cltypes.IsSlashableAttestationData(attestation1.Data, attestation2.Data) {
		return fmt.Errorf("attestation data is not slashable")
	}
	// Retrieve justified state
	s, err := f.forkGraph.GetState(f.justifiedCheckpoint.Root, false)
	if err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("justified checkpoint state not accessible")
	}
	// Verify validity of slashings
	valid, err := state.IsValidIndexedAttestation(s.BeaconState, attestation1)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 1 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 1")
	}

	valid, err = state.IsValidIndexedAttestation(s.BeaconState, attestation2)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 2 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 2")
	}
	for _, index := range utils.IntersectionOfSortedSets(attestation1.AttestingIndices, attestation2.AttestingIndices) {
		f.equivocatingIndicies[index] = struct{}{}
	}
	// add attestation indicies to equivocating indicies.
	return nil
}
