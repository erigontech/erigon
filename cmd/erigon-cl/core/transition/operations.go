package transition

import (
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func IsSlashableValidator(validator *cltypes.Validator, epoch uint64) bool {
	return !validator.Slashed && (validator.ActivationEpoch <= epoch) && (epoch < validator.WithdrawableEpoch)
}

func IsSlashableAttestationData(d1, d2 *cltypes.AttestationData) (bool, error) {
	hash1, err := d1.HashTreeRoot()
	if err != nil {
		return false, fmt.Errorf("unable to hash attestation data 1: %v", err)
	}

	hash2, err := d2.HashTreeRoot()
	if err != nil {
		return false, fmt.Errorf("unable to hash attestation data 2: %v", err)
	}

	return (hash1 != hash2 && d1.Target.Epoch == d2.Target.Epoch) || (d1.Source.Epoch < d2.Source.Epoch && d2.Target.Epoch < d1.Target.Epoch), nil
}

func IsSortedSet(vals []uint64) bool {
	for i := 0; i < len(vals)-1; i++ {
		if vals[i] >= vals[i+1] {
			return false
		}
	}
	return true
}

func GetSetIntersection(v1, v2 []uint64) []uint64 {
	intersection := []uint64{}
	present := map[uint64]bool{}

	for _, v := range v1 {
		present[v] = true
	}

	for _, v := range v2 {
		if present[v] {
			intersection = append(intersection, v)
		}
	}
	return intersection
}

func IsValidIndexedAttestation(state *state.BeaconState, att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if len(inds) == 0 || !IsSortedSet(inds) {
		return false, fmt.Errorf("invalid attesting indices")
	}

	pks := [][]byte{}
	for _, v := range inds {
		val := state.ValidatorAt(int(v))
		pks = append(pks, val.PublicKey[:])
	}

	domain, err := GetDomain(state, clparams.MainnetBeaconConfig.DomainBeaconAttester, att.Data.Target.Epoch)
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	valid, err := bls.VerifyAggregate(att.Signature[:], signingRoot[:], pks)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, fmt.Errorf("invalid aggregate signature")
	}
	return true, nil
}

func ProcessProposerSlashing(state *state.BeaconState, propSlashing *cltypes.ProposerSlashing) error {
	h1 := propSlashing.Header1.Header
	h2 := propSlashing.Header2.Header

	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf("non-matching proposer indices proposer slashing: %d != %d", h1.ProposerIndex, h2.ProposerIndex)
	}

	h1Root, err := h1.HashTreeRoot()
	if err != nil {
		return fmt.Errorf("unable to hash header1: %v", err)
	}
	h2Root, err := h2.HashTreeRoot()
	if err != nil {
		return fmt.Errorf("unable to hash header2: %v", err)
	}
	if h1Root == h2Root {
		return fmt.Errorf("propose slashing headers are the same: %v == %v", h1Root, h2Root)
	}

	proposer := state.ValidatorAt(int(h1.ProposerIndex))
	if !IsSlashableValidator(proposer, GetEpochAtSlot(state.Slot())) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}

	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{propSlashing.Header1, propSlashing.Header2} {
		domain, err := GetDomain(state, clparams.MainnetBeaconConfig.DomainBeaconProposer, GetEpochAtSlot(signedHeader.Header.Slot))
		if err != nil {
			return fmt.Errorf("unable to get domain: %v", err)
		}
		signingRoot, err := fork.ComputeSigningRoot(signedHeader.Header, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %v", err)
		}
		valid, err := bls.Verify(signedHeader.Signature[:], signingRoot[:], proposer.PublicKey[:])
		if err != nil {
			return fmt.Errorf("unable to verify signature: %v", err)
		}
		if !valid {
			return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", signedHeader.Signature[:], signingRoot[:], proposer.PublicKey[:])
		}
	}

	// Set whistleblower index to 0 so current proposer gets reward.
	SlashValidator(state, h1.ProposerIndex, 0)
	return nil
}

func ProcessAttesterSlashing(state *state.BeaconState, attSlashing *cltypes.AttesterSlashing) error {
	att1 := attSlashing.Attestation_1
	att2 := attSlashing.Attestation_2

	slashable, err := IsSlashableAttestationData(att1.Data, att2.Data)
	if err != nil {
		return fmt.Errorf("unable to determine if attestation data was slashable: %v", err)
	}
	if !slashable {
		return fmt.Errorf("attestation data not slashable: %+v; %+v", att1.Data, att2.Data)
	}

	valid, err := IsValidIndexedAttestation(state, att1)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 1 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 1")
	}

	valid, err = IsValidIndexedAttestation(state, att2)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 2 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 2")
	}

	slashedAny := false
	indices := GetSetIntersection(att1.AttestingIndices, att2.AttestingIndices)
	for _, ind := range indices {
		if IsSlashableValidator(state.ValidatorAt(int(ind)), GetEpochAtSlot(state.Slot())) {
			err := SlashValidator(state, ind, 0)
			if err != nil {
				return fmt.Errorf("unable to slash validator: %d", ind)
			}
			slashedAny = true
		}
	}

	if !slashedAny {
		return fmt.Errorf("no validators slashed")
	}
	return nil
}
