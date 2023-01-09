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
