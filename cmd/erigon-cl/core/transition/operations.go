package transition

import (
	"errors"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (s *StateTransistor) isSlashableAttestationData(d1, d2 *cltypes.AttestationData) bool {
	return (!d1.Equal(d2) && d1.Target.Epoch == d2.Target.Epoch) ||
		(d1.Source.Epoch < d2.Source.Epoch && d2.Target.Epoch < d1.Target.Epoch)
}

func (s *StateTransistor) isValidIndexedAttestation(att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if len(inds) == 0 || !utils.IsSliceSortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	for _, v := range inds {
		val, err := s.state.ValidatorAt(int(v))
		if err != nil {
			return false, err
		}
		pks = append(pks, val.PublicKey[:])
	}

	domain, err := s.state.GetDomain(clparams.MainnetBeaconConfig.DomainBeaconAttester, att.Data.Target.Epoch)
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

func (s *StateTransistor) ProcessProposerSlashing(propSlashing *cltypes.ProposerSlashing) error {
	h1 := propSlashing.Header1.Header
	h2 := propSlashing.Header2.Header

	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf("non-matching proposer indices proposer slashing: %d != %d", h1.ProposerIndex, h2.ProposerIndex)
	}

	h1Root, err := h1.HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to hash header1: %v", err)
	}
	h2Root, err := h2.HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to hash header2: %v", err)
	}
	if h1Root == h2Root {
		return fmt.Errorf("propose slashing headers are the same: %v == %v", h1Root, h2Root)
	}

	proposer, err := s.state.ValidatorAt(int(h1.ProposerIndex))
	if err != nil {
		return err
	}
	if !proposer.IsSlashable(s.state.Epoch()) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}

	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{propSlashing.Header1, propSlashing.Header2} {
		domain, err := s.state.GetDomain(s.beaconConfig.DomainBeaconProposer, s.state.GetEpochAtSlot(signedHeader.Header.Slot))
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
	s.state.SlashValidator(h1.ProposerIndex, nil)
	return nil
}

func (s *StateTransistor) ProcessAttesterSlashing(attSlashing *cltypes.AttesterSlashing) error {
	att1 := attSlashing.Attestation_1
	att2 := attSlashing.Attestation_2

	if !s.isSlashableAttestationData(att1.Data, att2.Data) {
		return fmt.Errorf("attestation data not slashable: %+v; %+v", att1.Data, att2.Data)
	}

	valid, err := s.isValidIndexedAttestation(att1)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 1 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 1")
	}

	valid, err = s.isValidIndexedAttestation(att2)
	if err != nil {
		return fmt.Errorf("error calculating indexed attestation 2 validity: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid indexed attestation 2")
	}

	slashedAny := false
	currentEpoch := s.state.GetEpochAtSlot(s.state.Slot())
	for _, ind := range utils.IntersectionOfSortedSets(att1.AttestingIndices, att2.AttestingIndices) {
		validator, err := s.state.ValidatorAt(int(ind))
		if err != nil {
			return err
		}
		if validator.IsSlashable(currentEpoch) {
			err := s.state.SlashValidator(ind, nil)
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

func (s *StateTransistor) ProcessDeposit(deposit *cltypes.Deposit) error {
	if deposit == nil {
		return nil
	}
	depositLeaf, err := deposit.Data.HashSSZ()
	if err != nil {
		return err
	}
	depositIndex := s.state.Eth1DepositIndex()
	eth1Data := s.state.Eth1Data()
	// Validate merkle proof for deposit leaf.
	if !s.noValidate && !utils.IsValidMerkleBranch(
		depositLeaf,
		deposit.Proof,
		s.beaconConfig.DepositContractTreeDepth+1,
		depositIndex,
		eth1Data.Root,
	) {
		return fmt.Errorf("processDepositForAltair: Could not validate deposit root")
	}

	// Increment index
	s.state.SetEth1DepositIndex(depositIndex + 1)
	publicKey := deposit.Data.PubKey
	amount := deposit.Data.Amount
	// Check if pub key is in validator set
	validatorIndex, has := s.state.ValidatorIndexByPubkey(publicKey)
	if !has {
		// Agnostic domain.
		domain, err := fork.ComputeDomain(s.beaconConfig.DomainDeposit[:], utils.Uint32ToBytes4(s.beaconConfig.GenesisForkVersion), [32]byte{})
		if err != nil {
			return err
		}
		depositMessageRoot, err := deposit.Data.MessageHash()
		if err != nil {
			return err
		}
		signedRoot := utils.Keccak256(depositMessageRoot[:], domain)
		// Perform BLS verification and if successful noice.
		valid, err := bls.Verify(deposit.Data.Signature[:], signedRoot[:], publicKey[:])
		if err != nil {
			return err
		}
		if !valid {
			return nil
		}
		// Append validator
		s.state.AddValidator(s.state.ValidatorFromDeposit(deposit), amount)
		// Altair forward
		if s.state.Version() >= clparams.AltairVersion {
			s.state.AddCurrentEpochParticipationFlags(cltypes.ParticipationFlags(0))
			s.state.AddPreviousEpochParticipationFlags(cltypes.ParticipationFlags(0))
			s.state.AddInactivityScore(0)
		}
		return nil
	}
	// Increase the balance if exists already
	return s.state.IncreaseBalance(validatorIndex, amount)
}

// ProcessVoluntaryExit takes a voluntary exit and applies state transition.
func (s *StateTransistor) ProcessVoluntaryExit(signedVoluntaryExit *cltypes.SignedVoluntaryExit) error {
	// Sanity checks so that we know it is good.
	voluntaryExit := signedVoluntaryExit.VolunaryExit
	currentEpoch := s.state.Epoch()
	validator, err := s.state.ValidatorAt(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return err
	}
	if !validator.Active(currentEpoch) {
		return errors.New("ProcessVoluntaryExit: validator is not active")
	}
	if validator.ExitEpoch != s.beaconConfig.FarFutureEpoch {
		return errors.New("ProcessVoluntaryExit: another exit for the same validator is already getting processed")
	}
	if currentEpoch < voluntaryExit.Epoch {
		return errors.New("ProcessVoluntaryExit: exit is happening in the future")
	}
	if currentEpoch < validator.ActivationEpoch+s.beaconConfig.ShardCommitteePeriod {
		return errors.New("ProcessVoluntaryExit: exit is happening too fast")
	}

	// We can skip it in some instances if we want to optimistically sync up.
	if !s.noValidate {
		domain, err := s.state.GetDomain(s.beaconConfig.DomainVoluntaryExit, voluntaryExit.Epoch)
		if err != nil {
			return err
		}
		signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
		if err != nil {
			return err
		}
		valid, err := bls.Verify(signedVoluntaryExit.Signature[:], signingRoot[:], validator.PublicKey[:])
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessVoluntaryExit: BLS verification failed")
		}
	}
	// Do the exit (same process in slashing).
	return s.state.InitiateValidatorExit(voluntaryExit.ValidatorIndex)
}
