package forkchoice

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// NOTE: This file implements non-official handlers for other types of iterations. what it does is,using the forkchoices
// and verify external operations and eventually push them in the operations pool.

// OnVoluntaryExit is a non-official handler for voluntary exit operations. it pushes the voluntary exit in the pool.
func (f *ForkChoiceStore) OnVoluntaryExit(signedVoluntaryExit *cltypes.SignedVoluntaryExit, test bool) error {
	voluntaryExit := signedVoluntaryExit.VoluntaryExit
	if f.operationsPool.VoluntaryExistsPool.Has(voluntaryExit.ValidatorIndex) {
		f.emitters.Publish("voluntary_exit", voluntaryExit)
		return nil
	}

	s := f.syncedDataManager.HeadState()
	if s == nil {
		return nil
	}

	val, err := s.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return err
	}

	if val.ExitEpoch() != f.beaconCfg.FarFutureEpoch {
		return nil
	}

	pk := val.PublicKey()

	domainType := f.beaconCfg.DomainVoluntaryExit
	var domain []byte

	if s.Version() < clparams.DenebVersion {
		domain, err = s.GetDomain(domainType, voluntaryExit.Epoch)
	} else if s.Version() >= clparams.DenebVersion {
		domain, err = fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(s.BeaconConfig().CapellaForkVersion)), s.GenesisValidatorsRoot())
	}
	if err != nil {
		return err
	}

	signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return err
	}
	if !test {
		valid, err := bls.Verify(signedVoluntaryExit.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("ProcessVoluntaryExit: BLS verification failed")
		}
	}
	f.emitters.Publish("voluntary_exit", voluntaryExit)
	f.operationsPool.VoluntaryExistsPool.Insert(voluntaryExit.ValidatorIndex, signedVoluntaryExit)
	return nil
}

// OnProposerSlashing is a non-official handler for proposer slashing operations. it pushes the proposer slashing in the pool.
func (f *ForkChoiceStore) OnProposerSlashing(proposerSlashing *cltypes.ProposerSlashing, test bool) (err error) {
	if f.operationsPool.ProposerSlashingsPool.Has(pool.ComputeKeyForProposerSlashing(proposerSlashing)) {
		return nil
	}
	h1 := proposerSlashing.Header1.Header
	h2 := proposerSlashing.Header2.Header

	if h1.Slot != h2.Slot {
		return fmt.Errorf("non-matching slots on proposer slashing: %d != %d", h1.Slot, h2.Slot)
	}

	if h1.ProposerIndex != h2.ProposerIndex {
		return fmt.Errorf("non-matching proposer indices proposer slashing: %d != %d", h1.ProposerIndex, h2.ProposerIndex)
	}

	if *h1 == *h2 {
		return fmt.Errorf("proposee slashing headers are the same")
	}

	// Take lock as we interact with state.
	s := f.syncedDataManager.HeadState()
	if err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("no head state avaible")
	}
	proposer, err := s.ValidatorForValidatorIndex(int(h1.ProposerIndex))
	if err != nil {
		return fmt.Errorf("unable to retrieve state: %v", err)
	}
	if !proposer.IsSlashable(state.Epoch(s)) {
		return fmt.Errorf("proposer is not slashable: %v", proposer)
	}
	domain1, err := s.GetDomain(s.BeaconConfig().DomainBeaconProposer, state.GetEpochAtSlot(s.BeaconConfig(), h1.Slot))
	if err != nil {
		return fmt.Errorf("unable to get domain: %v", err)
	}
	domain2, err := s.GetDomain(s.BeaconConfig().DomainBeaconProposer, state.GetEpochAtSlot(s.BeaconConfig(), h2.Slot))
	if err != nil {
		return fmt.Errorf("unable to get domain: %v", err)
	}
	pk := proposer.PublicKey()
	if test {
		f.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(proposerSlashing), proposerSlashing)
		return nil
	}
	signingRoot, err := fork.ComputeSigningRoot(h1, domain1)
	if err != nil {
		return fmt.Errorf("unable to compute signing root: %v", err)
	}
	valid, err := bls.Verify(proposerSlashing.Header1.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return fmt.Errorf("unable to verify signature: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", proposerSlashing.Header1.Signature[:], signingRoot[:], pk)
	}
	signingRoot, err = fork.ComputeSigningRoot(h2, domain2)
	if err != nil {
		return fmt.Errorf("unable to compute signing root: %v", err)
	}

	valid, err = bls.Verify(proposerSlashing.Header2.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return fmt.Errorf("unable to verify signature: %v", err)
	}
	if !valid {
		return fmt.Errorf("invalid signature: signature %v, root %v, pubkey %v", proposerSlashing.Header2.Signature[:], signingRoot[:], pk)
	}
	f.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(proposerSlashing), proposerSlashing)

	return nil
}

func (f *ForkChoiceStore) OnBlsToExecutionChange(signedChange *cltypes.SignedBLSToExecutionChange, test bool) error {
	if f.operationsPool.BLSToExecutionChangesPool.Has(signedChange.Signature) {
		f.emitters.Publish("bls_to_execution_change", signedChange)
		return nil
	}
	change := signedChange.Message

	// Take lock as we interact with state.
	s := f.syncedDataManager.HeadState()
	if s == nil {
		return fmt.Errorf("no head state avaible")
	}
	validator, err := s.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return fmt.Errorf("unable to retrieve state: %v", err)
	}
	wc := validator.WithdrawalCredentials()

	if wc[0] != byte(f.beaconCfg.BLSWithdrawalPrefixByte) {
		return fmt.Errorf("invalid withdrawal credentials prefix")
	}
	genesisValidatorRoot := s.GenesisValidatorsRoot()
	// Perform full validation if requested.
	if !test {
		// Check the validator's withdrawal credentials against the provided message.
		hashedFrom := utils.Sha256(change.From[:])
		if !bytes.Equal(hashedFrom[1:], wc[1:]) {
			return fmt.Errorf("invalid withdrawal credentials")
		}

		// Compute the signing domain and verify the message signature.
		domain, err := fork.ComputeDomain(f.beaconCfg.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(uint32(f.beaconCfg.GenesisForkVersion)), genesisValidatorRoot)
		if err != nil {
			return err
		}
		signedRoot, err := fork.ComputeSigningRoot(change, domain)
		if err != nil {
			return err
		}
		valid, err := bls.Verify(signedChange.Signature[:], signedRoot[:], change.From[:])
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid signature")
		}
	}

	f.operationsPool.BLSToExecutionChangesPool.Insert(signedChange.Signature, signedChange)

	// emit bls_to_execution_change
	f.emitters.Publish("bls_to_execution_change", signedChange)
	return nil
}

func (f *ForkChoiceStore) OnSignedContributionAndProof(signedChange *cltypes.SignedContributionAndProof, test bool) error {
	if f.operationsPool.SignedContributionAndProofPool.Has(signedChange.Signature) {
		f.emitters.Publish("contribution_and_proof", signedChange)
		return nil
	}
	// TODO: implement the validation logic for the handler

	// Insert in the pool
	f.operationsPool.SignedContributionAndProofPool.Insert(signedChange.Signature, signedChange)

	// emit contribution_and_proof
	f.emitters.Publish("contribution_and_proof", signedChange)
	return nil
}
