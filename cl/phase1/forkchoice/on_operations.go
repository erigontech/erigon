package forkchoice

import (
	"bytes"
	"encoding/binary"
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
		domain, err = fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(s.BeaconConfig().CapellaForkVersion), s.GenesisValidatorsRoot())
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

	if wc[0] != f.beaconCfg.BLSWithdrawalPrefixByte {
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
		domain, err := fork.ComputeDomain(f.beaconCfg.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(f.beaconCfg.GenesisForkVersion), genesisValidatorRoot)
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

	contributionAndProof := signedChange.Message
	signiture := signedChange.Signature
	fmt.Println("Printing Debug for contributionAndProof", contributionAndProof)
	fmt.Println("Printing Debug for signiture", signiture)

	//Take lock as we interact with state.
	s := f.syncedDataManager.HeadState()
	if s == nil {
		return fmt.Errorf("no head state avaible")
	}
	// https: //github.com/ethereum/consensus-specs/blob/dev/specs/altair/p2p-interface.md#sync_committee_contribution_and_proof

	// [IGNORE] The contribution's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. contribution.slot == current_slot.
	maximumGossipClockDisparity := uint64(clparams.NetworkConfigs[clparams.MainnetNetwork].MaximumGossipClockDisparity)
	currentSlot := s.Slot()
	if contributionAndProof.Contribution.Slot != currentSlot && contributionAndProof.Contribution.Slot > currentSlot+maximumGossipClockDisparity {
		return nil
	}

	// [REJECT] The subcommittee index is in the allowed range, i.e. contribution.subcommittee_index < SYNC_COMMITTEE_SUBNET_COUNT.
	if contributionAndProof.Contribution.SubcommitteeIndex >= clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount {
		return fmt.Errorf("subcommittee index is out of range")
	}

	// [REJECT] The contribution has participants -- that is, any(contribution.aggregation_bits).
	aggregationBits := contributionAndProof.Contribution.AggregationBits
	if aggregationBits == nil {
		return fmt.Errorf("contribution has no participants")
	}

	found := false
	for _, bit := range contributionAndProof.Contribution.AggregationBits {
		if bit != 0 {
			found = true
			break
		}
	}
	if aggregationBits == nil || !found {
		return fmt.Errorf("contribution has no participants")
	}

	// [REJECT] contribution_and_proof.selection_proof selects the validator as an aggregator for the slot -- i.e. is_sync_committee_aggregator(contribution_and_proof.selection_proof) returns True.
	selectionProof := contributionAndProof.SelectionProof
	if len(selectionProof) != 96 {
		return fmt.Errorf("incorrect signiture length")
	}
	modulo := utils.Max64(1, f.beaconCfg.SyncCommitteeSize/f.beaconCfg.SyncCommitteeSubnetCount/f.beaconCfg.TargetAggregatorsPerSyncSubcommittee)
	hashSignature := utils.Sha256(selectionProof[:])
	if binary.LittleEndian.Uint64(hashSignature[:8])%modulo != 0 {
		return fmt.Errorf("selects the validator as an aggregator")
	}

	// [REJECT] The aggregator's validator index is in the declared subcommittee of the current sync committee
	// -- i.e. state.validators[contribution_and_proof.aggregator_index].pubkey in get_sync_subcommittee_pubkeys(state, contribution.subcommittee_index).
	committee := s.CurrentSyncCommittee()
	committeeKeys := committee.GetCommittee()
	aggregatorPubKey, err := s.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
	if err != nil {
		return err
	}
	inSubcommittee := false
	for _, pubKey := range committeeKeys {
		if bytes.Equal(pubKey[:], aggregatorPubKey[:]) {
			inSubcommittee = true
			break
		}
	}
	if !inSubcommittee {
		return fmt.Errorf("aggregator's validator index is not in subcommittee")
	}

	// [IGNORE] A valid sync committee contribution with equal slot, beacon_block_root and subcommittee_index whose aggregation_bits is non-strict superset has not already been seen.
	// [IGNORE] The sync committee contribution is the first valid contribution received for the aggregator with index contribution_and_proof.aggregator_index for the slot contribution.slot and subcommittee index contribution.subcommittee_index (this requires maintaining a cache of size SYNC_COMMITTEE_SIZE for this topic that can be flushed after each slot).
	// [REJECT] The contribution_and_proof.selection_proof is a valid signature of the SyncAggregatorSelectionData derived from the contribution by the validator with index contribution_and_proof.aggregator_index.
	// [REJECT] The aggregator signature, signed_contribution_and_proof.signature, is valid.
	// [REJECT] The aggregate signature is valid for the message beacon_block_root and aggregate pubkey derived from the participation info in aggregation_bits for the subcommittee specified by the contribution.subcommittee_index.

	// Insert in the pool
	f.operationsPool.SignedContributionAndProofPool.Insert(signedChange.Signature, signedChange)

	// emit contribution_and_proof
	f.emitters.Publish("contribution_and_proof", signedChange)
	return nil
}
