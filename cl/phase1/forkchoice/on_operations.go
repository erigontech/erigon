package forkchoice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/network/subnets"
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
		return nil
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
		return nil
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

// verifySyncContributionProof verifies the sync contribution proof.
func (f *ForkChoiceStore) verifySyncContributionSelectionProof(s *state.CachingBeaconState, contributionAndProof *cltypes.ContributionAndProof) error {
	syncAggregatorSelectionData := &cltypes.SyncAggregatorSelectionData{
		Slot:              contributionAndProof.Contribution.Slot,
		SubcommitteeIndex: contributionAndProof.Contribution.SubcommitteeIndex,
	}
	selectionProof := contributionAndProof.SelectionProof

	aggregatorPubKey, err := s.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
	if err != nil {
		return err
	}

	domain, err := s.GetDomain(s.BeaconConfig().DomainSyncCommitteeSelectionProof, state.GetEpochAtSlot(s.BeaconConfig(), contributionAndProof.Contribution.Slot))
	if err != nil {
		return err
	}

	selectionDataRoot, err := fork.ComputeSigningRoot(syncAggregatorSelectionData, domain)
	if err != nil {
		return err
	}

	valid, err := bls.Verify(selectionProof[:], selectionDataRoot[:], aggregatorPubKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid selectionProof signature")
	}
	return nil
}

// verifySyncContributionProof verifies the contribution aggregated signature.
func (f *ForkChoiceStore) verifySyncContributionProofAggregatedSignature(s *state.CachingBeaconState, contribution *cltypes.Contribution, subCommitteeKeys []libcommon.Bytes48) error {
	domain, err := s.GetDomain(s.BeaconConfig().DomainSyncCommittee, state.Epoch(s))
	if err != nil {
		return err
	}

	msg := utils.Sha256(contribution.BeaconBlockRoot[:], domain)
	if err != nil {
		return err
	}
	// only use the ones pertaining to the aggregation bits
	subCommitteePubsKeys := make([][]byte, 0, len(subCommitteeKeys))
	for i, key := range subCommitteeKeys {
		if utils.IsBitOn(contribution.AggregationBits, i) {
			subCommitteePubsKeys = append(subCommitteePubsKeys, common.Copy(key[:]))
		}
	}

	valid, err := bls.VerifyAggregate(contribution.Signature[:], msg[:], subCommitteePubsKeys)
	if err != nil {
		return err
	}

	if !valid {
		return fmt.Errorf("invalid signature for aggregate sync contribution")
	}
	return nil
}

// wasContributionSeen checks if the contribution was seen before.
func (f *ForkChoiceStore) wasContributionSeen(contribution *cltypes.ContributionAndProof) bool {
	key := seenSyncCommitteeContribution{
		aggregatorIndex:   contribution.AggregatorIndex,
		slot:              contribution.Contribution.Slot,
		subCommitteeIndex: contribution.Contribution.SubcommitteeIndex,
	}

	_, ok := f.seenSyncCommitteeContributions[key]
	return ok
}

// markContributionAsSeen marks the contribution as seen.
func (f *ForkChoiceStore) markContributionAsSeen(contribution *cltypes.ContributionAndProof) {
	key := seenSyncCommitteeContribution{
		aggregatorIndex:   contribution.AggregatorIndex,
		slot:              contribution.Contribution.Slot,
		subCommitteeIndex: contribution.Contribution.SubcommitteeIndex,
	}
	f.seenSyncCommitteeContributions[key] = struct{}{}
}

// OnSignedContributionAndProof is the handler for signed contribution and proof operations. it pushes the signed contribution and proof in the pool.
func (f *ForkChoiceStore) OnSignedContributionAndProof(signedContribution *cltypes.SignedContributionAndProof, test bool) error {
	f.muSyncContribution.Lock()
	defer f.muSyncContribution.Unlock()

	contributionAndProof := signedContribution.Message
	selectionProof := contributionAndProof.SelectionProof
	aggregationBits := contributionAndProof.Contribution.AggregationBits

	headState := f.syncedDataManager.HeadState()
	if headState == nil {
		return nil
	}

	aggregatorPubKey, err := headState.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
	if err != nil {
		return err
	}
	subcommiteePubsKeys, err := f.getSyncSubcommitteePubkeys(headState, contributionAndProof.Contribution.SubcommitteeIndex)
	if err != nil {
		return err
	}

	// [IGNORE] The contribution's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. contribution.slot == current_slot.
	if !utils.IsCurrentSlotWithMaximumClockDisparity(headState.GenesisTime(), f.beaconCfg.SecondsPerSlot, contributionAndProof.Contribution.Slot) {
		return nil
	}

	// [REJECT] The subcommittee index is in the allowed range, i.e. contribution.subcommittee_index < SYNC_COMMITTEE_SUBNET_COUNT.
	if contributionAndProof.Contribution.SubcommitteeIndex >= clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount {
		return fmt.Errorf("subcommittee index is out of range")
	}

	// [REJECT] The contribution has participants -- that is, any(contribution.aggregation_bits).
	if bytes.Equal(aggregationBits, make([]byte, len(aggregationBits))) { // check if the aggregation bits are all zeros
		return fmt.Errorf("contribution has no participants")
	}

	// [REJECT] contribution_and_proof.selection_proof selects the validator as an aggregator for the slot -- i.e. is_sync_committee_aggregator(contribution_and_proof.selection_proof) returns True.
	if len(selectionProof) != 96 {
		return fmt.Errorf("incorrect signiture length")
	}
	modulo := utils.Max64(1, f.beaconCfg.SyncCommitteeSize/f.beaconCfg.SyncCommitteeSubnetCount/f.beaconCfg.TargetAggregatorsPerSyncSubcommittee)
	hashSignature := utils.Sha256(selectionProof[:])
	if binary.LittleEndian.Uint64(hashSignature[:8])%modulo != 0 {
		return fmt.Errorf("selects the validator as an aggregator")
	}

	// [REJECT] The aggregator's validator index is in the declared subcommittee of the current sync committee -- i.e. state.validators[contribution_and_proof.aggregator_index].pubkey in get_sync_subcommittee_pubkeys(state, contribution.subcommittee_index).
	if !slices.Contains(subcommiteePubsKeys, aggregatorPubKey) {
		return fmt.Errorf("aggregator's validator index is not in subcommittee")
	}

	// [IGNORE] The sync committee contribution is the first valid contribution received for the aggregator with index contribution_and_proof.aggregator_index for the slot contribution.slot and subcommittee index contribution.subcommittee_index (this requires maintaining a cache of size SYNC_COMMITTEE_SIZE for this topic that can be flushed after each slot).
	if f.wasContributionSeen(contributionAndProof) {
		return nil
	}

	// [REJECT] The contribution_and_proof.selection_proof is a valid signature of the SyncAggregatorSelectionData derived from the contribution by the validator with index contribution_and_proof.aggregator_index.
	if err := f.verifySyncContributionSelectionProof(headState, contributionAndProof); err != nil {
		return err
	}
	// [REJECT] The aggregator signature, signed_contribution_and_proof.signature, is valid.
	if err := verifyAggregatorSignatureForSyncContribution(headState, signedContribution); err != nil {
		return err
	}
	// [REJECT] The aggregate signature is valid for the message beacon_block_root and aggregate pubkey derived
	// from the participation info in aggregation_bits for the subcommittee specified by the contribution.subcommittee_index.
	if err := f.verifySyncContributionProofAggregatedSignature(headState, contributionAndProof.Contribution, subcommiteePubsKeys); err != nil {
		return err
	}
	// mark the valid contribution as seen
	f.markContributionAsSeen(contributionAndProof)

	// emit contribution_and_proof
	f.emitters.Publish("contribution_and_proof", signedContribution)
	// add the contribution to the pool
	return f.syncContributionPool.AddSyncContribution(headState, contributionAndProof.Contribution)

}

func verifySyncCommitteeMessageSignature(s *state.CachingBeaconState, msg *cltypes.SyncCommitteeMessage) error {
	publicKey, err := s.ValidatorPublicKey(int(msg.ValidatorIndex))
	if err != nil {
		return err
	}
	cfg := s.BeaconConfig()
	domain, err := s.GetDomain(cfg.DomainSyncCommittee, state.Epoch(s))
	if err != nil {
		return err
	}
	signingRoot, err := utils.Sha256(msg.BeaconBlockRoot[:], domain), nil
	if err != nil {
		return err
	}
	valid, err := bls.Verify(msg.Signature[:], signingRoot[:], publicKey[:])
	if err != nil {
		return errors.New("invalid signature")
	}
	if !valid {
		return errors.New("invalid signature")
	}
	return nil
}

func verifyAggregatorSignatureForSyncContribution(s *state.CachingBeaconState, signedContributionAndProof *cltypes.SignedContributionAndProof) error {
	contribution := signedContributionAndProof.Message.Contribution
	domain, err := s.GetDomain(s.BeaconConfig().DomainContributionAndProof, contribution.Slot/s.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return err
	}

	signingRoot, err := fork.ComputeSigningRoot(signedContributionAndProof.Message, domain)
	if err != nil {
		return err
	}
	aggregatorPubKey, err := s.ValidatorPublicKey(int(signedContributionAndProof.Message.AggregatorIndex))
	if err != nil {
		return err
	}
	valid, err := bls.Verify(signedContributionAndProof.Signature[:], signingRoot[:], aggregatorPubKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid aggregator signature")
	}
	return nil
}

func (f *ForkChoiceStore) cleanupOldSyncCommitteeMessages() {
	headSlot := f.syncedDataManager.HeadSlot()
	for k := range f.seenSyncCommitteeMessages {
		if headSlot != k.slot {
			delete(f.seenSyncCommitteeMessages, k)
		}
	}
}

func (f *ForkChoiceStore) OnSyncCommitteeMessage(msg *cltypes.SyncCommitteeMessage, subnetID uint64) error {
	f.muSyncCommitteeMessages.Lock()
	defer f.muSyncCommitteeMessages.Unlock()
	if f.syncedDataManager.Syncing() {
		return nil
	}
	headState := f.syncedDataManager.HeadState()
	// [IGNORE] The message's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. sync_committee_message.slot == current_slot.
	if !utils.IsCurrentSlotWithMaximumClockDisparity(headState.GenesisTime(), f.beaconCfg.SecondsPerSlot, msg.Slot) {
		return nil
	}

	// [REJECT] The subnet_id is valid for the given validator, i.e. subnet_id in compute_subnets_for_sync_committee(state, sync_committee_message.validator_index).
	// Note this validation implies the validator is part of the broader current sync committee along with the correct subcommittee.
	subnets, err := subnets.ComputeSubnetsForSyncCommittee(headState, msg.ValidatorIndex)
	if err != nil {
		return err
	}
	seenSyncCommitteeMessageIdentifier := seenSyncCommitteeMessage{
		subnet:         subnetID,
		slot:           msg.Slot,
		validatorIndex: msg.ValidatorIndex,
	}

	if !slices.Contains(subnets, subnetID) {
		return fmt.Errorf("validator is not into any subnet %d", subnetID)
	}
	// [IGNORE] There has been no other valid sync committee message for the declared slot for the validator referenced by sync_committee_message.validator_index.
	if _, ok := f.seenSyncCommitteeMessages[seenSyncCommitteeMessageIdentifier]; ok {
		return nil
	}
	// [REJECT] The signature is valid for the message beacon_block_root for the validator referenced by validator_index

	if err := verifySyncCommitteeMessageSignature(headState, msg); err != nil {
		return err
	}
	f.seenSyncCommitteeMessages[seenSyncCommitteeMessageIdentifier] = struct{}{}
	f.cleanupOldSyncCommitteeMessages() // cleanup old messages
	// Aggregate the message
	return f.syncContributionPool.AddSyncCommitteeMessage(headState, subnetID, msg)
}
