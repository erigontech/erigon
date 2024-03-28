package forkchoice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
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

	contributionAndProof := signedChange.Message
	signature := signedChange.Signature

	//Take lock as we interact with state.
	s := f.syncedDataManager.HeadState()
	if s == nil {
		return fmt.Errorf("no head state avaible")
	}

	// [IGNORE] The contribution's slot is for the current slot (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance), i.e. contribution.slot == current_slot.
	messageTime := utils.GetSlotTime(s.GenesisTime(), f.beaconCfg.SecondsPerSlot, contributionAndProof.Contribution.Slot)
	slotTime := utils.GetSlotTime(s.GenesisTime(), f.beaconCfg.SecondsPerSlot, s.Slot())

	clockDisparity := clparams.NetworkConfigs[clparams.NetworkType(f.beaconCfg.DepositNetworkID)].MaximumGossipClockDisparity
	lowerBound := slotTime.Add(-clockDisparity)
	upperBound := slotTime.Add(clockDisparity)

	if messageTime.Before(lowerBound) {
		return fmt.Errorf("slot is too old")
	}
	if messageTime.After(upperBound) {
		return fmt.Errorf("slot is too new")
	}

	// [REJECT] The subcommittee index is in the allowed range, i.e. contribution.subcommittee_index < SYNC_COMMITTEE_SUBNET_COUNT.
	if contributionAndProof.Contribution.SubcommitteeIndex >= clparams.MainnetBeaconConfig.SyncCommitteeSubnetCount {
		return fmt.Errorf("subcommittee index is out of range")
	}

	// [REJECT] The contribution has participants -- that is, any(contribution.aggregation_bits).
	aggregationBits := contributionAndProof.Contribution.AggregationBits

	if aggregationBits == nil {
		return fmt.Errorf("aggretationBits is nil")
	}

	found := false
	for _, bit := range contributionAndProof.Contribution.AggregationBits {
		count := bits.OnesCount8(bit)
		if count != 0 {
			found = true
			break
		}
	}
	if !found {
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

	// [REJECT] The aggregator's validator index is in the declared subcommittee of the current sync committee -- i.e. state.validators[contribution_and_proof.aggregator_index].pubkey in get_sync_subcommittee_pubkeys(state, contribution.subcommittee_index).
	committee := s.CurrentSyncCommittee()
	committeeKeys := committee.GetCommittee()
	aggregatorPubKey, err := s.ValidatorPublicKey(int(contributionAndProof.AggregatorIndex))
	if err != nil {
		return err
	}

	subcommiteePubsKeys, err := f.getSyncSubcommitteePubkeys(committeeKeys, contributionAndProof.Contribution.SubcommitteeIndex)
	if err != nil {
		return err
	}

	inSubcommittee := false
	for _, pubKey := range subcommiteePubsKeys {
		if bytes.Equal(pubKey, aggregatorPubKey[:]) {
			inSubcommittee = true
			break
		}
	}
	if !inSubcommittee {
		return fmt.Errorf("aggregator's validator index is not in subcommittee1111")
	}

	// [IGNORE] The sync committee contribution is the first valid contribution received for the aggregator with index contribution_and_proof.aggregator_index for the slot contribution.slot and subcommittee index contribution.subcommittee_index (this requires maintaining a cache of size SYNC_COMMITTEE_SIZE for this topic that can be flushed after each slot).
	indexSlotKey := make([]byte, 24)
	indexSlotKey = binary.BigEndian.AppendUint64(indexSlotKey[0:], contributionAndProof.AggregatorIndex)
	indexSlotKey = binary.BigEndian.AppendUint64(indexSlotKey[8:], contributionAndProof.Contribution.Slot)
	indexSlotKey = binary.BigEndian.AppendUint64(indexSlotKey[16:], contributionAndProof.Contribution.SubcommitteeIndex)

	if f.operationsPool.IndexSlotHasSeen.Has(string(indexSlotKey)) {
		return nil
	}
	f.operationsPool.IndexSlotHasSeen.Insert(string(indexSlotKey), true)

	// [REJECT] The contribution_and_proof.selection_proof is a valid signature of the SyncAggregatorSelectionData derived from the contribution by the validator with index contribution_and_proof.aggregator_index.
	syncAggregatorSelectionData := &cltypes.SyncAggregatorSelectionData{
		Slot:              contributionAndProof.Contribution.Slot,
		SubcommitteeIndex: contributionAndProof.Contribution.SubcommitteeIndex,
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

	// [REJECT] The aggregator signature, signed_contribution_and_proof.signature, is valid.
	domain, err = s.GetDomain(s.BeaconConfig().DomainContributionAndProof, state.GetEpochAtSlot(s.BeaconConfig(), contributionAndProof.Contribution.Slot))
	if err != nil {
		return err
	}

	aggregatorSignatureRoot, err := fork.ComputeSigningRoot(signedChange.Message, domain)
	if err != nil {
		return err
	}

	valid, err = bls.Verify(signature[:], aggregatorSignatureRoot[:], aggregatorPubKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid aggregator signature")
	}

	// [REJECT] The aggregate signature is valid for the message beacon_block_root and aggregate pubkey derived
	// from the participation info in aggregation_bits for the subcommittee specified by the contribution.subcommittee_index.
	message := contributionAndProof.Contribution.BeaconBlockRoot[:]
	domain, err = s.GetDomain(s.BeaconConfig().DomainSyncCommittee, state.GetEpochAtSlot(s.BeaconConfig(), contributionAndProof.Contribution.Slot))
	if err != nil {
		return err
	}
	hash, err := merkle_tree.HashTreeRoot(message)
	if err != nil {
		return err
	}

	signatureRoot := utils.Sha256(hash[:], domain)
	if err != nil {
		return err
	}

	if len(subcommiteePubsKeys) != utils.GetBitlistLength(aggregationBits)+1 {
		return fmt.Errorf("invalid length of aggregation bits or public keys")
	}

	valid, err = bls.VerifyAggregate(signature[:], signatureRoot[:], subcommiteePubsKeys)
	if err != nil {
		return err
	}

	if !valid {
		//FIXME: Add more debug info
		fmt.Println("invalid aggregate signature")
		// return fmt.Errorf("invalid aggregate signature")
	}

	// Insert in the pool
	f.operationsPool.SignedContributionAndProofPool.Insert(signedChange.Signature, signedChange)

	// emit contribution_and_proof
	f.emitters.Publish("contribution_and_proof", signedChange)
	return nil
}
