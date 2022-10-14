package lightclient

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/fork"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/erigon/common"
	//"github.com/prysmaticlabs/go-bls"
)

const (
	MinSyncCommitteeParticipants = 1
	FinalizedRootIndex           = 105
)

var DomainSyncCommittee = common.Hex2Bytes("07000000")

func (l *LightClient) validateLegacyUpdate(update *cltypes.LightClientUpdate, finalityUpdate bool) (bool, error) {
	if update.SyncAggregate.Sum() < MinSyncCommitteeParticipants {
		return false, fmt.Errorf("not enough participants")
	}
	isNextSyncCommitteeKnown := l.store.nextSyncCommittee != nil
	// Check if the timings and slot are valid
	current_slot := utils.GetCurrentSlot(l.genesisConfig.GenesisTime, l.beaconConfig.SecondsPerSlot)
	if current_slot < update.SignatureSlot || update.SignatureSlot <= update.AttestedHeader.Slot ||
		update.AttestedHeader.Slot < update.FinalizedHeader.Slot {
		return false, fmt.Errorf("too far in the future")
	}
	storePeriod := (update.FinalizedHeader.Slot / 32) / 256
	storeSignaturePeriod := (update.SignatureSlot / 32) / 256

	if !isNextSyncCommitteeKnown {
		if storeSignaturePeriod != storePeriod && storeSignaturePeriod != storePeriod+1 {
			return false, fmt.Errorf("mismatching periods")
		}
	} else if storePeriod != storeSignaturePeriod {
		return false, fmt.Errorf("mismatching periods")
	}

	// Verify update is relevant
	attestedPeriod := (update.AttestedHeader.Slot / 32) / 256
	hasNextSyncCommittee := l.store.nextSyncCommittee == nil &&
		update.HasNextSyncCommittee() && attestedPeriod == storePeriod

	if update.AttestedHeader.Slot <= l.store.finalizedHeader.Slot && !hasNextSyncCommittee {
		return false, fmt.Errorf("invalid sync committee")
	}

	// Verify that the `finality_branch`, if present, confirms `finalized_header`
	if finalityUpdate {
		finalizedRoot, err := update.FinalizedHeader.HashTreeRoot()
		if err != nil {
			return false, err
		}
		if !isValidMerkleBranch(
			finalizedRoot,
			update.FinalityBranch,
			6,  // floorlog2(FINALIZED_ROOT_INDEX)
			41, // get_subtree_index(FINALIZED_ROOT_INDEX),
			update.AttestedHeader.Root,
		) {
			return false, fmt.Errorf("update is not part of the merkle tree")
		}
	}
	if update.HasNextSyncCommittee() {
		if attestedPeriod == storePeriod && isNextSyncCommitteeKnown &&
			!update.NextSyncCommitee.Equal(l.store.nextSyncCommittee) {
			return false, fmt.Errorf("mismatching sync committee")
		}
		syncRoot, err := update.NextSyncCommitee.HashTreeRoot()
		if err != nil {
			return false, err
		}
		if !isValidMerkleBranch(
			syncRoot,
			update.NextSyncCommitteeBranch,
			5,  // floorlog2(NEXT_SYNC_COMMITTEE_INDEX)
			23, // get_subtree_index(NEXT_SYNC_COMMITTEE_INDEX),
			update.AttestedHeader.Root,
		) {
			return false, fmt.Errorf("sync committee is not part of the merkle tree")
		}
	}
	var syncCommittee *cltypes.SyncCommittee
	if storeSignaturePeriod == storePeriod {
		syncCommittee = l.store.currentSyncCommittee
	} else {
		syncCommittee = l.store.nextSyncCommittee
	}
	syncAggregateBits := update.SyncAggregate.SyncCommiteeBits

	var pubkeys [][48]byte
	currPubKeyIndex := 0
	for i := range syncAggregateBits {
		for bit := 1; bit <= 128; bit *= 2 {
			if syncAggregateBits[i]&byte(bit) > 0 {
				pubkeys = append(pubkeys, syncCommittee.PubKeys[currPubKeyIndex])
			}
			currPubKeyIndex++
		}
	}
	// Support only post-bellatrix forks
	forkVersion := fork.GetLastFork(l.beaconConfig, l.genesisConfig)
	domain, err := fork.ComputeDomain(DomainSyncCommittee, forkVersion, l.genesisConfig)
	if err != nil {
		return false, err
	}
	// Computing signing root
	signingRoot, err := fork.ComputeSigningRoot(update.AttestedHeader, domain)
	if err != nil {
		return false, err
	}
	// Create signature objects
	/*var signature bls.Sign
	if err := signature.Deserialize(update.SyncAggregate.SyncCommiteeSignature[:]); err != nil {
		return false, err
	}
	// Aggreggate public keys
	var pubKey bls.PublicKey
	for _, key := range pubkeys {
		var currPubKey bls.PublicKey
		if err := currPubKey.Deserialize(key[:]); err != nil {
			return false, err
		}
		pubKey.Add(&currPubKey)
	}*/
	_ = signingRoot
	_ = pubkeys
	return true, nil
	// return signature.Verify(&pubKey, signingRoot[:]), nil
}

func (l *LightClient) validateOptimisticUpdate(update *cltypes.LightClientOptimisticUpdate) (bool, error) {
	return true, nil
}

func (l *LightClient) validateFinalityUpdate(update *cltypes.LightClientFinalityUpdate) (bool, error) {
	return true, nil
}

/*def validate_light_client_update(store: LightClientStore,
                               update: LightClientUpdate,
                               current_slot: Slot,
                               genesis_validators_root: Root) -> None:
  # Verify sync committee has sufficient participants
  sync_aggregate = update.sync_aggregate
  assert sum(sync_aggregate.sync_committee_bits) >= MIN_SYNC_COMMITTEE_PARTICIPANTS

  # Verify update does not skip a sync committee period
  assert current_slot >= update.signature_slot > update.attested_header.slot >= update.finalized_header.slot
  store_period = compute_sync_committee_period_at_slot(store.finalized_header.slot)
  update_signature_period = compute_sync_committee_period_at_slot(update.signature_slot)
  if is_next_sync_committee_known(store):
      assert update_signature_period in (store_period, store_period + 1)
  else:
      assert update_signature_period == store_period

  # Verify update is relevant
  update_attested_period = compute_sync_committee_period_at_slot(update.attested_header.slot)
  update_has_next_sync_committee = not is_next_sync_committee_known(store) and (
      is_sync_committee_update(update) and update_attested_period == store_period
  )
  assert (
      update.attested_header.slot > store.finalized_header.slot
      or update_has_next_sync_committee
  )

  # Verify that the `finality_branch`, if present, confirms `finalized_header`
  # to match the finalized checkpoint root saved in the state of `attested_header`.
  # Note that the genesis finalized checkpoint root is represented as a zero hash.
  if not is_finality_update(update):
      assert update.finalized_header == BeaconBlockHeader()
  else:
      if update.finalized_header.slot == GENESIS_SLOT:
          assert update.finalized_header == BeaconBlockHeader()
          finalized_root = Bytes32()
      else:
          finalized_root = hash_tree_root(update.finalized_header)
      assert is_valid_merkle_branch(
          leaf=finalized_root,
          branch=update.finality_branch,
          depth=floorlog2(FINALIZED_ROOT_INDEX),
          index=get_subtree_index(FINALIZED_ROOT_INDEX),
          root=update.attested_header.state_root,
      )

  # Verify that the `next_sync_committee`, if present, actually is the next sync committee saved in the
  # state of the `attested_header`
  if not is_sync_committee_update(update):
      assert update.next_sync_committee == SyncCommittee()
  else:
      if update_attested_period == store_period and is_next_sync_committee_known(store):
          assert update.next_sync_committee == store.next_sync_committee
      assert is_valid_merkle_branch(
          leaf=hash_tree_root(update.next_sync_committee),
          branch=update.next_sync_committee_branch,
          depth=floorlog2(NEXT_SYNC_COMMITTEE_INDEX),
          index=get_subtree_index(NEXT_SYNC_COMMITTEE_INDEX),
          root=update.attested_header.state_root,
      )

  # Verify sync committee aggregate signature
  if update_signature_period == store_period:
      sync_committee = store.current_sync_committee
  else:
      sync_committee = store.next_sync_committee
  participant_pubkeys = [
      pubkey for (bit, pubkey) in zip(sync_aggregate.sync_committee_bits, sync_committee.pubkeys)
      if bit
  ]
  fork_version = compute_fork_version(compute_epoch_at_slot(update.signature_slot))
  domain = compute_domain(DOMAIN_SYNC_COMMITTEE, fork_version, genesis_validators_root)
  signing_root = compute_signing_root(update.attested_header, domain)
  assert bls.FastAggregateVerify(participant_pubkeys, signing_root, sync_aggregate.sync_committee_signature)*/
