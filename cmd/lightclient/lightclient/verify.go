package lightclient

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/fork"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/erigon/common"
	blst "github.com/supranational/blst/bindings/go"
)

const MinSyncCommitteeParticipants = 1

var (
	DomainSyncCommittee = common.Hex2Bytes("07000000")
	dst                 = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")
)

func (l *LightClient) validateUpdate(update *cltypes.LightClientUpdate) (bool, error) {
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
	if update.IsFinalityUpdate() {
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
	_ = signingRoot
	signature, err := utils.SignatureFromBytes(update.SyncAggregate.SyncCommiteeSignature[:])
	if err != nil {
		return false, err
	}

	pks := []*blst.P1Affine{}

	for _, key := range pubkeys {
		pk, err := utils.PublicKeyFromBytes(key[:])
		if err != nil {
			return false, err
		}
		pks = append(pks, pk)
	}

	return signature.FastAggregateVerify(true, pks, signingRoot[:], dst), nil
}
