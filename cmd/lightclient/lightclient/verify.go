package lightclient

import (
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
)

const MinSyncCommitteeParticipants = 1

var DomainSyncCommittee = common.Hex2Bytes("07000000")

func (l *LightClient) validateUpdate(update *cltypes.LightClientUpdate) (bool, error) {
	if update.SyncAggregate.Sum() < MinSyncCommitteeParticipants {
		return false, fmt.Errorf("not enough participants")
	}
	isNextSyncCommitteeKnown := l.store.nextSyncCommittee != nil
	// Check if the timings and slot are valid
	current_slot := utils.GetCurrentSlot(l.genesisConfig.GenesisTime, l.beaconConfig.SecondsPerSlot)
	if current_slot < update.SignatureSlot || update.SignatureSlot <= update.AttestedHeader.HeaderEth2.Slot ||
		(update.IsFinalityUpdate() && update.AttestedHeader.HeaderEth2.Slot < update.FinalizedHeader.HeaderEth2.Slot) {
		return false, fmt.Errorf("too far in the future")
	}
	storePeriod := utils.SlotToPeriod(l.store.finalizedHeader.Slot)
	updateSignaturePeriod := utils.SlotToPeriod(update.SignatureSlot)

	if !isNextSyncCommitteeKnown &&
		updateSignaturePeriod != storePeriod && updateSignaturePeriod != storePeriod+1 {
		return false, fmt.Errorf("mismatching periods")
	}

	// Verify whether update is relevant
	attestedPeriod := utils.SlotToPeriod(update.AttestedHeader.HeaderEth2.Slot)
	hasNextSyncCommittee := l.store.nextSyncCommittee == nil &&
		update.HasNextSyncCommittee() && attestedPeriod == storePeriod

	if update.AttestedHeader.HeaderEth2.Slot <= l.store.finalizedHeader.Slot && !hasNextSyncCommittee {
		return false, fmt.Errorf("invalid sync committee")
	}

	// Verify that the `finality_branch`, if present, confirms `finalized_header`
	if update.IsFinalityUpdate() {
		finalizedRoot, err := update.FinalizedHeader.HeaderEth2.HashSSZ()
		if err != nil {
			return false, err
		}
		if !utils.IsValidMerkleBranch(
			finalizedRoot,
			update.FinalityBranch,
			6,  // floorlog2(FINALIZED_ROOT_INDEX)
			41, // get_subtree_index(FINALIZED_ROOT_INDEX),
			update.AttestedHeader.HeaderEth2.Root,
		) {
			return false, fmt.Errorf("update is not part of the merkle tree")
		}
	}
	if update.HasNextSyncCommittee() {
		if attestedPeriod == storePeriod && isNextSyncCommitteeKnown &&
			!update.NextSyncCommitee.Equal(l.store.nextSyncCommittee) {
			return false, fmt.Errorf("mismatching sync committee")
		}
		syncRoot, err := update.NextSyncCommitee.HashSSZ()
		if err != nil {
			return false, err
		}
		if !utils.IsValidMerkleBranch(
			syncRoot,
			update.NextSyncCommitteeBranch,
			5,  // floorlog2(NEXT_SYNC_COMMITTEE_INDEX)
			23, // get_subtree_index(NEXT_SYNC_COMMITTEE_INDEX),
			update.AttestedHeader.HeaderEth2.Root,
		) {
			return false, fmt.Errorf("sync committee is not part of the merkle tree")
		}
	}
	var syncCommittee *cltypes.SyncCommittee
	if updateSignaturePeriod == storePeriod {
		syncCommittee = l.store.currentSyncCommittee
	} else {
		syncCommittee = l.store.nextSyncCommittee
	}
	syncAggregateBits := update.SyncAggregate.SyncCommiteeBits

	var pubkeys [][]byte
	currPubKeyIndex := 0
	for i := range syncAggregateBits {
		for bit := 1; bit <= 128; bit *= 2 {
			if syncAggregateBits[i]&byte(bit) > 0 {
				pubkeys = append(pubkeys, syncCommittee.PubKeys[currPubKeyIndex][:])
			}
			currPubKeyIndex++
		}
	}
	// Support only post-bellatrix forks
	forkVersion := fork.GetLastFork(l.beaconConfig, l.genesisConfig)
	domain, err := fork.ComputeDomain(DomainSyncCommittee, forkVersion, l.genesisConfig.GenesisValidatorRoot)
	if err != nil {
		return false, err
	}
	// Computing signing root
	signingRoot, err := fork.ComputeSigningRoot(update.AttestedHeader.HeaderEth2, domain)
	if err != nil {
		return false, err
	}
	return bls.VerifyAggregate(update.SyncAggregate.SyncCommiteeSignature[:], signingRoot[:], pubkeys)
}
