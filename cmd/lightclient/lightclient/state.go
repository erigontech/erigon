package lightclient

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/log/v3"
)

type LightClientEvent interface {
}

type LightState struct {

	// none of the fields below are protected by a mutex.
	// the original bootstrap a ala trusted block root
	bootstrap *lightrpc.LightClientBootstrap
	genesis   [32]byte

	// channel of events
	events chan LightClientEvent

	// light client state https://github.com/ethereum/consensus-specs/blob/dev/specs/altair/light-client/sync-protocol.md#lightclientstore
	finalized_header                 *lightrpc.BeaconBlockHeader
	current_sync_committee           *lightrpc.SyncCommittee
	next_sync_committee              *lightrpc.SyncCommittee
	best_valid_update                *lightrpc.LightClientUpdate
	optimistic_header                *lightrpc.BeaconBlockHeader
	previous_max_active_participants uint64
	current_max_active_participants  uint64
}

func NewLightState(ctx context.Context, bootstrap *lightrpc.LightClientBootstrap, genesis [32]byte) *LightState {
	// makes copy of light client bootstrap
	l := &LightState{
		bootstrap:              bootstrap,
		genesis:                genesis,
		finalized_header:       bootstrap.Header,
		current_sync_committee: bootstrap.CurrentSyncCommittee,
		optimistic_header:      bootstrap.Header,
		events:                 make(chan LightClientEvent, 1280),
	}
	return l
}

func (l *LightState) CurrentSlot() uint64 {
	return 0
}

func (l *LightState) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-l.events:
			var err error
			switch evt := ev.(type) {
			case *lightrpc.LightClientUpdate:
				err = l.onLightClientUpdate(evt)
			case *lightrpc.LightClientFinalityUpdate:
				err = l.onFinalityUpdate(evt)
			case *lightrpc.LightClientOptimisticUpdate:
				err = l.onOptimisticUpdate(evt)
			}
			if err != nil {
				log.Warn("failed processing state update", "err", err)
			}
		}
	}
}

func (l *LightState) AddUpdateEvent(u *lightrpc.LightClientUpdate) {
	l.events <- u
}
func (l *LightState) AddOptimisticUpdateEvent(u *lightrpc.LightClientOptimisticUpdate) {
	l.events <- u
}
func (l *LightState) AddFinalityUpdateEvent(u *lightrpc.LightClientFinalityUpdate) {
	l.events <- u
}

func (l *LightState) onOptimisticUpdate(u *lightrpc.LightClientOptimisticUpdate) error {
	// TODO: validate update
	return nil
}

func (l *LightState) onFinalityUpdate(u *lightrpc.LightClientFinalityUpdate) error {
	// TODO: validate update
	return nil
}

func (l *LightState) onLightClientUpdate(u *lightrpc.LightClientUpdate) error {
	if err := l.validateLightClientUpdate(u); err != nil {
		return err
	}
	return nil
}
func (l *LightState) validateLightClientUpdate(u *lightrpc.LightClientUpdate) error {
	// need to do this but im too lazy to. maybe someone else knows an elegant solution...
	// if u.SyncAggregate.SyncCommiteeBits < min_sync_participants  {
	// return fmt.Errorf("not enough participants in commmittee (%d/%d)", )
	//}
	if l.CurrentSlot() < u.SignatureSlot {
		return fmt.Errorf("current slot must be bigger or eq to sig slot")
	}
	if u.SignatureSlot <= u.AttestedHeader.Slot {
		return fmt.Errorf("current sig slot must be larger than attested slot")
	}
	if u.AttestedHeader.Slot < u.FinalizedHeader.Slot {
		return fmt.Errorf("attested header slot must be lower than finalized header slot")
	}
	storePeriod := computeSyncCommitteePeriodAtSlot(l.finalized_header.Slot)
	updateSigPeriod := computeSyncCommitteePeriodAtSlot(u.SignatureSlot)

	if l.next_sync_committee != nil {
		if updateSigPeriod != storePeriod && updateSigPeriod != storePeriod+1 {
			return fmt.Errorf("update sig period must match store period or be store period + 1 if next sync committee not")
		}
	} else {
		if updateSigPeriod != storePeriod {
			return fmt.Errorf("update sig period must match store period if next sync committee nil")
		}
	}

	updateAttestedPeriod := computeSyncCommitteePeriodAtSlot(u.AttestedHeader.Slot)
	if !(l.next_sync_committee == nil && (isSyncCommitteeUpdate(u) && updateAttestedPeriod == storePeriod)) {
		if u.AttestedHeader.Slot <= l.finalized_header.Slot {
			return fmt.Errorf("if up has next sync committee, the update header slot must be strictly larger than the store's finalized header")
		}
	}

	if isFinalityUpdate(u) {
		// TODO: what is the genesis slot
		GENESIS_SLOT := uint64(1)
		finalized_root := [32]byte{}
		if u.FinalizedHeader.Slot != GENESIS_SLOT {
			finalized_root = hashTreeRoot(u.FinalizedHeader)
		}

		if !isValidMerkleBranch(finalized_root, utils.BytesSliceToBytes32Slice(u.FinalityBranch), 0, 0, utils.BytesToBytes32(u.AttestedHeader.Root)) {
			return fmt.Errorf("merkle branch invalid for finality update")
		}
	}
	if isSyncCommitteeUpdate(u) {
		leaf, _ := u.NextSyncCommitee.HashTreeRoot()
		if !isValidMerkleBranch(leaf, utils.BytesSliceToBytes32Slice(u.NextSyncCommitteeBranch), 0, 0, utils.BytesToBytes32(u.AttestedHeader.Root)) {
			return fmt.Errorf("merkle branch invalid for sync committee update")
		}
	}

	var curSyncCommittee *lightrpc.SyncCommittee
	if updateSigPeriod == storePeriod {
		curSyncCommittee = l.current_sync_committee
	} else {
		if l.next_sync_committee != nil {
			curSyncCommittee = l.next_sync_committee
		}
	}
	_ = curSyncCommittee
	//TODO: remaining validation
	///   participant_pubkeys = [
	///       pubkey for (bit, pubkey) in zip(sync_aggregate.sync_committee_bits, sync_committee.pubkeys)
	///       if bit
	///   ]
	///   fork_version = compute_fork_version(compute_epoch_at_slot(update.signature_slot))
	///   domain = compute_domain(DOMAIN_SYNC_COMMITTEE, fork_version, genesis_validators_root)
	///   signing_root = compute_signing_root(update.attested_header, domain)
	///   assert bls.FastAggregateVerify(participant_pubkeys, signing_root, sync_aggregate.sync_committee_signature)
	return nil
}

// TODO: implement
func isValidMerkleBranch(
	leaf [32]byte,
	branch [][32]byte,
	depth int,
	index int,
	root [32]byte,
) bool {
	return false
}

// TODO: implement
func hashTreeRoot(h *lightrpc.BeaconBlockHeader) [32]byte {
	return [32]byte{}
}

// TODO: implement
func computeEpochAtSlot(slot uint64) uint64 {
	return 0
}

// TODO: implement
func computeSyncCommitteePeriodAtSlot(slot uint64) uint64 {
	return computeSyncCommitteePeriod(computeEpochAtSlot(slot))
}

// TODO: implement
func computeSyncCommitteePeriod(slot uint64) uint64 {
	return slot
}

// TODO: implement
func isSyncCommitteeUpdate(update *lightrpc.LightClientUpdate) bool {
	//   return update.next_sync_committee_branch != [Bytes32() for _ in range(floorlog2(NEXT_SYNC_COMMITTEE_INDEX))]
	return true
}

// TODO: implement
func isFinalityUpdate(update *lightrpc.LightClientUpdate) bool {
	//   return update.next_sync_committee_branch != [Bytes32() for _ in range(floorlog2(NEXT_SYNC_COMMITTEE_INDEX))]
	return true
}
