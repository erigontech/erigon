package lightclient

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (l *LightClient) isBetterUpdate(oldUpdate *cltypes.LightClientUpdate, newUpdate *cltypes.LightClientUpdate) bool {
	var (
		maxActiveParticipants = len(newUpdate.SyncAggregate.SyncCommiteeBits) * 8 // Bits
		newActiveParticipants = newUpdate.SyncAggregate.Sum()
		oldActiveParticipants = oldUpdate.SyncAggregate.Sum()
		newHasSuperMajority   = newActiveParticipants*3 >= maxActiveParticipants*2
		oldHasSuperMajority   = oldActiveParticipants*3 >= maxActiveParticipants*2
	)

	// Compare supermajority (> 2/3) sync committee participation
	if newHasSuperMajority != oldHasSuperMajority {
		return newHasSuperMajority && !oldHasSuperMajority
	}

	if !newHasSuperMajority && newActiveParticipants != oldActiveParticipants {
		return newActiveParticipants > oldActiveParticipants
	}

	// Compare presence of relevant sync committee
	isNewUpdateRelevant := newUpdate.HasNextSyncCommittee() &&
		utils.SlotToPeriod(newUpdate.AttestedHeader.HeaderEth2.Slot) == utils.SlotToPeriod(newUpdate.SignatureSlot)
	isOldUpdateRelevant := oldUpdate.HasNextSyncCommittee() &&
		utils.SlotToPeriod(oldUpdate.AttestedHeader.HeaderEth2.Slot) == utils.SlotToPeriod(oldUpdate.SignatureSlot)

	if isNewUpdateRelevant != isOldUpdateRelevant {
		return isNewUpdateRelevant
	}

	isNewFinality := newUpdate.IsFinalityUpdate()
	isOldFinality := oldUpdate.IsFinalityUpdate()

	if isNewFinality != isOldFinality {
		return isNewFinality
	}

	// Compare sync committee finality
	if isNewFinality && newUpdate.HasSyncFinality() != oldUpdate.HasSyncFinality() {
		return newUpdate.HasSyncFinality()
	}

	// Tie Breakers
	if newActiveParticipants != oldActiveParticipants {
		return newActiveParticipants > oldActiveParticipants
	}
	if newUpdate.AttestedHeader.HeaderEth2.Slot != oldUpdate.AttestedHeader.HeaderEth2.Slot {
		return newUpdate.AttestedHeader.HeaderEth2.Slot < oldUpdate.AttestedHeader.HeaderEth2.Slot
	}
	return newUpdate.SignatureSlot < oldUpdate.SignatureSlot
}

func (l *LightClient) applyLightClientUpdate(update *cltypes.LightClientUpdate) error {
	storePeriod := utils.SlotToPeriod(l.store.finalizedHeader.Slot)
	finalizedPeriod := utils.SlotToPeriod(update.FinalizedHeader.HeaderEth2.Slot)
	if l.store.nextSyncCommittee == nil {
		if storePeriod != finalizedPeriod {
			return fmt.Errorf("periods shall be matching")
		}
		l.store.nextSyncCommittee = update.NextSyncCommitee
	} else if finalizedPeriod == storePeriod+1 {
		l.store.currentSyncCommittee = l.store.nextSyncCommittee
		l.store.nextSyncCommittee = update.NextSyncCommitee
		l.store.previousMaxActivePartecipants = l.store.currentMaxActivePartecipants
		l.store.currentMaxActivePartecipants = 0
	}
	if update.FinalizedHeader.HeaderEth2.Slot > l.store.finalizedHeader.Slot {
		l.store.finalizedHeader = update.FinalizedHeader.HeaderEth2
		if update.FinalizedHeader.HeaderEth2.Slot > l.store.optimisticHeader.Slot {
			l.store.optimisticHeader = update.FinalizedHeader.HeaderEth2
		}
	}
	return nil
}

func (l *LightClient) processLightClientUpdate(update *cltypes.LightClientUpdate) error {
	valid, err := l.validateUpdate(update)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("BLS validation failed")
	}

	if l.store.bestValidUpdate == nil || l.isBetterUpdate(update, l.store.bestValidUpdate) {
		l.store.bestValidUpdate = update
	}
	updateParticipants := uint64(update.SyncAggregate.Sum())
	if updateParticipants > l.store.currentMaxActivePartecipants {
		l.store.currentMaxActivePartecipants = updateParticipants
	}

	// Apply lc update (should happen when every 27 hours)
	if update.SyncAggregate.Sum()*3 >= len(update.SyncAggregate.SyncCommiteeBits)*16 &&
		((update.IsFinalityUpdate() && update.FinalizedHeader.HeaderEth2.Slot > l.store.finalizedHeader.Slot) ||
			l.store.nextSyncCommittee == nil && update.HasNextSyncCommittee() &&
				update.IsFinalityUpdate() && update.HasSyncFinality()) {
		// Conditions are met so we can make all changes
		err = l.applyLightClientUpdate(update)
		l.store.bestValidUpdate = nil
	}

	return err
}
