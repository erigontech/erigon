package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
)

type syncDutyResponse struct {
	Pubkey                         libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex                 uint64            `json:"validator_index,string"`
	ValidatorSyncCommitteeIndicies []string          `json:"validator_sync_committee_indicies"`
}

func (a *ApiHandler) getSyncDuties(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, err
	}

	// compute the sync committee period
	period := epoch / a.beaconChainCfg.EpochsPerSyncCommitteePeriod

	var idxsStr []string
	if err := json.NewDecoder(r.Body).Decode(&idxsStr); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not decode request body: %w. request body is required.", err))
	}
	if len(idxsStr) == 0 {
		return newBeaconResponse([]string{}).WithOptimistic(false), nil
	}
	duplicates := map[int]struct{}{}
	// convert the request to uint64
	idxs := make([]uint64, 0, len(idxsStr))
	for _, idxStr := range idxsStr {

		idx, err := strconv.ParseUint(idxStr, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not parse validator index: %w", err))
		}
		if _, ok := duplicates[int(idx)]; ok {
			continue
		}
		idxs = append(idxs, idx)
		duplicates[int(idx)] = struct{}{}
	}

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Try to find a slot in the epoch or close to it
	referenceSlot := ((epoch + 1) * a.beaconChainCfg.SlotsPerEpoch) - 1

	// Find the first slot in the epoch (or close enough that have a sync committee)
	var referenceRoot libcommon.Hash
	for ; referenceRoot != (libcommon.Hash{}); referenceSlot-- {
		referenceRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, referenceSlot)
		if err != nil {
			return nil, err
		}
	}
	referencePeriod := (referenceSlot / a.beaconChainCfg.SlotsPerEpoch) / a.beaconChainCfg.EpochsPerSyncCommitteePeriod
	// Now try reading the sync committee
	currentSyncCommittee, nextSyncCommittee, ok := a.forkchoiceStore.GetSyncCommittees(referenceRoot)
	if !ok {
		roundedSlotToPeriod := a.beaconChainCfg.RoundSlotToSyncCommitteePeriod(referenceSlot)
		switch {
		case referencePeriod == period:
			currentSyncCommittee, err = state_accessors.ReadCurrentSyncCommittee(tx, roundedSlotToPeriod)
		case referencePeriod+1 == period:
			nextSyncCommittee, err = state_accessors.ReadNextSyncCommittee(tx, roundedSlotToPeriod)
		default:
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find sync committee for epoch %d", epoch))
		}
		if err != nil {
			return nil, err
		}
	}
	var syncCommittee *solid.SyncCommittee
	// Determine which one to use. TODO(Giulio2002): Make this less rendundant.
	switch {
	case referencePeriod == period:
		syncCommittee = currentSyncCommittee
	case referencePeriod+1 == period:
		syncCommittee = nextSyncCommittee
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find sync committee for epoch %d", epoch))
	}
	if syncCommittee == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find sync committee for epoch %d", epoch))
	}
	// Now we have the sync committee, we can initialize our response set
	dutiesSet := map[uint64]*syncDutyResponse{}
	for _, idx := range idxs {
		publicKey, err := state_accessors.ReadPublicKeyByIndex(tx, idx)
		if err != nil {
			return nil, err
		}
		if publicKey == (libcommon.Bytes48{}) {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find validator with index %d", idx))
		}
		dutiesSet[idx] = &syncDutyResponse{
			Pubkey:         publicKey,
			ValidatorIndex: idx,
		}
	}
	// Now we can iterate over the sync committee and fill the response
	for idx, committeePartecipantPublicKey := range syncCommittee.GetCommittee() {
		committeePartecipantIndex, ok, err := state_accessors.ReadValidatorIndexByPublicKey(tx, committeePartecipantPublicKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find validator with public key %x", committeePartecipantPublicKey))
		}
		if _, ok := dutiesSet[committeePartecipantIndex]; !ok {
			continue
		}
		dutiesSet[committeePartecipantIndex].ValidatorSyncCommitteeIndicies = append(
			dutiesSet[committeePartecipantIndex].ValidatorSyncCommitteeIndicies,
			strconv.FormatUint(uint64(idx), 10))
	}
	// Now we can convert the map to a slice
	duties := make([]*syncDutyResponse, 0, len(dutiesSet))
	for _, duty := range dutiesSet {
		if len(duty.ValidatorSyncCommitteeIndicies) == 0 {
			continue
		}
		duties = append(duties, duty)
	}
	sort.Slice(duties, func(i, j int) bool {
		return duties[i].ValidatorIndex < duties[j].ValidatorIndex
	})

	return newBeaconResponse(duties).WithOptimistic(false), nil
}
