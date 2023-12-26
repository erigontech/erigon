package handler

import (
	"encoding/json"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

func (a *ApiHandler) getAttestationsRewards(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	epoch, err := epochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	req := []string{}
	// parse json body request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, err
	}

	filterIndicies, err := parseQueryValidatorIndicies(tx, req)
	if err != nil {
		return nil, err
	}
	_, headSlot, err := a.forkchoiceStore.GetHead()
	if err != nil {
		return nil, err
	}
	headEpoch := headSlot / a.beaconChainCfg.SlotsPerEpoch
	if epoch > headEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, "epoch is in the future")
	}
	// Few cases to handle:
	// 1) finalized data
	// 2) not finalized data

	// finalized data
	if epoch > a.forkchoiceStore.FinalizedCheckpoint().Epoch() {
		minRange := epoch * a.beaconChainCfg.SlotsPerEpoch
		maxRange := (epoch + 1) * a.beaconChainCfg.SlotsPerEpoch
		var blockRoot libcommon.Hash
		for i := maxRange - 1; i >= minRange; i-- {
			blockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, i)
			if err != nil {
				return nil, err
			}
			if blockRoot == (libcommon.Hash{}) {
				continue
			}
			state, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, true)
			if err != nil {
				return nil, err
			}
			if state == nil {
				continue
			}
			return computeAttestationsRewards(state.ValidatorSet(), state.PreviousEpochParticipation(), filterIndicies, epoch)
		}
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "no block found for this epoch")
	}
	lastSlot := epoch*a.beaconChainCfg.SlotsPerEpoch + a.beaconChainCfg.SlotsPerEpoch - 1
	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, lastSlot)
	if err != nil {
		return nil, err
	}

	_, previousIdx, err := a.stateReader.ReadPartecipations(tx, lastSlot)
	if err != nil {
		return nil, err
	}
	return computeAttestationsRewards(validatorSet, previousIdx, filterIndicies, epoch)
}

func computeAttestationsRewards(validatorSet *solid.ValidatorSet, previousParticipation *solid.BitList, filterIndicies []uint64, epoch uint64) (*beaconResponse, error) {
	return nil, nil
}
