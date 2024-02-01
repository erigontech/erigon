package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (a *ApiHandler) GetEthV1BeaconLightClientBootstrap(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}

	bootstrap, ok := a.forkchoiceStore.GetLightClientBootstrap(root)
	if !ok {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("bootstrap object evicted"))
	}
	return newBeaconResponse(bootstrap).WithVersion(bootstrap.Header.Version()), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientOptimisticUpdate(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	update := a.forkchoiceStore.NewestLightClientUpdate()
	if update == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no optimistic update loaded yet, try again later. it may take a few minutes for it to load."))
	}
	version := update.AttestedHeader.Version()
	return newBeaconResponse(&cltypes.LightClientOptimisticUpdate{
		AttestedHeader: update.AttestedHeader,
		SyncAggregate:  update.SyncAggregate,
		SignatureSlot:  update.SignatureSlot,
	}).WithVersion(version), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientFinalityUpdate(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	update := a.forkchoiceStore.NewestLightClientUpdate()
	if update == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no finility update loaded yet, try again later. it may take a few minutes for it to load."))
	}
	version := update.AttestedHeader.Version()
	return newBeaconResponse(&cltypes.LightClientFinalityUpdate{
		AttestedHeader:  update.AttestedHeader,
		FinalizedHeader: update.FinalizedHeader,
		FinalityBranch:  update.FinalityBranch,
		SyncAggregate:   update.SyncAggregate,
		SignatureSlot:   update.SignatureSlot,
	}).WithVersion(version), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientUpdates(w http.ResponseWriter, r *http.Request) {

	startPeriod, err := beaconhttp.Uint64FromQueryParams(r, "start_period")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if startPeriod == nil {
		http.Error(w, "start_period is required", http.StatusBadRequest)
		return
	}
	count, err := beaconhttp.Uint64FromQueryParams(r, "count")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if count == nil {
		http.Error(w, "count is required", http.StatusBadRequest)
		return
	}

	resp := []interface{}{}
	endPeriod := *startPeriod + *count
	currentSlot := utils.GetCurrentSlot(a.genesisCfg.GenesisTime, a.beaconChainCfg.SecondsPerSlot)
	if endPeriod > a.beaconChainCfg.SyncCommitteePeriod(currentSlot) {
		endPeriod = a.beaconChainCfg.SyncCommitteePeriod(currentSlot)
	}

	notFoundPrev := false
	// Fetch from [start_period, start_period + count]
	for i := *startPeriod; i <= endPeriod; i++ {
		respUpdate := map[string]interface{}{}
		update, has := a.forkchoiceStore.GetLightClientUpdate(i)
		if !has {
			notFoundPrev = true
			continue
		}
		if notFoundPrev {
			resp = []interface{}{}
			notFoundPrev = false
		}
		respUpdate["data"] = update
		respUpdate["version"] = clparams.ClVersionToString(update.AttestedHeader.Version())
		resp = append(resp, respUpdate)
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
