// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("bootstrap object evicted"))
	}
	return newBeaconResponse(bootstrap).WithVersion(bootstrap.Header.Version()), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientOptimisticUpdate(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	update := a.forkchoiceStore.NewestLightClientUpdate()
	if update == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no optimistic update loaded yet, try again later. it may take a few minutes for it to load."))
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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no finility update loaded yet, try again later. it may take a few minutes for it to load."))
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
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if startPeriod == nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("start_period is required")).WriteTo(w)
		return
	}
	count, err := beaconhttp.Uint64FromQueryParams(r, "count")
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if count == nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("count is required")).WriteTo(w)
		return
	}

	resp := []interface{}{}
	endPeriod := *startPeriod + *count
	currentSlot := a.ethClock.GetCurrentSlot()
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
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
}
