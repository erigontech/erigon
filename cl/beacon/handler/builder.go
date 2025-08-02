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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func (a *ApiHandler) GetEth1V1BuilderStatesExpectedWithdrawals(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	root, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
	}
	if a.beaconChainCfg.GetCurrentStateVersion(*slot/a.beaconChainCfg.SlotsPerEpoch) < clparams.CapellaVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("the specified state is not a capella state"))
	}
	headRoot, _, statusCode, err := a.getHead()
	if err != nil {
		return nil, beaconhttp.NewEndpointError(statusCode, err)
	}

	if a.syncedData.Syncing() {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("beacon node is syncing"))
	}
	if root == headRoot {
		var expectedWithdrawals []*cltypes.Withdrawal

		if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
			expectedWithdrawals, _ = state.ExpectedWithdrawals(headState, state.Epoch(headState))
			return nil
		}); err != nil {
			return nil, err
		}
		return newBeaconResponse(expectedWithdrawals).WithFinalized(false), nil
	}
	lookAhead := 1024
	for currSlot := *slot + 1; currSlot < *slot+uint64(lookAhead); currSlot++ {
		if currSlot > a.syncedData.HeadSlot() {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
		}
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currSlot)
		if err != nil {
			return nil, err
		}
		if blockRoot == (common.Hash{}) {
			continue
		}
		blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, blockRoot)
		if err != nil {
			return nil, err
		}
		return newBeaconResponse(blk.Block.Body.ExecutionPayload.Withdrawals).WithFinalized(false).WithOptimistic(isOptimistic), nil
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
}

func (a *ApiHandler) PostEthV1BuilderRegisterValidator(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	registerReq := []*cltypes.ValidatorRegistration{}
	if err := json.NewDecoder(r.Body).Decode(&registerReq); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if len(registerReq) == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("empty request"))
	}
	if err := a.builderClient.RegisterValidator(r.Context(), registerReq); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	for _, v := range registerReq {
		a.logger.Debug("[Caplin] Registered new validator", "fee_recipient", v.Message.FeeRecipient)
	}
	log.Info("Registered new validator", "count", len(registerReq))
	return newBeaconResponse(nil), nil
}
