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
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
	if blockId.Head() {
		response, _, err := a.memoizedExpectedWithdrawals(nil)
		if err != nil {
			return nil, err
		}
		return response, nil
	}
	root, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}
	if response, matched, err := a.memoizedExpectedWithdrawals(&root); err != nil {
		return nil, err
	} else if matched {
		return response, nil
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
		// [Modified in Gloas:EIP7732] GLOAS blocks have no ExecutionPayload; withdrawals are in the envelope
		if blk.Version() >= clparams.GloasVersion {
			continue
		}
		return newBeaconResponse(blk.Block.Body.ExecutionPayload.Withdrawals).WithFinalized(false).WithOptimistic(isOptimistic), nil
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
}

func (a *ApiHandler) memoizedExpectedWithdrawals(requestedRoot *common.Hash) (*beaconhttp.BeaconResponse, bool, error) {
	var response *beaconhttp.BeaconResponse
	matched := false
	err := a.viewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
		if requestedRoot != nil && *requestedRoot != root {
			return nil
		}
		matched = true
		if a.beaconChainCfg.GetCurrentStateVersion(slot/a.beaconChainCfg.SlotsPerEpoch) < clparams.CapellaVersion {
			return beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("the specified state is not a capella state"))
		}
		expectedWithdrawals, err := state.GetExpectedWithdrawals(headState, state.Epoch(headState))
		if err != nil {
			return err
		}
		response = newBeaconResponse(expectedWithdrawals.Withdrawals).
			WithFinalized(false).
			WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root))
		return nil
	})
	return response, matched, err
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
