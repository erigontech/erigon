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
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const maxStateBuildersRequestSize = 1 << 20

type stateBuildersRequest struct {
	Ids      []string `json:"ids"`
	Statuses []string `json:"statuses"`
}

type stateBuilderResponse struct {
	Index   string                  `json:"index"`
	Status  string                  `json:"status"`
	Builder stateBuilderAPIResponse `json:"builder"`
}

type stateBuilderAPIResponse struct {
	Pubkey            common.Bytes48 `json:"pubkey"`
	Version           string         `json:"version"`
	ExecutionAddress  common.Address `json:"execution_address"`
	Balance           string         `json:"balance"`
	DepositEpoch      string         `json:"deposit_epoch"`
	WithdrawableEpoch string         `json:"withdrawable_epoch"`
}

func decodeStateBuildersRequest(w http.ResponseWriter, r *http.Request) (stateBuildersRequest, error) {
	request := new(stateBuildersRequest)
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxStateBuildersRequestSize))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return stateBuildersRequest{}, err
	}
	if request == nil {
		return stateBuildersRequest{}, errors.New("request body must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return stateBuildersRequest{}, errors.New("request body must contain one JSON object")
	}
	return *request, nil
}

func parseStateBuilderFilters(request stateBuildersRequest) (map[uint64]struct{}, map[common.Bytes48]struct{}, map[string]struct{}, error) {
	indices := make(map[uint64]struct{})
	pubkeys := make(map[common.Bytes48]struct{})
	for _, id := range request.Ids {
		if len(id) >= 2 && id[:2] == "0x" {
			var pubkey common.Bytes48
			if err := pubkey.UnmarshalText([]byte(id)); err != nil {
				return nil, nil, nil, fmt.Errorf("invalid builder id %q: %w", id, err)
			}
			if _, duplicate := pubkeys[pubkey]; duplicate {
				return nil, nil, nil, fmt.Errorf("duplicate builder id %q", id)
			}
			pubkeys[pubkey] = struct{}{}
			continue
		}
		index, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("invalid builder id %q: %w", id, err)
		}
		if _, duplicate := indices[index]; duplicate {
			return nil, nil, nil, fmt.Errorf("duplicate builder id %q", id)
		}
		indices[index] = struct{}{}
	}
	statuses := make(map[string]struct{})
	for _, status := range request.Statuses {
		switch status {
		case "pending", "active", "exited":
		default:
			return nil, nil, nil, fmt.Errorf("invalid builder status %q", status)
		}
		if _, duplicate := statuses[status]; duplicate {
			return nil, nil, nil, fmt.Errorf("duplicate builder status %q", status)
		}
		statuses[status] = struct{}{}
	}
	return indices, pubkeys, statuses, nil
}

func stateBuilderStatus(builder *cltypes.Builder, finalizedEpoch uint64) string {
	if builder.WithdrawableEpoch != math.MaxUint64 {
		return "exited"
	}
	if builder.DepositEpoch < finalizedEpoch {
		return "active"
	}
	return "pending"
}

func stateBuildersResponse(s *state.CachingBeaconState, indices map[uint64]struct{}, pubkeys map[common.Bytes48]struct{}, statuses map[string]struct{}) ([]stateBuilderResponse, error) {
	if s.Version() < clparams.GloasVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("the specified state is not a gloas state"))
	}
	builders := s.GetBuilders()
	if builders == nil {
		return nil, errors.New("builder registry is unavailable")
	}
	responseCapacity := builders.Len()
	if selected := len(indices) + len(pubkeys); selected > 0 && selected < responseCapacity {
		responseCapacity = selected
	}
	responses := make([]stateBuilderResponse, 0, responseCapacity)
	var registryErr error
	builders.Range(func(index int, builder *cltypes.Builder, _ int) bool {
		if builder == nil {
			registryErr = errors.New("builder registry contains a nil builder")
			return false
		}
		if len(indices)+len(pubkeys) > 0 {
			_, indexSelected := indices[uint64(index)]
			_, pubkeySelected := pubkeys[builder.Pubkey]
			if !indexSelected && !pubkeySelected {
				return true
			}
		}
		status := stateBuilderStatus(builder, s.FinalizedCheckpoint().Epoch)
		if len(statuses) > 0 {
			if _, selected := statuses[status]; !selected {
				return true
			}
		}
		responses = append(responses, stateBuilderResponse{
			Index:  strconv.FormatUint(uint64(index), 10),
			Status: status,
			Builder: stateBuilderAPIResponse{
				Pubkey:            builder.Pubkey,
				Version:           strconv.FormatUint(uint64(builder.Version), 10),
				ExecutionAddress:  builder.ExecutionAddress,
				Balance:           strconv.FormatUint(builder.Balance, 10),
				DepositEpoch:      strconv.FormatUint(builder.DepositEpoch, 10),
				WithdrawableEpoch: strconv.FormatUint(builder.WithdrawableEpoch, 10),
			},
		})
		return true
	})
	if registryErr != nil {
		return nil, registryErr
	}
	return responses, nil
}

func (a *ApiHandler) PostEthV1BeaconStatesBuilders(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	request, err := decodeStateBuildersRequest(w, r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	indices, pubkeys, statuses, err := parseStateBuilderFilters(request)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	stateID, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if stateID.Head() {
		var response *beaconhttp.BeaconResponse
		err := a.viewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, slot uint64) error {
			data, err := stateBuildersResponse(headState, indices, pubkeys, statuses)
			if err != nil {
				return err
			}
			response = newBeaconResponse(data).
				WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)).
				WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot())
			return nil
		})
		return response, err
	}

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	root, statusCode, err := a.blockRootFromStateId(r.Context(), tx, stateID)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(statusCode, err)
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
	}
	requestedState, err := a.forkchoiceStore.GetStateAtBlockRoot(root, true)
	if err != nil && !errors.Is(err, fork_graph.ErrStateNotFound) {
		return nil, err
	}
	if requestedState == nil {
		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
		if err != nil {
			return nil, err
		}
		if canonicalRoot != root {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
		}
		requestedState, err = a.stateReader.ReadHistoricalState(r.Context(), tx, *slot)
		if err != nil {
			return nil, err
		}
		if requestedState == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
		}
	}
	data, err := stateBuildersResponse(requestedState, indices, pubkeys, statuses)
	if err != nil {
		return nil, err
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(data).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)).
		WithFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

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
