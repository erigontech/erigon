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
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/db/kv"
)

var stringsBuilderPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

type validatorStatus int

var validatorJsonTemplate = "{\"index\":\"%d\",\"status\":\"%s\",\"balance\":\"%d\",\"validator\":{\"pubkey\":\"0x%x\",\"withdrawal_credentials\":\"0x%x\",\"effective_balance\":\"%d\",\"slashed\":%t,\"activation_eligibility_epoch\":\"%d\",\"activation_epoch\":\"%d\",\"exit_epoch\":\"%d\",\"withdrawable_epoch\":\"%d\"}}"

const (
	validatorPendingInitialized validatorStatus = 1  //"pending_initialized"
	validatorPendingQueued      validatorStatus = 2  //"pending_queued"
	validatorActiveOngoing      validatorStatus = 3  //"active_ongoing"
	validatorActiveExiting      validatorStatus = 4  //"active_exiting"
	validatorActiveSlashed      validatorStatus = 5  //"active_slashed"
	validatorExitedUnslashed    validatorStatus = 6  //"exited_unslashed"
	validatorExitedSlashed      validatorStatus = 7  //"exited_slashed"
	validatorWithdrawalPossible validatorStatus = 8  //"withdrawal_possible"
	validatorWithdrawalDone     validatorStatus = 9  //"withdrawal_done"
	validatorActive             validatorStatus = 10 //"active"
	validatorPending            validatorStatus = 11 //"pending"
	validatorExited             validatorStatus = 12 //"exited"
	validatorWithdrawal         validatorStatus = 13 //"withdrawal"
)

func validatorStatusFromString(s string) (validatorStatus, error) {
	switch s {
	case "pending_initialized":
		return validatorPendingInitialized, nil
	case "pending_queued":
		return validatorPendingQueued, nil
	case "active_ongoing":
		return validatorActiveOngoing, nil
	case "active_exiting":
		return validatorActiveExiting, nil
	case "active_slashed":
		return validatorActiveSlashed, nil
	case "exited_unslashed":
		return validatorExitedUnslashed, nil
	case "exited_slashed":
		return validatorExitedSlashed, nil
	case "withdrawal_possible":
		return validatorWithdrawalPossible, nil
	case "withdrawal_done":
		return validatorWithdrawalDone, nil
	case "active":
		return validatorActive, nil
	case "pending":
		return validatorPending, nil
	case "exited":
		return validatorExited, nil
	case "withdrawal":
		return validatorWithdrawal, nil
	default:
		return 0, fmt.Errorf("invalid validator status %s", s)
	}
}

func validatorStatusFromValidator(v solid.Validator, currentEpoch uint64, balance uint64) validatorStatus {
	activationEpoch := v.ActivationEpoch()
	// pending section
	if activationEpoch > currentEpoch {
		activationEligibilityEpoch := v.ActivationEligibilityEpoch()
		if activationEligibilityEpoch == math.MaxUint64 {
			return validatorPendingInitialized
		}
		return validatorPendingQueued
	}

	exitEpoch := v.ExitEpoch()
	// active section
	if activationEpoch <= currentEpoch && currentEpoch < exitEpoch {
		if exitEpoch == math.MaxUint64 {
			return validatorActiveOngoing
		}
		slashed := v.Slashed()
		if slashed {
			return validatorActiveSlashed
		}
		return validatorActiveExiting
	}

	withdrawableEpoch := v.WithdrawableEpoch()
	// exited section
	if exitEpoch <= currentEpoch && currentEpoch < withdrawableEpoch {
		if v.Slashed() {
			return validatorExitedSlashed
		}
		return validatorExitedUnslashed
	}

	if balance == 0 {
		return validatorWithdrawalDone
	}
	return validatorWithdrawalPossible

}

func (s validatorStatus) String() string {
	switch s {
	case validatorPendingInitialized:
		return "pending_initialized"
	case validatorPendingQueued:
		return "pending_queued"
	case validatorActiveOngoing:
		return "active_ongoing"
	case validatorActiveExiting:
		return "active_exiting"
	case validatorActiveSlashed:
		return "active_slashed"
	case validatorExitedUnslashed:
		return "exited_unslashed"
	case validatorExitedSlashed:
		return "exited_slashed"
	case validatorWithdrawalPossible:
		return "withdrawal_possible"
	case validatorWithdrawalDone:
		return "withdrawal_done"
	case validatorActive:
		return "active"
	case validatorPending:
		return "pending"
	case validatorExited:
		return "exited"
	case validatorWithdrawal:
		return "withdrawal"
	default:
		panic("invalid validator status")
	}
}

func parseStatuses(s []string) ([]validatorStatus, error) {
	seenAlready := make(map[validatorStatus]struct{})
	statuses := make([]validatorStatus, 0, len(s))

	for _, status := range s {
		s, err := validatorStatusFromString(status)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if _, ok := seenAlready[s]; ok {
			continue
		}
		seenAlready[s] = struct{}{}
		statuses = append(statuses, s)
	}
	return statuses, nil
}

func checkValidValidatorId(s string) (bool, error) {
	// If it starts with 0x, then it must a 48bytes 0x prefixed string
	if len(s) == 98 && s[:2] == "0x" {
		// check if it is a valid hex string
		if _, err := hex.DecodeString(s[2:]); err != nil {
			return false, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		return true, nil
	}
	// If it is not 0x prefixed, then it must be a number, check if it is a base-10 number
	if _, err := strconv.ParseUint(s, 10, 64); err != nil {
		return false, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("invalid validator id"))
	}
	return false, nil
}

func (a *ApiHandler) GetEthV1BeaconStatesValidators(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		beaconhttp.NewEndpointError(httpStatus, err).WriteTo(w)
		return
	}

	queryFilters, err := beaconhttp.StringListFromQueryParams(r, "status")
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	validatorIds, err := beaconhttp.StringListFromQueryParams(r, "id")
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	a.writeValidatorsResponse(w, r, tx, blockId, blockRoot, validatorIds, queryFilters)
}

type validatorsRequest struct {
	Ids      []string `json:"ids"`
	Statuses []string `json:"statuses"`
}

func (a *ApiHandler) PostEthV1BeaconStatesValidators(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		beaconhttp.NewEndpointError(httpStatus, err).WriteTo(w)
		return
	}

	var req validatorsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	a.writeValidatorsResponse(w, r, tx, blockId, blockRoot, req.Ids, req.Statuses)
}

func (a *ApiHandler) writeValidatorsResponse(
	w http.ResponseWriter,
	r *http.Request,
	tx kv.Tx,
	blockId *beaconhttp.SegmentID,
	blockRoot common.Hash,
	validatorIds,
	queryFilters []string,
) {
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
	filterIndicies, err := parseQueryValidatorIndicies(a.syncedData, validatorIds)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	// Check the filters' validity
	statusFilters, err := parseStatuses(queryFilters)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			responseValidators(w, filterIndicies, statusFilters, state.Epoch(s), s.Balances(), s.Validators(), false, isOptimistic)
			return nil
		}); err != nil {
			beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("node is not synced")).WriteTo(w)
		}
		return
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}

	if slot == nil {
		beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found")).WriteTo(w)
		return
	}
	stateEpoch := *slot / a.beaconChainCfg.SlotsPerEpoch

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	getter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	if *slot < a.forkchoiceStore.LowestAvailableSlot() {
		validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, getter, *slot)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		} else if validatorSet == nil {
			beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found for slot %v", *slot)).WriteTo(w)
			return
		}
		balances, err := a.stateReader.ReadValidatorsBalances(tx, getter, *slot)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		responseValidators(w, filterIndicies, statusFilters, stateEpoch, balances, validatorSet, true, isOptimistic)
		return
	}
	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	if balances == nil {
		beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("balances not found")).WriteTo(w)
		return
	}
	validators, err := a.forkchoiceStore.GetValidatorSet(blockRoot)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	if validators == nil {
		beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validators not found")).WriteTo(w)
		return
	}
	responseValidators(w, filterIndicies, statusFilters, stateEpoch, balances, validators, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic)
}

func parseQueryValidatorIndex(syncedData synced_data.SyncedData, id string) (uint64, error) {
	isPublicKey, err := checkValidValidatorId(id)
	if err != nil {
		return 0, err
	}
	if isPublicKey {
		var b48 common.Bytes48
		if err := b48.UnmarshalText([]byte(id)); err != nil {
			return 0, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		idx, has, err := syncedData.ValidatorIndexByPublicKey(b48)
		if err != nil {
			return 0, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator not found: %s", err))
		}
		if !has {
			return math.MaxUint64, nil
		}
		return idx, nil
	}
	idx, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	return idx, nil

}

func parseQueryValidatorIndicies(syncedData synced_data.SyncedData, ids []string) ([]uint64, error) {
	filterIndicies := make([]uint64, 0, len(ids))

	for _, id := range ids {
		idx, err := parseQueryValidatorIndex(syncedData, id)
		if err != nil {
			return nil, err
		}
		filterIndicies = append(filterIndicies, idx)
	}
	return filterIndicies, nil
}

func (a *ApiHandler) GetEthV1BeaconStatesValidator(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)

	validatorId, err := beaconhttp.StringFromRequest(r, "validator_id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	validatorIndex, err := parseQueryValidatorIndex(a.syncedData, validatorId)
	if err != nil {
		return nil, err
	}

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	getter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		var (
			resp *beaconhttp.BeaconResponse
			err  error
		)
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			if s.ValidatorLength() <= int(validatorIndex) {
				resp = newBeaconResponse([]int{}).WithFinalized(false)
				return nil
			}
			resp, err = responseValidator(validatorIndex, state.Epoch(s), s.Balances(), s.Validators(), false, isOptimistic)
			return nil // return err later
		}); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("node is not synced"))
		}

		return resp, err
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}

	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
	}
	stateEpoch := *slot / a.beaconChainCfg.SlotsPerEpoch

	if *slot < a.forkchoiceStore.LowestAvailableSlot() {
		validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, getter, *slot)
		if err != nil {
			return nil, err
		}
		if validatorSet == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validators not found"))
		}
		balances, err := a.stateReader.ReadValidatorsBalances(tx, getter, *slot)
		if err != nil {
			return nil, err
		}
		if balances == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("balances not found"))
		}
		return responseValidator(validatorIndex, stateEpoch, balances, validatorSet, true, isOptimistic)
	}

	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		return nil, err
	}
	if balances == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("balances not found"))
	}
	validators, err := a.forkchoiceStore.GetValidatorSet(blockRoot)
	if err != nil {
		return nil, err
	}
	if validators == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validators not found"))
	}
	return responseValidator(validatorIndex, stateEpoch, balances, validators, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic)
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/postStateValidatorBalances
func (a *ApiHandler) PostEthV1BeaconValidatorsBalances(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	validatorIds := []string{}
	// read from request body
	if err := json.NewDecoder(r.Body).Decode(&validatorIds); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	return a.getValidatorBalances(r.Context(), w, blockId, validatorIds)
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/getStateValidatorBalances
func (a *ApiHandler) GetEthV1BeaconValidatorsBalances(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	validatorIds, err := beaconhttp.StringListFromQueryParams(r, "id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	return a.getValidatorBalances(r.Context(), w, blockId, validatorIds)
}

func (a *ApiHandler) getValidatorBalances(ctx context.Context, w http.ResponseWriter, blockId *beaconhttp.SegmentID, validatorIds []string) (*beaconhttp.BeaconResponse, error) {
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	defer tx.Rollback()

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}

	filterIndicies, err := parseQueryValidatorIndicies(a.syncedData, validatorIds)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		var response *beaconhttp.BeaconResponse
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			response = responseValidatorsBalances(w, filterIndicies, s.Balances(), false, isOptimistic)
			return nil
		}); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("node is not synced"))
		}
		return response, nil
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
	}

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	getter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	if *slot < a.forkchoiceStore.LowestAvailableSlot() {
		balances, err := a.stateReader.ReadValidatorsBalances(tx, getter, *slot)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
		if balances == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validators not found, node may node be running in archivial node"))
		}
		return responseValidatorsBalances(w, filterIndicies, balances, true, isOptimistic), nil
	}
	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if balances == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("balances not found"))
	}
	return responseValidatorsBalances(w, filterIndicies, balances, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic), nil
}

type directString string

func (d directString) MarshalJSON() ([]byte, error) {
	return []byte(d), nil
}

func responseValidators(w http.ResponseWriter, filterIndicies []uint64, filterStatuses []validatorStatus, stateEpoch uint64, balances solid.Uint64ListSSZ, validators *solid.ValidatorSet, finalized bool, optimistic bool) {
	// todo: refactor this function
	b := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(b)
	b.Reset()

	var isOptimistic string = "false"
	if optimistic {
		isOptimistic = "true"
	}
	if _, err := b.WriteString("{\"execution_optimistic\":" + isOptimistic + ",\"finalized\":" + strconv.FormatBool(finalized) + ",\"data\":"); err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	b.WriteString("[")
	first := true
	var err error
	validators.Range(func(i int, v solid.Validator, l int) bool {
		if len(filterIndicies) > 0 && !slices.Contains(filterIndicies, uint64(i)) {
			return true
		}
		// "{\"index\":\"%d\",\"status\":\"%s\",\"balance\":\"%d\",\"validator\":{\"pubkey\":\"0x%x\",\"withdrawal_credentials\":\"0x%x\",\"effective_balance\":\"%d\",\"slashed\":%t,\"activation_eligibility_epoch\":\"%d\",\"activation_epoch\":\"%d\",\"exit_epoch\":\"%d\",\"withdrawable_epoch\":\"%d\"}}"
		status := validatorStatusFromValidator(v, stateEpoch, balances.Get(i))
		if shouldStatusBeFiltered(status, filterStatuses) {
			return true
		}
		if !first {
			if _, err = b.WriteString(","); err != nil {
				return false
			}
		}
		first = false
		// if _, err = b.WriteString(fmt.Sprintf(validatorJsonTemplate, i, status.String(), balances.Get(i), v.PublicKey(), v.WithdrawalCredentials(), v.EffectiveBalance(), v.Slashed(), v.ActivationEligibilityEpoch(), v.ActivationEpoch(), v.ExitEpoch(), v.WithdrawableEpoch())); err != nil {
		// 	return false
		// }
		if _, err = b.WriteString("{\"index\":\"" + strconv.FormatUint(uint64(i), 10) +
			"\",\"status\":\"" + status.String() +
			"\",\"balance\":\"" + strconv.FormatUint(balances.Get(i), 10) +
			"\",\"validator\":{\"pubkey\":\"" + common.Bytes48(v.PublicKey()).Hex() +
			"\",\"withdrawal_credentials\":\"" + v.WithdrawalCredentials().Hex() +
			"\",\"effective_balance\":\"" + strconv.FormatUint(v.EffectiveBalance(), 10) +
			"\",\"slashed\":" + strconv.FormatBool(v.Slashed()) +
			",\"activation_eligibility_epoch\":\"" + strconv.FormatUint(v.ActivationEligibilityEpoch(), 10) +
			"\",\"activation_epoch\":\"" + strconv.FormatUint(v.ActivationEpoch(), 10) +
			"\",\"exit_epoch\":\"" + strconv.FormatUint(v.ExitEpoch(), 10) +
			"\",\"withdrawable_epoch\":\"" + strconv.FormatUint(v.WithdrawableEpoch(), 10) + "\"}}"); err != nil {
			return false
		}

		return true
	})
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}
	_, err = b.WriteString("]}\n")

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(b.String())); err != nil {
		log.Error("failed to write response", "err", err)
	}
}

func responseValidator(idx uint64, stateEpoch uint64, balances solid.Uint64ListSSZ, validators *solid.ValidatorSet, finalized bool, optimistic bool) (*beaconhttp.BeaconResponse, error) {
	var b strings.Builder
	var err error
	if validators.Length() <= int(idx) {
		return newBeaconResponse([]int{}).WithFinalized(finalized), nil
	}
	if idx >= uint64(validators.Length()) {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validator not found"))
	}

	v := validators.Get(int(idx))
	status := validatorStatusFromValidator(v, stateEpoch, balances.Get(int(idx)))

	if _, err = b.WriteString(fmt.Sprintf(validatorJsonTemplate, idx, status.String(), balances.Get(int(idx)), v.PublicKey(), v.WithdrawalCredentials(), v.EffectiveBalance(), v.Slashed(), v.ActivationEligibilityEpoch(), v.ActivationEpoch(), v.ExitEpoch(), v.WithdrawableEpoch())); err != nil {
		return nil, err
	}

	_, err = b.WriteString("\n")

	return newBeaconResponse(directString(b.String())).WithFinalized(finalized).WithOptimistic(optimistic), err
}

func responseValidatorsBalances(w http.ResponseWriter, filterIndicies []uint64, balances solid.Uint64ListSSZ, finalized bool, optimistic bool) *beaconhttp.BeaconResponse {
	type BalanceResponse struct {
		Index   string `json:"index"`
		Balance string `json:"balance"`
	}

	balancesResponse := make([]BalanceResponse, 0)
	balances.Range(func(i int, v uint64, l int) bool {
		if len(filterIndicies) > 0 && !slices.Contains(filterIndicies, uint64(i)) {
			return true
		}
		balancesResponse = append(balancesResponse, BalanceResponse{
			Index:   strconv.FormatUint(uint64(i), 10),
			Balance: strconv.FormatUint(v, 10),
		})
		return true
	})

	return newBeaconResponse(balancesResponse).
		WithFinalized(finalized).
		WithOptimistic(optimistic)
}

func shouldStatusBeFiltered(status validatorStatus, statuses []validatorStatus) bool {
	if len(statuses) == 0 {
		return false
	}
	for _, s := range statuses {
		if (s == status) || (s == validatorActive && (status == validatorActiveOngoing || status == validatorActiveExiting || status == validatorActiveSlashed)) ||
			(s == validatorPending && (status == validatorPendingInitialized || status == validatorPendingQueued)) ||
			(s == validatorExited && (status == validatorExitedUnslashed || status == validatorExitedSlashed)) ||
			(s == validatorWithdrawal && (status == validatorWithdrawalPossible || status == validatorWithdrawalDone)) {
			return false
		}
	}
	return true // filter if no filter condition is met
}

func (a *ApiHandler) GetEthV1ValidatorAggregateAttestation(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	attDataRoot := r.URL.Query().Get("attestation_data_root")
	if attDataRoot == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("attestation_data_root is required"))
	}
	slot := r.URL.Query().Get("slot")
	if slot == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("slot is required"))
	}
	slotNum, err := strconv.ParseUint(slot, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %w", err))
	}

	attDataRootHash := common.HexToHash(attDataRoot)
	att := a.aggregatePool.GetAggregatationByRoot(attDataRootHash)
	if att == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("attestation %s not found", attDataRoot))
	}
	if slotNum != att.Data.Slot {
		log.Debug("attestation slot does not match", "attestation_data_root", attDataRoot, "slot_inquire", slot)
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("attestation slot mismatch"))
	}

	return newBeaconResponse(att), nil
}

func (a *ApiHandler) GetEthV2ValidatorAggregateAttestation(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	attDataRoot := r.URL.Query().Get("attestation_data_root")
	if attDataRoot == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("attestation_data_root is required"))
	}
	slot := r.URL.Query().Get("slot")
	if slot == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("slot is required"))
	}
	slotNum, err := strconv.ParseUint(slot, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %w", err))
	}
	committeeIndex := r.URL.Query().Get("committee_index")
	if committeeIndex == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("committee_index is required"))
	}
	committeeIndexNum, err := strconv.ParseUint(committeeIndex, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid committee_index: %w", err))
	}

	attDataRootHash := common.HexToHash(attDataRoot)
	att := a.aggregatePool.GetAggregatationByRootAndCommittee(attDataRootHash, committeeIndexNum)
	if att == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("attestation %s not found", attDataRoot))
	}
	if slotNum != att.Data.Slot {
		log.Debug("attestation slot does not match", "attestation_data_root", attDataRoot, "slot_inquire", slot)
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("attestation slot mismatch"))
	}

	version := a.ethClock.StateVersionByEpoch(slotNum / a.beaconChainCfg.SlotsPerEpoch)
	return newBeaconResponse(att).WithVersion(version), nil
}

func (a *ApiHandler) GetEthV1ValidatorIdentities(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	defer tx.Rollback()
	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)

	var (
		validators  *solid.ValidatorSet
		isFinalized bool
	)
	if blockId.Head() {
		// If we are looking at the head, we can just read the validators from the head state
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			validators = s.Validators()
			return nil
		}); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
		isFinalized = false
	} else {
		snRoTx := a.caplinStateSnapshots.View()
		defer snRoTx.Close()
		getter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		} else if slot == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("state not found"))
		}
		if *slot < a.forkchoiceStore.LowestAvailableSlot() {
			// If the slot is less than the lowest available slot, we need to read the validators from the historical state
			validators, err = a.stateReader.ReadValidatorsForHistoricalState(tx, getter, *slot)
			if err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
			}
		} else {
			validators, err = a.forkchoiceStore.GetValidatorSet(blockRoot)
			if err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
			}
		}
		isFinalized = *slot <= a.forkchoiceStore.FinalizedSlot()
	}
	if validators == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validators not found"))
	}

	// compose the response
	validatorIdentities := solid.NewStaticListSSZ[*validatorIdentityResponse](validators.Length(), (&validatorIdentityResponse{}).EncodingSizeSSZ())
	validators.Range(func(i int, v solid.Validator, l int) bool {
		validatorIdentities.Append(&validatorIdentityResponse{
			Index:           uint64(i),
			Pubkey:          v.PublicKey(),
			ActivationEpoch: v.ActivationEpoch(),
		})
		return true
	})

	return newBeaconResponse(validatorIdentities).
		WithOptimistic(isOptimistic).
		WithFinalized(isFinalized), nil
}

type validatorIdentityResponse struct {
	/* Example:
	{
		"index": "1",
		"pubkey": "0x93247f2209abcacf57b75a51dafae777f9dd38bc7053d1af526f220a7489a6d3a2753e5f3e8b1cfe39b56f43611df74a",
		"activation_epoch": "1"
	}
	*/
	Index           uint64         `json:"index"`
	Pubkey          common.Bytes48 `json:"pubkey"`
	ActivationEpoch uint64         `json:"activation_epoch"`
}

func (v *validatorIdentityResponse) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, v.Index, v.Pubkey[:], v.ActivationEpoch)
}

func (v *validatorIdentityResponse) DecodeSSZ(buf []byte, version int) error {
	// Don't care
	return errors.New("not implemented")
}

func (v *validatorIdentityResponse) EncodingSizeSSZ() int {
	return 48 + 8 + 8
}

func (v *validatorIdentityResponse) Clone() clonable.Clonable {
	// Don't care
	return &validatorIdentityResponse{}
}

func (v *validatorIdentityResponse) HashSSZ() ([32]byte, error) {
	// Don't care
	return [32]byte{}, errors.New("not implemented")
}
