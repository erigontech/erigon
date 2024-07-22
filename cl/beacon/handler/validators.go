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
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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

const maxValidatorsLookupFilter = 128

func parseStatuses(s []string) ([]validatorStatus, error) {
	seenAlready := make(map[validatorStatus]struct{})
	statuses := make([]validatorStatus, 0, len(s))

	if len(s) > maxValidatorsLookupFilter {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("too many statuses requested"))
	}

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
		return false, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid validator id"))
	}
	return false, nil
}

func (a *ApiHandler) GetEthV1BeaconStatesValidators(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		http.Error(w, err.Error(), httpStatus)
		return
	}

	queryFilters, err := beaconhttp.StringListFromQueryParams(r, "status")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	validatorIds, err := beaconhttp.StringListFromQueryParams(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(validatorIds) > maxValidatorsLookupFilter {
		http.Error(w, fmt.Errorf("too many validators requested").Error(), http.StatusBadRequest)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		http.Error(w, err.Error(), httpStatus)
		return
	}

	var req validatorsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Ids) > maxValidatorsLookupFilter {
		http.Error(w, fmt.Errorf("too many validators requested").Error(), http.StatusBadRequest)
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
	filterIndicies, err := parseQueryValidatorIndicies(tx, validatorIds)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Check the filters' validity
	statusFilters, err := parseStatuses(queryFilters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		s := a.syncedData.HeadState()
		if s == nil {
			http.Error(w, fmt.Errorf("node is not synced").Error(), http.StatusServiceUnavailable)
			return
		}
		responseValidators(w, filterIndicies, statusFilters, state.Epoch(s), s.Balances(), s.Validators(), false, isOptimistic)
		return
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if slot == nil {
		http.Error(w, fmt.Errorf("state not found").Error(), http.StatusNotFound)
		return
	}
	stateEpoch := *slot / a.beaconChainCfg.SlotsPerEpoch

	if *slot < a.forkchoiceStore.LowestAvaiableSlot() {
		validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, *slot)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if validatorSet == nil {
			http.Error(w, fmt.Errorf("state not found for slot %v", *slot).Error(), http.StatusNotFound)
			return
		}
		balances, err := a.stateReader.ReadValidatorsBalances(tx, *slot)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseValidators(w, filterIndicies, statusFilters, stateEpoch, balances, validatorSet, true, isOptimistic)
		return
	}
	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if balances == nil {
		http.Error(w, fmt.Errorf("balances not found").Error(), http.StatusNotFound)
		return
	}
	validators, err := a.forkchoiceStore.GetValidatorSet(blockRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if validators == nil {
		http.Error(w, fmt.Errorf("validators not found").Error(), http.StatusNotFound)
		return
	}
	responseValidators(w, filterIndicies, statusFilters, stateEpoch, balances, validators, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic)
}

func parseQueryValidatorIndex(tx kv.Tx, id string) (uint64, error) {
	isPublicKey, err := checkValidValidatorId(id)
	if err != nil {
		return 0, err
	}
	if isPublicKey {
		var b48 libcommon.Bytes48
		if err := b48.UnmarshalText([]byte(id)); err != nil {
			return 0, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		has, err := tx.Has(kv.InvertedValidatorPublicKeys, b48[:])
		if err != nil {
			return 0, err
		}
		if !has {
			return math.MaxUint64, nil
		}
		idx, ok, err := state_accessors.ReadValidatorIndexByPublicKey(tx, b48)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator not found"))
		}
		return idx, nil
	}
	idx, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	return idx, nil

}

func parseQueryValidatorIndicies(tx kv.Tx, ids []string) ([]uint64, error) {
	filterIndicies := make([]uint64, 0, len(ids))

	for _, id := range ids {
		idx, err := parseQueryValidatorIndex(tx, id)
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

	validatorIndex, err := parseQueryValidatorIndex(tx, validatorId)
	if err != nil {
		return nil, err
	}

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		s := a.syncedData.HeadState()
		if s == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("node is not synced"))
		}
		if s.ValidatorLength() <= int(validatorIndex) {
			return newBeaconResponse([]int{}).WithFinalized(false), nil
		}
		if s == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("node is not synced"))
		}
		return responseValidator(validatorIndex, state.Epoch(s), s.Balances(), s.Validators(), false, isOptimistic)
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}

	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found"))
	}
	stateEpoch := *slot / a.beaconChainCfg.SlotsPerEpoch

	if *slot < a.forkchoiceStore.LowestAvaiableSlot() {
		validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, *slot)
		if err != nil {
			return nil, err
		}
		balances, err := a.stateReader.ReadValidatorsBalances(tx, *slot)
		if err != nil {
			return nil, err
		}
		return responseValidator(validatorIndex, stateEpoch, balances, validatorSet, true, isOptimistic)
	}
	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		return nil, err
	}
	if balances == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("balances not found"))
	}
	validators, err := a.forkchoiceStore.GetValidatorSet(blockRoot)
	if err != nil {
		return nil, err
	}
	if validators == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validators not found"))
	}
	return responseValidator(validatorIndex, stateEpoch, balances, validators, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic)
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/postStateValidatorBalances
func (a *ApiHandler) PostEthV1BeaconValidatorsBalances(w http.ResponseWriter, r *http.Request) {
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	validatorIds := []string{}
	// read from request body
	if err := json.NewDecoder(r.Body).Decode(&validatorIds); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(validatorIds) > maxValidatorsLookupFilter {
		http.Error(w, fmt.Errorf("too many validators requested").Error(), http.StatusBadRequest)
		return
	}

	a.getValidatorBalances(r.Context(), w, blockId, validatorIds)
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/getStateValidatorBalances
func (a *ApiHandler) GetEthV1BeaconValidatorsBalances(w http.ResponseWriter, r *http.Request) {
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	validatorIds, err := beaconhttp.StringListFromQueryParams(r, "id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(validatorIds) > maxValidatorsLookupFilter {
		http.Error(w, fmt.Errorf("too many validators requested").Error(), http.StatusBadRequest)
		return
	}
	a.getValidatorBalances(r.Context(), w, blockId, validatorIds)
}

func (a *ApiHandler) getValidatorBalances(ctx context.Context, w http.ResponseWriter, blockId *beaconhttp.SegmentID, validatorIds []string) {
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		http.Error(w, err.Error(), httpStatus)
		return
	}

	filterIndicies, err := parseQueryValidatorIndicies(tx, validatorIds)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)

	if blockId.Head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		s := a.syncedData.HeadState()
		if s == nil {
			http.Error(w, fmt.Errorf("node is not synced").Error(), http.StatusServiceUnavailable)
			return
		}
		responseValidatorsBalances(w, filterIndicies, s.Balances(), false, isOptimistic)
		return
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if slot == nil {
		http.Error(w, fmt.Errorf("state not found").Error(), http.StatusNotFound)
		return
	}

	if *slot < a.forkchoiceStore.LowestAvaiableSlot() {
		balances, err := a.stateReader.ReadValidatorsBalances(tx, *slot)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if balances == nil {

			http.Error(w, fmt.Errorf("validators not found, node may node be running in archivial node").Error(), http.StatusNotFound)
		}
		responseValidatorsBalances(w, filterIndicies, balances, true, isOptimistic)
		return
	}
	balances, err := a.forkchoiceStore.GetBalances(blockRoot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if balances == nil {
		http.Error(w, fmt.Errorf("balances not found").Error(), http.StatusNotFound)
		return
	}
	responseValidatorsBalances(w, filterIndicies, balances, *slot <= a.forkchoiceStore.FinalizedSlot(), isOptimistic)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
			"\",\"validator\":{\"pubkey\":\"" + libcommon.Bytes48(v.PublicKey()).Hex() +
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator not found"))
	}

	v := validators.Get(int(idx))
	status := validatorStatusFromValidator(v, stateEpoch, balances.Get(int(idx)))

	if _, err = b.WriteString(fmt.Sprintf(validatorJsonTemplate, idx, status.String(), balances.Get(int(idx)), v.PublicKey(), v.WithdrawalCredentials(), v.EffectiveBalance(), v.Slashed(), v.ActivationEligibilityEpoch(), v.ActivationEpoch(), v.ExitEpoch(), v.WithdrawableEpoch())); err != nil {
		return nil, err
	}

	_, err = b.WriteString("\n")

	return newBeaconResponse(directString(b.String())).WithFinalized(finalized).WithOptimistic(optimistic), err
}

func responseValidatorsBalances(w http.ResponseWriter, filterIndicies []uint64, balances solid.Uint64ListSSZ, finalized bool, optimistic bool) {
	// todo: refactor this
	b := stringsBuilderPool.Get().(*strings.Builder)
	defer stringsBuilderPool.Put(b)
	b.Reset()

	isOptimistic := "false"
	if optimistic {
		isOptimistic = "true"
	}
	if _, err := b.WriteString("{\"execution_optimistic\":" + isOptimistic + ",\"finalized\":" + strconv.FormatBool(finalized) + ",\"data\":"); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b.WriteString("[")

	//jsonTemplate := "{\"index\":\"%d\",\"balance\":\"%d\"}"
	first := true
	var err error
	balances.Range(func(i int, v uint64, l int) bool {
		if len(filterIndicies) > 0 && !slices.Contains(filterIndicies, uint64(i)) {
			return true
		}

		if !first {
			if _, err = b.WriteString(","); err != nil {
				return false
			}
		}
		first = false
		if _, err = b.WriteString("{\"index\":\"" + strconv.FormatUint(uint64(i), 10) + "\",\"balance\":\"" + strconv.FormatUint(v, 10) + "\"}"); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = b.WriteString("]}\n")

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(b.String())); err != nil {
		log.Error("failed to write response", "err", err)
	}
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
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("attestation_data_root is required"))
	}
	slot := r.URL.Query().Get("slot")
	if slot == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("slot is required"))
	}
	slotNum, err := strconv.ParseUint(slot, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.WithMessage(err, "invalid slot"))
	}

	attDataRootHash := libcommon.HexToHash(attDataRoot)
	att := a.aggregatePool.GetAggregatationByRoot(attDataRootHash)
	if att == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("attestation %s not found", attDataRoot))
	}
	if slotNum != att.AttestantionData().Slot() {
		log.Debug("attestation slot does not match", "attestation_data_root", attDataRoot, "slot_inquire", slot)
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("attestation slot mismatch"))
	}

	return newBeaconResponse(att), nil
}
