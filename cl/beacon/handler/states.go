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
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
)

func (a *ApiHandler) blockRootFromStateId(ctx context.Context, tx kv.Tx, stateId *beaconhttp.SegmentID) (root common.Hash, httpStatusErr int, err error) {

	switch {
	case stateId.Head():
		root, _, httpStatusErr, err = a.getHead()
		return
	case stateId.Finalized():
		root = a.forkchoiceStore.FinalizedCheckpoint().Root
		return
	case stateId.Justified():
		root = a.forkchoiceStore.JustifiedCheckpoint().Root
		return
	case stateId.Genesis():
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return common.Hash{}, http.StatusInternalServerError, err
		}
		if root == (common.Hash{}) {
			return common.Hash{}, http.StatusNotFound, errors.New("genesis block not found")
		}
		return
	case stateId.GetSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *stateId.GetSlot())
		if err != nil {
			return common.Hash{}, http.StatusInternalServerError, err
		}
		if root == (common.Hash{}) {
			return common.Hash{}, http.StatusNotFound, fmt.Errorf("block not found %d", *stateId.GetSlot())
		}
		return
	case stateId.GetRoot() != nil:
		root, err = beacon_indicies.ReadBlockRootByStateRoot(tx, *stateId.GetRoot())
		if err != nil {
			return common.Hash{}, http.StatusInternalServerError, err
		}
		return
	default:
		return common.Hash{}, http.StatusInternalServerError, errors.New("cannot parse state id")
	}
}

type rootResponse struct {
	Root common.Hash `json:"root"`
}

func previousVersion(v clparams.StateVersion) clparams.StateVersion {
	if v == clparams.Phase0Version {
		return clparams.Phase0Version
	}
	return v - 1
}

func (a *ApiHandler) getStateFork(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", root))
	}
	epoch := *slot / a.beaconChainCfg.SlotsPerEpoch

	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	forkEpoch := a.beaconChainCfg.GetForkEpochByVersion(stateVersion)
	currentVersion := a.beaconChainCfg.GetForkVersionByVersion(stateVersion)
	previousVersion := a.beaconChainCfg.GetForkVersionByVersion(previousVersion(stateVersion))

	return newBeaconResponse(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(previousVersion),
		CurrentVersion:  utils.Uint32ToBytes4(currentVersion),
		Epoch:           forkEpoch,
	}).WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) getStateRoot(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	stateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if stateRoot == (common.Hash{}) {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block header: %x", root))
	}

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block header: %x", root))
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(&rootResponse{Root: stateRoot}).
		WithOptimistic(isOptimistic).
		WithFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

func (a *ApiHandler) getFullState(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("could not read block slot: %x", blockRoot))
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("could not read canonical block root: %x", blockRoot))
	}

	state, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, true)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if state == nil {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			return nil, err
		}
		// Sanity checks slot and canonical data.
		if slot == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
		}
		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
		if err != nil {
			return nil, err
		}
		if canonicalRoot != blockRoot {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read state: %x", blockRoot))
		}
		historicalState, err := a.stateReader.ReadHistoricalState(ctx, tx, *slot)
		if err != nil {
			return nil, err
		}
		if historicalState == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read state: %x", blockRoot))
		}
		state = historicalState
	}

	return newBeaconResponse(state).
		WithHeader("Eth-Consensus-Version", state.Version().String()).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(state.Version()).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
}

type finalityCheckpointsResponse struct {
	FinalizedCheckpoint         solid.Checkpoint `json:"finalized"`
	CurrentJustifiedCheckpoint  solid.Checkpoint `json:"current_justified"`
	PreviousJustifiedCheckpoint solid.Checkpoint `json:"previous_justified"`
}

func (a *ApiHandler) getFinalityCheckpoints(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	finalizedCheckpoint, currentJustifiedCheckpoint, previousJustifiedCheckpoint, ok := a.forkchoiceStore.GetFinalityCheckpoints(blockRoot)
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	if !ok {
		currentJustifiedCheckpoint, previousJustifiedCheckpoint, finalizedCheckpoint, ok, err = state_accessors.ReadCheckpoints(stateGetter, a.beaconChainCfg.RoundSlotToEpoch(*slot), a.beaconChainCfg)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read checkpoints: %x, %d", blockRoot, a.beaconChainCfg.RoundSlotToEpoch(*slot)))
		}
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(finalityCheckpointsResponse{
		FinalizedCheckpoint:         finalizedCheckpoint,
		CurrentJustifiedCheckpoint:  currentJustifiedCheckpoint,
		PreviousJustifiedCheckpoint: previousJustifiedCheckpoint,
	}).WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).WithOptimistic(isOptimistic), nil
}

type syncCommitteesResponse struct {
	Validators          []string   `json:"validators"`
	ValidatorAggregates [][]string `json:"validator_aggregates"`
}

func (a *ApiHandler) getSyncCommittees(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	// Code here
	currentSyncCommittee, nextSyncCommittee, ok := a.forkchoiceStore.GetSyncCommittees(a.beaconChainCfg.SyncCommitteePeriod(*slot))
	if !ok {
		syncCommitteeSlot := a.beaconChainCfg.RoundSlotToSyncCommitteePeriod(*slot)
		// Check the main database if it cannot be found in the forkchoice store
		currentSyncCommittee, err = state_accessors.ReadCurrentSyncCommittee(stateGetter, syncCommitteeSlot)
		if err != nil {
			return nil, err
		}
		nextSyncCommittee, err = state_accessors.ReadNextSyncCommittee(stateGetter, syncCommitteeSlot)
		if err != nil {
			return nil, err
		}
		if currentSyncCommittee == nil || nextSyncCommittee == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read sync committees: %x, %d", blockRoot, *slot))
		}
	}
	// Now fetch the data we need
	statePeriod := a.beaconChainCfg.SyncCommitteePeriod(*slot)
	queryEpoch, err := beaconhttp.Uint64FromQueryParams(r, "epoch")
	if err != nil {
		return nil, err
	}

	committee := currentSyncCommittee.GetCommittee()
	if queryEpoch != nil {
		requestPeriod := a.beaconChainCfg.SyncCommitteePeriod(*queryEpoch * a.beaconChainCfg.SlotsPerEpoch)
		if requestPeriod == statePeriod+1 {
			committee = nextSyncCommittee.GetCommittee()
		} else if requestPeriod != statePeriod {
			return nil, errors.New("epoch is outside the sync committee period of the state")
		}
	}
	// Lastly construct the response
	validatorsPerSubcommittee := a.beaconChainCfg.SyncCommitteeSize / a.beaconChainCfg.SyncCommitteeSubnetCount
	response := syncCommitteesResponse{
		Validators:          make([]string, a.beaconChainCfg.SyncCommitteeSize),
		ValidatorAggregates: make([][]string, a.beaconChainCfg.SyncCommitteeSubnetCount),
	}
	for i, publicKey := range committee {
		// get the validator index of the committee
		validatorIndex, _, err := a.syncedData.ValidatorIndexByPublicKey(publicKey)
		if err != nil {
			return nil, fmt.Errorf("could not read validator index: %x. %s", publicKey, err)
		}
		idx := strconv.FormatInt(int64(validatorIndex), 10)
		response.Validators[i] = idx
		// add the index to the subcommittee
		subCommitteeIndex := uint64(i) / validatorsPerSubcommittee
		if len(response.ValidatorAggregates[subCommitteeIndex]) == 0 {
			response.ValidatorAggregates[subCommitteeIndex] = make([]string, validatorsPerSubcommittee)
		}
		response.ValidatorAggregates[subCommitteeIndex][uint64(i)%validatorsPerSubcommittee] = idx
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(response).
		WithOptimistic(isOptimistic).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

type randaoResponse struct {
	Randao common.Hash `json:"randao"`
}

func (a *ApiHandler) getRandao(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	// Check if the block is optimistic
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)

	epochReq, err := beaconhttp.Uint64FromQueryParams(r, "epoch")
	if err != nil {
		return nil, err
	}
	slotPtr, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slotPtr == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}
	slot := *slotPtr
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epochReq != nil {
		epoch = *epochReq
	}
	randaoMixes := a.randaoMixesPool.Get().(solid.HashListSSZ)
	defer a.randaoMixesPool.Put(randaoMixes)

	if a.forkchoiceStore.RandaoMixes(blockRoot, randaoMixes) {
		mix := randaoMixes.Get(int(epoch % a.beaconChainCfg.EpochsPerHistoricalVector))
		return newBeaconResponse(randaoResponse{Randao: mix}).
			WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot()).
			WithOptimistic(isOptimistic), nil
	}
	// check if the block is canonical
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, err
	}
	if canonicalRoot != blockRoot {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read randao: %x", blockRoot))
	}
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(tx, stateGetter, slot, epoch%a.beaconChainCfg.EpochsPerHistoricalVector)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(randaoResponse{Randao: mix}).
		WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot()).
		WithOptimistic(isOptimistic), nil
}

// Implements /eth/v1/beacon/states/{state_id}/pending_consolidations
func (a *ApiHandler) GetEthV1BeaconStatesPendingConsolidations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	pendingConsolidations := solid.NewPendingConsolidationList(a.beaconChainCfg)

	isSlotAvailableInMemory := a.forkchoiceStore.LowestAvailableSlot() < *slot

	if isSlotAvailableInMemory {
		var ok bool
		pendingConsolidations, ok = a.forkchoiceStore.GetPendingConsolidations(blockRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no pending	 consolidations found for block root: %x", blockRoot))
		}
		// If we have the pending consolidations in memory, we can return them directly.
	} else {
		stateView := a.caplinStateSnapshots.View()
		defer stateView.Close()

		if err := historical_states_reader.ReadQueueSSZ(state_accessors.GetValFnTxAndSnapshot(tx, stateView), *slot, kv.PendingConsolidationsDump, kv.PendingConsolidations, pendingConsolidations); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("failed to read pending consolidations: %w", err))
		}
	}

	return newBeaconResponse(pendingConsolidations).
		WithOptimistic(isOptimistic).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

// Implements /eth/v1/beacon/states/{state_id}/pending_deposits
func (a *ApiHandler) GetEthV1BeaconStatesPendingDeposits(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	pendingDeposits := solid.NewPendingDepositList(a.beaconChainCfg)

	isSlotAvailableInMemory := a.forkchoiceStore.LowestAvailableSlot() < *slot

	if isSlotAvailableInMemory {
		var ok bool
		pendingDeposits, ok = a.forkchoiceStore.GetPendingDeposits(blockRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no pending	 deposits found for block root: %x", blockRoot))
		}
	} else {
		stateView := a.caplinStateSnapshots.View()
		defer stateView.Close()

		if err := historical_states_reader.ReadQueueSSZ(state_accessors.GetValFnTxAndSnapshot(tx, stateView), *slot, kv.PendingDepositsDump, kv.PendingDeposits, pendingDeposits); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("failed to read pending deposits: %w", err))
		}
	}

	return newBeaconResponse(pendingDeposits).
		WithOptimistic(isOptimistic).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

// Implements /eth/v1/beacon/states/{state_id}/pending_partial_withdrawals
func (a *ApiHandler) GetEthV1BeaconStatesPendingPartialWithdrawals(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	pendingWithdrawals := solid.NewPendingWithdrawalList(a.beaconChainCfg)

	isSlotAvailableInMemory := a.forkchoiceStore.LowestAvailableSlot() < *slot

	if isSlotAvailableInMemory {
		var ok bool
		pendingWithdrawals, ok = a.forkchoiceStore.GetPendingPartialWithdrawals(blockRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no pending partial withdrawals found for block root: %x", blockRoot))
		}
	} else {
		stateView := a.caplinStateSnapshots.View()
		defer stateView.Close()

		if err := historical_states_reader.ReadQueueSSZ(state_accessors.GetValFnTxAndSnapshot(tx, stateView), *slot, kv.PendingPartialWithdrawalsDump, kv.PendingPartialWithdrawals, pendingWithdrawals); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("failed to read pending partial withdrawals: %w", err))
		}
	}

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	return newBeaconResponse(pendingWithdrawals).
		WithHeader("Eth-Consensus-Version", version.String()).
		WithVersion(version).
		WithOptimistic(isOptimistic).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

func (a *ApiHandler) GetEthV1BeaconStatesProposerLookahead(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	stateId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			err)
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusInternalServerError,
			fmt.Errorf("failed to read indicies db: %w", err),
		)
	}
	defer tx.Rollback()
	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, stateId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusInternalServerError,
			fmt.Errorf("failed to read block slot: %w", err),
		)
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusNotFound,
			fmt.Errorf("could not read block slot: %x", blockRoot),
		)
	}

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusInternalServerError,
			fmt.Errorf("failed to read canonical block root: %w", err),
		)
	}

	proposerLookahead, ok := a.forkchoiceStore.GetProposerLookahead(*slot)
	if !ok {
		stateView := a.caplinStateSnapshots.View()
		defer stateView.Close()
		// read epoch data
		epochData, err := state_accessors.ReadEpochData(state_accessors.GetValFnTxAndSnapshot(tx, stateView), *slot/a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(
				http.StatusInternalServerError,
				fmt.Errorf("failed to read historical epoch data: %w", err),
			)
		}
		proposerLookahead = epochData.ProposerLookahead
	}

	respProposerLookahead := []string{}
	proposerLookahead.Range(func(i int, v uint64, length int) bool {
		respProposerLookahead = append(respProposerLookahead, strconv.FormatUint(v, 10))
		return true
	})

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	return newBeaconResponse(respProposerLookahead).
		WithHeader("Eth-Consensus-Version", version.String()).
		WithVersion(version).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
