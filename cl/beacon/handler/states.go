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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/utils"
)

func (a *ApiHandler) blockRootFromStateId(ctx context.Context, tx kv.Tx, stateId *beaconhttp.SegmentID) (root libcommon.Hash, httpStatusErr int, err error) {

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
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, errors.New("genesis block not found")
		}
		return
	case stateId.GetSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *stateId.GetSlot())
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("block not found %d", *stateId.GetSlot())
		}
		return
	case stateId.GetRoot() != nil:
		root, err = beacon_indicies.ReadBlockRootByStateRoot(tx, *stateId.GetRoot())
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		return
	default:
		return libcommon.Hash{}, http.StatusInternalServerError, errors.New("cannot parse state id")
	}
}

type rootResponse struct {
	Root libcommon.Hash `json:"root"`
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
	if stateRoot == (libcommon.Hash{}) {
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
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
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
		state, err := a.stateReader.ReadHistoricalState(ctx, tx, *slot)
		if err != nil {
			return nil, err
		}
		if state == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read state: %x", blockRoot))
		}
		return newBeaconResponse(state).WithFinalized(true).WithVersion(state.Version()).WithOptimistic(isOptimistic), nil
	}

	return newBeaconResponse(state).WithFinalized(false).WithVersion(state.Version()).WithOptimistic(isOptimistic), nil
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
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if !ok {
		currentJustifiedCheckpoint, previousJustifiedCheckpoint, finalizedCheckpoint, ok, err = state_accessors.ReadCheckpoints(tx, a.beaconChainCfg.RoundSlotToEpoch(*slot))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read checkpoints: %x, %d", blockRoot, a.beaconChainCfg.RoundSlotToEpoch(*slot)))
		}
	}
	version := a.beaconChainCfg.GetCurrentStateVersion(*slot / a.beaconChainCfg.SlotsPerEpoch)
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(finalityCheckpointsResponse{
		FinalizedCheckpoint:         finalizedCheckpoint,
		CurrentJustifiedCheckpoint:  currentJustifiedCheckpoint,
		PreviousJustifiedCheckpoint: previousJustifiedCheckpoint,
	}).WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(version).WithOptimistic(isOptimistic), nil
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

	// Code here
	currentSyncCommittee, nextSyncCommittee, ok := a.forkchoiceStore.GetSyncCommittees(a.beaconChainCfg.SyncCommitteePeriod(*slot))
	if !ok {
		syncCommitteeSlot := a.beaconChainCfg.RoundSlotToSyncCommitteePeriod(*slot)
		// Check the main database if it cannot be found in the forkchoice store
		currentSyncCommittee, err = state_accessors.ReadCurrentSyncCommittee(tx, syncCommitteeSlot)
		if err != nil {
			return nil, err
		}
		nextSyncCommittee, err = state_accessors.ReadNextSyncCommittee(tx, syncCommitteeSlot)
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
		validatorIndex, ok, err := state_accessors.ReadValidatorIndexByPublicKey(tx, publicKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("could not read validator index: %x", publicKey)
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
	Randao libcommon.Hash `json:"randao"`
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
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(tx, slot, epoch%a.beaconChainCfg.EpochsPerHistoricalVector)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(randaoResponse{Randao: mix}).
		WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot()).
		WithOptimistic(isOptimistic), nil
}
