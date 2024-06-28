package handler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (a *ApiHandler) blockRootFromStateId(ctx context.Context, tx kv.Tx, stateId *beaconhttp.SegmentID) (root libcommon.Hash, httpStatusErr int, err error) {
	switch {
	case stateId.Head():
		root, _, err = a.forkchoiceStore.GetHead()
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		return
	case stateId.Finalized():
		root = a.forkchoiceStore.FinalizedCheckpoint().BlockRoot()
		return
	case stateId.Justified():
		root = a.forkchoiceStore.JustifiedCheckpoint().BlockRoot()
		return
	case stateId.Genesis():
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("genesis block not found")
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
		return libcommon.Hash{}, http.StatusInternalServerError, fmt.Errorf("cannot parse state id")
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
	}), nil
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
		return newBeaconResponse(state).WithFinalized(true).WithVersion(state.Version()), nil
	}

	return newBeaconResponse(state).WithFinalized(false).WithVersion(state.Version()), nil
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

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}

	ok, finalizedCheckpoint, currentJustifiedCheckpoint, previousJustifiedCheckpoint := a.forkchoiceStore.GetFinalityCheckpoints(blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if !ok {
		currentJustifiedCheckpoint, previousJustifiedCheckpoint, finalizedCheckpoint, err = state_accessors.ReadCheckpoints(tx, a.beaconChainCfg.RoundSlotToEpoch(*slot))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if currentJustifiedCheckpoint == nil {
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
	}).WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).WithVersion(version), nil
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
			return nil, fmt.Errorf("Epoch is outside the sync committee period of the state")
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

	return newBeaconResponse(response).WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
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
		return newBeaconResponse(randaoResponse{Randao: mix}).WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot()), nil
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
	return newBeaconResponse(randaoResponse{Randao: mix}).WithFinalized(slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
