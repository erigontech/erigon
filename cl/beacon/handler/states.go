package handler

import (
	"context"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (a *ApiHandler) rootFromStateId(ctx context.Context, tx kv.Tx, stateId *segmentID) (root libcommon.Hash, httpStatusErr int, err error) {
	var blockRoot libcommon.Hash
	switch {
	case stateId.head():
		blockRoot, _, err = a.forkchoiceStore.GetHead()
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
	case stateId.finalized():
		blockRoot = a.forkchoiceStore.FinalizedCheckpoint().BlockRoot()
	case stateId.justified():
		blockRoot = a.forkchoiceStore.JustifiedCheckpoint().BlockRoot()
	case stateId.genesis():
		blockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if blockRoot == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("genesis block not found")
		}
	case stateId.getSlot() != nil:
		blockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *stateId.getSlot())
		if err != nil {
			return libcommon.Hash{}, http.StatusInternalServerError, err
		}
		if blockRoot == (libcommon.Hash{}) {
			return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("block not found %d", *stateId.getSlot())
		}
	case stateId.getRoot() != nil:
		root = *stateId.getRoot()
		return
	default:
		return libcommon.Hash{}, http.StatusInternalServerError, fmt.Errorf("cannot parse state id")
	}
	root, err = beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, blockRoot)
	if err != nil {
		return libcommon.Hash{}, http.StatusInternalServerError, err
	}
	if root == (libcommon.Hash{}) {
		return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("block not found")
	}
	return
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

func (a *ApiHandler) getStateFork(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := stateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	root, httpStatus, err := a.rootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err.Error())
	}

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, err.Error())
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

func (a *ApiHandler) getStateRoot(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := stateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	root, httpStatus, err := a.rootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err.Error())
	}

	stateRoot, err := beacon_indicies.ReadStateRootByBlockRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if stateRoot == (libcommon.Hash{}) {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("could not read block header: %x", root))
	}

	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("could not read block header: %x", root))
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(&rootResponse{Root: stateRoot}).withFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}

func (a *ApiHandler) getFullState(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := stateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	root, httpStatus, err := a.rootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err.Error())
	}

	blockRoot, err := beacon_indicies.ReadBlockRootByStateRoot(tx, root)
	if err != nil {
		return nil, err
	}

	state, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, true)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	return newBeaconResponse(state).withFinalized(false).withVersion(state.Version()), nil
}
