package handler

import (
	"context"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
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
		return libcommon.Hash{}, http.StatusNotFound, fmt.Errorf("block not found %d", *stateId.getSlot())
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

func (a *ApiHandler) getStateFork(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	var (
		tx        kv.Tx
		blockId   *segmentID
		root      libcommon.Hash
		blkHeader *cltypes.SignedBeaconBlockHeader
	)

	ctx := r.Context()

	tx, err = a.indiciesDB.BeginRo(ctx)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()

	blockId, err = stateIdFromRequest(r)
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}
	root, httpStatus, err = a.rootFromStateId(ctx, tx, blockId)
	if err != nil {
		return
	}

	blkHeader, _, err = beacon_indicies.ReadSignedHeaderByStateRoot(ctx, tx, root)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	if blkHeader == nil {
		err = fmt.Errorf("could not read block header: %x", root)
		httpStatus = http.StatusNotFound
		return
	}
	slot := blkHeader.Header.Slot

	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	currentVersion := a.beaconChainCfg.GetForkVersionByVersion(stateVersion)
	previousVersion := a.beaconChainCfg.GetForkVersionByVersion(previousVersion(stateVersion))

	data = &cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(previousVersion),
		CurrentVersion:  utils.Uint32ToBytes4(currentVersion),
		Epoch:           epoch,
	}
	httpStatus = http.StatusAccepted
	return
}

func (a *ApiHandler) getStateRoot(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	var (
		tx        kv.Tx
		blockId   *segmentID
		root      libcommon.Hash
		blkHeader *cltypes.SignedBeaconBlockHeader
		canonical bool
	)

	ctx := r.Context()

	tx, err = a.indiciesDB.BeginRo(ctx)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()

	blockId, err = stateIdFromRequest(r)
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}
	root, httpStatus, err = a.rootFromStateId(ctx, tx, blockId)
	if err != nil {
		return
	}

	blkHeader, canonical, err = beacon_indicies.ReadSignedHeaderByStateRoot(ctx, tx, root)
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	if blkHeader == nil {
		err = fmt.Errorf("could not read block header: %x", root)
		httpStatus = http.StatusNotFound
		return
	}

	data = rootResponse{Root: blkHeader.Header.Root}
	slot := blkHeader.Header.Slot

	finalized = new(bool)
	*finalized = canonical && slot <= a.forkchoiceStore.FinalizedSlot()
	httpStatus = http.StatusAccepted
	return
}
