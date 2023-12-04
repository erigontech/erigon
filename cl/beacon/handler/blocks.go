package handler

import (
	"context"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
)

type headerResponse struct {
	Root      libcommon.Hash                   `json:"root"`
	Canonical bool                             `json:"canonical"`
	Header    *cltypes.SignedBeaconBlockHeader `json:"header"`
}

type getHeadersRequest struct {
	Slot       *uint64         `json:"slot,omitempty"`
	ParentRoot *libcommon.Hash `json:"root,omitempty"`
}

func (a *ApiHandler) rootFromBlockId(ctx context.Context, tx kv.Tx, blockId *segmentID) (root libcommon.Hash, err error) {
	switch {
	case blockId.head():
		root, _, err = a.forkchoiceStore.GetHead()
		if err != nil {
			return libcommon.Hash{}, err
		}
	case blockId.finalized():
		root = a.forkchoiceStore.FinalizedCheckpoint().BlockRoot()
	case blockId.justified():
		root = a.forkchoiceStore.JustifiedCheckpoint().BlockRoot()
	case blockId.genesis():
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return libcommon.Hash{}, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, "genesis block not found")
		}
	case blockId.getSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *blockId.getSlot())
		if err != nil {
			return libcommon.Hash{}, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %d", *blockId.getSlot()))
		}
	case blockId.getRoot() != nil:
		// first check if it exists
		root = *blockId.getRoot()
	default:
		return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusInternalServerError, "cannot parse block id")
	}
	return
}

func (a *ApiHandler) getBlock(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := blockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}

	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %x", root))
	}
	// Check if the block is canonical
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, blk.Block.Slot)
	if err != nil {
		return nil, beaconhttp.WrapEndpointError(err)
	}
	return newBeaconResponse(blk).
		withFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		withVersion(blk.Version()), nil
}

func (a *ApiHandler) getBlockAttestations(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := blockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %x", root))
	}
	// Check if the block is canonical
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, blk.Block.Slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(blk.Block.Body.Attestations).withFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		withVersion(blk.Version()), nil
}

func (a *ApiHandler) getBlockRoot(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := blockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	// check if the root exist
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %x", root))
	}
	// Check if the block is canonical
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(struct{ Root libcommon.Hash }{Root: root}).withFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
