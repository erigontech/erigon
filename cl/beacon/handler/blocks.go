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
	Slot       *uint64         `json:"slot,omitempty,string"`
	ParentRoot *libcommon.Hash `json:"root,omitempty"`
}

func (a *ApiHandler) rootFromBlockId(ctx context.Context, tx kv.Tx, blockId *beaconhttp.SegmentID) (root libcommon.Hash, err error) {
	switch {
	case blockId.Head():
		root, _, err = a.forkchoiceStore.GetHead()
		if err != nil {
			return libcommon.Hash{}, err
		}
	case blockId.Finalized():
		root = a.forkchoiceStore.FinalizedCheckpoint().BlockRoot()
	case blockId.Justified():
		root = a.forkchoiceStore.JustifiedCheckpoint().BlockRoot()
	case blockId.Genesis():
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return libcommon.Hash{}, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, "genesis block not found")
		}
	case blockId.GetSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *blockId.GetSlot())
		if err != nil {
			return libcommon.Hash{}, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("block not found %d", *blockId.GetSlot()))
		}
	case blockId.GetRoot() != nil:
		// first check if it exists
		root = *blockId.GetRoot()
	default:
		return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusInternalServerError, "cannot parse block id")
	}
	return
}

func (a *ApiHandler) getBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
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
		return nil, err
	}
	return newBeaconResponse(blk).
		WithFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(blk.Version()), nil
}

func (a *ApiHandler) getBlindedBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
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
		return nil, err
	}
	blinded, err := blk.Blinded()
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(blinded).
		WithFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(blk.Version()), nil
}

func (a *ApiHandler) getBlockAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := beaconhttp.BlockIdFromRequest(r)
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
	return newBeaconResponse(blk.Block.Body.Attestations).
		WithFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(blk.Version()), nil
}

func (a *ApiHandler) getBlockRoot(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := beaconhttp.BlockIdFromRequest(r)
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
	return newBeaconResponse(struct {
		Root libcommon.Hash `json:"root"`
	}{Root: root}).WithFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
