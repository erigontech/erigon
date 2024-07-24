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
	"fmt"
	"net/http"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
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
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("genesis block not found"))
		}
	case blockId.GetSlot() != nil:
		root, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *blockId.GetSlot())
		if err != nil {
			return libcommon.Hash{}, err
		}
		if root == (libcommon.Hash{}) {
			return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %d", *blockId.GetSlot()))
		}
	case blockId.GetRoot() != nil:
		// first check if it exists
		root = *blockId.GetRoot()
	default:
		return libcommon.Hash{}, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("cannot parse block id"))
	}
	return
}

func (a *ApiHandler) GetEthV1BeaconBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)

	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", root))
	}
	// Check if the block is canonical
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, blk.Block.Slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(blk).
		WithFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(blk.Version()).WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) GetEthV1BlindedBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", root))
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
		WithVersion(blk.Version()).WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) GetEthV1BeaconBlockAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", root))
	}
	// Check if the block is canonical
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, blk.Block.Slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(blk.Block.Body.Attestations).
		WithFinalized(root == canonicalRoot && blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(blk.Version()).WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) GetEthV1BeaconBlockRoot(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	// check if the root exist
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", root))
	}
	// Check if the block is canonical
	var canonicalRoot libcommon.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}
	return newBeaconResponse(struct {
		Root libcommon.Hash `json:"root"`
	}{Root: root}).WithFinalized(canonicalRoot == root && *slot <= a.forkchoiceStore.FinalizedSlot()).WithOptimistic(isOptimistic), nil
}
