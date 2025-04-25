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
	"fmt"
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
)

func (a *ApiHandler) getHeaders(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	querySlot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	queryParentHash, err := beaconhttp.HashFromQueryParams(r, "parent_root")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var candidates []common.Hash
	var potentialRoot common.Hash
	// First lets find some good candidates for the query. TODO(Giulio2002): this does not give all the headers.
	switch {
	case queryParentHash != nil:
		// get all blocks with this parent
		blockRoots, err := beacon_indicies.ReadBlockRootsByParentRoot(tx, *queryParentHash)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, blockRoots...)
	case querySlot != nil:
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, *querySlot)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, potentialRoot)
	default:
		headSlot := a.syncedData.HeadSlot()
		if headSlot == 0 {
			break
		}
		potentialRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, headSlot)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, potentialRoot)
	}
	// Now we assemble the response
	anyOptimistic := false
	headers := make([]*headerResponse, 0, len(candidates))
	for _, root := range candidates {
		signedHeader, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
		if err != nil {
			return nil, err
		}
		if signedHeader == nil || (queryParentHash != nil && signedHeader.Header.ParentRoot != *queryParentHash) || (querySlot != nil && signedHeader.Header.Slot != *querySlot) {
			continue
		}

		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
		if err != nil {
			return nil, err
		}
		headers = append(headers, &headerResponse{
			Root:      root,
			Canonical: canonicalRoot == root,
			Header:    signedHeader,
		})
		anyOptimistic = anyOptimistic || a.forkchoiceStore.IsRootOptimistic(root)
	}
	return newBeaconResponse(headers).WithOptimistic(anyOptimistic), nil
}

func (a *ApiHandler) getHeader(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	signedHeader, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}

	if signedHeader == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", root))
	}
	var canonicalRoot common.Hash
	canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, signedHeader.Header.Slot)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(&headerResponse{
		Root:      root,
		Canonical: canonicalRoot == root,
		Header:    signedHeader,
	}).WithFinalized(canonicalRoot == root && signedHeader.Header.Slot <= a.forkchoiceStore.FinalizedSlot()).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)), nil
}
