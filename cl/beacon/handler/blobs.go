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
	"errors"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
)

var blobSidecarSSZLenght = (*cltypes.BlobSidecar)(nil).EncodingSizeSSZ()

func (a *ApiHandler) GetEthV1BeaconBlobSidecars(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
	blockRoot, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}
	if a.caplinSnapshots != nil && *slot <= a.caplinSnapshots.FrozenBlobs() {
		out, err := a.caplinSnapshots.ReadBlobSidecars(*slot)
		if err != nil {
			return nil, err
		}
		resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
		for _, v := range out {
			resp.Append(v)
		}
		return beaconhttp.NewBeaconResponse(resp), nil

	}
	out, found, err := a.blobStoage.ReadBlobSidecars(ctx, *slot, blockRoot)
	if err != nil {
		return nil, err
	}
	strIdxs, err := beaconhttp.StringListFromQueryParams(r, "indices")
	if err != nil {
		return nil, err
	}
	resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
	if !found {
		return beaconhttp.NewBeaconResponse(resp), nil
	}
	if len(strIdxs) == 0 {
		for _, v := range out {
			resp.Append(v)
		}
	} else {
		included := make(map[uint64]struct{})
		for _, idx := range strIdxs {
			i, err := strconv.ParseUint(idx, 10, 64)
			if err != nil {
				return nil, err
			}
			included[i] = struct{}{}
		}
		for _, v := range out {
			if _, ok := included[v.Index]; ok {
				resp.Append(v)
			}
		}
	}

	return beaconhttp.NewBeaconResponse(resp), nil
}

func (a *ApiHandler) GetEthV1DebugBeaconDataColumnSidecars(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	blockRoot, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	// indicies query
	indices, err := beaconhttp.StringListFromQueryParams(r, "indices")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	columnIndices := []uint64{}
	if len(indices) == 0 {
		// take all custodies
		var err error
		columnIndices, err = a.columnStorage.GetSavedColumnIndex(ctx, *slot, blockRoot)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
	} else {
		for _, index := range indices {
			i, err := strconv.ParseUint(index, 10, 64)
			if err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
			}
			columnIndices = append(columnIndices, i)
		}
	}
	// read the columns
	dataColumnSidecars := []*cltypes.DataColumnSidecar{}
	for _, index := range columnIndices {
		sidecar, err := a.columnStorage.ReadColumnSidecarByColumnIndex(ctx, *slot, blockRoot, int64(index))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
		if sidecar != nil {
			dataColumnSidecars = append(dataColumnSidecars, sidecar)
		}
	}

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	return beaconhttp.NewBeaconResponse(dataColumnSidecars).
		WithHeader("Eth-Consensus-Version", version.String()).
		WithVersion(version).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
