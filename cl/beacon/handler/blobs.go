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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/utils"
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

	// reject after fulu fork
	if a.beaconChainCfg.GetCurrentStateVersion(*slot/a.beaconChainCfg.SlotsPerEpoch) >= clparams.FuluVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("blobs are not supported after fulu fork"))
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

func (a *ApiHandler) GetEthV1BeaconBlobs(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	reqVersion := a.beaconChainCfg.GetCurrentStateVersion(*slot / a.beaconChainCfg.SlotsPerEpoch)
	if reqVersion >= clparams.FuluVersion {
		if !a.peerDas.IsArchivedMode() && !a.peerDas.StateReader().IsSupernode() {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("blobs are not supported for non-archived mode and non-supernode"))
		}
	}

	// versioned_hashes: Array of versioned hashes for blobs to request for in the specified block. Returns all blobs in the block if not specified.
	versionedHashes, err := beaconhttp.StringListFromQueryParams(r, "versioned_hashes")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	// read the blobs
	block, err := a.blockReader.ReadBlockByRoot(ctx, tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if block == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}

	indicies := []uint64{}
	if versionedHashes == nil {
		// take all blobs
		indicies = make([]uint64, block.Block.Body.BlobKzgCommitments.Len())
		for i := range indicies {
			indicies[i] = uint64(i)
		}
	} else {
		// take the blobs by the versioned hashes
		versionedHashesToIndex := make(map[common.Hash]uint64)
		block.Block.Body.BlobKzgCommitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
			hash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*value))
			if err != nil {
				return false
			}
			versionedHashesToIndex[hash] = uint64(index)
			return true
		})
		for _, hash := range versionedHashes {
			index, ok := versionedHashesToIndex[common.HexToHash(hash)]
			if ok {
				indicies = append(indicies, index)
			}
		}
	}

	// collect the blobs
	blobs := solid.NewStaticListSSZ[*cltypes.Blob](int(a.beaconChainCfg.MaxBlobCommittmentsPerBlock), int(cltypes.BYTES_PER_BLOB))
	blobSidecars, _, err := a.blobStoage.ReadBlobSidecars(ctx, *slot, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	for _, index := range indicies {
		if index >= uint64(len(blobSidecars)) {
			log.Warn("blob index out of range", "index", index, "len", len(blobSidecars))
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, errors.New("blob index out of range"))
		}
		blobs.Append(&blobSidecars[index].Blob)
	}

	return beaconhttp.NewBeaconResponse(blobs).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()), nil
}
