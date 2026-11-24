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
	"strings"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var blobSidecarSSZLenght = (*cltypes.BlobSidecar)(nil).EncodingSizeSSZ()

type caplinBlobSnapshotReader interface {
	FrozenBlobs() uint64
	ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error)
}

func parseSelector[K comparable](r *http.Request, name string, parse func(string) (K, error)) (map[K]struct{}, error) {
	values, err := beaconhttp.StringListFromQueryParams(r, name)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if values == nil {
		return nil, nil
	}
	selected := make(map[K]struct{})
	for _, value := range values {
		key, err := parse(value)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid %s %q: %w", name, value, err))
		}
		if _, ok := selected[key]; ok {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("duplicate %s %q", name, value))
		}
		selected[key] = struct{}{}
	}
	return selected, nil
}

func parseVersionedHash(value string) (hash common.Hash, err error) {
	if !strings.HasPrefix(value, "0x") {
		return hash, errors.New("missing 0x prefix")
	}
	err = hash.UnmarshalText([]byte(value))
	return hash, err
}

func (a *ApiHandler) GetEthV1BeaconBlobSidecars(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	selectedIndices, err := parseSelector(r, "indices", func(value string) (uint64, error) { return strconv.ParseUint(value, 10, 64) })
	if err != nil {
		return nil, err
	}
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	blockRoot, err := a.rootFromBlockId(tx, blockId)
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

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	out, found, err := a.readBlobSidecars(ctx, *slot, blockRoot, canonicalRoot)
	if err != nil {
		return nil, err
	}

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
	isFinalized := canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()

	resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
	if !found {
		return beaconhttp.NewBeaconResponse(resp).
			WithFinalized(isFinalized).
			WithVersion(version).
			WithOptimistic(isOptimistic), nil
	}
	for _, v := range out {
		if _, ok := selectedIndices[v.Index]; ok || selectedIndices == nil {
			resp.Append(v)
		}
	}

	return beaconhttp.NewBeaconResponse(resp).
		WithFinalized(isFinalized).
		WithVersion(version).
		WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) readBlobSidecars(ctx context.Context, slot uint64, blockRoot, canonicalRoot common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	if blockRoot == canonicalRoot && a.caplinSnapshots != nil && slot < a.caplinSnapshots.FrozenBlobs() {
		sidecars, err := a.caplinSnapshots.ReadBlobSidecars(slot)
		if err != nil {
			return nil, false, err
		}
		return sidecars, len(sidecars) != 0, nil
	}
	return a.blobStoage.ReadBlobSidecars(ctx, slot, blockRoot)
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
	blockRoot, err := a.rootFromBlockId(tx, blockId)
	if err != nil {
		return nil, err
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
			sidecar.BlockRoot = blockRoot
			sidecar.Slot = *slot
			dataColumnSidecars = append(dataColumnSidecars, sidecar)
		}
	}

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	return beaconhttp.NewBeaconResponse(dataColumnSidecars).
		WithHeader("Eth-Consensus-Version", version.String()).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
		WithVersion(version).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
}

func (a *ApiHandler) GetEthV1BeaconBlobs(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	// versioned_hashes: Array of versioned hashes for blobs to request for in the specified block. Returns all blobs in the block if not specified.
	selectedHashes, err := parseSelector(r, "versioned_hashes", parseVersionedHash)
	if err != nil {
		return nil, err
	}
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	blockRoot, err := a.rootFromBlockId(tx, blockId)
	if err != nil {
		return nil, err
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

	// read the blobs
	block, err := a.blockReader.ReadBlockByRoot(ctx, tx, blockRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if block == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}

	var indicies []uint64
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil {
		commitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48)
	}
	if selectedHashes == nil {
		// take all blobs
		indicies = make([]uint64, commitments.Len())
		for i := range indicies {
			indicies[i] = uint64(i)
		}
	} else {
		// take the blobs by the versioned hashes
		filtered := make([]uint64, 0, len(selectedHashes))
		commitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
			hash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*value))
			if err != nil {
				return false
			}
			if _, ok := selectedHashes[hash]; ok {
				filtered = append(filtered, uint64(index))
			}
			return true
		})
		indicies = filtered
	}

	// collect the blobs
	blobs := solid.NewStaticListSSZ[*cltypes.Blob](int(a.beaconChainCfg.MaxBlobCommittmentsPerBlock), int(cltypes.BYTES_PER_BLOB))
	blobSidecars, found, err := a.readBlobSidecars(ctx, *slot, blockRoot, canonicalRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if !found {
		return beaconhttp.NewBeaconResponse(blobs).
			WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
			WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
	}
	for _, index := range indicies {
		if index >= uint64(len(blobSidecars)) {
			log.Warn("blob index out of range", "index", index, "len", len(blobSidecars))
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, errors.New("blob index out of range"))
		}
		blobs.Append(&blobSidecars[index].Blob)
	}

	return beaconhttp.NewBeaconResponse(blobs).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
}
