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

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
)

var blobSidecarSSZLenght = (*cltypes.BlobSidecar)(nil).EncodingSizeSSZ()

type caplinBlobSnapshotReader interface {
	FrozenBlobs() uint64
	ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error)
}

// BlobBackfillStatus reports whether canonical blob history is still incomplete at a slot.
type BlobBackfillStatus interface {
	BlobBackfillPending(slot uint64) bool
}

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

	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, *slot)
	if err != nil {
		return nil, err
	}

	strIdxs, err := beaconhttp.StringListFromQueryParams(r, "indices")
	if err != nil {
		return nil, err
	}
	included := make(map[uint64]struct{}, len(strIdxs))
	for _, idx := range strIdxs {
		i, err := strconv.ParseUint(idx, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if _, duplicate := included[i]; duplicate {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("duplicate blob index %d", i))
		}
		included[i] = struct{}{}
	}

	out, complete, pending, err := a.readBlobSidecarsWithBackfillStatus(ctx, *slot, blockRoot, canonicalRoot)
	if err != nil {
		return nil, err
	}
	{
		block, err := a.blockReader.ReadBlockByRoot(ctx, tx, blockRoot)
		if (err != nil || block == nil) && blockRoot == canonicalRoot {
			block, err = a.blockReader.ReadBeaconBlockBodyBySlot(ctx, tx, *slot)
		}
		if err != nil {
			return nil, err
		}
		if block == nil || block.Block == nil || block.Block.Body == nil {
			return nil, errors.New("requested block body is unavailable")
		}
		if block.Version() < clparams.DenebVersion {
			out = nil
			complete = true
		} else {
			var semanticallyComplete bool
			out, semanticallyComplete, err = validateBlobSidecars(block, blockRoot, out)
			if err != nil {
				return nil, err
			}
			complete = complete && semanticallyComplete
		}
	}

	version := a.ethClock.StateVersionByEpoch(*slot / a.beaconChainCfg.SlotsPerEpoch)
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
	isFinalized := canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()

	resp := solid.NewStaticListSSZ[*cltypes.BlobSidecar](696969, blobSidecarSSZLenght)
	includeAll := len(strIdxs) == 0
	if includeAll {
		for _, v := range out {
			resp.Append(v)
		}
	} else {
		for _, v := range out {
			if _, ok := included[v.Index]; ok {
				resp.Append(v)
			}
		}
	}
	unfrozenCanonicalIncomplete := !complete && a.shouldRequireCompleteCanonicalBlobs(*slot, blockRoot, canonicalRoot)
	if pending || unfrozenCanonicalIncomplete {
		missingExpected, err := a.requestedBlobSidecarsMissing(ctx, tx, *slot, included, includeAll, out)
		if err != nil {
			return nil, err
		}
		if missingExpected {
			return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("blob sidecars are still being backfilled"))
		}
	}

	return beaconhttp.NewBeaconResponse(resp).
		WithFinalized(isFinalized).
		WithVersion(version).
		WithOptimistic(isOptimistic), nil
}

func (a *ApiHandler) blobBackfillPending(slot uint64, blockRoot, canonicalRoot common.Hash) bool {
	if a.blobBackfillStatus == nil || !a.blobBackfillStatus.BlobBackfillPending(slot) || blockRoot != canonicalRoot {
		return false
	}
	if a.caplinSnapshots != nil && slot < a.caplinSnapshots.FrozenBlobs() {
		return false
	}
	return true
}

func (a *ApiHandler) shouldRequireCompleteCanonicalBlobs(slot uint64, blockRoot, canonicalRoot common.Hash) bool {
	return blockRoot == canonicalRoot && (a.caplinSnapshots == nil || slot >= a.caplinSnapshots.FrozenBlobs())
}

func validateBlobSidecars(block *cltypes.SignedBeaconBlock, blockRoot common.Hash, sidecars []*cltypes.BlobSidecar) ([]*cltypes.BlobSidecar, bool, error) {
	if block == nil || block.Block == nil || block.Block.Body == nil {
		return nil, false, errors.New("canonical block body is unavailable")
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil {
		return nil, false, errors.New("canonical block has nil blob commitments")
	}
	canonicalRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, false, err
	}
	if canonicalRoot != blockRoot {
		return nil, false, errors.New("canonical block body does not match block root")
	}
	validByIndex := make([]*cltypes.BlobSidecar, commitments.Len())
	seen := make(map[uint64]struct{}, len(sidecars))
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil ||
			sidecar.Index >= uint64(commitments.Len()) || commitments.Get(int(sidecar.Index)) == nil {
			continue
		}
		if _, duplicate := seen[sidecar.Index]; duplicate {
			continue
		}
		sidecarRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil || sidecarRoot != blockRoot || sidecar.SignedBlockHeader.Signature != block.Signature ||
			sidecar.KzgCommitment != common.Bytes48(*commitments.Get(int(sidecar.Index))) {
			continue
		}
		if err := kzg.Ctx().VerifyBlobKZGProof(
			(*goethkzg.Blob)(&sidecar.Blob),
			goethkzg.KZGCommitment(sidecar.KzgCommitment),
			goethkzg.KZGProof(sidecar.KzgProof),
		); err != nil {
			continue
		}
		if block.Version() < clparams.GloasVersion && !cltypes.VerifyCommitmentInclusionProof(
			sidecar.KzgCommitment,
			sidecar.CommitmentInclusionProof,
			sidecar.Index,
			clparams.DenebVersion,
			sidecar.SignedBlockHeader.Header.BodyRoot,
		) {
			continue
		}
		seen[sidecar.Index] = struct{}{}
		validByIndex[sidecar.Index] = sidecar
	}
	valid := make([]*cltypes.BlobSidecar, 0, len(seen))
	for _, sidecar := range validByIndex {
		if sidecar != nil {
			valid = append(valid, sidecar)
		}
	}
	return valid, len(seen) == commitments.Len(), nil
}

func (a *ApiHandler) requestedBlobSidecarsMissing(ctx context.Context, tx kv.Tx, slot uint64, included map[uint64]struct{}, includeAll bool, available []*cltypes.BlobSidecar) (bool, error) {
	block, err := a.blockReader.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
	if err != nil {
		return false, err
	}
	if block == nil {
		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return false, err
		}
		if canonicalRoot != (common.Hash{}) {
			return false, errors.New("canonical block body is unavailable")
		}
		return false, nil
	}
	if block.Block == nil || block.Block.Body == nil {
		return false, errors.New("block body is missing")
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil {
		return false, errors.New("canonical block has nil blob commitments")
	}
	if commitments.Len() == 0 {
		return false, nil
	}
	availableIndices := make(map[uint64]struct{}, len(available))
	for _, sidecar := range available {
		availableIndices[sidecar.Index] = struct{}{}
	}
	if includeAll {
		return len(availableIndices) < commitments.Len(), nil
	}
	for index := range included {
		_, available := availableIndices[index]
		if index < uint64(commitments.Len()) && !available {
			return true, nil
		}
	}
	return false, nil
}

func (a *ApiHandler) readBlobSidecars(ctx context.Context, slot uint64, blockRoot, canonicalRoot common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	if blockRoot == canonicalRoot && a.caplinSnapshots != nil && slot < a.caplinSnapshots.FrozenBlobs() {
		sidecars, err := a.caplinSnapshots.ReadBlobSidecars(slot)
		if err != nil {
			return nil, false, err
		}
		return sidecars, len(sidecars) != 0, nil
	}
	sidecars, complete, err := a.blobStoage.ReadBlobSidecars(ctx, slot, blockRoot)
	if err != nil || complete || blockRoot != canonicalRoot || a.caplinSnapshots == nil || slot >= a.caplinSnapshots.FrozenBlobs() {
		return sidecars, complete, err
	}
	snapshotSidecars, err := a.caplinSnapshots.ReadBlobSidecars(slot)
	if err != nil || len(snapshotSidecars) == 0 {
		return sidecars, complete, err
	}
	return snapshotSidecars, true, nil
}

func (a *ApiHandler) readBlobSidecarsWithBackfillStatus(ctx context.Context, slot uint64, blockRoot, canonicalRoot common.Hash) ([]*cltypes.BlobSidecar, bool, bool, error) {
	pendingBefore := a.blobBackfillPending(slot, blockRoot, canonicalRoot)
	sidecars, complete, err := a.readBlobSidecars(ctx, slot, blockRoot, canonicalRoot)
	if err != nil {
		return sidecars, complete, false, err
	}
	pendingAfter := a.blobBackfillPending(slot, blockRoot, canonicalRoot)
	if complete || pendingBefore == pendingAfter || pendingAfter {
		return sidecars, complete, pendingAfter, nil
	}
	reread, complete, err := a.readBlobSidecars(ctx, slot, blockRoot, canonicalRoot)
	if err != nil || complete {
		return reread, complete, false, err
	}
	byIndex := make(map[uint64]struct{}, len(sidecars)+len(reread))
	merged := make([]*cltypes.BlobSidecar, 0, len(sidecars)+len(reread))
	for _, source := range [][]*cltypes.BlobSidecar{sidecars, reread} {
		for _, sidecar := range source {
			if sidecar == nil {
				continue
			}
			if _, exists := byIndex[sidecar.Index]; exists {
				continue
			}
			byIndex[sidecar.Index] = struct{}{}
			merged = append(merged, sidecar)
		}
	}
	return merged, false, true, nil
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
	if block.Block == nil || block.Block.Body == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, errors.New("block body is missing"))
	}

	indicies := []uint64{}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, errors.New("block has nil blob commitments"))
	}
	if versionedHashes == nil {
		// take all blobs
		indicies = make([]uint64, commitments.Len())
		for i := range indicies {
			indicies[i] = uint64(i)
		}
	} else {
		selected := make(map[common.Hash]struct{}, len(versionedHashes))
		for _, encodedHash := range versionedHashes {
			var hash common.Hash
			if err := hash.UnmarshalText([]byte(encodedHash)); err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid versioned hash %q: %w", encodedHash, err))
			}
			if _, duplicate := selected[hash]; duplicate {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("duplicate versioned hash %s", hash))
			}
			selected[hash] = struct{}{}
		}
		commitments.Range(func(index int, value *cltypes.KZGCommitment, length int) bool {
			hash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*value))
			if err != nil {
				return false
			}
			if _, ok := selected[hash]; ok {
				indicies = append(indicies, uint64(index))
			}
			return true
		})
	}

	// collect the blobs
	blobs := solid.NewStaticListSSZ[*cltypes.Blob](int(a.beaconChainCfg.MaxBlobCommittmentsPerBlock), int(cltypes.BYTES_PER_BLOB))
	blobSidecars, complete, pending, err := a.readBlobSidecarsWithBackfillStatus(ctx, *slot, blockRoot, canonicalRoot)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if block.Version() >= clparams.DenebVersion {
		var semanticallyComplete bool
		blobSidecars, semanticallyComplete, err = validateBlobSidecars(block, blockRoot, blobSidecars)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
		complete = complete && semanticallyComplete
	}
	byIndex := make(map[uint64]*cltypes.BlobSidecar, len(blobSidecars))
	for _, sidecar := range blobSidecars {
		if sidecar == nil {
			continue
		}
		byIndex[sidecar.Index] = sidecar
	}
	for _, index := range indicies {
		if sidecar := byIndex[index]; sidecar != nil {
			blobs.Append(&sidecar.Blob)
		}
	}
	unfrozenCanonicalIncomplete := !complete && a.shouldRequireCompleteCanonicalBlobs(*slot, blockRoot, canonicalRoot)
	if (pending || unfrozenCanonicalIncomplete) && blobs.Len() < len(indicies) {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("blobs are still being backfilled"))
	}

	return beaconhttp.NewBeaconResponse(blobs).
		WithFinalized(canonicalRoot == blockRoot && *slot <= a.forkchoiceStore.FinalizedSlot()).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
}
