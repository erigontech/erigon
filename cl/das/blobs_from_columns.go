// Copyright 2026 The Erigon Authors
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

package das

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	// ErrColumnsNotKept means the node does not keep the columns of the slot, so blobs cannot be built from them.
	ErrColumnsNotKept = errors.New("the node does not keep the columns of the slot")
	// ErrBlobsUnavailable means a column that the blobs need is missing or malformed. Recovery is asked to rebuild it.
	ErrBlobsUnavailable = errors.New("blob data not available")
	// ErrReconstructionBusy means too many reconstructions are running or waiting.
	ErrReconstructionBusy = errors.New("blob reconstruction busy")
)

type reconstructionAdmission struct {
	permits chan struct{}
	waiting atomic.Int32
}

func newReconstructionAdmission(permits int) *reconstructionAdmission {
	return &reconstructionAdmission{permits: make(chan struct{}, permits)}
}

var blobReconstructions = newReconstructionAdmission(max(1, runtime.GOMAXPROCS(0)/2))

func (a *reconstructionAdmission) acquire(ctx context.Context) (func(), error) {
	release := func() { <-a.permits }
	select {
	case a.permits <- struct{}{}:
		return release, nil
	default:
	}
	if a.waiting.Add(1) > int32(cap(a.permits)) {
		a.waiting.Add(-1)
		return nil, ErrReconstructionBusy
	}
	defer a.waiting.Add(-1)
	select {
	case a.permits <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *peerdas) BlobsFromColumns(ctx context.Context, slot uint64, blockRoot common.Hash, indices []uint64, withSidecars bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
	if !d.keepsAllColumns(slot) || slot < d.state.GetEarliestAvailableSlot() {
		return nil, nil, ErrColumnsNotKept
	}
	isGloas := d.beaconConfig.GetCurrentStateVersion(slot/d.beaconConfig.SlotsPerEpoch) >= clparams.GloasVersion
	if isGloas && withSidecars {
		return nil, nil, errors.New("blob sidecars are not built from the columns of a Gloas block")
	}
	release, err := blobReconstructions.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	var gloasCommitments *solid.ListSSZ[*cltypes.KZGCommitment]
	if isGloas {
		if gloasCommitments, err = d.getKzgCommitmentsForGloas(ctx, blockRoot); err != nil {
			return nil, nil, err
		}
	}
	var (
		rows           []int
		blobs          []*cltypes.Blob
		commitments    *solid.ListSSZ[*cltypes.KZGCommitment]
		header         *cltypes.SignedBeaconBlockHeader
		inclusionProof solid.HashVectorSSZ
	)
	for column := range d.beaconConfig.NumberOfColumns / 2 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		sidecar, err := d.readBlockColumn(ctx, slot, blockRoot, column, isGloas, gloasCommitments)
		if err != nil {
			return nil, nil, err
		}
		if column == 0 {
			for _, index := range indices {
				if index < uint64(sidecar.Column.Len()) {
					rows = append(rows, int(index))
				}
			}
			if len(rows) == 0 {
				return nil, nil, nil
			}
			blobs = make([]*cltypes.Blob, len(rows))
			for i := range blobs {
				blobs[i] = new(cltypes.Blob)
			}
			commitments, header, inclusionProof = sidecar.KzgCommitments, sidecar.SignedBlockHeader, sidecar.KzgCommitmentsInclusionProof
		}
		for i, row := range rows {
			copy(blobs[i][int(column)*cltypes.BytesPerCell:], sidecar.Column.Get(row)[:])
		}
	}
	if !withSidecars {
		return blobs, nil, nil
	}
	sidecars := make([]*cltypes.BlobSidecar, len(rows))
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if sidecars[i], err = newBlobSidecar(row, blobs[i], commitments, header, inclusionProof, 1); err != nil {
			return nil, nil, err
		}
	}
	return blobs, sidecars, nil
}

func (d *peerdas) readBlockColumn(ctx context.Context, slot uint64, blockRoot common.Hash, column uint64, isGloas bool, gloasCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (*cltypes.DataColumnSidecar, error) {
	sidecar, err := d.columnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, int64(column))
	indexed := errors.Is(err, os.ErrNotExist) && d.columnIsIndexed(ctx, slot, blockRoot, column)
	if indexed {
		sidecar, err = d.columnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, int64(column))
	}
	if err == nil && isBlockColumn(sidecar, blockRoot, isGloas, gloasCommitments) {
		return sidecar, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// Recovery never rebuilds an indexed column, so an unreadable one is removed.
	if err == nil || indexed {
		if removeErr := d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, int64(column)); removeErr != nil {
			log.Warn("[peerdas] failed to remove a column that cannot serve blobs", "slot", slot, "blockRoot", blockRoot, "column", column, "err", removeErr)
			return nil, ErrBlobsUnavailable
		}
	}
	if err := d.TryScheduleRecover(slot, blockRoot); err != nil {
		log.Warn("[peerdas] failed to schedule blob recovery for a column that cannot serve blobs", "slot", slot, "blockRoot", blockRoot, "column", column, "err", err)
	}
	return nil, ErrBlobsUnavailable
}

func (d *peerdas) columnIsIndexed(ctx context.Context, slot uint64, blockRoot common.Hash, column uint64) bool {
	exists, err := d.columnStorage.ColumnSidecarExists(ctx, slot, blockRoot, int64(column))
	return err == nil && exists
}
