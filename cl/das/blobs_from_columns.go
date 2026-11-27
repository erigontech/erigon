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
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	// ErrColumnsNotKept means the node does not keep the columns of the slot, so blobs cannot be built from them.
	ErrColumnsNotKept = errors.New("the node does not keep the columns of the slot")
	// ErrBlobsUnavailable means a column that the blobs need is missing or malformed. Recovery is asked to rebuild it.
	ErrBlobsUnavailable = errors.New("blob data not available")
	// ErrReconstructionBusy means too many reconstructions are running or waiting, or a reconstruction did not finish before its deadline.
	ErrReconstructionBusy = errors.New("blob reconstruction busy")
)

type reconstructionAdmission struct {
	permits chan struct{}
	waiting atomic.Int32
	joined  atomic.Int32
}

func newReconstructionAdmission(permits int) *reconstructionAdmission {
	return &reconstructionAdmission{permits: make(chan struct{}, permits)}
}

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

func (a *reconstructionAdmission) join() (leave func(), err error) {
	if a.joined.Add(1) > int32(2*cap(a.permits)) {
		a.joined.Add(-1)
		return nil, ErrReconstructionBusy
	}
	return func() { a.joined.Add(-1) }, nil
}

type blobBuild struct {
	done     chan struct{}
	deadline time.Time
	sidecars []*cltypes.BlobSidecar
	err      error
}

const builtBlobSidecarBlocks = 4

func (d *peerdas) initBlobBuilds() {
	d.reconstructions = newReconstructionAdmission(max(1, runtime.GOMAXPROCS(0)/2))
	d.blobBuilds = make(map[common.Hash]*blobBuild)
	d.builtBlobSidecars, _ = lru.New[common.Hash, []*cltypes.BlobSidecar]("builtBlobSidecars", builtBlobSidecarBlocks)
}

func (d *peerdas) BlobsFromColumns(ctx context.Context, slot uint64, blockRoot common.Hash, indices []uint64, withSidecars bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
	if !d.keepsAllColumns(slot) || slot < d.state.GetEarliestAvailableSlot() {
		return nil, nil, ErrColumnsNotKept
	}
	isGloas := d.beaconConfig.GetCurrentStateVersion(slot/d.beaconConfig.SlotsPerEpoch) >= clparams.GloasVersion
	if isGloas && withSidecars {
		return nil, nil, errors.New("blob sidecars are not built from the columns of a Gloas block")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if isGloas {
		return d.buildBlobs(ctx, slot, blockRoot, indices, withSidecars, true)
	}
	if built, ok := d.builtBlobSidecars.Get(blockRoot); ok {
		blobs, sidecars := selectBuilt(built, indices, withSidecars)
		return blobs, sidecars, nil
	}
	if indices != nil || !withSidecars {
		return d.buildBlobs(ctx, slot, blockRoot, indices, withSidecars, false)
	}
	built, err := d.sharedBlobSidecars(ctx, slot, blockRoot)
	if err != nil {
		return nil, nil, err
	}
	blobs, sidecars := selectBuilt(built, nil, true)
	return blobs, sidecars, nil
}

func selectBuilt(built []*cltypes.BlobSidecar, indices []uint64, withSidecars bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar) {
	selected := built
	if indices != nil {
		selected = make([]*cltypes.BlobSidecar, 0, len(indices))
		for _, index := range indices {
			if index < uint64(len(built)) {
				selected = append(selected, built[index])
			}
		}
	}
	if len(selected) == 0 {
		return nil, nil
	}
	blobs := make([]*cltypes.Blob, len(selected))
	for i, sidecar := range selected {
		blobs[i] = &sidecar.Blob
	}
	if !withSidecars {
		return blobs, nil
	}
	return blobs, selected
}

func (d *peerdas) sharedBlobSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) ([]*cltypes.BlobSidecar, error) {
	d.blobBuildsMu.Lock()
	if built, ok := d.builtBlobSidecars.Peek(blockRoot); ok {
		d.blobBuildsMu.Unlock()
		return built, nil
	}
	build, running := d.blobBuilds[blockRoot]
	if !running {
		build = &blobBuild{done: make(chan struct{}), deadline: time.Now().Add(d.slotDuration())}
		d.blobBuilds[blockRoot] = build
	}
	d.blobBuildsMu.Unlock()
	if running {
		return d.joinBlobBuild(ctx, build)
	}
	go d.runBlobBuild(ctx, slot, blockRoot, build)
	return d.waitBlobBuild(ctx, build)
}

func (d *peerdas) joinBlobBuild(ctx context.Context, build *blobBuild) ([]*cltypes.BlobSidecar, error) {
	leave, err := d.reconstructions.join()
	if err != nil {
		return nil, err
	}
	defer leave()
	return d.waitBlobBuild(ctx, build)
}

func (d *peerdas) waitBlobBuild(ctx context.Context, build *blobBuild) ([]*cltypes.BlobSidecar, error) {
	timer := time.NewTimer(time.Until(build.deadline))
	defer timer.Stop()
	select {
	case <-build.done:
		return build.sidecars, build.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		select {
		case <-build.done:
			return build.sidecars, build.err
		default:
			return nil, fmt.Errorf("%w: %w", ErrReconstructionBusy, context.DeadlineExceeded)
		}
	}
}

func (d *peerdas) runBlobBuild(ctx context.Context, slot uint64, blockRoot common.Hash, build *blobBuild) {
	// The starting request's cancellation must not end the build: other requests wait for its result.
	buildCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), build.deadline)
	defer cancel()
	var sidecars []*cltypes.BlobSidecar
	err := errors.New("blob reconstruction aborted")
	defer func() { d.publishBlobBuild(blockRoot, build, sidecars, err) }()
	defer func() {
		if r := recover(); r != nil {
			log.Error("[peerdas] panic in a blob reconstruction", "slot", slot, "blockRoot", blockRoot, "err", r, "stack", dbg.Stack())
			sidecars, err = nil, fmt.Errorf("blob reconstruction panicked: %v", r)
		}
	}()
	_, sidecars, err = d.buildBlobs(buildCtx, slot, blockRoot, nil, true, false)
	if buildErr := buildCtx.Err(); buildErr != nil && errors.Is(err, buildErr) {
		err = fmt.Errorf("%w: %w", ErrReconstructionBusy, err)
	}
}

func (d *peerdas) publishBlobBuild(blockRoot common.Hash, build *blobBuild, sidecars []*cltypes.BlobSidecar, err error) {
	d.blobBuildsMu.Lock()
	defer d.blobBuildsMu.Unlock()
	if err == nil && len(sidecars) > 0 {
		d.builtBlobSidecars.Add(blockRoot, sidecars)
	}
	build.sidecars, build.err = sidecars, err
	close(build.done)
	if d.blobBuilds[blockRoot] == build {
		delete(d.blobBuilds, blockRoot)
	}
}

func (d *peerdas) buildBlobs(ctx context.Context, slot uint64, blockRoot common.Hash, indices []uint64, withSidecars, isGloas bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
	saved, err := d.columnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
	if err != nil {
		return nil, nil, err
	}
	if len(saved) == 0 {
		return nil, nil, nil
	}
	var gloasCommitments *solid.ListSSZ[*cltypes.KZGCommitment]
	if isGloas {
		if gloasCommitments, err = d.getKzgCommitmentsForGloas(ctx, blockRoot); err != nil {
			return nil, nil, err
		}
	}
	first, err := d.readBlockColumn(ctx, slot, blockRoot, 0, isGloas, gloasCommitments)
	if err != nil {
		return nil, nil, err
	}
	blobCount := first.Column.Len()
	if limit := d.beaconConfig.GetBlobParameters(slot / d.beaconConfig.SlotsPerEpoch).MaxBlobsPerBlock; uint64(blobCount) > limit {
		return nil, nil, fmt.Errorf("the columns of the block hold %d blobs, more than the %d that slot %d allows", blobCount, limit, slot)
	}
	rows := make([]int, 0, blobCount)
	if indices == nil {
		for row := range blobCount {
			rows = append(rows, row)
		}
	} else {
		for _, index := range indices {
			if index < uint64(blobCount) {
				rows = append(rows, int(index))
			}
		}
	}
	if len(rows) == 0 {
		return nil, nil, nil
	}
	release, err := d.reconstructions.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	blobs := make([]*cltypes.Blob, len(rows))
	for i := range blobs {
		blobs[i] = new(cltypes.Blob)
	}
	sidecar := first
	for column := range d.beaconConfig.NumberOfColumns / 2 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if column > 0 {
			if sidecar, err = d.readBlockColumn(ctx, slot, blockRoot, column, isGloas, gloasCommitments); err != nil {
				return nil, nil, err
			}
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
		if sidecars[i], err = newBlobSidecar(row, blobs[i], first.KzgCommitments, first.SignedBlockHeader, first.KzgCommitmentsInclusionProof, 1); err != nil {
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
	if err == nil {
		log.Warn("[peerdas] removing malformed column sidecar", "slot", slot, "blockRoot", blockRoot, "column", column)
	}
	// Recovery never rebuilds an indexed column, so an unreadable one is removed. A column that a concurrent recovery
	// rebuilt after the read is removed too, and the next request schedules its rebuild.
	if err == nil || indexed {
		if removeErr := d.columnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, int64(column)); removeErr != nil {
			log.Warn("[peerdas] failed to remove a column that cannot serve blobs", "slot", slot, "blockRoot", blockRoot, "column", column, "err", removeErr)
			return nil, ErrBlobsUnavailable
		}
	}
	if err := d.scheduleRepair(slot, blockRoot); err != nil {
		log.Warn("[peerdas] failed to schedule blob recovery for a column that cannot serve blobs", "slot", slot, "blockRoot", blockRoot, "column", column, "err", err)
	}
	return nil, ErrBlobsUnavailable
}

func (d *peerdas) columnIsIndexed(ctx context.Context, slot uint64, blockRoot common.Hash, column uint64) bool {
	exists, err := d.columnStorage.ColumnSidecarExists(ctx, slot, blockRoot, int64(column))
	return err == nil && exists
}
