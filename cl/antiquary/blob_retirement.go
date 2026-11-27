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

package antiquary

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
)

type BlobRetireResult struct {
	From            uint64
	To              uint64
	Gap             uint64
	Blocked         bool
	Files           int
	SidecarsRemoved int
}

type BlobRetireHooks struct {
	CheckedTo          *uint64
	ReportGap          func(from, gap uint64)
	CompressionWorkers func(from, backlogTo uint64) (int, func())
	Seed               func([]string) error
	RemoveOverlaps     func([]string) error
	PruneError         func(error)
}

func RetireBlobSidecars(ctx context.Context, db kv.RwDB, storage blob_storage.BlobStorage, snapshots *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, salt uint32, logger log.Logger, hooks BlobRetireHooks) (BlobRetireResult, error) {
	if err := ctx.Err(); err != nil {
		return BlobRetireResult{}, err
	}
	from, to, backlogTo, ok := nextBlobSegment(snapshots, cfg)
	if !ok {
		return BlobRetireResult{}, nil
	}
	var err error
	checkedTo := hooks.CheckedTo
	if checkedTo == nil {
		checkedTo = new(uint64)
	}
	blobCount := freezeblocks.BlobCountFromBlocks(ctx, snapshots, reader)
	var complete bool
	var gap uint64
	err = db.View(ctx, func(tx kv.Tx) error {
		var err error
		complete, gap, err = blobSegmentComplete(ctx, tx, storage, from, to, checkedTo, blobCount, hooks.ReportGap)
		return err
	})
	if err != nil {
		return BlobRetireResult{}, err
	}
	if !complete {
		return BlobRetireResult{From: from, To: to, Gap: gap, Blocked: true}, nil
	}

	logger.Info("[Antiquary] Antiquating blobs", "from", from, "to", to, "backlogTo", backlogTo)
	workers := 1
	release := func() {}
	if hooks.CompressionWorkers != nil {
		workers, release = hooks.CompressionWorkers(from, backlogTo)
	}
	err = freezeblocks.DumpBlobsSidecar(ctx, storage, db, from, to, salt, dirs, workers, blobCount, log.LvlDebug, logger)
	release()
	if err != nil {
		return BlobRetireResult{}, err
	}
	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	logger.Info("[Antiquary] Finished Antiquating blobs", "from", from, "to", to)
	if err := snapshots.OpenFolder(); err != nil {
		return BlobRetireResult{}, err
	}
	if err := snapshots.RemoveOverlaps(hooks.RemoveOverlaps); err != nil {
		logger.Warn("[Antiquary] Failed to remove block overlaps", "err", err)
	}
	paths := snapshots.SegFileNames(from, to)
	if hooks.Seed != nil {
		if err := hooks.Seed(paths); err != nil {
			logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}

	newRemoved, err := removePublishedBlobSidecars(ctx, db, storage, [][2]uint64{{from, to}}, hooks.PruneError)
	if err != nil {
		return BlobRetireResult{}, err
	}
	return BlobRetireResult{From: from, To: to, Files: len(paths) * 2, SidecarsRemoved: newRemoved}, nil
}

func PrunePublishedBlobSidecars(ctx context.Context, db kv.RwDB, storage blob_storage.BlobStorage, snapshots *freezeblocks.CaplinSnapshots, onError func(error)) (int, error) {
	hotFrom, hotFound, err := storage.EarliestStoredSlot()
	if err != nil || !hotFound {
		return 0, err
	}
	view := snapshots.View()
	ranges := make([][2]uint64, 0, len(view.BlobSidecars()))
	for _, segment := range view.BlobSidecars() {
		from := max(segment.From(), hotFrom)
		if from < segment.To() {
			ranges = append(ranges, [2]uint64{from, segment.To()})
		}
	}
	view.Close()
	return removePublishedBlobSidecars(ctx, db, storage, ranges, onError)
}

func removePublishedBlobSidecars(ctx context.Context, db kv.RwDB, storage blob_storage.BlobStorage, ranges [][2]uint64, onError func(error)) (int, error) {
	removed := 0
	err := db.View(ctx, func(tx kv.Tx) error {
		for _, span := range ranges {
			for slot := span[0]; slot < span[1]; slot++ {
				blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
				if err != nil {
					return err
				}
				count, err := storage.KzgCommitmentsCount(ctx, blockRoot)
				if err != nil {
					return err
				}
				if count == 0 {
					continue
				}
				if err := storage.RemoveBlobSidecars(ctx, slot, blockRoot); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if onError == nil {
						return err
					}
					onError(err)
					continue
				}
				removed++
			}
		}
		return nil
	})
	return removed, err
}

func blobSegmentComplete(ctx context.Context, tx kv.Tx, storage blob_storage.BlobStorage, from, to uint64, checkedTo *uint64, blobCount freezeblocks.BlobCountBySlotFn, reportGap func(from, gap uint64)) (bool, uint64, error) {
	start := max(from, *checkedTo)
	for slot := start; slot < to; slot++ {
		want, skipped, err := blobCount(tx, slot)
		if err != nil {
			return false, 0, err
		}
		if skipped {
			continue
		}
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return false, 0, err
		}
		stored, err := blob_storage.BlobsStored(ctx, storage, slot, blockRoot, int(want))
		if err != nil {
			return false, 0, err
		}
		if !stored {
			*checkedTo = slot
			if reportGap != nil {
				reportGap(from, slot)
			}
			return false, slot, nil
		}
	}
	if start > from {
		*checkedTo = from
		return blobSegmentComplete(ctx, tx, storage, from, to, checkedTo, blobCount, reportGap)
	}
	mxBlobRetirementGapSlot.SetUint64(0)
	return true, 0, nil
}
