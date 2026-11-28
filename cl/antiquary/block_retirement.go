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

	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
)

type BlockRetireResult struct {
	From          uint64
	To            uint64
	Files         int
	RowsPruned    int
	CanonicalHead uint64
	UncappedTo    uint64
}

type blockRetireRange struct {
	from          uint64
	to            uint64
	canonicalHead uint64
	uncappedTo    uint64
}

func caplinBlockRetireRange(tx kv.Tx, snapshots *freezeblocks.CaplinSnapshots) (blockRetireRange, error) {
	from := snapshots.BlocksAvailable() + 1
	highestFinalized, err := beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return blockRetireRange{}, err
	}
	canonicalHead, _, err := beacon_indicies.ReadCanonicalHead(tx)
	if err != nil {
		return blockRetireRange{}, err
	}
	to := highestFinalized
	from = (from / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	to -= min(to, safetyMargin)
	uncappedTo := to
	if canonicalHead+1 < to {
		to = canonicalHead + 1
	}
	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	snapshots.WalkDirtySegments(snaptype.CaplinEnums.BeaconBlocks, func(segment *snapshotsync.DirtySegment) bool {
		if segmentFrom, _ := segment.GetRange(); segmentFrom >= from && segment.IsIndexed() {
			if segmentFrom < to {
				to = segmentFrom
			}
			uncappedTo = min(uncappedTo, segmentFrom)
			return false
		}
		return true
	})
	uncappedTo = (uncappedTo / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	return blockRetireRange{from: from, to: to, canonicalHead: canonicalHead, uncappedTo: uncappedTo}, nil
}

func CaplinBlockRetireRange(tx kv.Tx, snapshots *freezeblocks.CaplinSnapshots) (uint64, uint64, error) {
	rangeInfo, err := caplinBlockRetireRange(tx, snapshots)
	if err != nil {
		return 0, 0, err
	}
	if rangeInfo.from >= rangeInfo.to || rangeInfo.to-rangeInfo.from < snaptype.CaplinMergeLimit {
		return 0, 0, nil
	}
	return rangeInfo.from, rangeInfo.to, nil
}

func RetireBeaconBlocks(ctx context.Context, db kv.RwDB, snapshots *freezeblocks.CaplinSnapshots, dirs datadir.Dirs, logger log.Logger, seed func([]string) error, remove func([]string) error) (BlockRetireResult, error) {
	var rangeInfo blockRetireRange
	if err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		rangeInfo, err = caplinBlockRetireRange(tx, snapshots)
		return err
	}); err != nil {
		return BlockRetireResult{}, err
	}
	result := BlockRetireResult{
		From:          rangeInfo.from,
		To:            rangeInfo.to,
		CanonicalHead: rangeInfo.canonicalHead,
		UncappedTo:    rangeInfo.uncappedTo,
	}
	if rangeInfo.from >= rangeInfo.to || rangeInfo.to-rangeInfo.from < snaptype.CaplinMergeLimit {
		view := snapshots.View()
		hasBlocks := len(view.BeaconBlocks()) > 0
		coveredTo := snapshots.BlocksAvailable()
		view.Close()
		if !hasBlocks {
			return result, nil
		}
		pending, err := hasBeaconBlocksBelow(ctx, db, coveredTo+1)
		if err != nil {
			return BlockRetireResult{}, err
		}
		if !pending {
			return result, nil
		}
		pruned, err := pruneBeaconBlocksAndWriteProgress(ctx, db, coveredTo+1, coveredTo, snaptype.CaplinMergeLimit)
		if err != nil {
			return BlockRetireResult{}, err
		}
		result.RowsPruned = pruned
		return result, nil
	}

	logger.Info("[Antiquary] Antiquating", "from", rangeInfo.from, "to", rangeInfo.to)
	if err := freezeblocks.DumpBeaconBlocks(ctx, db, rangeInfo.from, rangeInfo.to, snapshots.Salt, dirs, 1, log.LvlDebug, logger); err != nil {
		return BlockRetireResult{}, err
	}
	if err := snapshots.OpenFolder(); err != nil {
		return BlockRetireResult{}, err
	}
	pruned, err := pruneBeaconBlocksAndWriteProgress(ctx, db, rangeInfo.to, rangeInfo.to-1, snaptype.CaplinMergeLimit)
	if err != nil {
		return BlockRetireResult{}, err
	}
	if err := snapshots.OpenFolder(); err != nil {
		return BlockRetireResult{}, err
	}
	if err := snapshots.RemoveOverlaps(remove); err != nil {
		logger.Warn("[Antiquary] Failed to remove block overlaps", "err", err)
	}
	paths := snapshots.SegFileNames(rangeInfo.from, rangeInfo.to)
	if seed != nil {
		if err := seed(paths); err != nil {
			logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}
	result.From = rangeInfo.from
	result.To = rangeInfo.to
	result.Files = len(paths) * 2
	result.RowsPruned = pruned
	return result, nil
}

func hasBeaconBlocksBelow(ctx context.Context, db kv.RwDB, to uint64) (bool, error) {
	var pending bool
	err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		pending, err = CaplinBeaconBlocksBelow(tx, to)
		return err
	})
	return pending, err
}

func CaplinBeaconBlocksBelow(tx kv.Tx, to uint64) (bool, error) {
	cursor, err := tx.Cursor(kv.BeaconBlocks)
	if err != nil {
		return false, err
	}
	defer cursor.Close()
	for key, _, err := cursor.First(); ; key, _, err = cursor.Next() {
		if err != nil {
			return false, err
		}
		if key == nil {
			return false, nil
		}
		if len(key) != 40 {
			continue
		}
		slot, err := dbutils.DecodeBlockNumber(key[:8])
		if err != nil {
			return false, err
		}
		return slot < to, nil
	}
}

func CaplinBeaconBlocksFirstSlot(tx kv.Tx) (uint64, bool, error) {
	key, err := kv.FirstKey(tx, kv.BeaconBlocks)
	if err != nil {
		return 0, false, err
	}
	if len(key) < dbutils.NumberLength {
		return 0, false, nil
	}
	slot, err := dbutils.DecodeBlockNumber(key[:dbutils.NumberLength])
	return slot, true, err
}

func MergeBeaconBlocks(ctx context.Context, snapshots *freezeblocks.CaplinSnapshots, tmpDir string, logger log.Logger, seed func([]string) error, remove func([]string) error, warnOverlap func(error)) (BlockRetireResult, error) {
	merger := snapshotsync.NewMerger(tmpDir, 1, log.LvlDebug, nil, &chain.Config{}, logger)
	ranges := merger.FindMergeRanges(snapshots.Ranges(false), snaptype.CaplinEnums.BeaconBlocks)
	if len(ranges) == 0 {
		return BlockRetireResult{}, nil
	}
	var onDelete func(context.Context, []string) error
	if remove != nil {
		onDelete = func(_ context.Context, names []string) error { return remove(names) }
	}
	if err := merger.Merge(ctx, &snapshots.BaseRoSnapshots, []snaptype.Type{snaptype.BeaconBlocks}, ranges[:1], snapshots.Dir(), true, seed, onDelete); err != nil {
		return BlockRetireResult{}, err
	}
	if err := snapshots.RemoveOverlaps(remove); err != nil {
		if warnOverlap == nil {
			return BlockRetireResult{}, err
		}
		warnOverlap(err)
	}
	r := ranges[0]
	return BlockRetireResult{From: r.From(), To: r.To(), Files: 2}, nil
}
