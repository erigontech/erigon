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

package freezeblocks

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/erigontech/erigon-lib/common"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func (br *BlockRetire) dbHasEnoughDataForBorRetire(ctx context.Context) (bool, error) {
	return true, nil
}

func (br *BlockRetire) retireBorBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	snapshots := br.borSnapshots()
	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, int(br.workers.Load())

	blocksRetired := false

	for _, snap := range blockReader.BorSnapshots().Types() {
		minSnapBlockNum := max(snapshots.DirtyBlocksAvailable(snap.Enum()), minBlockNum)

		if maxBlockNum <= minSnapBlockNum {
			continue
		}

		blockFrom, blockTo, ok := CanRetire(maxBlockNum, minSnapBlockNum, snap.Enum(), br.chainConfig)
		if ok {
			blocksRetired = true

			if has, err := br.dbHasEnoughDataForBorRetire(ctx); err != nil {
				return false, err
			} else if !has {
				return false, nil
			}

			logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "type", snap,
				"range", fmt.Sprintf("%s-%s", common.PrettyCounter(blockFrom), common.PrettyCounter(blockTo)))

			var firstKeyGetter snaptype.FirstKeyGetter

			if snap.Enum() == heimdall.Events.Enum() {
				firstKeyGetter = func(ctx context.Context) uint64 {
					return br.bridgeStore.LastFrozenEventId() + 1
				}
			}

			rangeExtractor := snapshots.RangeExtractor(snap)
			indexBuilder := snapshots.IndexBuilder(snap)

			for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, snap.Enum(), chainConfig) {
				end := chooseSegmentEnd(i, blockTo, snap.Enum(), chainConfig)
				if _, err := snap.ExtractRange(ctx, snap.FileInfo(snapshots.Dir(), i, end), rangeExtractor, indexBuilder, firstKeyGetter, db, chainConfig, tmpDir, workers, lvl, logger, br.blockReader); err != nil {
					return ok, fmt.Errorf("ExtractRange: %d-%d: %w", i, end, err)
				}
			}
		}
	}

	if blocksRetired {
		if err := snapshots.OpenFolder(); err != nil {
			return blocksRetired, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("bor:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merged, err := br.MergeBorBlocks(ctx, lvl, seedNewSnapshots, onDelete)
	return blocksRetired || merged, err
}

func (br *BlockRetire) MergeBorBlocks(ctx context.Context, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error) (mergedBlocks bool, err error) {
	notifier, logger, _, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, int(br.workers.Load())
	snapshots := br.borSnapshots()
	chainConfig := fromdb.ChainConfig(br.db)
	merger := snapshotsync.NewMerger(tmpDir, workers, lvl, db, chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	if len(rangesToMerge) > 0 {
		logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "rangesToMerge", snapshotsync.Ranges(rangesToMerge))
	}
	if len(rangesToMerge) == 0 {
		return false, nil
	}
	onMerge := func(r snapshotsync.Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []snapshotsync.DownloadRequest{
				snapshotsync.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}
	if err := merger.Merge(ctx, &snapshots.RoSnapshots, heimdall.SnapshotTypes(), rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete); err != nil {
		return false, err
	}

	{
		files, _, err := snapshotsync.TypedSegments(br.borSnapshots().Dir(), heimdall.SnapshotTypes(), false)
		if err != nil {
			return true, err
		}

		// this is one off code to fix an issue in 2.49.x->2.52.x which missed
		// removal of intermediate segments after a merge operation
		removeBorOverlaps(br.borSnapshots().Dir(), files, br.borSnapshots().BlocksAvailable())
	}

	return true, nil
}

// this is one off code to fix an issue in 2.49.x->2.52.x which missed
// removal of intermediate segments after a merge operation
func removeBorOverlaps(dir string, active []snaptype.FileInfo, _max uint64) {
	list, err := snaptype.Segments(dir)

	if err != nil {
		return
	}

	var toDel []string
	l := make([]snaptype.FileInfo, 0, len(list))

	for _, f := range list {
		if !(f.Type.Enum() == heimdall.Enums.Spans || f.Type.Enum() == heimdall.Enums.Events) {
			continue
		}
		l = append(l, f)
	}

	// added overhead to make sure we don't delete in the
	// current 500k block segment
	if _max > 500_001 {
		_max -= 500_001
	}

	for _, f := range l {
		if _max < f.From {
			continue
		}

		for _, a := range active {
			if a.Type.Enum() != heimdall.Enums.Spans {
				continue
			}

			if f.From < a.From {
				continue
			}

			if f.From == a.From {
				if f.To < a.To {
					toDel = append(toDel, f.Path)
				}

				break
			}

			if f.From < a.To {
				toDel = append(toDel, f.Path)
				break
			}
		}
	}

	for _, f := range toDel {
		_ = dir2.RemoveFile(f)
		_ = dir2.RemoveFile(f + ".torrent")
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = dir2.RemoveFile(withoutExt + ".idx")
		_ = dir2.RemoveFile(withoutExt + ".idx.torrent")
	}
}
