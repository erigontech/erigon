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
	"os"
	"path/filepath"
	"reflect"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/eth/ethconfig"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/turbo/services"
)

var BorProduceFiles = dbg.EnvBool("BOR_PRODUCE_FILES", false)

func (br *BlockRetire) dbHasEnoughDataForBorRetire(ctx context.Context) (bool, error) {
	return true, nil
}

func (br *BlockRetire) retireBorBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	if !BorProduceFiles {
		return false, nil
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	snapshots := br.borSnapshots()

	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers

	blocksRetired := false

	minBlockNum = max(blockReader.FrozenBorBlocks(), minBlockNum)
	for _, snaptype := range blockReader.BorSnapshots().Types() {
		if maxBlockNum <= minBlockNum {
			continue
		}

		blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, snaptype.Enum(), br.chainConfig)
		if ok {
			blocksRetired = true

			if has, err := br.dbHasEnoughDataForBorRetire(ctx); err != nil {
				return false, err
			} else if !has {
				return false, nil
			}

			logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "type", snaptype, "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))

			for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, snaptype.Enum(), chainConfig) {
				end := chooseSegmentEnd(i, blockTo, snaptype.Enum(), chainConfig)
				if _, err := snaptype.ExtractRange(ctx, snaptype.FileInfo(snapshots.Dir(), i, end), nil, db, chainConfig, tmpDir, workers, lvl, logger); err != nil {
					return ok, fmt.Errorf("ExtractRange: %d-%d: %w", i, end, err)
				}
			}
		}
	}

	if blocksRetired {
		if err := snapshots.ReopenFolder(); err != nil {
			return blocksRetired, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("bor:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := NewMerger(tmpDir, workers, lvl, db, chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	if len(rangesToMerge) > 0 {
		logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "rangesToMerge", Ranges(rangesToMerge))
	}
	if len(rangesToMerge) == 0 {
		return blocksRetired, nil
	}
	blocksRetired = true // have something to merge
	onMerge := func(r Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []services.DownloadRequest{
				services.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}

	err := merger.Merge(ctx, &snapshots.RoSnapshots, borsnaptype.BorSnapshotTypes(), rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)
	if err != nil {
		return blocksRetired, err
	}

	{
		files, _, err := typedSegments(br.borSnapshots().dir, br.borSnapshots().segmentsMin.Load(), borsnaptype.BorSnapshotTypes(), false)
		if err != nil {
			return blocksRetired, err
		}

		// this is one off code to fix an issue in 2.49.x->2.52.x which missed
		// removal of intermediate segments after a merge operation
		removeBorOverlaps(br.borSnapshots().dir, files, br.borSnapshots().BlocksAvailable())
	}

	return blocksRetired, nil
}

// Bor Events
// value: event_rlp
// bor_transaction_hash  -> bor_event_segment_offset

// Bor Spans
// value: span_json
// span_id -> offset

type BorRoSnapshots struct {
	RoSnapshots
}

// NewBorRoSnapshots - opens all bor snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to] semantic
func NewBorRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *BorRoSnapshots {
	return &BorRoSnapshots{*newRoSnapshots(cfg, snapDir, borsnaptype.BorSnapshotTypes(), segmentsMin, logger)}
}

func (s *BorRoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

// this is one off code to fix an issue in 2.49.x->2.52.x which missed
// removal of intermediate segments after a merge operation
func removeBorOverlaps(dir string, active []snaptype.FileInfo, max uint64) {
	list, err := snaptype.Segments(dir)

	if err != nil {
		return
	}

	var toDel []string
	l := make([]snaptype.FileInfo, 0, len(list))

	for _, f := range list {
		if !(f.Type.Enum() == borsnaptype.Enums.BorSpans || f.Type.Enum() == borsnaptype.Enums.BorEvents) {
			continue
		}
		l = append(l, f)
	}

	// added overhead to make sure we don't delete in the
	// current 500k block segment
	if max > 500_001 {
		max -= 500_001
	}

	for _, f := range l {
		if max < f.From {
			continue
		}

		for _, a := range active {
			if a.Type.Enum() != borsnaptype.Enums.BorSpans {
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
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
	}
}

func (s *BorRoSnapshots) ReopenFolder() error {
	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), borsnaptype.BorSnapshotTypes(), false)
	if err != nil {
		return err
	}

	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	if err := s.ReopenList(list, false); err != nil {
		return err
	}
	return nil
}

type BorView struct {
	base *View
}

func (s *BorRoSnapshots) View() *BorView {
	v := &BorView{base: s.RoSnapshots.View()}
	v.base.baseSegType = borsnaptype.BorSpans
	return v
}

func (v *BorView) Close() {
	v.base.Close()
}

func (v *BorView) Events() []*Segment      { return v.base.segments(borsnaptype.BorEvents) }
func (v *BorView) Spans() []*Segment       { return v.base.segments(borsnaptype.BorSpans) }
func (v *BorView) Checkpoints() []*Segment { return v.base.segments(borsnaptype.BorCheckpoints) }
func (v *BorView) Milestones() []*Segment  { return v.base.segments(borsnaptype.BorMilestones) }

func (v *BorView) EventsSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(borsnaptype.BorEvents, blockNum)
}

func (v *BorView) SpansSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(borsnaptype.BorSpans, blockNum)
}
