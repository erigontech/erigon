package freezeblocks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	borsnaptype "github.com/ledgerwatch/erigon/polygon/bor/snaptype"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func (br *BlockRetire) dbHasEnoughDataForBorRetire(ctx context.Context) (bool, error) {
	return true, nil
}

func (br *BlockRetire) retireBorBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	snapshots := br.borSnapshots()

	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers

	blocksRetired := false

	for _, snaptype := range blockReader.BorSnapshots().Types() {
		minSnapNum := minBlockNum

		if available := blockReader.BorSnapshots().SegmentsMax(); available < minBlockNum {
			minSnapNum = available
		}

		if maxBlockNum <= minSnapNum {
			continue
		}

		blockFrom, blockTo, ok := canRetire(minSnapNum, maxBlockNum+1, snaptype.Enum(), br.chainConfig)

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

	err := merger.Merge(ctx, &snapshots.RoSnapshots, borsnaptype.BorSnapshotTypes, rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)

	if err != nil {
		return blocksRetired, err
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
	return &BorRoSnapshots{*newRoSnapshots(cfg, snapDir, borsnaptype.BorSnapshotTypes, segmentsMin, logger)}
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
	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), borsnaptype.BorSnapshotTypes, false)
	if err != nil {
		return err
	}

	// this is one off code to fix an issue in 2.49.x->2.52.x which missed
	// removal of intermediate segments after a merge operation
	removeBorOverlaps(s.dir, files, s.BlocksAvailable())

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

func (v *BorView) Events() []*Segment      { return v.base.Segments(borsnaptype.BorEvents) }
func (v *BorView) Spans() []*Segment       { return v.base.Segments(borsnaptype.BorSpans) }
func (v *BorView) Checkpoints() []*Segment { return v.base.Segments(borsnaptype.BorCheckpoints) }
func (v *BorView) Milestones() []*Segment  { return v.base.Segments(borsnaptype.BorMilestones) }

func (v *BorView) EventsSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(borsnaptype.BorEvents, blockNum)
}

func (v *BorView) SpansSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(borsnaptype.BorSpans, blockNum)
}
