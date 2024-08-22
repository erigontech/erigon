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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
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

	minBlockNum = max(blockReader.FrozenBorBlocks(), minBlockNum)
	for _, snap := range blockReader.BorSnapshots().Types() {
		if maxBlockNum <= minBlockNum {
			continue
		}

		blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, snap.Enum(), br.chainConfig)
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

			if snap.Enum() == borsnaptype.BorEvents.Enum() {
				firstKeyGetter = func(ctx context.Context) uint64 {
					return blockReader.LastFrozenEventId() + 1
				}
			}

			for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, snap.Enum(), chainConfig) {
				end := chooseSegmentEnd(i, blockTo, snap.Enum(), chainConfig)
				if _, err := snap.ExtractRange(ctx, snap.FileInfo(snapshots.Dir(), i, end), firstKeyGetter, db, chainConfig, tmpDir, workers, lvl, logger); err != nil {
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

func checkBlockEvents(ctx context.Context, config *borcfg.BorConfig, blockReader services.FullBlockReader,
	block uint64, prevBlock uint64, eventId uint64, prevBlockStartId uint64, prevEventTime *time.Time, tx kv.Tx, failFast bool) (*time.Time, error) {
	header, err := blockReader.HeaderByNumber(ctx, tx, prevBlock)

	if err != nil {
		if failFast {
			return nil, fmt.Errorf("can't get header for block %d: %w", block, err)
		}

		log.Error("[integrity] NoGapsInBorEvents: can't get header for block", "block", block, "err", err)
	}

	events, err := blockReader.EventsByBlock(ctx, tx, header.Hash(), header.Number.Uint64())

	if err != nil {
		if failFast {
			return nil, fmt.Errorf("can't get events for block %d: %w", block, err)
		}

		log.Error("[integrity] NoGapsInBorEvents: can't get events for block", "block", block, "err", err)
	}

	if prevBlockStartId != 0 {
		if len(events) != int(eventId-prevBlockStartId) {
			if failFast {
				return nil, fmt.Errorf("block event mismatch at %d: expected: %d, got: %d", block, eventId-prevBlockStartId, len(events))
			}

			log.Error("[integrity] NoGapsInBorEvents: block event count mismatch", "block", block, "eventId", eventId, "expected", eventId-prevBlockStartId, "got", len(events))
		}
	}

	var lastBlockEventTime time.Time
	var firstBlockEventTime *time.Time

	for i, event := range events {

		var eventId uint64

		if prevBlockStartId != 0 {
			eventId = heimdall.EventId(event)

			if eventId != prevBlockStartId+uint64(i) {
				if failFast {
					return nil, fmt.Errorf("invalid event id %d for event %d in block %d: expected: %d", eventId, i, block, prevBlockStartId+uint64(i))
				}

				log.Error("[integrity] NoGapsInBorEvents: invalid event id", "block", block, "event", i, "expected", prevBlockStartId+uint64(i), "got", eventId)
			}
		} else {
			eventId = prevBlockStartId + uint64(i)
		}

		eventTime := heimdall.EventTime(event)

		//if i != 0 {
		//	if eventTime.Before(lastBlockEventTime) {
		//		eventTime = lastBlockEventTime
		//	}
		//}

		if i == 0 {
			lastBlockEventTime = eventTime
		}

		const warnPrevTimes = false

		if prevEventTime != nil {
			if eventTime.Before(*prevEventTime) && warnPrevTimes {
				log.Warn("[integrity] NoGapsInBorEvents: event time before prev", "block", block, "event", eventId, "time", eventTime, "prev", *prevEventTime, "diff", -prevEventTime.Sub(eventTime))
			}
		}

		prevEventTime = &eventTime

		if !checkBlockWindow(ctx, eventTime, firstBlockEventTime, config, header, tx, blockReader) {
			from, to, _ := bor.CalculateEventWindow(ctx, config, header, tx, blockReader)

			var diff time.Duration

			if eventTime.Before(from) {
				diff = -from.Sub(eventTime)
			} else if eventTime.After(to) {
				diff = to.Sub(eventTime)
			}

			if failFast {
				return nil, fmt.Errorf("invalid time %s for event %d in block %d: expected %s-%s", eventTime, eventId, block, from, to)
			}

			log.Error(fmt.Sprintf("[integrity] NoGapsInBorEvents: invalid event time at %d of %d", i, len(events)), "block", block, "event", eventId, "time", eventTime, "diff", diff, "expected", fmt.Sprintf("%s-%s", from, to), "block-start", prevBlockStartId, "first-time", lastBlockEventTime, "timestamps", fmt.Sprintf("%d-%d", from.Unix(), to.Unix()))
		}

		if firstBlockEventTime == nil {
			firstBlockEventTime = &eventTime
		}
	}

	return prevEventTime, nil
}

func ValidateBorEvents(ctx context.Context, config *borcfg.BorConfig, db kv.RoDB, blockReader services.FullBlockReader, eventSegment *Segment, prevEventId uint64, maxBlockNum uint64, failFast bool, logEvery *time.Ticker) (uint64, error) {
	g := eventSegment.Decompressor.MakeGetter()

	word := make([]byte, 0, 4096)

	var prevBlock, prevBlockStartId uint64
	var prevEventTime *time.Time

	for g.HasNext() {
		word, _ = g.Next(word[:0])

		block := binary.BigEndian.Uint64(word[length.Hash : length.Hash+length.BlockNum])
		eventId := binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
		event := word[length.Hash+length.BlockNum+8:]

		recordId := heimdall.EventId(event)

		if recordId != eventId {
			if failFast {
				return prevEventId, fmt.Errorf("invalid event id %d in block %d: expected: %d", recordId, block, eventId)
			}

			log.Error("[integrity] NoGapsInBorEvents: invalid event id", "block", block, "event", recordId, "expected", eventId)
		}

		if prevEventId > 0 {
			switch {
			case eventId < prevEventId:
				if failFast {
					return prevEventId, fmt.Errorf("invaid bor event %d (prev=%d) at block=%d", eventId, prevEventId, block)
				}

				log.Error("[integrity] NoGapsInBorEvents: invalid bor event", "event", eventId, "prev", prevEventId, "block", block)

			case eventId != prevEventId+1:
				if failFast {
					return prevEventId, fmt.Errorf("missing bor event %d (prev=%d) at block=%d", eventId, prevEventId, block)
				}

				log.Error("[integrity] NoGapsInBorEvents: missing bor event", "event", eventId, "prev", prevEventId, "block", block)
			}
		}

		//if prevEventId == 0 {
		//log.Info("[integrity] checking bor events", "event", eventId, "block", block)
		//}

		if prevBlock != 0 && prevBlock != block {
			var err error

			if db != nil {
				err = db.View(ctx, func(tx kv.Tx) error {
					prevEventTime, err = checkBlockEvents(ctx, config, blockReader, block, prevBlock, eventId, prevBlockStartId, prevEventTime, tx, failFast)
					return err
				})
			} else {
				prevEventTime, err = checkBlockEvents(ctx, config, blockReader, block, prevBlock, eventId, prevBlockStartId, prevEventTime, nil, failFast)
			}

			if err != nil {
				return prevEventId, err
			}

			prevBlockStartId = eventId
		}

		prevEventId = eventId
		prevBlock = block

		var logChan <-chan time.Time

		if logEvery != nil {
			logChan = logEvery.C
		}

		select {
		case <-ctx.Done():
			return prevEventId, ctx.Err()
		case <-logChan:
			log.Info("[integrity] NoGapsInBorEvents", "blockNum", fmt.Sprintf("%dK/%dK", binary.BigEndian.Uint64(word[length.Hash:length.Hash+length.BlockNum])/1000, maxBlockNum/1000))
		default:
		}
	}

	return prevEventId, nil
}

func checkBlockWindow(ctx context.Context, eventTime time.Time, firstBlockEventTime *time.Time, config *borcfg.BorConfig, header *types.Header, tx kv.Getter, headerReader services.HeaderReader) bool {
	from, to, err := bor.CalculateEventWindow(ctx, config, header, tx, headerReader)

	if err != nil {
		return false
	}

	var afterCheck = func(limitTime time.Time, eventTime time.Time, initialTime *time.Time) bool {
		if initialTime == nil {
			return eventTime.After(from)
		}

		return initialTime.After(from)
	}

	return !(afterCheck(from, eventTime, firstBlockEventTime) || eventTime.After(to))
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
