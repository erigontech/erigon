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

package heimdall

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

// Bor Events
// value: event_rlp
// bor_transaction_hash  -> bor_event_segment_offset

// Bor Spans
// value: span_json
// span_id -> offset

type RoSnapshots struct {
	snapshotsync.RoSnapshots
}

// NewBorRoSnapshots - opens all bor snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to] semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, SnapshotTypes(), segmentsMin, logger)}
}

func (s *RoSnapshots) Ranges() []snapshotsync.Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

func (s *RoSnapshots) ReopenFolder() error {
	files, _, err := snapshotsync.TypedSegments(s.Dir(), s.SegmentsMin(), SnapshotTypes(), false)
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

type blockReader interface {
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
	EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error)
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
			eventId = heimdall.EventId(event)
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

func ValidateBorEvents(ctx context.Context, config *borcfg.BorConfig, db kv.RoDB, blockReader services.FullBlockReader, eventSegment *VisibleSegment, prevEventId uint64, maxBlockNum uint64, failFast bool, logEvery *time.Ticker) (uint64, error) {
	g := eventSegment.src.Decompressor.MakeGetter()

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
	from, to, err := CalculateEventWindow(ctx, config, header, tx, headerReader)

	if err != nil {
		return false
	}

	var afterCheck = func(limitTime time.Time, eventTime time.Time, initialTime *time.Time) bool {
		if initialTime == nil {
			return eventTime.After(from)
		}

		return initialTime.After(from)
	}

	return !afterCheck(from, eventTime, firstBlockEventTime) || !eventTime.After(to)
}

type View struct {
	base *snapshotsync.View
}

func (s *RoSnapshots) View() *View {
	v := &View{base: s.RoSnapshots.View().WithBaseSegType(Spans)}
	return v
}

func (v *View) Close() {
	v.base.Close()
}

func (v *View) Events() []*snapshotsync.VisibleSegment { return v.base.Segments(Events) }
func (v *View) Spans() []*snapshotsync.VisibleSegment  { return v.base.Segments(Spans) }
func (v *View) Checkpoints() []*snapshotsync.VisibleSegment {
	return v.base.Segments(Checkpoints)
}
func (v *View) Milestones() []*snapshotsync.VisibleSegment {
	return v.base.Segments(Milestones)
}

func (v *View) EventsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(Events, blockNum)
}

func (v *View) SpansSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(Spans, blockNum)
}
