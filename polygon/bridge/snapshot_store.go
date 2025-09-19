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

package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type SnapshotStore struct {
	Store
	snapshots              *heimdall.RoSnapshots
	sprintLengthCalculator sprintLengthCalculator
}

type sprintLengthCalculator interface {
	CalculateSprintLength(number uint64) uint64
}

func NewSnapshotStore(base Store, snapshots *heimdall.RoSnapshots, sprintLengthCalculator sprintLengthCalculator) *SnapshotStore {
	return &SnapshotStore{base, snapshots, sprintLengthCalculator}
}

func (s *SnapshotStore) Prepare(ctx context.Context) error {
	if err := s.Store.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *SnapshotStore) WithTx(tx kv.Tx) Store {
	return &SnapshotStore{txStore{tx: tx}, s.snapshots, s.sprintLengthCalculator}
}

func (s *SnapshotStore) RangeExtractor() snaptype.RangeExtractor {
	type extractableStore interface {
		RangeExtractor() snaptype.RangeExtractor
	}

	if extractableStore, ok := s.Store.(extractableStore); ok {
		return extractableStore.RangeExtractor()
	}
	return heimdall.Events.RangeExtractor()
}

func (s *SnapshotStore) LastFrozenEventBlockNum() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built non-empty index
	var lastSegment *snapshotsync.VisibleSegment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Src().Index() != nil {
			gg := segments[i].Src().MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}
	var lastBlockNum uint64
	var buf []byte
	gg := lastSegment.Src().MakeGetter()
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastBlockNum = binary.BigEndian.Uint64(buf[length.Hash : length.Hash+length.BlockNum])
	}

	return lastBlockNum
}

func (s *SnapshotStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
	if blockInfo, ok, err := s.Store.LastProcessedBlockInfo(ctx); ok {
		return blockInfo, ok, err
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return ProcessedBlockInfo{}, false, nil
	}

	if s.sprintLengthCalculator == nil {
		return ProcessedBlockInfo{}, false, errors.New("can't calculate last block: missing sprint length calculator")
	}

	lastBlockNum := segments[len(segments)-1].To() - 1
	sprintLen := s.sprintLengthCalculator.CalculateSprintLength(lastBlockNum)
	lastBlockNum = (lastBlockNum / sprintLen) * sprintLen

	return ProcessedBlockInfo{
		BlockNum: lastBlockNum,
	}, true, nil
}

func (s *SnapshotStore) LastEventId(ctx context.Context) (uint64, error) {
	lastEventId, err := s.Store.LastEventId(ctx)

	if err != nil {
		return 0, err
	}

	snapshotLastEventId := s.LastFrozenEventId()

	return max(snapshotLastEventId, lastEventId), nil
}

func (s *SnapshotStore) LastFrozenEventId() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built non-empty index
	var lastSegment *snapshotsync.VisibleSegment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Src().Index() != nil {
			gg := segments[i].Src().MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}
	var lastEventId uint64
	gg := lastSegment.Src().MakeGetter()
	var buf []byte
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastEventId = binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
	}
	return lastEventId
}

func (s *SnapshotStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	lastEventId, err := s.Store.LastProcessedEventId(ctx)

	if err != nil {
		return 0, err
	}

	snapshotLastEventId := s.LastFrozenEventId()

	return max(snapshotLastEventId, lastEventId), nil
}

func (s *SnapshotStore) EventTxnToBlockNum(ctx context.Context, txnHash common.Hash) (uint64, bool, error) {
	blockNum, ok, err := s.Store.EventTxnToBlockNum(ctx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return blockNum, ok, nil
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	blockNum, ok, err = s.borBlockByEventHash(txnHash, segments, nil)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return blockNum, true, nil
}

func (s *SnapshotStore) BlockEventIdsRange(ctx context.Context, blockHash common.Hash, blockNum uint64) (uint64, uint64, bool, error) {
	maxBlockNumInFiles := s.snapshots.VisibleBlocksAvailable(heimdall.Events.Enum())
	if maxBlockNumInFiles == 0 || blockNum > maxBlockNumInFiles {
		return s.Store.(interface {
			blockEventIdsRange(context.Context, common.Hash, uint64, uint64) (uint64, uint64, bool, error)
		}).blockEventIdsRange(ctx, blockHash, blockNum, s.LastFrozenEventId())
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.From() > blockNum {
			continue
		}
		if sn.To() <= blockNum {
			break
		}

		idxBorTxnHash := sn.Src().Index()
		if idxBorTxnHash == nil || idxBorTxnHash.KeyCount() == 0 {
			continue
		}

		reader := recsplit.NewIndexReader(idxBorTxnHash)
		txnHash := bortypes.ComputeBorTxHash(blockNum, blockHash)
		blockEventId, exists := reader.Lookup(txnHash[:])
		var offset uint64

		gg := sn.Src().MakeGetter()
		if exists {
			offset = idxBorTxnHash.OrdinalLookup(blockEventId)
			gg.Reset(offset)
			if !gg.MatchPrefix(txnHash[:]) {
				continue
			}
		}

		var buf []byte
		for gg.HasNext() {
			buf, _ = gg.Next(buf[:0])
			if blockNum == binary.BigEndian.Uint64(buf[length.Hash:length.Hash+length.BlockNum]) {
				start := binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
				end := start
				for gg.HasNext() {
					buf, _ = gg.Next(buf[:0])
					if blockNum != binary.BigEndian.Uint64(buf[length.Hash:length.Hash+length.BlockNum]) {
						break
					}
					end = binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
				}
				return start, end, true, nil
			}
		}
	}

	return 0, 0, false, nil
}

func (s *SnapshotStore) events(ctx context.Context, start, end, blockNumber uint64) ([][]byte, error) {
	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	var buf []byte
	var result [][]byte

	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].From() > blockNumber {
			continue
		}
		if segments[i].To() <= blockNumber {
			break
		}

		gg0 := segments[i].Src().MakeGetter()

		if !gg0.HasNext() {
			continue
		}

		buf0, _ := gg0.Next(nil)
		if end <= binary.BigEndian.Uint64(buf0[length.Hash+length.BlockNum:length.Hash+length.BlockNum+8]) {
			continue
		}

		gg0.Reset(0)
		for gg0.HasNext() {
			buf, _ = gg0.Next(buf[:0])

			eventId := binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])

			if eventId < start {
				continue
			}

			if eventId >= end {
				return result, nil
			}

			result = append(result, bytes.Clone(buf[length.Hash+length.BlockNum+8:]))
		}
	}

	return result, nil
}

func (s *SnapshotStore) borBlockByEventHash(txnHash common.Hash, segments []*snapshotsync.VisibleSegment, buf []byte) (blockNum uint64, ok bool, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idxBorTxnHash := sn.Src().Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, exists := reader.Lookup(txnHash[:])
		if !exists {
			continue
		}
		offset := idxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		if !gg.MatchPrefix(txnHash[:]) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		blockNum = binary.BigEndian.Uint64(buf[length.Hash:])
		ok = true
		return
	}
	return
}

func (s *SnapshotStore) BorStartEventId(ctx context.Context, hash common.Hash, blockHeight uint64) (uint64, error) {
	startEventId, _, ok, err := s.BlockEventIdsRange(ctx, hash, blockHeight)
	if !ok || err != nil {
		return 0, err
	}
	return startEventId, nil
}

func (s *SnapshotStore) EventsByBlock(ctx context.Context, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	startEventId, endEventId, ok, err := s.BlockEventIdsRange(ctx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []rlp.RawValue{}, nil
	}

	lastFrozenEventId := s.LastFrozenEventId()
	if startEventId > lastFrozenEventId || lastFrozenEventId == 0 {
		return s.Store.EventsByBlock(ctx, hash, blockHeight)
	}

	bytevals, err := s.events(ctx, startEventId, endEventId+1, blockHeight)
	if err != nil {
		return nil, err
	}
	result := make([]rlp.RawValue, len(bytevals))
	for i, byteval := range bytevals {
		result[i] = byteval
	}
	return result, nil
}

// EventsByIdFromSnapshot returns the list of records limited by time, or the number of records along with a bool value to signify if the records were limited by time
func (s *SnapshotStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*EventRecordWithTime, bool, error) {
	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	var buf []byte
	var result []*EventRecordWithTime
	maxTime := false

	for _, sn := range segments {
		idxBorTxnHash := sn.Src().Index()

		if idxBorTxnHash == nil || idxBorTxnHash.KeyCount() == 0 {
			continue
		}

		offset := idxBorTxnHash.OrdinalLookup(0)
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		for gg.HasNext() {
			buf, _ = gg.Next(buf[:0])

			raw := rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:]))
			var event EventRecordWithTime
			if err := event.UnmarshallBytes(raw); err != nil {
				return nil, false, err
			}

			if event.ID < from {
				continue
			}
			if event.Time.After(to) {
				maxTime = true
				return result, maxTime, nil
			}

			result = append(result, &event)

			if len(result) == limit {
				return result, maxTime, nil
			}
		}
	}

	return result, maxTime, nil
}

func ValidateEvents(ctx context.Context, config *borcfg.BorConfig, db kv.RoDB, blockReader blockReader, snapshots *heimdall.RoSnapshots, eventSegment *snapshotsync.VisibleSegment, prevEventId uint64, maxBlockNum uint64, failFast bool, logEvery *time.Ticker) (uint64, error) {
	g := eventSegment.Src().MakeGetter()

	word := make([]byte, 0, 4096)

	var prevBlock, prevBlockStartId uint64
	var prevEventTime *time.Time

	for g.HasNext() {
		word, _ = g.Next(word[:0])

		block := binary.BigEndian.Uint64(word[length.Hash : length.Hash+length.BlockNum])
		eventId := binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
		event := word[length.Hash+length.BlockNum+8:]

		recordId := EventId(event)
		log.Trace("validating event", "id", eventId)
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
					prevEventTime, err = checkBlockEvents(ctx, config, blockReader, snapshots, block, prevBlock, eventId, prevBlockStartId, prevEventTime, tx, failFast)
					return err
				})
			} else {
				prevEventTime, err = checkBlockEvents(ctx, config, blockReader, snapshots, block, prevBlock, eventId, prevBlockStartId, prevEventTime, nil, failFast)
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

type blockReader interface {
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
}

func checkBlockEvents(ctx context.Context, config *borcfg.BorConfig, blockReader blockReader, snapshots *heimdall.RoSnapshots,
	block uint64, prevBlock uint64, eventId uint64, prevBlockStartId uint64, prevEventTime *time.Time, tx kv.Tx, failFast bool) (*time.Time, error) {
	header, err := blockReader.HeaderByNumber(ctx, tx, prevBlock)

	if err != nil {
		if failFast {
			return nil, fmt.Errorf("can't get header for block %d: %w", block, err)
		}

		log.Error("[integrity] NoGapsInBorEvents: can't get header for block", "block", block, "err", err)
	}

	events, err := NewSnapshotStore(NewTxStore(tx), snapshots, nil).EventsByBlock(ctx, header.Hash(), header.Number.Uint64())

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
			eventId = EventId(event)

			if eventId != prevBlockStartId+uint64(i) {
				if failFast {
					return nil, fmt.Errorf("invalid event id %d for event %d in block %d: expected: %d", eventId, i, block, prevBlockStartId+uint64(i))
				}

				log.Error("[integrity] NoGapsInBorEvents: invalid event id", "block", block, "event", i, "expected", prevBlockStartId+uint64(i), "got", eventId)
			}
		} else {
			eventId = EventId(event)
		}

		eventTime := EventTime(event)

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
			from, to, _ := heimdall.CalculateEventWindow(ctx, config, header, tx, blockReader)

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

type headerReader interface {
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
}

func checkBlockWindow(ctx context.Context, eventTime time.Time, firstBlockEventTime *time.Time, config *borcfg.BorConfig, header *types.Header, tx kv.Getter, headerReader headerReader) bool {
	from, to, err := heimdall.CalculateEventWindow(ctx, config, header, tx, headerReader)

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
