package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon/v3/polygon/heimdall"
	"github.com/erigontech/erigon/v3/rlp"
	"github.com/erigontech/erigon/v3/turbo/snapshotsync"
)

type snapshotStore struct {
	Store
	snapshots              *heimdall.RoSnapshots
	sprintLengthCalculator sprintLengthCalculator
}

type sprintLengthCalculator interface {
	CalculateSprintLength(number uint64) uint64
}

func NewSnapshotStore(base Store, snapshots *heimdall.RoSnapshots, sprintLengthCalculator sprintLengthCalculator) *snapshotStore {
	return &snapshotStore{base, snapshots, sprintLengthCalculator}
}

func (s *snapshotStore) Prepare(ctx context.Context) error {
	if err := s.Store.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *snapshotStore) WithTx(tx kv.Tx) Store {
	return &snapshotStore{txStore{tx: tx}, s.snapshots, s.sprintLengthCalculator}
}

func (s *snapshotStore) RangeExtractor() snaptype.RangeExtractor {
	type extractableStore interface {
		RangeExtractor() snaptype.RangeExtractor
	}

	if extractableStore, ok := s.Store.(extractableStore); ok {
		return extractableStore.RangeExtractor()
	}
	return heimdall.Events.RangeExtractor()
}

func (s *snapshotStore) LastFrozenEventBlockNum() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
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

func (s *snapshotStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
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

func (s *snapshotStore) LastEventId(ctx context.Context) (uint64, error) {
	lastEventId, err := s.Store.LastEventId(ctx)

	if err != nil {
		return 0, err
	}

	snapshotLastEventId := s.LastFrozenEventId()
	if snapshotLastEventId > lastEventId {
		return snapshotLastEventId, nil
	}

	return lastEventId, nil
}

func (s *snapshotStore) LastFrozenEventId() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
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

func (s *snapshotStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	lastEventId, err := s.Store.LastProcessedEventId(ctx)

	if err != nil {
		return 0, err
	}

	snapshotLastEventId := s.LastFrozenEventId()
	if snapshotLastEventId > lastEventId {
		return snapshotLastEventId, nil
	}

	return lastEventId, nil
}

func (s *snapshotStore) EventTxnToBlockNum(ctx context.Context, txnHash libcommon.Hash) (uint64, bool, error) {
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

func (s *snapshotStore) BlockEventIdsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	maxBlockNumInFiles := s.snapshots.VisibleBlocksAvailable(heimdall.Events.Enum())
	if maxBlockNumInFiles == 0 || blockNum > maxBlockNumInFiles {
		return s.Store.(interface {
			blockEventIdsRange(context.Context, uint64, uint64) (uint64, uint64, error)
		}).blockEventIdsRange(ctx, blockNum, s.LastFrozenEventId())
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

		gg := sn.Src().MakeGetter()
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
				return start, end, nil
			}
		}
	}

	return 0, 0, fmt.Errorf("%w: %d", ErrEventIdRangeNotFound, blockNum)
}

func (s *snapshotStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	if start > s.LastFrozenEventId() {
		return s.Store.Events(ctx, start, end)
	}

	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	var buf []byte
	var result [][]byte

	for i := len(segments) - 1; i >= 0; i-- {
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

			result = append(result, bytes.Clone(libcommon.Copy(buf[length.Hash+length.BlockNum+8:])))
		}
	}

	return result, nil
}

func (s *snapshotStore) borBlockByEventHash(txnHash libcommon.Hash, segments []*snapshotsync.VisibleSegment, buf []byte) (blockNum uint64, ok bool, err error) {
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

func (s *snapshotStore) BorStartEventId(ctx context.Context, hash libcommon.Hash, blockHeight uint64) (uint64, error) {
	startEventId, _, err := s.BlockEventIdsRange(ctx, blockHeight)
	if err != nil {
		return 0, err
	}
	return startEventId, nil
}

func (s *snapshotStore) EventsByBlock(ctx context.Context, hash libcommon.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	startEventId, endEventId, err := s.BlockEventIdsRange(ctx, blockHeight)
	if err != nil {
		return nil, err
	}
	bytevals, err := s.Events(ctx, startEventId, endEventId+1)
	if err != nil {
		return nil, err
	}
	result := make([]rlp.RawValue, len(bytevals))
	for i, byteval := range bytevals {
		result[i] = rlp.RawValue(byteval)
	}
	return result, nil
}

// EventsByIdFromSnapshot returns the list of records limited by time, or the number of records along with a bool value to signify if the records were limited by time
func (s *snapshotStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	tx := s.snapshots.ViewType(heimdall.Events)
	defer tx.Close()
	segments := tx.Segments

	var buf []byte
	var result []*heimdall.EventRecordWithTime
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

			raw := rlp.RawValue(libcommon.Copy(buf[length.Hash+length.BlockNum+8:]))
			var event heimdall.EventRecordWithTime
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
