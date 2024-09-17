package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/recsplit"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

type snapshotStore struct {
	Store
	snapshots *heimdall.RoSnapshots
}

func NewSnapshotStore(base Store, snapshots *heimdall.RoSnapshots) *snapshotStore {
	return &snapshotStore{base, snapshots}
}

func (s *snapshotStore) Prepare(ctx context.Context) error {
	if err := s.Store.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *snapshotStore) WithTx(tx kv.Tx) Store {
	return &snapshotStore{txStore{tx: tx}, s.snapshots}
}

func (s *snapshotStore) LastFrozenEventBlockNum() uint64 {
	if s.snapshots == nil {
		return 0
	}

	segments, release := s.snapshots.ViewType(heimdall.Events)
	defer release()

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *snapshotsync.Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			gg := segments[i].MakeGetter()
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
	gg := lastSegment.MakeGetter()
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastBlockNum = binary.BigEndian.Uint64(buf[length.Hash : length.Hash+length.BlockNum])
	}

	return lastBlockNum
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

	segments, release := s.snapshots.ViewType(heimdall.Events)
	defer release()

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *snapshotsync.Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			gg := segments[i].MakeGetter()
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
	gg := lastSegment.MakeGetter()
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

func (r *snapshotStore) EventLookup(ctx context.Context, txnHash libcommon.Hash) (uint64, bool, error) {
	blockNum, ok, err := r.Store.EventLookup(ctx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return blockNum, ok, nil
	}

	segs, release := r.snapshots.ViewType(heimdall.Events)
	defer release()

	blockNum, ok, err = r.borBlockByEventHash(txnHash, segs, nil)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return blockNum, true, nil
}

func (s *snapshotStore) BlockEventIdsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	maxBlockNumInFiles := s.snapshots.IndexedBlocksAvailable(heimdall.Events.Enum())
	if maxBlockNumInFiles == 0 || blockNum > maxBlockNumInFiles {
		return s.Store.BlockEventIdsRange(ctx, blockNum)
	}

	segments, release := s.snapshots.ViewType(heimdall.Events)
	defer release()

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.From() > blockNum {
			continue
		}
		if sn.To() <= blockNum {
			break
		}

		gg := sn.MakeGetter()
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

	return 0, 0, nil
}

func (s *snapshotStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	if start > s.LastFrozenEventId() {
		return s.Store.Events(ctx, start, end)
	}

	segments, release := s.snapshots.ViewType(heimdall.Events)
	defer release()

	var buf []byte
	var result [][]byte

	for i := range segments {
		gg0 := segments[i].MakeGetter()

		if i != len(segments)-1 {
			if !gg0.HasNext() {
				continue
			}

			buf0, _ := gg0.Next(nil)
			firstEventId0 := binary.BigEndian.Uint64(buf0[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])

			if start < firstEventId0 {
				break
			}

			var firstEventId1 uint64

			gg1 := segments[i+1].MakeGetter()
			if gg1.HasNext() {
				buf1, _ := gg1.Next(nil)

				firstEventId1 = binary.BigEndian.Uint64(buf1[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])

				if start > firstEventId1 {
					continue
				}
			} else {
				if i+1 != len(segments)-1 {
					continue
				}
			}
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

func (r *snapshotStore) borBlockByEventHash(txnHash libcommon.Hash, segments []*snapshotsync.Segment, buf []byte) (blockNum uint64, ok bool, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idxBorTxnHash := sn.Index()

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
		gg := sn.MakeGetter()
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

func (r *snapshotStore) BorStartEventId(ctx context.Context, hash libcommon.Hash, blockHeight uint64) (uint64, error) {
	maxBlockNumInFiles := r.snapshots.IndexedBlocksAvailable(heimdall.Events.Enum())
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		return r.Store.BorStartEventId(ctx, hash, blockHeight)
	}

	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.snapshots.ViewType(heimdall.Events)
	defer release()

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.From() > blockHeight {
			continue
		}
		if sn.To() <= blockHeight {
			break
		}

		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, found := reader.Lookup(borTxHash[:])
		if !found {
			return 0, fmt.Errorf("borTxHash %x not found in snapshot %s", borTxHash, sn.FilePath())
		}
		return idxBorTxnHash.BaseDataID() + blockEventId, nil
	}
	return 0, nil
}

func (r *snapshotStore) EventsByBlock(ctx context.Context, hash libcommon.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	maxBlockNumInFiles := r.snapshots.IndexedBlocksAvailable(heimdall.Events.Enum())
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		return r.Store.EventsByBlock(ctx, hash, blockHeight)
	}
	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.snapshots.ViewType(heimdall.Events)
	defer release()

	var buf []byte
	result := []rlp.RawValue{}
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.From() > blockHeight {
			continue
		}
		if sn.To() <= blockHeight {
			break
		}

		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, ok := reader.Lookup(borTxHash[:])
		if !ok {
			continue
		}
		offset := idxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.MakeGetter()
		gg.Reset(offset)
		for gg.HasNext() && gg.MatchPrefix(borTxHash[:]) {
			buf, _ = gg.Next(buf[:0])
			result = append(result, rlp.RawValue(libcommon.Copy(buf[length.Hash+length.BlockNum+8:])))
		}
	}
	return result, nil
}

// EventsByIdFromSnapshot returns the list of records limited by time, or the number of records along with a bool value to signify if the records were limited by time
func (r *snapshotStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	segments, release := r.snapshots.ViewType(heimdall.Events)
	defer release()

	var buf []byte
	var result []*heimdall.EventRecordWithTime
	maxTime := false

	for _, sn := range segments {
		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil || idxBorTxnHash.KeyCount() == 0 {
			continue
		}

		offset := idxBorTxnHash.OrdinalLookup(0)
		gg := sn.MakeGetter()
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
