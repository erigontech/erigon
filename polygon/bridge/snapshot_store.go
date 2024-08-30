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

func (s *snapshotStore) WithTx(tx kv.Tx) Store {
	return &snapshotStore{txStore{tx: tx}, s.snapshots}
}

func (s *snapshotStore) LastFrozenEventBlockNum() uint64 {
	if s.snapshots == nil {
		return 0
	}

	segments, release := s.snapshots.ViewType(heimdall.BorEvents)
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

	segments, release := s.snapshots.ViewType(heimdall.BorEvents)
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
	var lastEventID uint64
	gg := lastSegment.MakeGetter()
	var buf []byte
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastEventID = binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
	}
	return lastEventID
}

func (r *snapshotStore) EventLookup(ctx context.Context, txnHash libcommon.Hash) (uint64, bool, error) {
	blockNum, ok, err := r.Store.EventLookup(ctx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return blockNum, ok, nil
	}

	segs, release := r.snapshots.ViewType(heimdall.BorEvents)
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

func (r *snapshotStore) BorStartEventID(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (uint64, error) {
	maxBlockNumInFiles := r.FrozenBorBlocks()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		v, err := tx.GetOne(kv.BorEventNums, hexutility.EncodeTs(blockHeight))
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, fmt.Errorf("BorStartEventID(%d) not found", blockHeight)
		}
		startEventId := binary.BigEndian.Uint64(v)
		return startEventId, nil
	}

	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.borSn.ViewType(heimdall.BorEvents)
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

func (r *snapshotStore) EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	maxBlockNumInFiles := r.FrozenBorBlocks()
	if tx != nil && (maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles) {
		c, err := tx.Cursor(kv.BorEventNums)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		var k, v []byte
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], blockHeight)
		result := []rlp.RawValue{}
		if k, v, err = c.Seek(buf[:]); err != nil {
			return nil, err
		}
		if !bytes.Equal(k, buf[:]) {
			return result, nil
		}
		endEventId := binary.BigEndian.Uint64(v)
		var startEventId uint64
		if k, v, err = c.Prev(); err != nil {
			return nil, err
		}
		if k == nil {
			startEventId = 1
		} else {
			startEventId = binary.BigEndian.Uint64(v) + 1
		}
		c1, err := tx.Cursor(kv.BorEvents)
		if err != nil {
			return nil, err
		}
		defer c1.Close()
		binary.BigEndian.PutUint64(buf[:], startEventId)
		for k, v, err = c1.Seek(buf[:]); err == nil && k != nil; k, v, err = c1.Next() {
			eventId := binary.BigEndian.Uint64(k)
			if eventId > endEventId {
				break
			}
			result = append(result, common.Copy(v))
		}
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.borSn.ViewType(heimdall.BorEvents)
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
			result = append(result, rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:])))
		}
	}
	return result, nil
}

// EventsByIdFromSnapshot returns the list of records limited by time, or the number of records along with a bool value to signify if the records were limited by time
func (r *snapshotStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	segments, release := r.borSn.ViewType(heimdall.BorEvents)
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

			raw := rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:]))
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
