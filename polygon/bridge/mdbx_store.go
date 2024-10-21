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
	"math/big"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/rlp"
)

/*
	BorEventNums stores the last event Id of the last sprint.

	e.g. For block 10 with events [1,2,3], block 15 with events [4,5,6] and block 20 with events [7,8].
	The DB will have the following.
		10: 0 (initialized at zero, NOTE: Polygon does not have and event 0)
		15: 3
		20: 6

	To get the events for block 15, we look up the map for 15 and 20 and get back 3 and 6. So our
	Id range is [4,6].
*/

var databaseTablesCfg = kv.TableCfg{
	kv.BorEvents:               {},
	kv.BorEventNums:            {},
	kv.BorEventProcessedBlocks: {},
	kv.BorTxLookup:             {},
}

var ErrEventIdRangeNotFound = errors.New("event id range not found")

type mdbxStore struct {
	db *polygoncommon.Database
}

type txStore struct {
	tx kv.Tx
}

func NewMdbxStore(dataDir string, logger log.Logger, accede bool, roTxLimit int64) *mdbxStore {
	return &mdbxStore{db: polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger, accede, roTxLimit)}
}

func NewDbStore(db kv.RoDB) *mdbxStore {
	return &mdbxStore{db: polygoncommon.AsDatabase(db)}
}

func (s *mdbxStore) WithTx(tx kv.Tx) Store {
	return txStore{tx: tx}
}

func (s *mdbxStore) Prepare(ctx context.Context) error {
	err := s.db.OpenOnce(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *mdbxStore) Close() {
	s.db.Close()
}

// LastEventId the latest state sync event Id in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func (s *mdbxStore) LastEventId(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastEventId(ctx)
}

// LastProcessedEventId gets the last seen event Id in the BorEventNums table
func (s *mdbxStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastProcessedEventId(ctx)
}

func (s *mdbxStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return ProcessedBlockInfo{}, false, err
	}

	defer tx.Rollback()
	return txStore{tx}.LastProcessedBlockInfo(ctx)
}

func (s *mdbxStore) PutProcessedBlockInfo(ctx context.Context, info ProcessedBlockInfo) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = (txStore{tx}).PutProcessedBlockInfo(ctx, info); err != nil {
		return err
	}

	return tx.Commit()
}

func putProcessedBlockInfo(tx kv.RwTx, info ProcessedBlockInfo) error {
	k, v := info.MarshallBytes()
	return tx.Put(kv.BorEventProcessedBlocks, k, v)
}

func (s *mdbxStore) LastFrozenEventBlockNum() uint64 {
	return 0
}

func (s *mdbxStore) LastFrozenEventId() uint64 {
	return 0
}

func (s *mdbxStore) PutEventTxnToBlockNum(ctx context.Context, eventTxnToBlockNum map[libcommon.Hash]uint64) error {
	if len(eventTxnToBlockNum) == 0 {
		return nil
	}

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := (txStore{tx}).PutEventTxnToBlockNum(ctx, eventTxnToBlockNum); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *mdbxStore) EventLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	return txStore{tx}.EventLookup(ctx, borTxHash)
}

// LastEventIdWithinWindow gets the last event id where event.Id >= fromId and event.Time < toTime.
func (s *mdbxStore) LastEventIdWithinWindow(ctx context.Context, fromId uint64, toTime time.Time) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastEventIdWithinWindow(ctx, fromId, toTime)
}

func lastEventIdWithinWindow(tx kv.Tx, fromId uint64, toTime time.Time) (uint64, error) {
	count, err := tx.Count(kv.BorEvents)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, fromId)

	it, err := tx.RangeAscend(kv.BorEvents, k, nil, -1)
	if err != nil {
		return 0, err
	}
	defer it.Close()

	var eventId uint64
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return 0, err
		}

		var event heimdall.EventRecordWithTime
		if err := event.UnmarshallBytes(v); err != nil {
			return 0, err
		}

		if !event.Time.Before(toTime) {
			return eventId, nil
		}

		eventId = event.ID
	}

	return eventId, nil
}

func (s *mdbxStore) PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = (txStore{tx}).PutEvents(ctx, events); err != nil {
		return err
	}

	return tx.Commit()
}

// Events gets raw events, start inclusive, end exclusive
func (s *mdbxStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return txStore{tx}.Events(ctx, start, end)
}

func (s *mdbxStore) PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	if len(blockNumToEventId) == 0 {
		return nil
	}

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = (txStore{tx}).PutBlockNumToEventId(ctx, blockNumToEventId); err != nil {
		return err
	}

	return tx.Commit()
}

// BlockEventIdsRange returns the [start, end] event Id for the given block number
// ErrEventIdRangeNotFound is thrown if the block number is not found in the database.
// If the given block number is the first in the database, then the first uint64 (representing start Id) is 0.
func (s *mdbxStore) BlockEventIdsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	return s.blockEventIdsRange(ctx, blockNum, 9)
}

func (s *mdbxStore) blockEventIdsRange(ctx context.Context, blockNum uint64, lastFrozenId uint64) (uint64, uint64, error) {
	var start, end uint64

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return start, end, err
	}
	defer tx.Rollback()

	return txStore{tx}.blockEventIdsRange(ctx, blockNum, lastFrozenId)
}

func (s *mdbxStore) Unwind(ctx context.Context, blockNum uint64) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = txStore{tx}.Unwind(ctx, blockNum)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *mdbxStore) BorStartEventId(ctx context.Context, hash libcommon.Hash, blockHeight uint64) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.BorStartEventId(ctx, hash, blockHeight)
}

func (s *mdbxStore) EventsByBlock(ctx context.Context, hash libcommon.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return txStore{tx}.EventsByBlock(ctx, hash, blockHeight)
}

func (s *mdbxStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	return nil, false, nil
}

func (s *mdbxStore) PruneEvents(ctx context.Context, blocksTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.PruneEvents(ctx, blocksTo, blocksDeleteLimit)
}

func NewTxStore(tx kv.Tx) txStore {
	return txStore{tx: tx}
}

func (s txStore) Prepare(ctx context.Context) error {
	return nil
}

func (s txStore) Close() {
}

// EventLookup the latest state sync event Id in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func (s txStore) LastEventId(ctx context.Context) (uint64, error) {
	cursor, err := s.tx.Cursor(kv.BorEvents)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	k, _, err := cursor.Last()
	if err != nil {
		return 0, err
	}

	if len(k) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(k), err
}

// LastProcessedEventId gets the last seen event Id in the BorEventNums table
func (s txStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	cursor, err := s.tx.Cursor(kv.BorEventNums)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	_, v, err := cursor.Last()
	if err != nil {
		return 0, err
	}

	if len(v) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(v), err
}

func (s txStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
	var info ProcessedBlockInfo

	cursor, err := s.tx.Cursor(kv.BorEventProcessedBlocks)
	if err != nil {
		return info, false, err
	}

	defer cursor.Close()
	k, v, err := cursor.Last()
	if err != nil {
		return info, false, err
	}
	if len(k) == 0 {
		return info, false, nil
	}

	info.UnmarshallBytes(k, v)
	return info, true, nil
}

func (s txStore) PutProcessedBlockInfo(ctx context.Context, info ProcessedBlockInfo) error {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	return putProcessedBlockInfo(tx, info)
}

func (s txStore) LastFrozenEventBlockNum() uint64 {
	return 0
}

func (s txStore) LastFrozenEventId() uint64 {
	return 0
}

func (s txStore) PutEventTxnToBlockNum(ctx context.Context, eventTxnToBlockNum map[libcommon.Hash]uint64) error {
	if len(eventTxnToBlockNum) == 0 {
		return nil
	}

	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	vBigNum := new(big.Int)
	for k, v := range eventTxnToBlockNum {
		err := tx.Put(kv.BorTxLookup, k.Bytes(), vBigNum.SetUint64(v).Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s txStore) EventLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	var blockNum uint64

	v, err := s.tx.GetOne(kv.BorTxLookup, borTxHash.Bytes())
	if err != nil {
		return blockNum, false, err
	}
	if v == nil { // we don't have a map
		return blockNum, false, nil
	}

	blockNum = new(big.Int).SetBytes(v).Uint64()
	return blockNum, true, nil
}

// LastEventIdWithinWindow gets the last event id where event.Id >= fromId and event.Time < toTime.
func (s txStore) LastEventIdWithinWindow(ctx context.Context, fromId uint64, toTime time.Time) (uint64, error) {
	return lastEventIdWithinWindow(s.tx, fromId, toTime)
}

func (s txStore) PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	for _, event := range events {
		v, err := event.MarshallBytes()
		if err != nil {
			return err
		}

		k := event.MarshallIdBytes()
		err = tx.Put(kv.BorEvents, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Events gets raw events, start inclusive, end exclusive
func (s txStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	var events [][]byte

	kStart := make([]byte, 8)
	binary.BigEndian.PutUint64(kStart, start)

	kEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(kEnd, end)

	it, err := s.tx.Range(kv.BorEvents, kStart, kEnd)
	if err != nil {
		return nil, err
	}

	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return nil, err
		}

		events = append(events, bytes.Clone(v))
	}

	return events, err
}

func (s txStore) PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	if len(blockNumToEventId) == 0 {
		return nil
	}

	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	kByte := make([]byte, 8)
	vByte := make([]byte, 8)

	for k, v := range blockNumToEventId {
		binary.BigEndian.PutUint64(kByte, k)
		binary.BigEndian.PutUint64(vByte, v)

		err := tx.Put(kv.BorEventNums, kByte, vByte)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s txStore) BlockEventIdsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	return s.blockEventIdsRange(ctx, blockNum, 0)
}

// BlockEventIdsRange returns the [start, end] event Id for the given block number
// ErrEventIdRangeNotFound is thrown if the block number is not found in the database.
// If the given block number is the first in the database, then the first uint64 (representing start Id) is 0.
func (s txStore) blockEventIdsRange(ctx context.Context, blockNum uint64, lastFrozenId uint64) (uint64, uint64, error) {
	var start, end uint64

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

	cursor, err := s.tx.Cursor(kv.BorEventNums)
	if err != nil {
		return start, end, err
	}

	_, v, err := cursor.SeekExact(kByte)
	if err != nil {
		return start, end, err
	}
	if v == nil {
		return start, end, fmt.Errorf("%w: %d", ErrEventIdRangeNotFound, blockNum)
	}

	end = binary.BigEndian.Uint64(v)

	_, v, err = cursor.Prev()
	if err != nil {
		return start, end, err
	}

	if v == nil { // may be empty if blockNum is the first entry
		if lastFrozenId > 0 {
			start = lastFrozenId + 1
		}
	} else {
		start = binary.BigEndian.Uint64(v) + 1
	}

	return start, end, nil
}

func (s txStore) BorStartEventId(ctx context.Context, hash libcommon.Hash, blockHeight uint64) (uint64, error) {
	startEventId, _, err := s.blockEventIdsRange(ctx, blockHeight, 0)
	if err != nil {
		return 0, err
	}
	return startEventId, nil
}

func (s txStore) EventsByBlock(ctx context.Context, hash libcommon.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	startEventId, endEventId, err := s.blockEventIdsRange(ctx, blockHeight, 0)
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

func (s txStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	return nil, false, nil
}

func (s txStore) PruneEvents(ctx context.Context, blocksTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return 0, fmt.Errorf("expected RW tx")
	}

	// events
	c, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return deleted, err
	}
	defer c.Close()
	var blockNumBytes [8]byte
	binary.BigEndian.PutUint64(blockNumBytes[:], blocksTo)
	_, _, err = c.Seek(blockNumBytes[:])
	if err != nil {
		return deleted, err
	}
	k, v, err := c.Prev()
	if err != nil {
		return deleted, err
	}
	var eventIdTo uint64 = 0
	if k != nil {
		eventIdTo = binary.BigEndian.Uint64(v) + 1
	}

	c1, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return deleted, err
	}
	defer c1.Close()
	counter := blocksDeleteLimit
	for k, _, err = c1.First(); err == nil && k != nil && counter > 0; k, _, err = c1.Next() {
		eventId := binary.BigEndian.Uint64(k)
		if eventId >= eventIdTo {
			break
		}
		if err = c1.DeleteCurrent(); err != nil {
			return deleted, err
		}
		deleted++
		counter--
	}
	if err != nil {
		return deleted, err
	}

	epbCursor, err := tx.RwCursor(kv.BorEventProcessedBlocks)
	if err != nil {
		return deleted, err
	}

	defer epbCursor.Close()
	counter = blocksDeleteLimit
	for k, _, err = epbCursor.First(); err == nil && k != nil && counter > 0; k, _, err = epbCursor.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blocksTo {
			break
		}

		if err = epbCursor.DeleteCurrent(); err != nil {
			return deleted, err
		}

		deleted++
		counter--
	}

	return deleted, err
}

// Unwind deletes unwindable bridge data.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func (s txStore) Unwind(ctx context.Context, blockNum uint64) error {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	if err := UnwindBlockNumToEventID(tx, blockNum); err != nil {
		return err
	}

	if err := UnwindEventProcessedBlocks(tx, blockNum); err != nil {
		return err
	}

	return UnwindEventTxnToBlockNum(tx, blockNum)
}

// UnwindBlockNumToEventID deletes data in kv.BorEventProcessedBlocks.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func UnwindBlockNumToEventID(tx kv.RwTx, blockNum uint64) error {
	c, err := tx.RwCursor(kv.BorEventNums)
	if err != nil {
		return err
	}

	defer c.Close()
	var k []byte
	for k, _, err = c.Last(); err == nil && k != nil; k, _, err = c.Prev() {
		if currentBlockNum := binary.BigEndian.Uint64(k); currentBlockNum <= blockNum {
			break
		}

		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

// UnwindEventProcessedBlocks deletes data in kv.BorEventProcessedBlocks.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func UnwindEventProcessedBlocks(tx kv.RwTx, blockNum uint64) error {
	c, err := tx.RwCursor(kv.BorEventProcessedBlocks)
	if err != nil {
		return err
	}

	defer c.Close()
	firstK, _, err := c.First()
	if err != nil {
		return err
	}
	if len(firstK) == 0 {
		return errors.New("unexpected missing first processed block info entry when unwinding")
	}
	if first := binary.BigEndian.Uint64(firstK); blockNum < first {
		// we always want to have at least 1 entry in the table
		return fmt.Errorf("unwind blockNumber is too far back: first=%d, unwind=%d", first, blockNum)
	}

	var k []byte
	for k, _, err = c.Last(); err == nil && k != nil; k, _, err = c.Prev() {
		if currentBlockNum := binary.BigEndian.Uint64(k); currentBlockNum <= blockNum {
			break
		}

		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

// UnwindEventTxnToBlockNum deletes data in kv.BorTxLookup.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func UnwindEventTxnToBlockNum(tx kv.RwTx, blockNum uint64) error {
	c, err := tx.RwCursor(kv.BorTxLookup)
	if err != nil {
		return err
	}

	defer c.Close()
	blockNumBig := new(big.Int)
	var k, v []byte
	for k, v, err = c.Last(); err == nil && k != nil; k, v, err = c.Prev() {
		if currentBlockNum := blockNumBig.SetBytes(v).Uint64(); currentBlockNum <= blockNum {
			break
		}

		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}
