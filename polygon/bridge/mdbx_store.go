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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

/*
	BorEventNums stores the last event ID of the last sprint.

	e.g. For block 10 with events [1,2,3], block 15 with events [4,5,6] and block 20 with events [7,8].
	The DB will have the following.
		10: 0 (initialized at zero, NOTE: Polygon does not have and event 0)
		15: 3
		20: 6

	To get the events for block 15, we look up the map for 15 and 20 and get back 3 and 6. So our
	ID range is [4,6].
*/

var databaseTablesCfg = kv.TableCfg{
	kv.BorEvents:               {},
	kv.BorEventNums:            {},
	kv.BorEventProcessedBlocks: {},
	kv.BorTxLookup:             {},
}

var ErrEventIDRangeNotFound = errors.New("event id range not found")

type mdbxStore struct {
	db *polygoncommon.Database
}

type txStore struct {
	tx kv.Tx
}

func NewMdbxStore(dataDir string, logger log.Logger) *mdbxStore {
	return &mdbxStore{db: polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger)}
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

// EventLookup the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func (s *mdbxStore) LastEventId(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastEventId(ctx)
}

// LastProcessedEventID gets the last seen event ID in the BorEventNums table
func (s *mdbxStore) LastProcessedEventID(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastProcessedEventID(ctx)
}

func lastProcessedEventID(tx kv.Tx) (uint64, error) {
	cursor, err := tx.Cursor(kv.BorEventNums)
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

func (s *mdbxStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return ProcessedBlockInfo{}, false, err
	}

	defer tx.Rollback()
	return txStore{tx}.LastProcessedBlockInfo(ctx)
}

func lastProcessedBlockInfo(tx kv.Tx) (ProcessedBlockInfo, bool, error) {
	var info ProcessedBlockInfo

	cursor, err := tx.Cursor(kv.BorEventProcessedBlocks)
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

// LastEventIDWithinWindow gets the last event id where event.ID >= fromID and event.Time < toTime.
func (s *mdbxStore) LastEventIDWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return txStore{tx}.LastEventIDWithinWindow(ctx, fromID, toTime)
}

func lastEventIDWithinWindow(tx kv.Tx, fromID uint64, toTime time.Time) (uint64, error) {
	count, err := tx.Count(kv.BorEvents)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, fromID)

	it, err := tx.RangeAscend(kv.BorEvents, k, nil, -1)
	if err != nil {
		return 0, err
	}
	defer it.Close()

	var eventID uint64
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
			return eventID, nil
		}

		eventID = event.ID
	}

	return eventID, nil
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

func putEvents(tx kv.RwTx, events []*heimdall.EventRecordWithTime) error {
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
func (s *mdbxStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return txStore{tx}.Events(ctx, start, end)
}

func (s *mdbxStore) PutBlockNumToEventID(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	if len(blockNumToEventId) == 0 {
		return nil
	}

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = (txStore{tx}).PutBlockNumToEventID(ctx, blockNumToEventId); err != nil {
		return err
	}

	return tx.Commit()
}

func putBlockNumToEventID(tx kv.RwTx, blockNumToEventId map[uint64]uint64) error {
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

// BlockEventIDsRange returns the [start, end] event ID for the given block number
// ErrEventIDRangeNotFound is thrown if the block number is not found in the database.
// If the given block number is the first in the database, then the first uint64 (representing start ID) is 0.
func (s *mdbxStore) BlockEventIDsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	var start, end uint64

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return start, end, err
	}
	defer tx.Rollback()

	return txStore{tx}.BlockEventIDsRange(ctx, blockNum)
}

func (s *mdbxStore) PruneEventIDs(ctx context.Context, blockNum uint64) error {
	//
	// TODO rename func to Unwind, unwind BorEventProcessedBlocks, BorTxnLookup - in separate PR
	//

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = txStore{tx}.PruneEventIDs(ctx, blockNum)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func NewTxStore(tx kv.Tx) txStore {
	return txStore{tx: tx}
}

func (s txStore) Prepare(ctx context.Context) error {
	return nil
}

func (s txStore) Close() {
}

// EventLookup the latest state sync event ID in given DB, 0 if DB is empty
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

// LastProcessedEventID gets the last seen event ID in the BorEventNums table
func (s txStore) LastProcessedEventID(ctx context.Context) (uint64, error) {
	return lastProcessedEventID(s.tx)
}

func (s txStore) LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error) {
	return lastProcessedBlockInfo(s.tx)
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

	vByte := make([]byte, 8)

	for k, v := range eventTxnToBlockNum {
		binary.BigEndian.PutUint64(vByte, v)

		err := tx.Put(kv.BorTxLookup, k.Bytes(), vByte)
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

	blockNum = binary.BigEndian.Uint64(v)
	return blockNum, true, nil
}

// LastEventIDWithinWindow gets the last event id where event.ID >= fromID and event.Time < toTime.
func (s txStore) LastEventIDWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error) {
	return lastEventIDWithinWindow(s.tx, fromID, toTime)
}

func (s txStore) PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error {
	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	return putEvents(tx, events)
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

func (s txStore) PutBlockNumToEventID(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	if len(blockNumToEventId) == 0 {
		return nil
	}

	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	return putBlockNumToEventID(tx, blockNumToEventId)
}

// BlockEventIDsRange returns the [start, end] event ID for the given block number
// ErrEventIDRangeNotFound is thrown if the block number is not found in the database.
// If the given block number is the first in the database, then the first uint64 (representing start ID) is 0.
func (s txStore) BlockEventIDsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
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
		return start, end, fmt.Errorf("%w: %d", ErrEventIDRangeNotFound, blockNum)
	}

	end = binary.BigEndian.Uint64(v)

	_, v, err = cursor.Prev()
	if err != nil {
		return start, end, err
	}

	if v != nil { // may be empty if blockNum is the first entry
		start = binary.BigEndian.Uint64(v) + 1
	}

	return start, end, nil
}

func (s txStore) PruneEventIDs(ctx context.Context, blockNum uint64) error {
	//
	// TODO rename func to Unwind, unwind BorEventProcessedBlocks, BorTxnLookup - in separate PR
	//

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

	tx, ok := s.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("expected RW tx")
	}

	cursor, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer cursor.Close()

	var k []byte
	for k, _, err = cursor.Seek(kByte); err == nil && k != nil; k, _, err = cursor.Next() {
		if err := tx.Delete(kv.BorEventNums, k); err != nil {
			return err
		}
	}

	return err
}
