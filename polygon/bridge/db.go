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
	kv.BorEvents:    {},
	kv.BorEventNums: {},
	kv.BorTxLookup:  {},
}

var ErrEventIDRangeNotFound = errors.New("event id range not found")

type Store interface {
	Prepare(ctx context.Context) error
	Close()

	LatestEventID(ctx context.Context) (uint64, error)
	LastProcessedEventID(ctx context.Context) (uint64, error)
	LastProcessedBlockNum(ctx context.Context) (uint64, error)
	PutEventTxnToBlockNum(ctx context.Context, txMap map[libcommon.Hash]uint64) error
	EventTxnToBlockNum(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
	LastEventIDWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error)
	PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error
	Events(ctx context.Context, start, end uint64) ([][]byte, error)
	PutBlockNumToEventID(ctx context.Context, blockNumToEventId map[uint64]uint64) error
	BlockEventIDsRange(ctx context.Context, blockNum uint64) (start uint64, end uint64, err error) // [start,end)
	PruneEventIDs(ctx context.Context, blockNum uint64) error
}

type MdbxStore struct {
	db *polygoncommon.Database
}

func NewStore(db *polygoncommon.Database) *MdbxStore {
	return &MdbxStore{db: db}
}

func (s *MdbxStore) Prepare(ctx context.Context) error {
	err := s.db.OpenOnce(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *MdbxStore) Close() {
	s.db.Close()
}

// LatestEventID the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func (s *MdbxStore) LatestEventID(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.BorEvents)
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
func (s *MdbxStore) LastProcessedEventID(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return LastProcessedEventID(tx)
}

func LastProcessedEventID(tx kv.Tx) (uint64, error) {
	//
	// TODO this is wrong - messes up things at restart, can fix in polygon sync stage store?
	//
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

func (s *MdbxStore) LastProcessedBlockNum(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	return LastProcessedBlockNum(tx)
}

func LastProcessedBlockNum(tx kv.Tx) (uint64, error) {
	cursor, err := tx.Cursor(kv.BorEventNums)
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

	return binary.BigEndian.Uint64(k), nil
}

func (s *MdbxStore) PutEventTxnToBlockNum(ctx context.Context, txMap map[libcommon.Hash]uint64) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	vByte := make([]byte, 8)

	for k, v := range txMap {
		binary.BigEndian.PutUint64(vByte, v)

		err = tx.Put(kv.BorTxLookup, k.Bytes(), vByte)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *MdbxStore) EventTxnToBlockNum(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	var blockNum uint64

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return blockNum, false, err
	}
	defer tx.Rollback()

	v, err := tx.GetOne(kv.BorTxLookup, borTxHash.Bytes())
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
func (s *MdbxStore) LastEventIDWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return LastEventIDWithinWindow(tx, fromID, toTime)
}

func LastEventIDWithinWindow(tx kv.Tx, fromID uint64, toTime time.Time) (uint64, error) {
	//println("I WAS HEREEE")

	count, err := tx.Count(kv.BorEvents)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, fromID)

	//it, err := tx.RangeAscend(kv.BorEvents, k, nil, -1)
	//if err != nil {
	//	return 0, err
	//}
	//defer it.Close()
	//
	var eventID uint64
	//
	//var testCase bool
	//if it.HasNext() {
	//	if toTime.Unix() == 1702794007 {
	//		testCase = true
	//		println(fmt.Sprintf("trying to GetOne(BorEvents,%d)", binary.BigEndian.Uint64(k)))
	//		v, err := tx.GetOne(kv.BorEvents, k)
	//		if err != nil {
	//			println(fmt.Sprintf("err: %s", err.Error()))
	//			//return 0, err
	//		} else if v == nil {
	//			println("no value after tx.GetOne")
	//		} else {
	//			var e heimdall.EventRecordWithTime
	//			if err := e.UnmarshallBytes(v); err != nil {
	//				return 0, err
	//			}
	//			println(fmt.Sprintf("ID: %d", e.ID))
	//			println(fmt.Sprintf("Time: %d", e.Time.Unix()))
	//		}
	//	}
	//}
	//
	//if toTime.Unix() == 1702794007 {
	//	c2, err := tx.Cursor(kv.BorEvents)
	//	if err != nil {
	//		return 0, err
	//	}
	//
	//	var k2 []byte
	//	for k2, _, err = c2.First(); err == nil && k2 != nil; k2, _, err = c2.Next() {
	//		println(fmt.Sprintf("k=%d", binary.BigEndian.Uint64(k2)))
	//	}
	//	if err != nil {
	//		return 0, err
	//	}
	//
	//	defer c2.Close()
	//}

	c, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return 0, err
	}

	defer c.Close()

	var v []byte
	for k, v, err = c.Seek(k); err == nil && k != nil; k, v, err = c.Next() {
		var event heimdall.EventRecordWithTime
		if err := event.UnmarshallBytes(v); err != nil {
			return 0, err
		}

		if toTime.Unix() == 1702794007 {
			println(fmt.Sprintf("cursor.Next is eventId=%d, fromID=%d", event.ID, fromID))
			continue
		}

		if !event.Time.Before(toTime) {
			return eventID, nil
		}

		eventID = event.ID
	}
	if err != nil {
		return 0, err
	}

	//for it.HasNext() {
	//	_, v, err := it.Next()
	//	if err != nil {
	//		return 0, err
	//	}
	//
	//	var event heimdall.EventRecordWithTime
	//	if err := event.UnmarshallBytes(v); err != nil {
	//		return 0, err
	//	}
	//
	//	if testCase {
	//		println(fmt.Sprintf("it.Next is eventId=%d", event.ID))
	//	}
	//
	//	if !event.Time.Before(toTime) {
	//		return eventID, nil
	//	}
	//
	//	eventID = event.ID
	//}

	return eventID, nil
}

func (s *MdbxStore) PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = PutEvents(tx, events); err != nil {
		return err
	}

	return tx.Commit()
}

func PutEvents(tx kv.RwTx, events []*heimdall.EventRecordWithTime) error {
	for _, event := range events {
		v, err := event.MarshallBytes()
		if err != nil {
			return err
		}

		k := event.MarshallIdBytes()
		actualId := binary.BigEndian.Uint64(k)

		var actualEvent heimdall.EventRecordWithTime
		if err := actualEvent.UnmarshallBytes(v); err != nil {
			return err
		}

		println(fmt.Sprintf("putting eventId=%d,actualId=%d,actualEventId=%d,actualEventTime=%d", event.ID, actualId, actualEvent.ID, actualEvent.Time.Unix()))

		err = tx.Put(kv.BorEvents, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Events gets raw events, start inclusive, end exclusive
func (s *MdbxStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	var events [][]byte

	kStart := make([]byte, 8)
	binary.BigEndian.PutUint64(kStart, start)

	kEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(kEnd, end)

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	it, err := tx.Range(kv.BorEvents, kStart, kEnd)
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

func (s *MdbxStore) PutBlockNumToEventID(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = PutBlockNumToEventID(tx, blockNumToEventId); err != nil {
		return err
	}

	return tx.Commit()
}

func PutBlockNumToEventID(tx kv.RwTx, blockNumToEventId map[uint64]uint64) error {
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

// BlockEventIDsRange returns the state sync event ID range for the given block number.
// ErrEventIDRangeNotFound is thrown if the block number is not found in the database.
// If the given block number is the last in the database, then the second uint64 (representing end ID) is 0.
// The range is [start, end).
func (s *MdbxStore) BlockEventIDsRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	var start, end uint64

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return start, end, err
	}
	defer tx.Rollback()

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

	cursor, err := tx.Cursor(kv.BorEventNums)
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

	start = binary.BigEndian.Uint64(v)

	_, v, err = cursor.Next()
	if err != nil {
		return start, end, err
	}

	if v != nil { // may be empty if blockNum is the last entry
		end = binary.BigEndian.Uint64(v)
	}

	return start, end, nil
}

func (s *MdbxStore) PruneEventIDs(ctx context.Context, blockNum uint64) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

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
	if err != nil {
		return err
	}

	return tx.Commit()
}
