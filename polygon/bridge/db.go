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

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/accounts/abi"
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
}

type Store interface {
	Prepare(ctx context.Context) error
	Close()

	GetLatestEventID(ctx context.Context) (uint64, error)
	GetSprintLastEventID(ctx context.Context, lastID uint64, timeLimit time.Time, stateContract abi.ABI) (uint64, error)
	AddEvents(ctx context.Context, events []*heimdall.EventRecordWithTime, stateContract abi.ABI) error
	GetEvents(ctx context.Context, start, end uint64) ([][]byte, error)
	StoreEventID(ctx context.Context, eventMap map[uint64]uint64) error
	GetEventIDRange(ctx context.Context, blockNum uint64) (uint64, uint64, error)
	PruneEventIDs(ctx context.Context, blockNum uint64) error
}

type MdbxStore struct {
	db *polygoncommon.Database
}

func NewStore(db *polygoncommon.Database) *MdbxStore {
	return &MdbxStore{db: db}
}

func (s *MdbxStore) Prepare(ctx context.Context) error {
	err := s.db.OpenOnce(ctx, kv.PolygonBridgeDB, databaseTablesCfg)
	if err != nil {
		return err
	}

	return nil
}

func (s *MdbxStore) Close() {
	s.db.Close()
}

// GetLatestEventID the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func (s *MdbxStore) GetLatestEventID(ctx context.Context) (uint64, error) {
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

// GetSprintLastEventID gets the last event id where event.ID >= lastID and event.Time <= time
func (s *MdbxStore) GetSprintLastEventID(ctx context.Context, lastID uint64, timeLimit time.Time, stateContract abi.ABI) (uint64, error) {
	var eventID uint64

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return eventID, err
	}
	defer tx.Rollback()

	count, err := tx.Count(kv.BorEvents)
	if err != nil {
		return eventID, err
	}
	if count == 0 {
		return eventID, nil
	}

	cursor, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return eventID, err
	}
	defer cursor.Close()

	kDBLast, _, err := cursor.Last()
	if err != nil {
		return eventID, err
	}

	kLastID := make([]byte, 8)
	binary.BigEndian.PutUint64(kLastID, lastID)

	_, _, err = cursor.Seek(kLastID)
	if err != nil {
		return eventID, err
	}

	for {
		k, v, err := cursor.Next()
		if err != nil {
			return eventID, err
		}

		event, err := heimdall.UnpackEventRecordWithTime(stateContract, v)
		if err != nil {
			return eventID, err
		}

		// The table stores the first event ID for the range. In the
		// case where event.Time == block.Time, we would want the table to
		// store the current ID instead of the previous one
		if !event.Time.Before(timeLimit) {
			return eventID, nil
		}

		eventID = event.ID

		if bytes.Equal(k, kDBLast) {
			return eventID, nil
		}
	}
}

func (s *MdbxStore) AddEvents(ctx context.Context, events []*heimdall.EventRecordWithTime, stateContract abi.ABI) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, event := range events {
		v, err := event.Pack(stateContract)
		if err != nil {
			return err
		}

		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, event.ID)
		err = tx.Put(kv.BorEvents, k, v)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetEvents gets raw events, start inclusive, end exclusive
func (s *MdbxStore) GetEvents(ctx context.Context, start, end uint64) ([][]byte, error) {
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

		events = append(events, v)
	}

	return events, err
}

func (s *MdbxStore) StoreEventID(ctx context.Context, eventMap map[uint64]uint64) error {
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	kByte := make([]byte, 8)
	vByte := make([]byte, 8)

	for k, v := range eventMap {
		binary.BigEndian.PutUint64(kByte, k)
		binary.BigEndian.PutUint64(vByte, v)

		err = tx.Put(kv.BorEventNums, kByte, vByte)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetEventIDRange returns the state sync event ID range for the given block number.
// An error is thrown if the block number is not found in the database. If the given block
// number is the last in the database, then the second uint64 (representing end ID) is 0.
func (s *MdbxStore) GetEventIDRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
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
	if v == nil { // we don't have a map
		return start, end, errors.New(fmt.Sprintf("map not available for block %d", blockNum))
	}

	err = binary.Read(bytes.NewReader(v), binary.BigEndian, &start)
	if err != nil {
		return start, end, err
	}

	_, v, err = cursor.Next()
	if err != nil {
		return start, end, err
	}

	if v != nil { // may be empty if blockNum is the last entry
		err = binary.Read(bytes.NewReader(v), binary.BigEndian, &end)
		if err != nil {
			return start, end, err
		}
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
