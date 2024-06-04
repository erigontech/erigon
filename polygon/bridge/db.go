package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

var databaseTablesCfg = kv.TableCfg{
	kv.PolygonBridgeEvents: {},
}

// GetLatestEventID the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func GetLatestEventID(ctx context.Context, db *polygoncommon.Database) (uint64, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
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

// GetSprintLastEventID gets the last event id where event.ID >= lastID and event.Time < time
func GetSprintLastEventID(ctx context.Context, db *polygoncommon.Database, lastID uint64, timeLimit time.Time, stateContract abi.ABI) (uint64, error) {
	var eventID uint64

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return eventID, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
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

	if bytes.Equal(kLastID, kDBLast) {
		return lastID, nil
	}

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

		if event.Time.After(timeLimit) {
			return eventID, nil
		}

		eventID = event.ID

		if bytes.Equal(k, kDBLast) {
			return eventID, nil
		}
	}
}

func AddEvents(ctx context.Context, db *polygoncommon.Database, events []*heimdall.EventRecordWithTime, stateContract abi.ABI) error {
	tx, err := db.BeginRw(ctx)
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
		err = tx.Put(kv.PolygonBridgeEvents, k, v)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetEvents gets raw events, start and end inclusive
func GetEvents(ctx context.Context, db *polygoncommon.Database, id IDRange) ([][]byte, error) {
	var events [][]byte

	kStart := make([]byte, 8)
	binary.BigEndian.PutUint64(kStart, id.start)

	kEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(kEnd, id.end+1)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var k, v []byte
	_, v, err = cursor.Seek(kStart)
	if err != nil {
		return nil, err
	}

	for {
		events = append(events, v)

		k, v, err = cursor.Next()
		if err != nil {
			return nil, err
		}
		if bytes.Equal(k, kEnd) {
			break
		}
	}

	return events, err
}
