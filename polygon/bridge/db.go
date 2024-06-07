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
	kv.BorEvents:    {},
	kv.BorEventNums: {},
}

// GetLatestEventID the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func GetLatestEventID(ctx context.Context, db *polygoncommon.Database) (uint64, error) {
	tx, err := db.BeginRo(ctx)
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
func GetSprintLastEventID(ctx context.Context, db *polygoncommon.Database, lastID uint64, timeLimit time.Time, stateContract abi.ABI) (uint64, error) {
	var eventID uint64

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return eventID, err
	}
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return eventID, err
	}
	defer cursor.Close()

	count, err := cursor.Count()
	if err != nil {
		return eventID, err
	}
	if count == 0 {
		return eventID, nil
	}

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
		err = tx.Put(kv.BorEvents, k, v)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetEvents gets raw events, start inclusive, end exclusive
func GetEvents(ctx context.Context, db *polygoncommon.Database, start, end uint64) ([][]byte, error) {
	var events [][]byte

	kStart := make([]byte, 8)
	binary.BigEndian.PutUint64(kStart, start)

	kEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(kEnd, end)

	tx, err := db.BeginRo(ctx)
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

func StoreEventID(ctx context.Context, db *polygoncommon.Database, eventMap map[uint64]uint64) error {
	tx, err := db.BeginRw(ctx)
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

func GetEventIDRange(ctx context.Context, db *polygoncommon.Database, blockNum uint64) (uint64, uint64, error) {
	var start, end uint64

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return start, end, err
	}
	defer tx.Rollback()

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

	it, err := tx.RangeAscend(kv.BorEventNums, kByte, nil, 2)
	if err != nil {
		return start, end, err
	}

	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return start, end, err
		}

		if start == 0 {
			err = binary.Read(bytes.NewReader(v), binary.BigEndian, &start)
		} else {
			err = binary.Read(bytes.NewReader(v), binary.BigEndian, &end)
		}
		if err != nil {
			return start, end, err
		}
	}

	return start, end, nil
}

func PruneEventIDs(ctx context.Context, db *polygoncommon.Database, blockNum uint64) error {
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	kByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, blockNum)

	it, err := tx.RangeDescend(kv.BorEvents, nil, kByte, 0)
	if err != nil {
		return err
	}

	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return err
		}

		err = tx.Delete(kv.BorEventNums, k)
		if err != nil {
			return err
		}
	}

	return nil
}
