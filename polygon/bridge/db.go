package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

// GetLatestEventID the latest state sync event ID in given DB, 0 if DB is empty
// NOTE: Polygon sync events start at index 1
func GetLatestEventID(ctx context.Context, db kv.RoDB) (uint64, error) {
	var eventID uint64
	err := db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
		if err != nil {
			return err
		}
		defer cursor.Close()

		k, _, err := cursor.Last()
		if err != nil {
			return err
		}

		if len(k) == 0 {
			eventID = 0
			return nil
		}

		eventID = binary.BigEndian.Uint64(k)
		return nil
	})

	return eventID, err
}

// GetLastSpanEventID gets the last event id where event.ID >= lastID and event.Time < time
func GetLastSpanEventID(ctx context.Context, db kv.RoDB, lastID uint64, timeLimit time.Time, stateContract abi.ABI) (uint64, error) {
	var eventID uint64

	err := db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
		if err != nil {
			return err
		}
		defer cursor.Close()

		kDBLast, _, err := cursor.Last()
		if err != nil {
			return err
		}

		kLastID := make([]byte, 8)
		binary.BigEndian.PutUint64(kLastID, lastID)

		if bytes.Equal(kLastID, kDBLast) {
			eventID = lastID
			return nil
		}

		_, _, err = cursor.Seek(kLastID)
		if err != nil {
			return err
		}

		for {
			_, v, err := cursor.Next()
			if err != nil {
				return err
			}

			event, err := heimdall.UnpackEventRecordWithTime(stateContract, v)
			if err != nil {
				return err
			}

			if event.Time.After(timeLimit) {
				return nil
			}

			eventID = event.ID
		}

	})

	return eventID, err
}

func AddEvents(ctx context.Context, db kv.RwDB, events []*heimdall.EventRecordWithTime, stateContract abi.ABI) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
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

		return nil
	})
}

// GetEvents gets raw events, start and end inclusive
func GetEvents(ctx context.Context, db kv.RwDB, id IDRange) ([][]byte, error) {
	var events [][]byte

	kStart := make([]byte, 8)
	binary.BigEndian.PutUint64(kStart, id.start)

	kEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(kEnd, id.end+1)

	err := db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.PolygonBridgeEvents)
		if err != nil {
			return err
		}
		defer cursor.Close()

		var k, v []byte
		_, v, err = cursor.Seek(kStart)
		if err != nil {
			return err
		}

		for {
			events = append(events, v)

			k, v, err = cursor.Next()
			if err != nil {
				return err
			}
			if bytes.Equal(k, kEnd) {
				return nil
			}
		}
	})

	return events, err
}
