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

func (s *MdbxStore) GetEventIDRange(ctx context.Context, blockNum uint64) (uint64, uint64, error) {
	var start, end uint64

	tx, err := s.db.BeginRo(ctx)
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

func (s *MdbxStore) PruneEventIDs(ctx context.Context, blockNum uint64) error {
	tx, err := s.db.BeginRw(ctx)
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
