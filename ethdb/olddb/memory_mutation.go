/*
   Copyright 2022 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package olddb

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/ethdb"
)

type MemoryMutation struct {
	// Bucket => Key => Value
	memTx          kv.RwTx
	memDb          kv.RwDB
	deletedEntries map[string]map[string]struct{}
	clearedTables  map[string]struct{}
	db             kv.Tx
}

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewMemoryBatch(tx kv.Tx) *MemoryMutation {
	tmpDB := mdbx.NewMDBX(log.New()).InMem().MustOpen()
	memTx, err := tmpDB.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	return &MemoryMutation{
		db:             tx,
		memDb:          tmpDB,
		memTx:          memTx,
		deletedEntries: make(map[string]map[string]struct{}),
		clearedTables:  make(map[string]struct{}),
	}
}

func (m *MemoryMutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
	}
	return nil
}

func (m *MemoryMutation) isTableCleared(table string) bool {
	_, ok := m.clearedTables[table]
	return ok
}

func (m *MemoryMutation) isEntryDeleted(table string, key []byte) bool {
	_, ok := m.deletedEntries[table]
	if !ok {
		return ok
	}
	_, ok = m.deletedEntries[table][string(key)]
	return ok
}

// getMem Retrieve database entry from memory (hashed storage will be left out for now because it is the only non auto-DupSorted table)
func (m *MemoryMutation) getMem(table string, key []byte) ([]byte, bool) {
	val, err := m.memTx.GetOne(table, key)
	if err != nil {
		panic(err)
	}
	return val, val != nil
}

func (m *MemoryMutation) DBSize() (uint64, error) { panic("not implemented") }
func (m *MemoryMutation) PageSize() uint64        { panic("not implemented") }

func (m *MemoryMutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	if err = m.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return 0, err
	}

	return currentV, nil
}

func (m *MemoryMutation) ReadSequence(bucket string) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}
	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

// Can only be called from the worker thread
func (m *MemoryMutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		if value == nil {
			return nil, nil
		}
		return value, nil
	}
	if m.db != nil && !m.isTableCleared(table) && !m.isEntryDeleted(table, key) {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

// Can only be called from the worker thread
func (m *MemoryMutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *MemoryMutation) Last(table string) ([]byte, []byte, error) {
	panic("not implemented. (MemoryMutation.Last)")
}

// Has return whether a key is present in a certain table.
func (m *MemoryMutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

// Put insert a new entry in the database, if it is hashed storage it will add it to a slice instead of a map.
func (m *MemoryMutation) Put(table string, key []byte, value []byte) error {
	return m.memTx.Put(table, key, value)
}

func (m *MemoryMutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *MemoryMutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *MemoryMutation) BatchSize() int {
	return 0
}

func (m *MemoryMutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *MemoryMutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *MemoryMutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *MemoryMutation) Delete(table string, k, v []byte) error {
	if _, ok := m.deletedEntries[table]; !ok {
		m.deletedEntries[table] = make(map[string]struct{})
	}
	m.deletedEntries[table][string(k)] = struct{}{}
	return m.memTx.Delete(table, k, v)
}

func (m *MemoryMutation) Commit() error {
	return nil
}

func (m *MemoryMutation) Rollback() {
	m.memTx.Rollback()
	m.memDb.Close()
	return
}

func (m *MemoryMutation) Close() {
	m.Rollback()
}

func (m *MemoryMutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *MemoryMutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *MemoryMutation) SetRwKV(kv kv.RwDB) {
	m.db.(ethdb.HasRwKV).SetRwKV(kv)
}

func (m *MemoryMutation) BucketSize(bucket string) (uint64, error) {
	return 0, nil
}

func (m *MemoryMutation) DropBucket(bucket string) error {
	panic("Not implemented")
}

func (m *MemoryMutation) ExistsBucket(bucket string) (bool, error) {
	panic("Not implemented")
}

func (m *MemoryMutation) ListBuckets() ([]string, error) {
	panic("Not implemented")
}

func (m *MemoryMutation) ClearBucket(bucket string) error {
	m.clearedTables[bucket] = struct{}{}
	return m.memTx.ClearBucket(bucket)
}

func (m *MemoryMutation) isBucketCleared(bucket string) bool {
	_, ok := m.clearedTables[bucket]
	return ok
}

func (m *MemoryMutation) CollectMetrics() {
}

func (m *MemoryMutation) CreateBucket(bucket string) error {
	panic("Not implemented")
}

func (m *MemoryMutation) Flush(tx kv.RwTx) error {
	// Obtain buckets touched.
	buckets, err := m.memTx.ListBuckets()
	if err != nil {
		return err
	}
	// Obliterate buckets who are to be deleted
	for bucket := range m.clearedTables {
		if err := tx.ClearBucket(bucket); err != nil {
			return err
		}
	}
	// Obliterate entries who are to be deleted
	for bucket, keys := range m.deletedEntries {
		for key := range keys {
			if err := tx.Delete(bucket, []byte(key), nil); err != nil {
				return err
			}
		}
	}
	// Iterate over each bucket and apply changes accordingly.
	for _, bucket := range buckets {
		if isTablePurelyDupsort(bucket) {
			cbucket, err := m.memTx.CursorDupSort(bucket)
			if err != nil {
				return err
			}
			defer cbucket.Close()
			dbCursor, err := tx.RwCursorDupSort(bucket)
			if err != nil {
				return err
			}
			defer dbCursor.Close()
			for k, v, err := cbucket.First(); k != nil; k, v, err = cbucket.Next() {
				if err != nil {
					return err
				}
				if err := dbCursor.Put(k, v); err != nil {
					return err
				}
			}
		} else {
			cbucket, err := m.memTx.Cursor(bucket)
			if err != nil {
				return err
			}
			defer cbucket.Close()
			for k, v, err := cbucket.First(); k != nil; k, v, err = cbucket.Next() {
				if err != nil {
					return err
				}
				if err := tx.Put(bucket, k, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Check if a bucket is dupsorted and has dupsort conversion off
func isTablePurelyDupsort(bucket string) bool {
	config, ok := kv.ChaindataTablesCfg[bucket]
	// If we do not have the configuration we assume it is not dupsorted
	if !ok {
		return false
	}
	return !config.AutoDupSortKeysConversion && config.Flags == kv.DupSort
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) makeCursor(bucket string) (kv.RwCursorDupSort, error) {
	c := &memoryMutationCursor{}
	// We can filter duplicates in dup sorted table
	c.table = bucket

	var err error
	// Initialize db cursors
	c.dupCursor, err = m.db.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	c.cursor = c.dupCursor
	// Initialize memory cursors
	c.memDupCursor, err = m.memTx.RwCursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	c.memCursor = c.memDupCursor
	c.mutation = m
	return c, err
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) RwCursorDupSort(bucket string) (kv.RwCursorDupSort, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) RwCursor(bucket string) (kv.RwCursor, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) Cursor(bucket string) (kv.Cursor, error) {
	return m.makeCursor(bucket)
}

// ViewID creates a new cursor (the real fun begins here)
func (m *MemoryMutation) ViewID() uint64 {
	panic("ViewID Not implemented")
}
