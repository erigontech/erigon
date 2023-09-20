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

package memdb

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

type MemoryMutation struct {
	memTx            kv.RwTx
	memDb            kv.RwDB
	deletedEntries   map[string]map[string]struct{}
	clearedTables    map[string]struct{}
	db               kv.Tx
	statelessCursors map[string]kv.RwCursor
}

// NewMemoryBatch - starts in-mem batch
//
// Common pattern:
//
// batch := NewMemoryBatch(db, tmpDir)
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewMemoryBatch(tx kv.Tx, tmpDir string) *MemoryMutation {
	tmpDB := mdbx.NewMDBX(log.New()).InMem(tmpDir).MustOpen()
	memTx, err := tmpDB.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	if err := initSequences(tx, memTx); err != nil {
		return nil
	}

	return &MemoryMutation{
		db:             tx,
		memDb:          tmpDB,
		memTx:          memTx,
		deletedEntries: make(map[string]map[string]struct{}),
		clearedTables:  make(map[string]struct{}),
	}
}

func NewMemoryBatchWithCustomDB(tx kv.Tx, db kv.RwDB, uTx kv.RwTx, tmpDir string) *MemoryMutation {
	return &MemoryMutation{
		db:             tx,
		memDb:          db,
		memTx:          uTx,
		deletedEntries: make(map[string]map[string]struct{}),
		clearedTables:  make(map[string]struct{}),
	}
}

func (m *MemoryMutation) UpdateTxn(tx kv.Tx) {
	m.db = tx
	m.statelessCursors = nil
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

func (m *MemoryMutation) DBSize() (uint64, error) {
	panic("not implemented")
}

func initSequences(db kv.Tx, memTx kv.RwTx) error {
	cursor, err := db.Cursor(kv.Sequence)
	if err != nil {
		return err
	}
	for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return err
		}
		if err := memTx.Put(kv.Sequence, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryMutation) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	return m.memTx.IncrementSequence(bucket, amount)
}

func (m *MemoryMutation) ReadSequence(bucket string) (uint64, error) {
	return m.memTx.ReadSequence(bucket)
}

func (m *MemoryMutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
	c, err := m.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(prefix); k != nil && amount > 0; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
		amount--
	}
	return nil
}

func (m *MemoryMutation) statelessCursor(table string) (kv.RwCursor, error) {
	if m.statelessCursors == nil {
		m.statelessCursors = make(map[string]kv.RwCursor)
	}
	c, ok := m.statelessCursors[table]
	if !ok {
		var err error
		c, err = m.RwCursor(table)
		if err != nil {
			return nil, err
		}
		m.statelessCursors[table] = c
	}
	return c, nil
}

// Can only be called from the worker thread
func (m *MemoryMutation) GetOne(table string, key []byte) ([]byte, error) {
	c, err := m.statelessCursor(table)
	if err != nil {
		return nil, err
	}
	_, v, err := c.SeekExact(key)
	return v, err
}

func (m *MemoryMutation) Last(table string) ([]byte, []byte, error) {
	panic("not implemented. (MemoryMutation.Last)")
}

// Has return whether a key is present in a certain table.
func (m *MemoryMutation) Has(table string, key []byte) (bool, error) {
	c, err := m.statelessCursor(table)
	if err != nil {
		return false, err
	}
	k, _, err := c.Seek(key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, k), nil
}

func (m *MemoryMutation) Put(table string, k, v []byte) error {
	return m.memTx.Put(table, k, v)
}

func (m *MemoryMutation) Append(table string, key []byte, value []byte) error {
	return m.memTx.Append(table, key, value)
}

func (m *MemoryMutation) AppendDup(table string, key []byte, value []byte) error {
	c, err := m.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.(*memoryMutationCursor).AppendDup(key, value)
}

func (m *MemoryMutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	c, err := m.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryMutation) Prefix(table string, prefix []byte) (iter.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return m.Stream(table, prefix, nil)
	}
	return m.Stream(table, prefix, nextPrefix)
}
func (m *MemoryMutation) Stream(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) StreamAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) StreamDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) Range(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	panic("please implement me")
}

func (m *MemoryMutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	c, err := m.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := walker(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryMutation) Delete(table string, k []byte) error {
	if _, ok := m.deletedEntries[table]; !ok {
		m.deletedEntries[table] = make(map[string]struct{})
	}
	m.deletedEntries[table][string(k)] = struct{}{}
	return m.memTx.Delete(table, k)
}

func (m *MemoryMutation) Commit() error {
	m.statelessCursors = nil
	return nil
}

func (m *MemoryMutation) Rollback() {
	m.memTx.Rollback()
	m.memDb.Close()
	m.statelessCursors = nil
}

func (m *MemoryMutation) Close() {
	m.Rollback()
}

func (m *MemoryMutation) BucketSize(bucket string) (uint64, error) {
	return m.memTx.BucketSize(bucket)
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

func (m *MemoryMutation) CollectMetrics() {
}

func (m *MemoryMutation) CreateBucket(bucket string) error {
	return m.memTx.CreateBucket(bucket)
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
			if err := tx.Delete(bucket, []byte(key)); err != nil {
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

func (m *MemoryMutation) Diff() (*MemoryDiff, error) {
	memDiff := &MemoryDiff{
		diff:           make(map[table][]entry),
		deletedEntries: make(map[string][]string),
	}
	// Obtain buckets touched.
	buckets, err := m.memTx.ListBuckets()
	if err != nil {
		return nil, err
	}
	// Obliterate buckets who are to be deleted
	for bucket := range m.clearedTables {
		memDiff.clearedTableNames = append(memDiff.clearedTableNames, bucket)
	}
	// Obliterate entries who are to be deleted
	for bucket, keys := range m.deletedEntries {
		for key := range keys {
			memDiff.deletedEntries[bucket] = append(memDiff.deletedEntries[bucket], key)
		}
	}
	// Iterate over each bucket and apply changes accordingly.
	for _, bucket := range buckets {
		if isTablePurelyDupsort(bucket) {
			cbucket, err := m.memTx.CursorDupSort(bucket)
			if err != nil {
				return nil, err
			}
			defer cbucket.Close()

			t := table{
				name:    bucket,
				dupsort: true,
			}
			for k, v, err := cbucket.First(); k != nil; k, v, err = cbucket.Next() {
				if err != nil {
					return nil, err
				}
				memDiff.diff[t] = append(memDiff.diff[t], entry{
					k: common.Copy(k),
					v: common.Copy(v),
				})
			}
		} else {
			cbucket, err := m.memTx.Cursor(bucket)
			if err != nil {
				return nil, err
			}
			defer cbucket.Close()
			t := table{
				name:    bucket,
				dupsort: false,
			}
			for k, v, err := cbucket.First(); k != nil; k, v, err = cbucket.Next() {
				if err != nil {
					return nil, err
				}
				memDiff.diff[t] = append(memDiff.diff[t], entry{
					k: common.Copy(k),
					v: common.Copy(v),
				})
			}
		}
	}
	return memDiff, nil
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

func (m *MemoryMutation) MemDB() kv.RwDB {
	return m.memDb
}

func (m *MemoryMutation) MemTx() kv.RwTx {
	return m.memTx
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) makeCursor(bucket string) (kv.RwCursorDupSort, error) {
	c := &memoryMutationCursor{}
	// We can filter duplicates in dup sorted table
	c.table = bucket

	var err error
	c.cursor, err = m.db.CursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
	c.memCursor, err = m.memTx.RwCursorDupSort(bucket)
	if err != nil {
		return nil, err
	}
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

func (m *MemoryMutation) ViewID() uint64 {
	panic("ViewID Not implemented")
}
