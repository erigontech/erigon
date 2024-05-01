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

package membatchwithdb

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/c2h5oh/datasize"
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
	deletedDups      map[string]map[string]map[string]struct{}
	clearedTables    map[string]struct{}
	db               kv.Tx
	statelessCursors map[string]kv.RwCursor
}

// NewMemoryBatch - starts in-mem batch
//
// Common pattern:
//
// batch := NewMemoryBatch(db, tmpDir)
// defer batch.Close()
// ... some calculations on `batch`
// batch.Commit()
func NewMemoryBatch(tx kv.Tx, tmpDir string, logger log.Logger) *MemoryMutation {
	tmpDB := mdbx.NewMDBX(logger).InMem(tmpDir).GrowthStep(64 * datasize.MB).MapSize(512 * datasize.GB).MustOpen()
	memTx, err := tmpDB.BeginRw(context.Background()) // nolint:gocritic
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
		deletedDups:    map[string]map[string]map[string]struct{}{},
		clearedTables:  make(map[string]struct{}),
	}
}

func NewMemoryBatchWithCustomDB(tx kv.Tx, db kv.RwDB, uTx kv.RwTx, tmpDir string) *MemoryMutation {
	return &MemoryMutation{
		db:             tx,
		memDb:          db,
		memTx:          uTx,
		deletedEntries: make(map[string]map[string]struct{}),
		deletedDups:    map[string]map[string]map[string]struct{}{},
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

func (m *MemoryMutation) isDupDeleted(table string, key []byte, val []byte) bool {
	t, ok := m.deletedDups[table]
	if !ok {
		return ok
	}
	k, ok := t[string(key)]
	if !ok {
		return ok
	}
	_, ok = k[string(val)]
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
	s := &rangeIter{orderAscend: false, limit: int64(limit)}
	var err error
	if s.iterDb, err = m.db.RangeDescend(table, fromPrefix, toPrefix, limit); err != nil {
		return s, err
	}
	if s.iterMem, err = m.memTx.RangeDescend(table, fromPrefix, toPrefix, limit); err != nil {
		return s, err
	}
	return s.init()
}

type rangeIter struct {
	iterDb, iterMem                      iter.KV
	hasNextDb, hasNextMem                bool
	nextKdb, nextVdb, nextKmem, nextVmem []byte
	orderAscend                          bool
	limit                                int64
}

func (s *rangeIter) init() (*rangeIter, error) {
	s.hasNextDb = s.iterDb.HasNext()
	s.hasNextMem = s.iterMem.HasNext()
	var err error
	if s.hasNextDb {
		if s.nextKdb, s.nextVdb, err = s.iterDb.Next(); err != nil {
			return s, err
		}
	}
	if s.hasNextMem {
		if s.nextKmem, s.nextVmem, err = s.iterMem.Next(); err != nil {
			return s, err
		}
	}
	return s, nil
}

func (s *rangeIter) HasNext() bool {
	if s.limit == 0 {
		return false
	}
	return s.hasNextDb || s.hasNextMem
}
func (s *rangeIter) Next() (k, v []byte, err error) {
	s.limit--
	c := bytes.Compare(s.nextKdb, s.nextKmem)
	if !s.hasNextMem || c == -1 && s.orderAscend || c == 1 && !s.orderAscend || c == 0 {
		if s.hasNextDb {
			k = s.nextKdb
			v = s.nextVdb
			s.hasNextDb = s.iterDb.HasNext()
			if s.nextKdb, s.nextVdb, err = s.iterDb.Next(); err != nil {
				return nil, nil, err
			}
		}
	}
	if !s.hasNextDb || c == 1 && s.orderAscend || c == -1 && !s.orderAscend || c == 0 {
		if s.hasNextMem {
			k = s.nextKmem
			v = s.nextVmem
			s.hasNextMem = s.iterMem.HasNext()
			if s.nextKmem, s.nextVmem, err = s.iterMem.Next(); err != nil {
				return nil, nil, err
			}
		}
	}
	return
}

func (m *MemoryMutation) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	s := &rangeDupSortIter{key: key, orderAscend: bool(asc), limit: int64(limit)}
	var err error
	if s.iterDb, err = m.db.RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	if s.iterMem, err = m.memTx.RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	return s.init()
}

type rangeDupSortIter struct {
	iterDb, iterMem       iter.KV
	hasNextDb, hasNextMem bool
	key                   []byte
	nextVdb, nextVmem     []byte
	orderAscend           bool
	limit                 int64
}

func (s *rangeDupSortIter) init() (*rangeDupSortIter, error) {
	s.hasNextDb = s.iterDb.HasNext()
	s.hasNextMem = s.iterMem.HasNext()
	var err error
	if s.hasNextDb {
		if _, s.nextVdb, err = s.iterDb.Next(); err != nil {
			return s, err
		}
	}
	if s.hasNextMem {
		if _, s.nextVmem, err = s.iterMem.Next(); err != nil {
			return s, err
		}
	}
	return s, nil
}

func (s *rangeDupSortIter) HasNext() bool {
	if s.limit == 0 {
		return false
	}
	return s.hasNextDb || s.hasNextMem
}
func (s *rangeDupSortIter) Next() (k, v []byte, err error) {
	s.limit--
	k = s.key
	c := bytes.Compare(s.nextVdb, s.nextVmem)
	if !s.hasNextMem || c == -1 && s.orderAscend || c == 1 && !s.orderAscend || c == 0 {
		if s.hasNextDb {
			v = s.nextVdb
			s.hasNextDb = s.iterDb.HasNext()
			if _, s.nextVdb, err = s.iterDb.Next(); err != nil {
				return nil, nil, err
			}
		}
	}
	if !s.hasNextDb || c == 1 && s.orderAscend || c == -1 && !s.orderAscend || c == 0 {
		if s.hasNextMem {
			v = s.nextVmem
			s.hasNextMem = s.iterMem.HasNext()
			if _, s.nextVmem, err = s.iterMem.Next(); err != nil {
				return nil, nil, err
			}
		}
	}
	return
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
	t, ok := m.deletedEntries[table]
	if !ok {
		t = make(map[string]struct{})
		m.deletedEntries[table] = t
	}
	t[string(k)] = struct{}{}
	return m.memTx.Delete(table, k)
}

func (m *MemoryMutation) deleteDup(table string, k, v []byte) {
	t, ok := m.deletedDups[table]
	if !ok {
		t = map[string]map[string]struct{}{}
		m.deletedDups[table] = t
	}
	km, ok := t[string(k)]
	if !ok {
		km = map[string]struct{}{}
		t[string(k)] = km
	}
	km[string(v)] = struct{}{}
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

func (m *MemoryMutation) Flush(ctx context.Context, tx kv.RwTx) error {
	// Obtain buckets touched.
	buckets, err := m.memTx.ListBuckets()
	if err != nil {
		return err
	}
	// Obliterate buckets who are to be deleted
	for bucket := range m.clearedTables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if isTablePurelyDupsort(bucket) {
			if err := func() error {
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
				return nil
			}(); err != nil {
				return err
			}
		} else {
			if err := func() error {
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
				return nil
			}(); err != nil {
				return err
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
	c := &memoryMutationCursor{pureDupSort: isTablePurelyDupsort(bucket)}
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

func (m *MemoryMutation) CHandle() unsafe.Pointer {
	panic("CHandle not implemented")
}

type hasAggCtx interface {
	AggCtx() interface{}
}

func (m *MemoryMutation) AggCtx() interface{} {
	return m.db.(hasAggCtx).AggCtx()
}

func (m *MemoryMutation) DomainGet(name kv.Domain, k, k2 []byte) (v []byte, step uint64, err error) {
	return m.db.(kv.TemporalTx).DomainGet(name, k, k2)
}

func (m *MemoryMutation) DomainGetAsOf(name kv.Domain, k, k2 []byte, ts uint64) (v []byte, ok bool, err error) {
	return m.db.(kv.TemporalTx).DomainGetAsOf(name, k, k2, ts)
}
func (m *MemoryMutation) HistoryGet(name kv.History, k []byte, ts uint64) (v []byte, ok bool, err error) {
	return m.db.(kv.TemporalTx).HistoryGet(name, k, ts)
}

func (m *MemoryMutation) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error) {
	return m.db.(kv.TemporalTx).IndexRange(name, k, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) HistoryRange(name kv.History, fromTs, toTs int, asc order.By, limit int) (it iter.KV, err error) {
	return m.db.(kv.TemporalTx).HistoryRange(name, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) DomainRange(name kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
	return m.db.(kv.TemporalTx).DomainRange(name, fromKey, toKey, ts, asc, limit)
}
