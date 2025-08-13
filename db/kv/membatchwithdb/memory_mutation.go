// Copyright 2022 The Erigon Authors
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

package membatchwithdb

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
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
	tmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(tmpDir).GrowthStep(64 * datasize.MB).MapSize(512 * datasize.GB).MustOpen()
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

func NewMemoryBatchWithCustomDB(tx kv.Tx, db kv.RwDB, uTx kv.RwTx) *MemoryMutation {
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
	defer cursor.Close()
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

func (m *MemoryMutation) ResetSequence(bucket string, newValue uint64) error {
	return m.memTx.ResetSequence(bucket, newValue)
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
		c, err = m.RwCursor(table) // nolint:gocritic
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

func (m *MemoryMutation) Prefix(table string, prefix []byte) (stream.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return m.Range(table, prefix, nil, order.Asc, kv.Unlim)
	}
	return m.Range(table, prefix, nextPrefix, order.Asc, kv.Unlim)
}
func (m *MemoryMutation) Stream(table string, fromPrefix, toPrefix []byte) (stream.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) StreamAscend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) StreamDescend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	panic("please implement me")
}
func (m *MemoryMutation) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s := &rangeIter{orderAscend: true, limit: int64(limit)}
	var err error
	if s.iterDb, err = m.db.Range(table, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	if s.iterMem, err = m.memTx.Range(table, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	if _, err := s.init(); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

type rangeIter struct {
	iterDb, iterMem                      stream.KV
	hasNextDb, hasNextMem                bool
	nextKdb, nextVdb, nextKmem, nextVmem []byte
	orderAscend                          bool
	limit                                int64
}

func (s *rangeIter) Close() {
	if s.iterDb != nil {
		s.iterDb.Close()
		s.iterDb = nil
	}
	if s.iterMem != nil {
		s.iterMem.Close()
		s.iterMem = nil
	}
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

func (m *MemoryMutation) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s := &rangeDupSortIter{key: key, orderAscend: bool(asc), limit: int64(limit)}
	var err error
	if s.iterDb, err = m.db.RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	if s.iterMem, err = m.memTx.RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit); err != nil {
		return s, err
	}
	if err := s.init(); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

type rangeDupSortIter struct {
	iterDb, iterMem       stream.KV
	hasNextDb, hasNextMem bool
	key                   []byte
	nextVdb, nextVmem     []byte
	orderAscend           bool
	limit                 int64
}

func (s *rangeDupSortIter) Close() {
	if s.iterDb != nil {
		s.iterDb.Close()
		s.iterDb = nil
	}
	if s.iterMem != nil {
		s.iterMem.Close()
		s.iterMem = nil
	}
}

func (s *rangeDupSortIter) init() error {
	s.hasNextDb = s.iterDb.HasNext()
	s.hasNextMem = s.iterMem.HasNext()
	var err error
	if s.hasNextDb {
		if _, s.nextVdb, err = s.iterDb.Next(); err != nil {
			return err
		}
	}
	if s.hasNextMem {
		if _, s.nextVmem, err = s.iterMem.Next(); err != nil {
			return err
		}
	}
	return nil
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

func (m *MemoryMutation) Count(bucket string) (uint64, error) {
	panic("not implemented")
}

func (m *MemoryMutation) DropTable(bucket string) error {
	panic("Not implemented")
}

func (m *MemoryMutation) ExistsTable(bucket string) (bool, error) {
	panic("Not implemented")
}

func (m *MemoryMutation) ListTables() ([]string, error) {
	panic("Not implemented")
}

func (m *MemoryMutation) ClearTable(bucket string) error {
	m.clearedTables[bucket] = struct{}{}
	return m.memTx.ClearTable(bucket)
}

func (m *MemoryMutation) CollectMetrics() {
}

func (m *MemoryMutation) CreateTable(bucket string) error {
	return m.memTx.CreateTable(bucket)
}

func (m *MemoryMutation) Flush(ctx context.Context, tx kv.RwTx) error {
	// Obtain buckets touched.
	buckets, err := m.memTx.ListTables()
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
		if err := tx.ClearTable(bucket); err != nil {
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
	buckets, err := m.memTx.ListTables()
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
	c.cursor, err = m.db.CursorDupSort(bucket) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	c.memCursor, err = m.memTx.RwCursorDupSort(bucket) //nolint:gocritic
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

func (m *MemoryMutation) Apply(_ context.Context, f func(tx kv.Tx) error) error {
	return f(m)
}

func (m *MemoryMutation) ApplyRw(_ context.Context, f func(tx kv.RwTx) error) error {
	return f(m)
}

func (m *MemoryMutation) ViewID() uint64 {
	panic("ViewID Not implemented")
}

func (m *MemoryMutation) CHandle() unsafe.Pointer {
	panic("CHandle not implemented")
}

type hasAggCtx interface {
	AggTx() any
}

func (m *MemoryMutation) AggTx() any {
	return m.db.(hasAggCtx).AggTx()
}

func (m *MemoryMutation) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	// panic("not supported")
	return m.db.(kv.TemporalTx).GetLatest(name, k)
}

func (m *MemoryMutation) GetAsOf(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	// panic("not supported")
	return m.db.(kv.TemporalTx).GetAsOf(name, k, ts)
}

func (m *MemoryMutation) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return m.db.(kv.TemporalTx).HasPrefix(name, prefix)
}

func (m *MemoryMutation) RangeAsOf(name kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error) {
	// panic("not supported")
	return m.db.(kv.TemporalTx).RangeAsOf(name, fromKey, toKey, ts, asc, limit)
}

func (m *MemoryMutation) HistorySeek(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	panic("not supported")
	// return m.db.(kv.TemporalTx).HistorySeek(name, k, ts)
}

func (m *MemoryMutation) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	// panic("not supported")
	return m.db.(kv.TemporalTx).IndexRange(name, k, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (it stream.KV, err error) {
	panic("not supported")
	// return m.db.(kv.TemporalTx).HistoryRange(name, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) HistoryStartFrom(name kv.Domain) uint64 {
	return m.db.(kv.TemporalTx).Debug().HistoryStartFrom(name)
}
func (m *MemoryMutation) FreezeInfo() kv.FreezeInfo {
	panic("not supported")
}
func (m *MemoryMutation) Debug() kv.TemporalDebugTx { return m.db.(kv.TemporalTx).Debug() }

func (m *MemoryMutation) AggForkablesTx(id kv.ForkableId) any {
	return m.db.(kv.TemporalTx).AggForkablesTx(id)
}

func (m *MemoryMutation) Unmarked(id kv.ForkableId) kv.UnmarkedTx {
	return m.db.(kv.TemporalTx).Unmarked(id)
}
