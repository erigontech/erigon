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
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

var _ kv.TemporalRwTx = &MemoryMutation{}

// domainEntry holds a value written via DomainPut/DomainDel.
// A nil value (deleted == true) means the key was deleted.
type domainEntry struct {
	value   []byte
	deleted bool
}

type MemoryMutation struct {
	// mu protects concurrent access to the mutation's maps and backing tx.
	// Read methods (GetOne, Has) acquire RLock; write methods (Put, Delete,
	// ClearTable, UpdateTxn) acquire Lock. Cursor-based methods are NOT
	// protected and must only be called single-threaded (under the semaphore).
	// Read views created via NewReadView share this pointer so they synchronize
	// with the parent's writers.
	mu               *sync.RWMutex
	memTx            kv.RwTx
	memDb            kv.RwDB
	deletedEntries   map[string]map[string]struct{}
	deletedDups      map[string]map[string]map[string]struct{}
	clearedTables    map[string]struct{}
	db               kv.TemporalTx
	statelessCursors map[string]kv.RwCursor

	// domainOverlay stores domain-level writes (DomainPut/DomainDel) in memory.
	// GetLatest checks this overlay before falling back to the underlying tx.
	domainOverlay [kv.DomainLen]map[string]domainEntry
}

// NewMemoryBatch creates a pure Go in-memory batch with no OS-thread affinity.
// This is safe to hold across goroutine migrations (e.g. between Engine API calls).
//
// Common pattern:
//
//	batch := NewMemoryBatch(db, tmpDir)
//	defer batch.Close()
//	... some calculations on `batch`
//	batch.Commit()
func NewMemoryBatch(tx kv.TemporalTx, tmpDir string, logger log.Logger) (*MemoryMutation, error) {
	mem := newMemStore()
	memDB := &memStoreDB{store: mem}
	if err := initSequences(tx, mem); err != nil {
		return nil, fmt.Errorf("NewMemoryBatch: init sequences: %w", err)
	}

	return &MemoryMutation{
		mu:             &sync.RWMutex{},
		db:             tx,
		memDb:          memDB,
		memTx:          mem,
		deletedEntries: make(map[string]map[string]struct{}),
		deletedDups:    map[string]map[string]map[string]struct{}{},
		clearedTables:  make(map[string]struct{}),
	}, nil
}

// NewMemoryBatchMDBX creates an MDBX-backed in-memory batch. The MDBX write
// transaction pins the goroutine to an OS thread via runtime.LockOSThread(),
// so this variant must not be held across goroutine migrations.
func NewMemoryBatchMDBX(tx kv.TemporalTx, tmpDir string, logger log.Logger) (mm *MemoryMutation, err error) {
	tmpDB := mdbx.New(dbcfg.TemporaryDB, logger).InMem(nil, tmpDir).GrowthStep(64 * datasize.MB).MapSize(512 * datasize.GB).MustOpen()
	defer func() {
		if err != nil {
			tmpDB.Close()
		}
	}()
	memTx, err := tmpDB.BeginRw(context.Background()) // nolint:gocritic
	if err != nil {
		return nil, fmt.Errorf("NewMemoryBatchMDBX: begin tx: %w", err)
	}
	if err = initSequences(tx, memTx); err != nil {
		memTx.Rollback()
		return nil, fmt.Errorf("NewMemoryBatchMDBX: init sequences: %w", err)
	}

	return &MemoryMutation{
		mu:             &sync.RWMutex{},
		db:             tx,
		memDb:          tmpDB,
		memTx:          memTx,
		deletedEntries: make(map[string]map[string]struct{}),
		deletedDups:    map[string]map[string]map[string]struct{}{},
		clearedTables:  make(map[string]struct{}),
	}, nil
}

func (m *MemoryMutation) UnderlyingTx() kv.TemporalTx {
	return m.db
}

func (m *MemoryMutation) UpdateTxn(tx kv.TemporalTx) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.db = tx
	m.statelessCursors = nil
}

// DetachDB removes and returns the backing DB tx. After this call, the overlay
// is a pure in-memory structure with no external resources — Close/Rollback
// only frees the in-memory memDb. This makes the overlay safe to publish via
// Events for concurrent RPC reads (consumers create ReadViews with their own tx).
func (m *MemoryMutation) DetachDB() kv.TemporalTx {
	m.mu.Lock()
	defer m.mu.Unlock()
	db := m.db
	m.db = nil
	m.statelessCursors = nil
	return db
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memTx.IncrementSequence(bucket, amount)
}

func (m *MemoryMutation) ReadSequence(bucket string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.memTx.ReadSequence(bucket)
}

func (m *MemoryMutation) ResetSequence(bucket string, newValue uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

	k, v, err := c.Seek(prefix)
	if err != nil {
		return err
	}
	for ; k != nil && amount > 0; k, v, err = c.Next() {
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

// GetOne returns the value for a key from the mutation overlay, falling back to the
// underlying DB transaction. Thread-safe: acquires RLock to allow concurrent reads
// while writes are serialized by Lock.
func (m *MemoryMutation) GetOne(table string, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// If table was cleared, only mem layer has valid data.
	if m.isTableCleared(table) {
		return m.memTx.GetOne(table, key)
	}
	// If key was explicitly deleted, check mem layer only (may have been re-Put).
	if m.isEntryDeleted(table, key) {
		return m.memTx.GetOne(table, key)
	}
	// Try mem layer first.
	v, err := m.memTx.GetOne(table, key)
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v, nil
	}
	// Fall back to underlying DB (nil when overlay is detached for publishing).
	if m.db == nil {
		return nil, nil
	}
	return m.db.GetOne(table, key)
}

func (m *MemoryMutation) Last(table string) ([]byte, []byte, error) {
	panic("not implemented. (MemoryMutation.Last)")
}

// Has returns whether a key is present in the mutation overlay or underlying DB.
// Thread-safe: acquires RLock.
func (m *MemoryMutation) Has(table string, key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.isTableCleared(table) {
		return m.memTx.Has(table, key)
	}
	if m.isEntryDeleted(table, key) {
		return m.memTx.Has(table, key)
	}
	has, err := m.memTx.Has(table, key)
	if err != nil || has {
		return has, err
	}
	if m.db == nil {
		return false, nil
	}
	return m.db.Has(table, key)
}

func (m *MemoryMutation) Put(table string, k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memTx.Put(table, k, v)
}

func (m *MemoryMutation) Append(table string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memTx.Append(table, key, value)
}

func (m *MemoryMutation) AppendDup(table string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

	k, v, err := c.Seek(fromPrefix)
	if err != nil {
		return err
	}
	for ; k != nil; k, v, err = c.Next() {
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
	s := &rangeIter{orderAscend: bool(asc), limit: int64(limit)}
	var err error
	if m.db != nil {
		if s.iterDb, err = m.db.Range(table, fromPrefix, toPrefix, asc, limit); err != nil {
			return s, err
		}
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
	if m.db != nil {
		if s.iterDb, err = m.db.RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit); err != nil {
			return s, err
		}
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
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

// Check if a bucket is a pure DupSort table
func isTablePurelyDupsort(bucket string) bool {
	config, ok := kv.ChaindataTablesCfg[bucket]
	if !ok {
		return false
	}
	return config.Flags == kv.DupSort
}

func (m *MemoryMutation) MemDB() kv.TemporalRwDB {
	return temporaldb{m}
}

// Cursor creates a new cursor (the real fun begins here)
func (m *MemoryMutation) makeCursor(bucket string) (kv.RwCursorDupSort, error) {
	c := &memoryMutationCursor{pureDupSort: isTablePurelyDupsort(bucket)}
	// We can filter duplicates in dup sorted table
	c.table = bucket

	var err error
	if m.db != nil {
		c.cursor, err = m.db.CursorDupSort(bucket) //nolint:gocritic
		if err != nil {
			return nil, err
		}
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
	return m.db.ViewID()
}

func (m *MemoryMutation) CHandle() unsafe.Pointer {
	panic("CHandle not implemented")
}

type hasAggCtx interface {
	AggTx() any
}

func (m *MemoryMutation) AggTx() any {
	if m.db == nil {
		return nil
	}
	return m.db.(hasAggCtx).AggTx()
}

func (m *MemoryMutation) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	// Manual RLock/RUnlock (not deferred) to avoid holding the lock during
	// the fallback m.db.GetLatest call. Both return paths unlock explicitly.
	m.mu.RLock()
	if tbl := m.domainOverlay[name]; tbl != nil {
		if e, ok := tbl[string(k)]; ok {
			m.mu.RUnlock()
			if e.deleted {
				return nil, 0, nil
			}
			return common.Copy(e.value), 0, nil
		}
	}
	m.mu.RUnlock()
	if m.db == nil {
		return nil, 0, fmt.Errorf("MemoryMutation: domain read requires backing tx (detached overlay)")
	}
	return m.db.GetLatest(name, k)
}

func (m *MemoryMutation) GetAsOf(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	if m.db == nil {
		return nil, false, fmt.Errorf("MemoryMutation: domain read requires backing tx (detached overlay)")
	}
	return m.db.GetAsOf(name, k, ts)
}

func (m *MemoryMutation) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	// Find the lexicographically smallest non-deleted matching key across
	// this overlay, parent overlays, and the root DB. This preserves the
	// interface contract that HasPrefix returns the first matching key.

	// Candidate from this overlay (smallest non-deleted match).
	var bestKey string
	var bestVal []byte
	m.mu.RLock()
	if tbl := m.domainOverlay[name]; tbl != nil {
		for k, e := range tbl {
			if !e.deleted && strings.HasPrefix(k, string(prefix)) {
				if bestVal == nil || k < bestKey {
					bestKey = k
					bestVal = common.Copy(e.value)
				}
			}
		}
	}
	m.mu.RUnlock()

	// Check parent overlay(s) for a match. When the backing tx is a
	// *MemoryMutation, Debug().RangeLatest() skips to the root DB and
	// misses keys written into intermediate overlays.
	if parentKeys := collectParentOverlayKeys(m.db, name, prefix); len(parentKeys) > 0 {
		for _, k := range parentKeys {
			// collectParentOverlayKeys returns keys non-deleted at their own
			// layer, but an intermediate overlay higher up may shadow them.
			// Check the full chain from this overlay down.
			if !m.isDeletedInOverlayChain(name, k) {
				if bestVal == nil || k < bestKey {
					v, _, err := m.db.GetLatest(name, []byte(k))
					if err != nil {
						return nil, nil, false, err
					}
					bestKey = k
					bestVal = v
				}
			}
		}
	}

	// Scan the root DB for a key with this prefix that hasn't been
	// deleted in any overlay. Uses RangeLatest to iterate past deleted keys
	// (a single-shot HasPrefix can't recover when its first match is masked).
	if m.db != nil {
		debugTx := m.db.Debug()
		if debugTx == nil {
			return nil, nil, false, fmt.Errorf("MemoryMutation.HasPrefix: backing tx has no Debug() — domain overlay requires a temporal backing tx")
		}
		dbKey, dbVal, dbFound, err := m.hasPrefixViaRange(debugTx, name, prefix)
		if err != nil {
			return nil, nil, false, err
		}
		if dbFound && (bestVal == nil || string(dbKey) < bestKey) {
			bestKey = string(dbKey)
			bestVal = dbVal
		}
	}

	if bestVal == nil {
		return nil, nil, false, nil
	}
	return []byte(bestKey), bestVal, true, nil
}

// isDeletedInOverlayChain checks whether a key is marked deleted in this
// overlay or any parent overlay in the chain.
func (m *MemoryMutation) isDeletedInOverlayChain(name kv.Domain, key string) bool {
	m.mu.RLock()
	if tbl := m.domainOverlay[name]; tbl != nil {
		if e, found := tbl[key]; found {
			m.mu.RUnlock()
			return e.deleted // entry exists: deleted or live — no need to check parents
		}
	}
	m.mu.RUnlock()
	if parent, ok := m.db.(*MemoryMutation); ok {
		return parent.isDeletedInOverlayChain(name, key)
	}
	return false
}

// hasPrefixViaRange scans the root DB via RangeLatest, skipping keys
// that are deleted in this overlay or any parent overlay. This handles the
// case where the first matching key was deleted but other matching keys
// still exist.
func (m *MemoryMutation) hasPrefixViaRange(debugTx kv.TemporalDebugTx, name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	to := prefixUpperBound(prefix)
	it, err := debugTx.RangeLatest(name, prefix, to, -1)
	if err != nil {
		return nil, nil, false, err
	}
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, nil, false, err
		}
		if !m.isDeletedInOverlayChain(name, string(k)) {
			return common.Copy(k), common.Copy(v), true, nil
		}
	}
	return nil, nil, false, nil
}

func (m *MemoryMutation) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	if m.db == nil {
		return 0
	}
	return m.db.StepsInFiles(entitySet...)
}

func (m *MemoryMutation) RangeAsOf(name kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error) {
	if m.db == nil {
		return nil, fmt.Errorf("MemoryMutation: domain read requires backing tx (detached overlay)")
	}
	return m.db.RangeAsOf(name, fromKey, toKey, ts, asc, limit)
}

func (m *MemoryMutation) HistorySeek(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	if m.db == nil {
		return nil, false, fmt.Errorf("MemoryMutation: history read requires backing tx (detached overlay)")
	}
	return m.db.HistorySeek(name, k, ts)
}

func (m *MemoryMutation) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	if m.db == nil {
		return nil, fmt.Errorf("MemoryMutation: domain read requires backing tx (detached overlay)")
	}
	return m.db.IndexRange(name, k, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (it stream.KV, err error) {
	if m.db == nil {
		return nil, fmt.Errorf("MemoryMutation: history read requires backing tx (detached overlay)")
	}
	return m.db.HistoryRange(name, fromTs, toTs, asc, limit)
}

func (m *MemoryMutation) HistoryStartFrom(name kv.Domain) uint64 {
	if m.db == nil {
		return 0
	}
	debugTx := m.db.Debug()
	if debugTx == nil {
		return 0
	}
	return debugTx.HistoryStartFrom(name)
}
func (m *MemoryMutation) FreezeInfo() kv.FreezeInfo {
	panic("not supported")
}
func (m *MemoryMutation) Debug() kv.TemporalDebugTx {
	if m.db == nil {
		return nil
	}
	return m.db.Debug()
}

func (m *MemoryMutation) AggForkablesTx(id kv.ForkableId) any {
	if m.db == nil {
		return nil
	}
	return m.db.AggForkablesTx(id)
}

func (m *MemoryMutation) Unmarked(id kv.ForkableId) kv.UnmarkedTx {
	if m.db == nil {
		return nil
	}
	return m.db.Unmarked(id)
}

func (m *MemoryMutation) domainTable(domain kv.Domain) map[string]domainEntry {
	if m.domainOverlay[domain] == nil {
		m.domainOverlay[domain] = make(map[string]domainEntry)
	}
	return m.domainOverlay[domain]
}

func (m *MemoryMutation) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte) error {
	m.mu.Lock()
	m.domainTable(domain)[string(k)] = domainEntry{value: common.Copy(v)}
	m.mu.Unlock()
	return nil
}

func (m *MemoryMutation) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte) error {
	m.mu.Lock()
	m.domainTable(domain)[string(k)] = domainEntry{deleted: true}
	m.mu.Unlock()
	return nil
}

func (m *MemoryMutation) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	// Phase 1: Mark all this overlay's matching keys as deleted.
	// The gaps between phases (especially Phase 2→3 where the lock is released
	// for the root-DB scan) are safe because overlay execution is single-threaded
	// (one writer per overlay at a time).
	m.mu.Lock()
	tbl := m.domainTable(domain)
	for k := range tbl {
		if strings.HasPrefix(k, string(prefix)) {
			tbl[k] = domainEntry{deleted: true}
		}
	}
	m.mu.Unlock()

	if m.db == nil {
		return nil
	}

	// Phase 2: Collect keys from the root DB and parent overlays that also
	// need deletion markers. Debug().RangeLatest() only sees the root DB;
	// collectParentOverlayKeys picks up keys written into intermediate
	// overlay(s) for the nested-overlay case (e.g. DryRunBlock).
	var extraKeys []string
	if debugTx := m.db.Debug(); debugTx != nil {
		to := prefixUpperBound(prefix)
		it, err := debugTx.RangeLatest(domain, prefix, to, -1)
		if err != nil {
			return fmt.Errorf("DomainDelPrefix: RangeLatest: %w", err)
		}
		defer it.Close()
		for it.HasNext() {
			k, _, err := it.Next()
			if err != nil {
				return fmt.Errorf("DomainDelPrefix: iterator: %w", err)
			}
			extraKeys = append(extraKeys, string(k))
		}
		it.Close()
	}
	extraKeys = append(extraKeys, collectParentOverlayKeys(m.db, domain, prefix)...)

	// Phase 3: Batch-mark collected keys in one lock acquisition.
	if len(extraKeys) > 0 {
		m.mu.Lock()
		for _, k := range extraKeys {
			if _, already := tbl[k]; !already {
				tbl[k] = domainEntry{deleted: true}
			}
		}
		m.mu.Unlock()
	}

	return nil
}

// prefixUpperBound returns the exclusive upper bound for a prefix range scan.
// Returns nil if the prefix is all 0xff bytes (unbounded — scan to end of domain).
func prefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			to := common.Copy(prefix[:i+1])
			to[i]++
			return to
		}
	}
	return nil
}

// collectParentOverlayKeys returns non-deleted keys from parent MemoryMutation
// overlay(s) matching the given prefix. Walks up the chain of nested overlays.
func collectParentOverlayKeys(backing kv.TemporalTx, domain kv.Domain, prefix []byte) []string {
	parent, ok := backing.(*MemoryMutation)
	if !ok {
		return nil
	}
	var keys []string
	parent.mu.RLock()
	if tbl := parent.domainOverlay[domain]; tbl != nil {
		for k, e := range tbl {
			if !e.deleted && strings.HasPrefix(k, string(prefix)) {
				keys = append(keys, k)
			}
		}
	}
	parent.mu.RUnlock()
	// Recurse for deeper nesting.
	keys = append(keys, collectParentOverlayKeys(parent.db, domain, prefix)...)
	return keys
}

func (m *MemoryMutation) UnmarkedRw(id kv.ForkableId) kv.UnmarkedRwTx {
	if m.db == nil {
		return nil
	}
	if rwTx, ok := m.db.(kv.TemporalRwTx); ok {
		return rwTx.UnmarkedRw(id)
	}
	return nil // overlay backed by RO tx
}

func (m *MemoryMutation) PruneSmallBatches(ctx context.Context, timeout time.Duration) (haveMore bool, err error) {
	if m.db == nil {
		return false, nil
	}
	if rwTx, ok := m.db.(kv.TemporalRwTx); ok {
		return rwTx.PruneSmallBatches(ctx, timeout)
	}
	return false, nil // overlay backed by RO tx — prune deferred to commit window
}

func (m *MemoryMutation) Unwind(ctx context.Context, txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	if m.db == nil {
		return fmt.Errorf("unwind requires backing tx (detached overlay)")
	}
	if rwTx, ok := m.db.(kv.TemporalRwTx); ok {
		return rwTx.Unwind(ctx, txNumUnwindTo, changeset)
	}
	return fmt.Errorf("unwind requires TemporalRwTx, got %T", m.db)
}

// NewReadView creates a lightweight read-only view of this overlay backed by
// the given tx for fallback reads. The view shares the same in-memory data
// (memTx, deletedEntries, clearedTables) and the parent's mutex, but has its
// own db field set to the caller's tx. All existing cursor/read logic works
// naturally — memTx first, then db fallback.
//
// The returned kv.TemporalTx only exposes read methods. Callers cannot write
// to the overlay through this view. The caller must not Close the returned
// view (it doesn't own the memDb).
func (m *MemoryMutation) NewReadView(tx kv.Tx) kv.TemporalTx {
	return m.newReadViewMut(tx)
}

// newReadViewMut is the internal constructor that returns the full
// *MemoryMutation. Used by NewTemporalReadView which needs to embed it.
//
// Note: the returned view shares the KV-level overlay (memTx, deletedEntries,
// clearedTables) but NOT domainOverlay. Domain-level writes made via
// DomainPut/DomainDel are invisible through read views. This is intentional:
// read views are for RPC consumers who read committed state through their own
// temporal tx, not in-progress overlay state.
func (m *MemoryMutation) newReadViewMut(tx kv.Tx) *MemoryMutation {
	var dbTx kv.TemporalTx
	if t, ok := tx.(kv.TemporalTx); ok {
		dbTx = t
	}
	return &MemoryMutation{
		mu:             m.mu, // share parent's mutex for synchronization
		memTx:          m.memTx,
		memDb:          nil, // caller doesn't own the memDb
		deletedEntries: m.deletedEntries,
		deletedDups:    m.deletedDups,
		clearedTables:  m.clearedTables,
		db:             dbTx,
	}
}

// OverlayTemporalReadView extends an overlay read view with kv.TemporalTx
// support. It embeds a *MemoryMutation for all overlay-aware KV methods
// (GetOne, Cursor, etc.) and delegates temporal methods (GetLatest, GetAsOf,
// etc.) to its own independent temporal tx.
//
// Use NewTemporalReadView to create one. The caller is responsible for rolling
// back the underlying temporalTx when done.
type OverlayTemporalReadView struct {
	*MemoryMutation
	temporalTx kv.TemporalTx
}

var _ kv.TemporalTx = (*OverlayTemporalReadView)(nil)

// NewTemporalReadView creates a temporal read-only view that checks the overlay's
// mem layer first, then falls back to temporalTx for DB reads. The temporalTx
// must be a fresh, independently-opened transaction — it is NOT shared with the
// overlay's internal backing tx.
func (m *MemoryMutation) NewTemporalReadView(temporalTx kv.TemporalTx) *OverlayTemporalReadView {
	return &OverlayTemporalReadView{
		MemoryMutation: m.newReadViewMut(temporalTx),
		temporalTx:     temporalTx,
	}
}

// GetOne explicitly delegates to MemoryMutation.GetOne so that reads check
// the in-memory overlay first. Without this, Go's method promotion creates an
// ambiguity: both *MemoryMutation and the embedded temporalTx (kv.TemporalTx
// → kv.Tx) promote GetOne. In practice the temporalTx promotion can win,
// causing reads to bypass the overlay and hit the stale DB snapshot — which
// breaks reorgs where canonical hashes were rewritten in the overlay.
func (v *OverlayTemporalReadView) GetOne(table string, key []byte) ([]byte, error) {
	return v.MemoryMutation.GetOne(table, key)
}

// Has explicitly delegates to MemoryMutation.Has for the same reason as GetOne.
func (v *OverlayTemporalReadView) Has(table string, key []byte) (bool, error) {
	return v.MemoryMutation.Has(table, key)
}

func (v *OverlayTemporalReadView) Apply(_ context.Context, f func(tx kv.Tx) error) error {
	return f(v)
}

// Temporal methods — delegate to the independent temporal tx.

func (v *OverlayTemporalReadView) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return v.temporalTx.GetLatest(name, k)
}
func (v *OverlayTemporalReadView) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return v.temporalTx.HasPrefix(name, prefix)
}
func (v *OverlayTemporalReadView) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return v.temporalTx.StepsInFiles(entitySet...)
}
func (v *OverlayTemporalReadView) GetAsOf(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	return v.temporalTx.GetAsOf(name, k, ts)
}
func (v *OverlayTemporalReadView) RangeAsOf(name kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (stream.KV, error) {
	return v.temporalTx.RangeAsOf(name, fromKey, toKey, ts, asc, limit)
}
func (v *OverlayTemporalReadView) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (stream.U64, error) {
	return v.temporalTx.IndexRange(name, k, fromTs, toTs, asc, limit)
}
func (v *OverlayTemporalReadView) HistorySeek(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	return v.temporalTx.HistorySeek(name, k, ts)
}
func (v *OverlayTemporalReadView) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	return v.temporalTx.HistoryRange(name, fromTs, toTs, asc, limit)
}
func (v *OverlayTemporalReadView) Debug() kv.TemporalDebugTx {
	return v.temporalTx.Debug()
}
func (v *OverlayTemporalReadView) AggTx() any {
	return v.temporalTx.AggTx()
}
func (v *OverlayTemporalReadView) AggForkablesTx(id kv.ForkableId) any {
	return v.temporalTx.AggForkablesTx(id)
}
func (v *OverlayTemporalReadView) Unmarked(id kv.ForkableId) kv.UnmarkedTx {
	return v.temporalTx.Unmarked(id)
}
func (v *OverlayTemporalReadView) FreezeInfo() kv.FreezeInfo {
	return v.temporalTx.FreezeInfo()
}

type temporaldb struct {
	memoryMutation *MemoryMutation
}

var _ kv.TemporalRwDB = temporaldb{}

func (td temporaldb) AllTables() kv.TableCfg {
	return td.memoryMutation.memDb.AllTables()
}

func (td temporaldb) BeginRo(ctx context.Context) (kv.Tx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) Close() {
	td.memoryMutation.memDb.Close()
}

func (td temporaldb) CHandle() unsafe.Pointer {
	return td.memoryMutation.memDb.CHandle()
}

func (td temporaldb) PageSize() datasize.ByteSize {
	return td.memoryMutation.memDb.PageSize()
}

func (td temporaldb) ReadOnly() bool {
	return td.memoryMutation.memDb.ReadOnly()
}

func (td temporaldb) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	return td.memoryMutation.memDb.Update(ctx, f)
}
func (td temporaldb) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	return td.memoryMutation.memDb.UpdateNosync(ctx, f)
}
func (td temporaldb) View(ctx context.Context, f func(tx kv.Tx) error) error {
	return td.memoryMutation.memDb.View(ctx, f)
}

func (td temporaldb) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) BeginTemporalRwNosync(ctx context.Context) (kv.TemporalRwTx, error) {
	return td.memoryMutation, nil
}

func (td temporaldb) Debug() kv.TemporalDebugDB {
	panic("not implemented")
}

func (td temporaldb) UpdateTemporal(ctx context.Context, f func(tx kv.TemporalRwTx) error) error {
	return f(td.memoryMutation)
}

func (td temporaldb) OnFilesChange(onChange kv.OnFilesChange, onDelete kv.OnFilesChange) {}

func (td temporaldb) ViewTemporal(ctx context.Context, f func(tx kv.TemporalTx) error) error {
	return f(td.memoryMutation)
}

func (td temporaldb) Path() string { return "<mem>" }
