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

package membatchwithdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	btree2 "github.com/tidwall/btree"
)

// Compile-time check that memStore implements kv.RwTx.
var _ kv.RwTx = (*memStore)(nil)

// memEntry is a key-value pair stored in the btree.
type memEntry struct {
	k []byte
	v []byte
}

// memTable is a single table backed by a btree.
type memTable struct {
	tree    *btree2.BTreeG[memEntry]
	dupSort bool
}

func newMemTable(dupSort bool) *memTable {
	var less func(a, b memEntry) bool
	if dupSort {
		less = func(a, b memEntry) bool {
			c := bytes.Compare(a.k, b.k)
			if c != 0 {
				return c < 0
			}
			return bytes.Compare(a.v, b.v) < 0
		}
	} else {
		less = func(a, b memEntry) bool {
			return bytes.Compare(a.k, b.k) < 0
		}
	}
	return &memTable{
		// NoLocks: true because all concurrency is handled by memStore.mu.
		tree:    btree2.NewBTreeGOptions(less, btree2.Options{NoLocks: true}),
		dupSort: dupSort,
	}
}

// memStore is a pure Go in-memory key-value store that replaces the MDBX
// in-memory database previously used by MemoryMutation. It has no OS-thread
// affinity (no runtime.LockOSThread), so it can be safely used across
// goroutine migrations between Engine API calls.
//
// Thread safety: all access is protected by mu. Btrees use NoLocks:true
// because the store-level mutex serializes all operations.
type memStore struct {
	mu        sync.RWMutex
	tables    map[string]*memTable
	sequences map[string]uint64
}

func newMemStore() *memStore {
	return &memStore{
		tables:    make(map[string]*memTable),
		sequences: make(map[string]uint64),
	}
}

func memTableIsDupSort(table string) bool {
	config, ok := kv.ChaindataTablesCfg[table]
	if !ok {
		return false
	}
	return config.Flags&kv.DupSort != 0
}

// getOrCreateTable must be called with mu held (read or write).
func (s *memStore) getOrCreateTable(table string) *memTable {
	t, ok := s.tables[table]
	if !ok {
		t = newMemTable(memTableIsDupSort(table))
		s.tables[table] = t
	}
	return t
}

// --- kv.Getter ---

func (s *memStore) Has(table string, key []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return false, nil
	}
	_, found := t.tree.Get(memEntry{k: key})
	return found, nil
}

func (s *memStore) GetOne(table string, key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return nil, nil
	}
	item, found := t.tree.Get(memEntry{k: key})
	if !found {
		return nil, nil
	}
	return common.Copy(item.v), nil
}

func (s *memStore) Rollback() {}

func (s *memStore) ReadSequence(bucket string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequences[bucket], nil
}

func (s *memStore) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return nil
	}
	iter := t.tree.Iter()
	defer iter.Release()

	var started bool
	if fromPrefix != nil {
		started = iter.Seek(memEntry{k: fromPrefix})
	} else {
		started = iter.First()
	}
	for started {
		item := iter.Item()
		if err := walker(item.k, item.v); err != nil {
			return err
		}
		started = iter.Next()
	}
	return nil
}

func (s *memStore) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return nil
	}
	iter := t.tree.Iter()
	defer iter.Release()

	if !iter.Seek(memEntry{k: prefix}) {
		return nil
	}
	for amount > 0 {
		item := iter.Item()
		if err := walker(item.k, item.v); err != nil {
			return err
		}
		amount--
		if !iter.Next() {
			break
		}
	}
	return nil
}

// --- kv.Putter ---

func (s *memStore) Put(table string, k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.getOrCreateTable(table)
	t.tree.Set(memEntry{k: common.Copy(k), v: common.Copy(v)})
	// Keep sequences map in sync when writing to the Sequence table.
	if table == kv.Sequence && len(v) >= 8 {
		s.sequences[string(k)] = binary.BigEndian.Uint64(v)
	}
	return nil
}

func (s *memStore) Delete(table string, k []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tables[table]
	if !ok {
		return nil
	}
	if !t.dupSort {
		t.tree.Delete(memEntry{k: k})
		return nil
	}
	// DupSort: delete all entries with this key.
	var toDelete []memEntry
	iter := t.tree.Iter()
	if iter.Seek(memEntry{k: k}) {
		for {
			item := iter.Item()
			if !bytes.Equal(item.k, k) {
				break
			}
			toDelete = append(toDelete, item)
			if !iter.Next() {
				break
			}
		}
	}
	iter.Release()
	for _, item := range toDelete {
		t.tree.Delete(item)
	}
	return nil
}

func (s *memStore) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.sequences[bucket]
	s.sequences[bucket] = current + amount
	// Keep the Sequence table in sync for Flush.
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, current+amount)
	t := s.getOrCreateTable(kv.Sequence)
	t.tree.Set(memEntry{k: common.Copy([]byte(bucket)), v: v})
	return current, nil
}

func (s *memStore) ResetSequence(bucket string, newValue uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequences[bucket] = newValue
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, newValue)
	t := s.getOrCreateTable(kv.Sequence)
	t.tree.Set(memEntry{k: common.Copy([]byte(bucket)), v: v})
	return nil
}

// Append delegates to Put. Unlike MDBX's Append (which requires keys in
// ascending order and errors on out-of-order inserts), the in-memory btree
// accepts keys in any order. Callers that rely on Append-order enforcement
// must validate ordering themselves.
func (s *memStore) Append(table string, k, v []byte) error {
	return s.Put(table, k, v)
}

func (s *memStore) AppendDup(table string, k, v []byte) error {
	return s.Put(table, k, v)
}

func (s *memStore) CollectMetrics() {}

// --- kv.BucketMigrator ---

func (s *memStore) ListTables() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.tables))
	for name, t := range s.tables {
		if t.tree.Len() > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *memStore) DropTable(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, table)
	return nil
}

func (s *memStore) CreateTable(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getOrCreateTable(table)
	return nil
}

func (s *memStore) ExistsTable(table string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.tables[table]
	return ok, nil
}

func (s *memStore) ClearTable(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tables[table]; ok {
		t.tree.Clear()
	}
	return nil
}

// --- kv.Tx ---

func (s *memStore) Cursor(table string) (kv.Cursor, error) {
	return s.newCursor(table), nil
}

func (s *memStore) CursorDupSort(table string) (kv.CursorDupSort, error) {
	return s.newCursor(table), nil
}

func (s *memStore) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return &memStoreSliceIter{}, nil
	}
	return s.collectRange(t, fromPrefix, toPrefix, bool(asc), int64(limit)), nil
}

func (s *memStore) Prefix(table string, prefix []byte) (stream.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return s.Range(table, prefix, nil, order.Asc, kv.Unlim)
	}
	return s.Range(table, prefix, nextPrefix, order.Asc, kv.Unlim)
}

func (s *memStore) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return &memStoreSliceIter{}, nil
	}
	return s.collectRangeDupSort(t, key, fromPrefix, toPrefix, bool(asc), int64(limit)), nil
}

func (s *memStore) BucketSize(table string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[table]
	if !ok {
		return 0, nil
	}
	return uint64(t.tree.Len()) * 64, nil
}

func (s *memStore) Count(bucket string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[bucket]
	if !ok {
		return 0, nil
	}
	return uint64(t.tree.Len()), nil
}

func (s *memStore) ViewID() uint64          { return 0 }
func (s *memStore) CHandle() unsafe.Pointer { return nil }
func (s *memStore) Apply(_ context.Context, f func(tx kv.Tx) error) error {
	return f(s)
}

// --- kv.RwTx ---

func (s *memStore) RwCursor(table string) (kv.RwCursor, error) {
	return s.newCursor(table), nil
}

func (s *memStore) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	return s.newCursor(table), nil
}

func (s *memStore) Commit() error { return nil }

func (s *memStore) ApplyRw(_ context.Context, f func(tx kv.RwTx) error) error {
	return f(s)
}

// Close is a no-op for the in-memory store.
func (s *memStore) Close() {}

// --- memStoreDB implements kv.RwDB for the temporaldb wrapper ---

type memStoreDB struct {
	store *memStore
}

var _ kv.RwDB = (*memStoreDB)(nil)

func (db *memStoreDB) BeginRo(_ context.Context) (kv.Tx, error)         { return db.store, nil }
func (db *memStoreDB) BeginRw(_ context.Context) (kv.RwTx, error)       { return db.store, nil }
func (db *memStoreDB) BeginRwNosync(_ context.Context) (kv.RwTx, error) { return db.store, nil }
func (db *memStoreDB) Update(_ context.Context, f func(tx kv.RwTx) error) error {
	return f(db.store)
}
func (db *memStoreDB) UpdateNosync(_ context.Context, f func(tx kv.RwTx) error) error {
	return f(db.store)
}
func (db *memStoreDB) View(_ context.Context, f func(tx kv.Tx) error) error {
	return f(db.store)
}
func (db *memStoreDB) Close()                      {}
func (db *memStoreDB) AllTables() kv.TableCfg      { return kv.ChaindataTablesCfg }
func (db *memStoreDB) PageSize() datasize.ByteSize { return 4096 }
func (db *memStoreDB) ReadOnly() bool              { return false }
func (db *memStoreDB) CHandle() unsafe.Pointer     { return nil }
func (db *memStoreDB) Path() string                { return "<mem>" }

// --- Cursor ---
//
// memStoreCursor does NOT hold a persistent btree iterator. Each cursor
// method acquires the store mutex, creates a temporary iterator, seeks to
// the tracked position, performs the operation, saves the new position, and
// releases the iterator. This avoids holding btree iterators across calls,
// which is required for thread safety (btree iterators with NoLocks:true
// are not safe for concurrent modification).

func (s *memStore) newCursor(table string) *memStoreCursor {
	s.mu.Lock()
	t := s.getOrCreateTable(table)
	s.mu.Unlock()
	return &memStoreCursor{
		store:  s,
		table:  t,
		bucket: table,
	}
}

var _ kv.RwCursorDupSort = (*memStoreCursor)(nil)

type memStoreCursor struct {
	store   *memStore
	table   *memTable
	bucket  string
	valid   bool
	current memEntry
}

func (c *memStoreCursor) First() ([]byte, []byte, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	c.valid = iter.First()
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) Last() ([]byte, []byte, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	c.valid = iter.Last()
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) Seek(seek []byte) ([]byte, []byte, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	c.valid = iter.Seek(memEntry{k: seek})
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	k, v, err := c.Seek(key)
	if err != nil {
		return nil, nil, err
	}
	if k != nil && bytes.Equal(k, key) {
		return k, v, nil
	}
	return nil, nil, nil
}

func (c *memStoreCursor) Next() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	// Re-seek to current position, then advance.
	if !c.seekToCurrent(&iter) {
		c.valid = false
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.valid = iter.Next()
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) Prev() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	if !c.seekToCurrent(&iter) {
		c.valid = false
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.valid = iter.Prev()
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) Current() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) NextDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	currentKey := c.current.k
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	if !c.seekToCurrent(&iter) {
		c.valid = false
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.valid = iter.Next()
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	if !bytes.Equal(c.current.k, currentKey) {
		return nil, nil, nil
	}
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) NextNoDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	if !c.seekToCurrent(&iter) {
		c.valid = false
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	currentKey := c.current.k
	for {
		c.valid = iter.Next()
		if !c.valid {
			c.current = memEntry{}
			iter.Release()
			return nil, nil, nil
		}
		c.current = iter.Item()
		if !bytes.Equal(c.current.k, currentKey) {
			iter.Release()
			return common.Copy(c.current.k), common.Copy(c.current.v), nil
		}
	}
}

func (c *memStoreCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	c.valid = iter.Seek(memEntry{k: key, v: value})
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	if !bytes.Equal(c.current.k, key) {
		return nil, nil
	}
	return common.Copy(c.current.v), nil
}

func (c *memStoreCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	iter := c.table.tree.Iter()
	c.valid = iter.Seek(memEntry{k: key, v: value})
	if !c.valid {
		c.current = memEntry{}
		iter.Release()
		return nil, nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	if !bytes.Equal(c.current.k, key) || !bytes.Equal(c.current.v, value) {
		return nil, nil, nil
	}
	return common.Copy(c.current.k), common.Copy(c.current.v), nil
}

func (c *memStoreCursor) FirstDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	currentKey := common.Copy(c.current.k)
	iter := c.table.tree.Iter()
	c.valid = iter.Seek(memEntry{k: currentKey})
	if !c.valid || !bytes.Equal(iter.Item().k, currentKey) {
		iter.Release()
		return nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.v), nil
}

func (c *memStoreCursor) LastDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	currentKey := common.Copy(c.current.k)
	iter := c.table.tree.Iter()
	nextKey, ok := kv.NextSubtree(currentKey)
	if ok {
		found := iter.Seek(memEntry{k: nextKey})
		if found {
			c.valid = iter.Prev()
		} else {
			c.valid = iter.Last()
		}
	} else {
		c.valid = iter.Last()
	}
	if !c.valid || !bytes.Equal(iter.Item().k, currentKey) {
		iter.Release()
		return nil, nil
	}
	c.current = iter.Item()
	iter.Release()
	return common.Copy(c.current.v), nil
}

func (c *memStoreCursor) PrevDup() ([]byte, []byte, error)   { panic("PrevDup not implemented") }
func (c *memStoreCursor) PrevNoDup() ([]byte, []byte, error) { panic("PrevNoDup not implemented") }
func (c *memStoreCursor) CountDuplicates() (uint64, error)   { panic("CountDuplicates not implemented") }

// Write methods — acquire write lock.

func (c *memStoreCursor) Put(k, v []byte) error       { return c.store.Put(c.bucket, k, v) }
func (c *memStoreCursor) Append(k, v []byte) error    { return c.store.Put(c.bucket, k, v) }
func (c *memStoreCursor) AppendDup(k, v []byte) error { return c.store.Put(c.bucket, k, v) }
func (c *memStoreCursor) Delete(k []byte) error       { return c.store.Delete(c.bucket, k) }

func (c *memStoreCursor) DeleteCurrent() error {
	if !c.valid {
		return nil
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.table.tree.Delete(c.current)
	return nil
}

func (c *memStoreCursor) DeleteExact(k, v []byte) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.table.tree.Delete(memEntry{k: k, v: v})
	return nil
}

func (c *memStoreCursor) DeleteCurrentDuplicates() error {
	if !c.valid {
		return nil
	}
	return c.store.Delete(c.bucket, c.current.k)
}

func (c *memStoreCursor) PutNoDupData(key, value []byte) error { panic("PutNoDupData not implemented") }

func (c *memStoreCursor) Close() {}

// seekToCurrent positions iter at c.current. Must be called with mu held.
// For dupSort tables, seeks by (key, value); for normal tables, seeks by key.
// Takes a pointer because IterG methods have pointer receivers.
func (c *memStoreCursor) seekToCurrent(iter *btree2.IterG[memEntry]) bool {
	if c.table.dupSort {
		if !iter.Seek(memEntry{k: c.current.k, v: c.current.v}) {
			return false
		}
		item := iter.Item()
		return bytes.Equal(item.k, c.current.k) && bytes.Equal(item.v, c.current.v)
	}
	if !iter.Seek(memEntry{k: c.current.k}) {
		return false
	}
	return bytes.Equal(iter.Item().k, c.current.k)
}

// --- Range helpers: collect into slices for thread safety ---

// collectRange collects entries from the btree into a slice. Must be called with mu held.
func (s *memStore) collectRange(t *memTable, fromPrefix, toPrefix []byte, asc bool, limit int64) *memStoreSliceIter {
	var entries []memEntry
	iter := t.tree.Iter()
	defer iter.Release()

	if asc {
		var valid bool
		if fromPrefix != nil {
			valid = iter.Seek(memEntry{k: fromPrefix})
		} else {
			valid = iter.First()
		}
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if toPrefix != nil && bytes.Compare(item.k, toPrefix) >= 0 {
				break
			}
			entries = append(entries, memEntry{k: common.Copy(item.k), v: common.Copy(item.v)})
			valid = iter.Next()
		}
	} else {
		var valid bool
		if toPrefix != nil {
			found := iter.Seek(memEntry{k: toPrefix})
			if found {
				if bytes.Compare(iter.Item().k, toPrefix) >= 0 {
					valid = iter.Prev()
				} else {
					valid = true
				}
			} else {
				valid = iter.Last()
			}
		} else {
			valid = iter.Last()
		}
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if fromPrefix != nil && bytes.Compare(item.k, fromPrefix) < 0 {
				break
			}
			entries = append(entries, memEntry{k: common.Copy(item.k), v: common.Copy(item.v)})
			valid = iter.Prev()
		}
	}

	return &memStoreSliceIter{entries: entries}
}

// collectRangeDupSort collects dupsort entries for a specific key into a slice. Must be called with mu held.
func (s *memStore) collectRangeDupSort(t *memTable, key []byte, fromPrefix, toPrefix []byte, asc bool, limit int64) *memStoreSliceIter {
	var entries []memEntry
	iter := t.tree.Iter()
	defer iter.Release()

	if asc {
		valid := iter.Seek(memEntry{k: key, v: fromPrefix})
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if !bytes.Equal(item.k, key) {
				break
			}
			if toPrefix != nil && bytes.Compare(item.v, toPrefix) >= 0 {
				break
			}
			entries = append(entries, memEntry{k: common.Copy(item.k), v: common.Copy(item.v)})
			valid = iter.Next()
		}
	} else {
		var valid bool
		if toPrefix != nil {
			found := iter.Seek(memEntry{k: key, v: toPrefix})
			if found {
				item := iter.Item()
				if !bytes.Equal(item.k, key) || bytes.Compare(item.v, toPrefix) >= 0 {
					valid = iter.Prev()
				} else {
					valid = true
				}
			} else {
				valid = iter.Last()
			}
		} else {
			nextKey, ok := kv.NextSubtree(key)
			if ok {
				found := iter.Seek(memEntry{k: nextKey})
				if found {
					valid = iter.Prev()
				} else {
					valid = iter.Last()
				}
			} else {
				valid = iter.Last()
			}
		}
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if !bytes.Equal(item.k, key) {
				break
			}
			if fromPrefix != nil && bytes.Compare(item.v, fromPrefix) < 0 {
				break
			}
			entries = append(entries, memEntry{k: common.Copy(item.k), v: common.Copy(item.v)})
			valid = iter.Prev()
		}
	}

	return &memStoreSliceIter{entries: entries}
}

// --- Slice-based iterator (thread-safe: data is copied at creation time) ---

type memStoreSliceIter struct {
	entries []memEntry
	pos     int
}

func (si *memStoreSliceIter) HasNext() bool {
	return si.pos < len(si.entries)
}

func (si *memStoreSliceIter) Next() ([]byte, []byte, error) {
	if si.pos >= len(si.entries) {
		return nil, nil, nil
	}
	e := si.entries[si.pos]
	si.pos++
	return e.k, e.v, nil
}

func (si *memStoreSliceIter) Close() {}
