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
	"maps"
	"sort"
	"sync"
	"unsafe"

	"github.com/c2h5oh/datasize"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// Compile-time check that memStore implements kv.RwTx.
var _ kv.RwTx = (*memStore)(nil)

// memEntry is a key-value pair stored in the btree, or a tombstone that
// hides the row of a lower layer.
type memEntry struct {
	k       []byte
	v       []byte
	deleted bool
}

// keyWide reports whether e is a DupSort tombstone for all values of its key:
// a value tombstone always has a non-nil value.
func (e memEntry) keyWide() bool { return e.deleted && e.v == nil }

// memTable is a single table backed by a btree.
type memTable struct {
	tree    *btree2.BTreeG[memEntry]
	dupSort bool
	cleared bool
}

func newMemTable(dupSort bool) *memTable {
	var less func(a, b memEntry) bool
	if dupSort {
		less = func(a, b memEntry) bool {
			c := bytes.Compare(a.k, b.k)
			if c != 0 {
				return c < 0
			}
			if aw, bw := a.keyWide(), b.keyWide(); aw || bw {
				return aw && !bw
			}
			return bytes.Compare(a.v, b.v) < 0
		}
	} else {
		less = func(a, b memEntry) bool {
			return bytes.Compare(a.k, b.k) < 0
		}
	}
	return &memTable{
		// NoLocks: true because each store's mu guards its trees, and snapshot copies share nodes copy-on-write.
		tree:    btree2.NewBTreeGOptions(less, btree2.Options{NoLocks: true}),
		dupSort: dupSort,
	}
}

// memStore is a pure Go in-memory key-value store that replaces the MDBX
// in-memory database previously used by MemoryMutation. It has no OS-thread
// affinity (no runtime.LockOSThread), so it can be safely used across
// goroutine migrations between Engine API calls.
//
// Thread safety: all access to a live store is protected by mu. Btrees use
// NoLocks:true because the store-level mutex serializes all operations. A
// snapshot owns copies of the trees and is never written, so it has a nil mu
// and its reads take no lock.
type memStore struct {
	mu        *sync.RWMutex
	tables    map[string]*memTable
	sequences map[string]uint64
}

func newMemStore() *memStore {
	return &memStore{
		mu:        &sync.RWMutex{},
		tables:    make(map[string]*memTable),
		sequences: make(map[string]uint64),
	}
}

func (s *memStore) rlock() {
	if s.mu != nil {
		s.mu.RLock()
	}
}

func (s *memStore) runlock() {
	if s.mu != nil {
		s.mu.RUnlock()
	}
}

// lock panics on a snapshot: its readers take no lock, so a write would race them.
func (s *memStore) lock() {
	if s.mu == nil {
		panic("membatchwithdb: write to a read-only overlay snapshot")
	}
	s.mu.Lock()
}

func (s *memStore) unlock() { s.mu.Unlock() }

func memTableIsDupSort(table string) bool {
	config, ok := kv.ChaindataTablesCfg[table]
	if !ok {
		return false
	}
	return config.Flags&kv.DupSort != 0
}

// snapshot returns a point-in-time copy that later writes to s do not change.
func (s *memStore) snapshot() *memStore {
	if s.mu == nil {
		return s
	}
	s.lock()
	defer s.unlock()
	snap := &memStore{
		tables:    make(map[string]*memTable, len(s.tables)),
		sequences: maps.Clone(s.sequences),
	}
	for name, t := range s.tables {
		snap.tables[name] = &memTable{tree: t.tree.Copy(), dupSort: t.dupSort, cleared: t.cleared}
	}
	return snap
}

// hides reports whether the lower-layer row (k, v) is hidden by ClearTable or
// a tombstone; a nil v checks only the key. Must be called with mu held.
func (t *memTable) hides(k, v []byte) bool {
	if t.cleared {
		return true
	}
	if !t.dupSort {
		e, found := t.tree.Get(memEntry{k: k})
		return found && e.deleted
	}
	if _, found := t.tree.Get(memEntry{k: k, deleted: true}); found {
		return true
	}
	if v == nil {
		return false
	}
	e, found := t.tree.Get(memEntry{k: k, v: v})
	return found && e.deleted
}

// firstLive returns the live entry of key: on a DupSort table, its smallest value.
func (t *memTable) firstLive(key []byte) (e memEntry, found bool) {
	if !t.dupSort {
		e, found = t.tree.Get(memEntry{k: key})
		return e, found && !e.deleted
	}
	t.tree.Ascend(memEntry{k: key, deleted: true}, func(item memEntry) bool {
		if !bytes.Equal(item.k, key) {
			return false
		}
		e, found = item, !item.deleted
		return !found
	})
	return e, found
}

// isHidden is memTable.hides for a table name.
func (s *memStore) isHidden(table string, k, v []byte) bool {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	return ok && t.hides(k, v)
}

// isCleared reports whether ClearTable hid the lower-layer rows of table.
func (s *memStore) isCleared(table string) bool {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	return ok && t.cleared
}

// tombstones returns the ClearTable flag and the tombstones of table; a row
// with a nil value stands for the whole key.
func (s *memStore) tombstones(table string) (cleared bool, rows []entry) {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return false, nil
	}
	t.tree.Scan(func(e memEntry) bool {
		if e.deleted {
			rows = append(rows, entry{k: e.k, v: e.v})
		}
		return true
	})
	return t.cleared, rows
}

// hasTombstone reports whether table holds a tombstone with fromKey <= k < toKey;
// nil bounds are open.
func (s *memStore) hasTombstone(table string, fromKey, toKey []byte) bool {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return false
	}
	iter := t.tree.Iter()
	defer iter.Release()
	for valid := iter.Seek(memEntry{k: fromKey, deleted: true}); valid; valid = iter.Next() {
		item := iter.Item()
		if toKey != nil && bytes.Compare(item.k, toKey) >= 0 {
			return false
		}
		if item.deleted {
			return true
		}
	}
	return false
}

// deleteExact replaces the (k, v) pair of a DupSort table, or the key of a
// plain table, with a tombstone. Must be called with mu held.
func (t *memTable) deleteExact(k, v []byte) {
	e := memEntry{k: bytes.Clone(k), deleted: true}
	if t.dupSort {
		e.v = append([]byte{}, v...)
	}
	t.tree.Set(e)
}

// skipDeleted moves a positioned iterator past tombstones.
func skipDeleted(iter *btree2.IterG[memEntry], valid, forward bool) bool {
	for valid && iter.Item().deleted {
		if forward {
			valid = iter.Next()
		} else {
			valid = iter.Prev()
		}
	}
	return valid
}

// skipDeletedDup moves a positioned iterator past the tombstones of key only.
func skipDeletedDup(iter *btree2.IterG[memEntry], valid bool, key []byte) bool {
	for valid && iter.Item().deleted && bytes.Equal(iter.Item().k, key) {
		valid = iter.Next()
	}
	return valid
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
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return false, nil
	}
	_, found := t.firstLive(key)
	return found, nil
}

func (s *memStore) GetOne(table string, key []byte) ([]byte, error) {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return nil, nil
	}
	item, found := t.firstLive(key)
	if !found {
		return nil, nil
	}
	return append([]byte{}, item.v...), nil // non-nil even for an empty value, so callers do not fall back to the db
}

func (s *memStore) Rollback() {}

func (s *memStore) ReadSequence(bucket string) (uint64, error) {
	s.rlock()
	defer s.runlock()
	return s.sequences[bucket], nil
}

func (s *memStore) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	s.rlock()
	defer s.runlock()
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
	for started = skipDeleted(&iter, started, true); started; started = skipDeleted(&iter, iter.Next(), true) {
		item := iter.Item()
		if err := walker(item.k, item.v); err != nil {
			return err
		}
	}
	return nil
}

func (s *memStore) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return nil
	}
	iter := t.tree.Iter()
	defer iter.Release()

	for valid := skipDeleted(&iter, iter.Seek(memEntry{k: prefix}), true); valid && amount > 0; valid = skipDeleted(&iter, iter.Next(), true) {
		item := iter.Item()
		if err := walker(item.k, item.v); err != nil {
			return err
		}
		amount--
	}
	return nil
}

// --- kv.Putter ---

func (s *memStore) Put(table string, k, v []byte) error {
	s.lock()
	defer s.unlock()
	t := s.getOrCreateTable(table)
	t.tree.Set(memEntry{k: bytes.Clone(k), v: bytes.Clone(v)})
	// Keep sequences map in sync when writing to the Sequence table.
	if table == kv.Sequence && len(v) >= 8 {
		s.sequences[string(k)] = binary.BigEndian.Uint64(v)
	}
	return nil
}

func (s *memStore) Delete(table string, k []byte) error {
	s.lock()
	defer s.unlock()
	t := s.getOrCreateTable(table)
	if !t.dupSort {
		t.tree.Set(memEntry{k: bytes.Clone(k), deleted: true})
		return nil
	}
	// DupSort: replace all entries with this key by one key-wide tombstone.
	var toDelete []memEntry
	iter := t.tree.Iter()
	if iter.Seek(memEntry{k: k, deleted: true}) {
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
	t.tree.Set(memEntry{k: bytes.Clone(k), deleted: true})
	return nil
}

func (s *memStore) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	s.lock()
	defer s.unlock()
	current := s.sequences[bucket]
	s.sequences[bucket] = current + amount
	// Keep the Sequence table in sync for Flush.
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, current+amount)
	t := s.getOrCreateTable(kv.Sequence)
	t.tree.Set(memEntry{k: bytes.Clone([]byte(bucket)), v: v})
	return current, nil
}

func (s *memStore) ResetSequence(bucket string, newValue uint64) error {
	s.lock()
	defer s.unlock()
	s.sequences[bucket] = newValue
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, newValue)
	t := s.getOrCreateTable(kv.Sequence)
	t.tree.Set(memEntry{k: bytes.Clone([]byte(bucket)), v: v})
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
	s.rlock()
	defer s.runlock()
	names := make([]string, 0, len(s.tables))
	for name, t := range s.tables {
		if t.tree.Len() > 0 || t.cleared {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *memStore) DropTable(table string) error {
	s.lock()
	defer s.unlock()
	delete(s.tables, table)
	return nil
}

func (s *memStore) CreateTable(table string) error {
	s.lock()
	defer s.unlock()
	s.getOrCreateTable(table)
	return nil
}

func (s *memStore) ExistsTable(table string) (bool, error) {
	s.rlock()
	defer s.runlock()
	_, ok := s.tables[table]
	return ok, nil
}

func (s *memStore) ClearTable(table string) error {
	s.lock()
	defer s.unlock()
	t := s.getOrCreateTable(table)
	t.cleared = true
	t.tree.Clear()
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
	s.rlock()
	defer s.runlock()
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
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return &memStoreSliceIter{}, nil
	}
	return s.collectRangeDupSort(t, key, fromPrefix, toPrefix, bool(asc), int64(limit)), nil
}

func (s *memStore) BucketSize(table string) (uint64, error) {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[table]
	if !ok {
		return 0, nil
	}
	return uint64(t.tree.Len()) * 64, nil
}

func (s *memStore) Count(bucket string) (uint64, error) {
	s.rlock()
	defer s.runlock()
	t, ok := s.tables[bucket]
	if !ok {
		return 0, nil
	}
	var n uint64
	t.tree.Scan(func(e memEntry) bool {
		if !e.deleted {
			n++
		}
		return true
	})
	return n, nil
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
	var t *memTable
	if s.mu == nil {
		// A snapshot map is read concurrently, so a missing table gets a detached empty one.
		if t = s.tables[table]; t == nil {
			t = newMemTable(memTableIsDupSort(table))
		}
	} else {
		s.lock()
		t = s.getOrCreateTable(table)
		s.unlock()
	}
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

// land records the entry the iterator stopped at, or an exhausted cursor.
func (c *memStoreCursor) land(iter *btree2.IterG[memEntry], valid bool) ([]byte, []byte, error) {
	c.valid = valid
	if !valid {
		c.current = memEntry{}
		return nil, nil, nil
	}
	c.current = iter.Item()
	return bytes.Clone(c.current.k), bytes.Clone(c.current.v), nil
}

func (c *memStoreCursor) First() ([]byte, []byte, error) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.land(&iter, skipDeleted(&iter, iter.First(), true))
}

func (c *memStoreCursor) Last() ([]byte, []byte, error) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.land(&iter, skipDeleted(&iter, iter.Last(), false))
}

func (c *memStoreCursor) Seek(seek []byte) ([]byte, []byte, error) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.land(&iter, skipDeleted(&iter, iter.Seek(memEntry{k: seek}), true))
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
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.land(&iter, skipDeleted(&iter, c.seekToNext(&iter), true))
}

func (c *memStoreCursor) Prev() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	var valid bool
	if iter.Seek(c.current) {
		valid = iter.Prev()
	} else {
		valid = iter.Last()
	}
	return c.land(&iter, skipDeleted(&iter, valid, false))
}

func (c *memStoreCursor) Current() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !skipDeleted(&iter, iter.Seek(c.current), true) {
		return nil, nil, nil
	}
	item := iter.Item()
	return bytes.Clone(item.k), bytes.Clone(item.v), nil
}

func (c *memStoreCursor) NextDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !skipDeletedDup(&iter, c.seekToNext(&iter), c.current.k) || !bytes.Equal(iter.Item().k, c.current.k) {
		return nil, nil, nil
	}
	c.current = iter.Item()
	return bytes.Clone(c.current.k), bytes.Clone(c.current.v), nil
}

func (c *memStoreCursor) NextNoDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	valid := skipDeleted(&iter, c.seekToNext(&iter), true)
	for valid && bytes.Equal(iter.Item().k, c.current.k) {
		valid = skipDeleted(&iter, iter.Next(), true)
	}
	return c.land(&iter, valid)
}

func (c *memStoreCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	k, v, _ := c.land(&iter, skipDeletedDup(&iter, iter.Seek(memEntry{k: key, v: value}), key))
	if !bytes.Equal(k, key) {
		return nil, nil
	}
	return v, nil
}

func (c *memStoreCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	k, v, _ := c.land(&iter, skipDeletedDup(&iter, iter.Seek(memEntry{k: key, v: value}), key))
	if !bytes.Equal(k, key) || !bytes.Equal(v, value) {
		return nil, nil, nil
	}
	return k, v, nil
}

func (c *memStoreCursor) FirstDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	currentKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	c.valid = skipDeleted(&iter, iter.Seek(memEntry{k: currentKey}), true)
	if !c.valid || !bytes.Equal(iter.Item().k, currentKey) {
		return nil, nil
	}
	c.current = iter.Item()
	return bytes.Clone(c.current.v), nil
}

func (c *memStoreCursor) LastDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	c.store.rlock()
	defer c.store.runlock()
	currentKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	nextKey, ok := kv.NextSubtree(currentKey)
	if ok && iter.Seek(memEntry{k: nextKey}) {
		c.valid = iter.Prev()
	} else {
		c.valid = iter.Last()
	}
	c.valid = skipDeleted(&iter, c.valid, false)
	if !c.valid || !bytes.Equal(iter.Item().k, currentKey) {
		return nil, nil
	}
	c.current = iter.Item()
	return bytes.Clone(c.current.v), nil
}

// firstRaw, seekRaw, nextRaw and nextNoDupRaw are First, Seek, Next and
// NextNoDup that can stop at a tombstone and report it, so a caller decides how
// many tombstones to walk.
func (c *memStoreCursor) firstRaw() ([]byte, []byte, bool) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.landRaw(&iter, iter.First())
}

func (c *memStoreCursor) seekRaw(seek []byte) ([]byte, []byte, bool) {
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.landRaw(&iter, iter.Seek(memEntry{k: seek}))
}

func (c *memStoreCursor) nextRaw() ([]byte, []byte, bool) {
	if !c.valid {
		return nil, nil, false
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	return c.landRaw(&iter, c.seekToNext(&iter))
}

func (c *memStoreCursor) nextNoDupRaw() ([]byte, []byte, bool) {
	if !c.valid {
		return nil, nil, false
	}
	c.store.rlock()
	defer c.store.runlock()
	iter := c.table.tree.Iter()
	defer iter.Release()
	valid := c.seekToNext(&iter)
	for valid && bytes.Equal(iter.Item().k, c.current.k) {
		valid = iter.Next()
	}
	return c.landRaw(&iter, valid)
}

func (c *memStoreCursor) landRaw(iter *btree2.IterG[memEntry], valid bool) ([]byte, []byte, bool) {
	k, v, _ := c.land(iter, valid)
	return k, v, c.current.deleted
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
	c.store.lock()
	defer c.store.unlock()
	iter := c.table.tree.Iter()
	valid := skipDeleted(&iter, iter.Seek(c.current), true)
	if valid {
		c.current = iter.Item()
	}
	iter.Release()
	if valid {
		c.table.deleteExact(c.current.k, c.current.v)
	}
	return nil
}

func (c *memStoreCursor) DeleteExact(k, v []byte) error {
	c.store.lock()
	defer c.store.unlock()
	c.table.deleteExact(k, v)
	return nil
}

func (c *memStoreCursor) DeleteCurrentDuplicates() error {
	if !c.valid {
		return nil
	}
	return c.store.Delete(c.bucket, c.current.k)
}

func (c *memStoreCursor) PutNoDupData(key, value []byte) error { panic("PutNoDupData not implemented") }

func (c *memStoreCursor) PutCurrent(key, value []byte) error { panic("PutCurrent not implemented") }

func (c *memStoreCursor) Close() {}

func (c *memStoreCursor) seekToNext(iter *btree2.IterG[memEntry]) bool {
	if !iter.Seek(c.current) {
		return false
	}
	if c.table.tree.Less(c.current, iter.Item()) {
		return true
	}
	return iter.Next()
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
			if item.deleted {
				valid = iter.Next()
				continue
			}
			entries = append(entries, memEntry{k: bytes.Clone(item.k), v: bytes.Clone(item.v)})
			valid = iter.Next()
		}
	} else { // like MDBX: from the last key with prefix fromPrefix down to toPrefix (exclusive)
		valid := iter.Last()
		if nextK, ok := kv.NextSubtree(fromPrefix); fromPrefix != nil && ok {
			valid = seekedPrev(&iter, iter.Seek(memEntry{k: nextK, deleted: true}))
		}
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if toPrefix != nil && bytes.Compare(item.k, toPrefix) <= 0 {
				break
			}
			if item.deleted {
				valid = iter.Prev()
				continue
			}
			entries = append(entries, memEntry{k: bytes.Clone(item.k), v: bytes.Clone(item.v)})
			valid = iter.Prev()
		}
	}

	return &memStoreSliceIter{entries: entries}
}

// seekedPrev steps a Seek result back to the entry before it, or to the last entry when the Seek found none.
func seekedPrev(iter *btree2.IterG[memEntry], found bool) bool {
	if found {
		return iter.Prev()
	}
	return iter.Last()
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
			if toPrefix != nil && !item.keyWide() && bytes.Compare(item.v, toPrefix) >= 0 {
				break
			}
			if item.deleted {
				valid = iter.Next()
				continue
			}
			entries = append(entries, memEntry{k: bytes.Clone(item.k), v: bytes.Clone(item.v)})
			valid = iter.Next()
		}
	} else { // like MDBX: from the last value with prefix fromPrefix down to toPrefix (exclusive)
		valid := iter.Last()
		if nextV, ok := kv.NextSubtree(fromPrefix); fromPrefix != nil && ok {
			valid = seekedPrev(&iter, iter.Seek(memEntry{k: key, v: nextV}))
		} else if nextKey, ok := kv.NextSubtree(key); ok {
			valid = seekedPrev(&iter, iter.Seek(memEntry{k: nextKey, deleted: true}))
		}
		for valid && (limit < 0 || int64(len(entries)) < limit) {
			item := iter.Item()
			if !bytes.Equal(item.k, key) {
				break
			}
			if toPrefix != nil && !item.keyWide() && bytes.Compare(item.v, toPrefix) <= 0 {
				break
			}
			if item.deleted {
				valid = iter.Prev()
				continue
			}
			entries = append(entries, memEntry{k: bytes.Clone(item.k), v: bytes.Clone(item.v)})
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
