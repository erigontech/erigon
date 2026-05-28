// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package memstoredb

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"unsafe"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// tx is both a kv.Tx (when rw==false) and a kv.RwTx (when rw==true).
// Single-goroutine semantics, matching MDBX (no internal mutex). Use a fresh
// tx per goroutine.
type tx struct {
	db     *DB
	rw     bool
	closed bool
	// tables is master's tables map.
	//   - RoTx: shared by reference (never mutated). Reads of missing tables
	//     are satisfied from roLocal.
	//   - RwTx: clone-on-first-write. tables starts as a maps.Clone of master;
	//     the first mutation of a table COW-clones the btree into a tx-private
	//     copy recorded in privateTables.
	tables        map[string]*table
	privateTables map[string]struct{} // RwTx only
	roLocal       map[string]*table   // RoTx only: empty-stub tables
	sequences     map[string]uint64
}

// Compile-time checks.
var (
	_ kv.Tx   = (*tx)(nil)
	_ kv.RwTx = (*tx)(nil)
)

// getOrCreateTable returns a tx-private table that the caller may safely
// mutate. For RwTx, the first write to a master-shared table triggers a
// cheap COW copy (btree.Copy is O(1)); subsequent writes mutate that copy.
// For RoTx, returns master's table if present, else a per-tx empty stub —
// never mutates the master map (which RoTx shares by reference, not clone).
func (t *tx) getOrCreateTable(name string) *table {
	tab, ok := t.tables[name]
	if ok && !t.rw {
		return tab
	}
	if !t.rw {
		// RoTx + missing table: return a per-tx empty stub. Cursors over it
		// behave like an empty table. The stub lives in roLocal so we don't
		// mutate the master map.
		if t.roLocal == nil {
			t.roLocal = make(map[string]*table)
		}
		if s, has := t.roLocal[name]; has {
			return s
		}
		s := newTable(t.db.isDupSort(name))
		t.roLocal[name] = s
		return s
	}
	if !ok {
		tab = newTable(t.db.isDupSort(name))
		t.tables[name] = tab
		t.privateTables[name] = struct{}{}
		return tab
	}
	if _, private := t.privateTables[name]; !private {
		tab = tab.cloneShallow()
		t.tables[name] = tab
		t.privateTables[name] = struct{}{}
	}
	return tab
}

func (t *tx) lookupTable(name string) (*table, bool) {
	tab, ok := t.tables[name]
	return tab, ok
}

// --- Lifecycle ---

func (t *tx) Commit() error {
	if t.closed {
		return nil
	}
	t.closed = true
	if !t.rw {
		return nil
	}
	t.db.mu.Lock()
	t.db.tables = t.tables
	t.db.sequences = t.sequences
	t.db.mu.Unlock()
	t.db.writeMu.Unlock()
	return nil
}

func (t *tx) Rollback() {
	if t.closed {
		return
	}
	t.closed = true
	if t.rw {
		t.db.writeMu.Unlock()
	}
}

func (t *tx) ViewID() uint64          { return 0 }
func (t *tx) CHandle() unsafe.Pointer { return nil }

// --- kv.Getter (read methods) ---

func (t *tx) Has(table string, key []byte) (bool, error) {
	tab, ok := t.lookupTable(table)
	if !ok {
		return false, nil
	}
	if !tab.dupSort {
		_, found := tab.tree.Get(entry{k: key})
		return found, nil
	}
	// DupSort: comparator sorts by (k, v), so Get with v=nil never matches a
	// stored (k, real-v); seek to first entry with this key and check.
	iter := tab.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: key}) {
		return false, nil
	}
	return bytes.Equal(iter.Item().k, key), nil
}

func (t *tx) GetOne(table string, key []byte) ([]byte, error) {
	tab, ok := t.lookupTable(table)
	if !ok {
		return nil, nil
	}
	if !tab.dupSort {
		it, found := tab.tree.Get(entry{k: key})
		if !found {
			return nil, nil
		}
		return common.Copy(it.v), nil
	}
	iter := tab.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: key}) {
		return nil, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, key) {
		return nil, nil
	}
	return common.Copy(it.v), nil
}

func (t *tx) ReadSequence(bucket string) (uint64, error) {
	return t.sequences[bucket], nil
}

func (t *tx) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	tab, ok := t.lookupTable(table)
	if !ok {
		return nil
	}
	// Snapshot entries first so the walker may safely mutate the table during
	// iteration (MDBX cursors tolerate concurrent in-tx writes; the btree iter
	// does not).
	snap := snapshotFrom(tab, fromPrefix, nil)
	for _, e := range snap {
		if err := walker(e.k, e.v); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount == 0 {
		return nil
	}
	tab, ok := t.lookupTable(table)
	if !ok {
		return nil
	}
	snap := snapshotFrom(tab, prefix, &amount)
	for _, e := range snap {
		if err := walker(e.k, e.v); err != nil {
			return err
		}
	}
	return nil
}

// snapshotFrom returns a copy of entries starting from `from` (nil = beginning).
// If limit != nil, at most *limit entries are returned.
func snapshotFrom(tab *table, from []byte, limit *uint32) []entry {
	iter := tab.tree.Iter()
	defer iter.Release()
	var ok bool
	if from != nil {
		ok = iter.Seek(entry{k: from})
	} else {
		ok = iter.First()
	}
	var out []entry
	for ok {
		if limit != nil && uint32(len(out)) >= *limit {
			break
		}
		it := iter.Item()
		out = append(out, entry{k: common.Copy(it.k), v: common.Copy(it.v)})
		ok = iter.Next()
	}
	return out
}

// --- kv.Putter (write methods) ---

func (t *tx) Put(table string, k, v []byte) error {
	tab := t.getOrCreateTable(table)
	tab.tree.Set(entry{k: common.Copy(k), v: common.Copy(v)})
	if table == kv.Sequence && len(v) >= 8 {
		t.sequences[string(k)] = binary.BigEndian.Uint64(v)
	}
	return nil
}

func (t *tx) Delete(table string, k []byte) error {
	tab, ok := t.lookupTable(table)
	if !ok {
		return nil
	}
	if !tab.dupSort {
		tab.tree.Delete(entry{k: k})
		return nil
	}
	// DupSort: delete every duplicate of key k.
	var toDel []entry
	iter := tab.tree.Iter()
	if iter.Seek(entry{k: k}) {
		for {
			it := iter.Item()
			if !bytes.Equal(it.k, k) {
				break
			}
			toDel = append(toDel, it)
			if !iter.Next() {
				break
			}
		}
	}
	iter.Release()
	for _, it := range toDel {
		tab.tree.Delete(it)
	}
	return nil
}

func (t *tx) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	current := t.sequences[bucket]
	t.sequences[bucket] = current + amount
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, current+amount)
	tab := t.getOrCreateTable(kv.Sequence)
	tab.tree.Set(entry{k: common.Copy([]byte(bucket)), v: v})
	return current, nil
}

func (t *tx) ResetSequence(bucket string, newValue uint64) error {
	t.sequences[bucket] = newValue
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, newValue)
	tab := t.getOrCreateTable(kv.Sequence)
	tab.tree.Set(entry{k: common.Copy([]byte(bucket)), v: v})
	return nil
}

// Append delegates to Put. MDBX's Append requires ascending key order and
// errors on out-of-order inserts, but the in-tree memStore (used in
// production for SharedDomains.blockOverlay) does not enforce this and the
// caller code does its own ordering. We follow the same convention; if a
// test surfaces a real reliance on the error path we'll add it then.
func (t *tx) Append(table string, k, v []byte) error    { return t.Put(table, k, v) }
func (t *tx) AppendDup(table string, k, v []byte) error { return t.Put(table, k, v) }

func (t *tx) CollectMetrics() {}

// --- kv.BucketMigrator ---

func (t *tx) ListTables() ([]string, error) {
	names := make([]string, 0, len(t.tables))
	for name, tab := range t.tables {
		if tab.tree.Len() > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (t *tx) DropTable(table string) error {
	delete(t.tables, table)
	return nil
}

func (t *tx) CreateTable(table string) error {
	t.getOrCreateTable(table)
	return nil
}

func (t *tx) ExistsTable(table string) (bool, error) {
	_, ok := t.tables[table]
	return ok, nil
}

func (t *tx) ClearTable(table string) error {
	if tab, ok := t.tables[table]; ok {
		tab.tree.Clear()
	}
	return nil
}

// --- kv.Tx (cursor + bulk read methods) ---

func (t *tx) Cursor(table string) (kv.Cursor, error) {
	return t.newCursor(table), nil
}

func (t *tx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	return t.newCursor(table), nil
}

func (t *tx) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	tab, ok := t.lookupTable(table)
	if !ok {
		return &sliceIter{}, nil
	}
	return collectRange(tab, fromPrefix, toPrefix, bool(asc), int64(limit)), nil
}

func (t *tx) Prefix(table string, prefix []byte) (stream.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return t.Range(table, prefix, nil, order.Asc, kv.Unlim)
	}
	return t.Range(table, prefix, nextPrefix, order.Asc, kv.Unlim)
}

func (t *tx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	tab, ok := t.lookupTable(table)
	if !ok {
		return &sliceIter{}, nil
	}
	return collectRangeDupSort(tab, key, fromPrefix, toPrefix, bool(asc), int64(limit)), nil
}

func (t *tx) BucketSize(table string) (uint64, error) {
	tab, ok := t.lookupTable(table)
	if !ok {
		return 0, nil
	}
	return uint64(tab.tree.Len()) * 64, nil
}

func (t *tx) Count(bucket string) (uint64, error) {
	tab, ok := t.lookupTable(bucket)
	if !ok {
		return 0, nil
	}
	return uint64(tab.tree.Len()), nil
}

func (t *tx) Apply(_ context.Context, f func(tx kv.Tx) error) error {
	return f(t)
}

// --- kv.RwTx ---

func (t *tx) RwCursor(table string) (kv.RwCursor, error) {
	return t.newCursor(table), nil
}

func (t *tx) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	return t.newCursor(table), nil
}

func (t *tx) ApplyRw(_ context.Context, f func(tx kv.RwTx) error) error {
	return f(t)
}
