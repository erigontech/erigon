// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package hybridkv

import (
	"context"
	"unsafe"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// Tx is the read-only hybrid transaction. It holds one underlying RoTx per
// backend; every per-table operation dispatches by name.
type Tx struct {
	db    *DB
	durTx kv.Tx
	memTx kv.Tx
}

// RwTx is the read-write hybrid transaction. Embeds Tx for read methods; the
// write-side handles also keep typed RwTx references to avoid repeated
// interface assertions.
type RwTx struct {
	Tx
	durRw kv.RwTx
	memRw kv.RwTx
}

// Compile-time checks.
var (
	_ kv.Tx   = (*Tx)(nil)
	_ kv.RwTx = (*RwTx)(nil)
)

// pick returns the read-only tx that owns this table.
func (t *Tx) pick(table string) kv.Tx {
	if t.db.isMem(table) {
		return t.memTx
	}
	return t.durTx
}

// pickRw returns the read-write tx that owns this table. Only valid on RwTx.
func (t *RwTx) pickRw(table string) kv.RwTx {
	if t.db.isMem(table) {
		return t.memRw
	}
	return t.durRw
}

// ---- kv.Getter ----

func (t *Tx) GetOne(table string, key []byte) ([]byte, error) {
	return t.pick(table).GetOne(table, key)
}

func (t *Tx) Has(table string, key []byte) (bool, error) {
	return t.pick(table).Has(table, key)
}

func (t *Tx) ReadSequence(table string) (uint64, error) {
	return t.pick(table).ReadSequence(table)
}

func (t *Tx) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return t.pick(table).ForEach(table, fromPrefix, walker)
}

func (t *Tx) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return t.pick(table).ForAmount(table, prefix, amount, walker)
}

// ---- kv.Tx (cursors + ranges) ----

func (t *Tx) Cursor(table string) (kv.Cursor, error) {
	return t.pick(table).Cursor(table)
}

func (t *Tx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	return t.pick(table).CursorDupSort(table)
}

func (t *Tx) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	return t.pick(table).Range(table, fromPrefix, toPrefix, asc, limit)
}

func (t *Tx) Prefix(table string, prefix []byte) (stream.KV, error) {
	return t.pick(table).Prefix(table, prefix)
}

func (t *Tx) RangeDupSort(table string, key, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	return t.pick(table).RangeDupSort(table, key, fromPrefix, toPrefix, asc, limit)
}

func (t *Tx) BucketSize(table string) (uint64, error) {
	return t.pick(table).BucketSize(table)
}

func (t *Tx) Count(table string) (uint64, error) {
	return t.pick(table).Count(table)
}

func (t *Tx) ListTables() ([]string, error) {
	durList, err := t.durTx.ListTables()
	if err != nil {
		return nil, err
	}
	memList, err := t.memTx.ListTables()
	if err != nil {
		return nil, err
	}
	return t.db.MergedListTables(durList, memList), nil
}

// ViewID returns the durable backend's ViewID. The delta backend assigns
// process-local IDs that wouldn't be meaningful across restarts.
func (t *Tx) ViewID() uint64 { return t.durTx.ViewID() }

// CHandle returns the durable backend's C handle so MDBX-specific optional
// optimizations (WarmupDB, LockDBInRam) still apply to the MDBX half. The
// delta backend has no C handle.
func (t *Tx) CHandle() unsafe.Pointer { return t.durTx.CHandle() }

func (t *Tx) Apply(ctx context.Context, f func(tx kv.Tx) error) error {
	return f(t)
}

func (t *Tx) Rollback() {
	if t.durTx != nil {
		t.durTx.Rollback()
	}
	if t.memTx != nil {
		t.memTx.Rollback()
	}
	t.durTx, t.memTx = nil, nil
}

// ---- kv.Putter ----

func (t *RwTx) Put(table string, k, v []byte) error {
	return t.pickRw(table).Put(table, k, v)
}

func (t *RwTx) Delete(table string, k []byte) error {
	return t.pickRw(table).Delete(table, k)
}

func (t *RwTx) IncrementSequence(table string, amount uint64) (uint64, error) {
	return t.pickRw(table).IncrementSequence(table, amount)
}

func (t *RwTx) ResetSequence(table string, newValue uint64) error {
	return t.pickRw(table).ResetSequence(table, newValue)
}

func (t *RwTx) Append(table string, k, v []byte) error {
	return t.pickRw(table).Append(table, k, v)
}

func (t *RwTx) AppendDup(table string, k, v []byte) error {
	return t.pickRw(table).AppendDup(table, k, v)
}

func (t *RwTx) CollectMetrics() {
	t.durRw.CollectMetrics()
	t.memRw.CollectMetrics()
}

// ---- kv.BucketMigrator ----

func (t *RwTx) DropTable(name string) error {
	return t.pickRw(name).DropTable(name)
}

func (t *RwTx) CreateTable(name string) error {
	return t.pickRw(name).CreateTable(name)
}

func (t *RwTx) ExistsTable(name string) (bool, error) {
	return t.pickRw(name).ExistsTable(name)
}

func (t *RwTx) ClearTable(name string) error {
	return t.pickRw(name).ClearTable(name)
}

// ---- kv.RwTx (write cursors + Commit) ----

func (t *RwTx) RwCursor(table string) (kv.RwCursor, error) {
	return t.pickRw(table).RwCursor(table)
}

func (t *RwTx) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	return t.pickRw(table).RwCursorDupSort(table)
}

// Commit commits the durable backend first; on success commits the delta.
// If the durable commit fails the delta is rolled back so caller-observable
// state is unchanged. The delta commit cannot fail in the memstoredb
// implementation (no I/O), so a partial-commit window is theoretical only;
// even if one materialized, the delta half is wiped on next restart and
// re-derived by the aggregator from MDBX + .kv snapshots.
func (t *RwTx) Commit() error {
	if t.durRw == nil {
		return nil
	}
	durErr := t.durRw.Commit()
	t.durRw, t.durTx = nil, nil
	if durErr != nil {
		if t.memRw != nil {
			t.memRw.Rollback()
			t.memRw, t.memTx = nil, nil
		}
		return durErr
	}
	memErr := t.memRw.Commit()
	t.memRw, t.memTx = nil, nil
	return memErr
}

func (t *RwTx) ApplyRw(ctx context.Context, f func(tx kv.RwTx) error) error {
	return f(t)
}
