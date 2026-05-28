// Package memstoredb is a pure-Go, in-memory implementation of kv.RwDB.
//
// It is the backend selected when the feature flag dbg.UseInMemoryKV is true
// (env USE_IN_MEMORY_KV=1 or --experimental.inmem-kv). When the flag is off
// MDBX remains the default. The store is **volatile** — data is lost on
// process exit — and is intended for measurement/benchmarking, not durable
// production use.
//
// Snapshot isolation: each Begin* takes a COW snapshot of all tables via
// tidwall/btree.Copy() (O(1) per table). RwTx writes go to the tx's own
// table copies; Commit atomically swaps the master; Rollback discards.
// Concurrent reads + a single writer at a time, mirroring MDBX semantics
// without any OS-thread affinity (no runtime.LockOSThread needed).
package memstoredb

import (
	"bytes"
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/c2h5oh/datasize"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/kv"
)

// DB is the in-memory kv.RwDB.
type DB struct {
	label kv.Label
	cfg   kv.TableCfg

	// writeMu serialises RwTx: only one writer at a time, matching MDBX.
	writeMu sync.Mutex

	// mu protects the master tables / sequences map pointers themselves.
	// Cloning takes RLock briefly; Commit takes Lock briefly to swap.
	mu        sync.RWMutex
	tables    map[string]*table
	sequences map[string]uint64

	closed atomic.Bool
}

// Compile-time check.
var _ kv.RwDB = (*DB)(nil)

// New constructs a DB for the given label using the provided table config.
// Callers typically pass kv.TablesCfgByLabel(label) for the default schema.
func New(label kv.Label, cfg kv.TableCfg) *DB {
	if cfg == nil {
		cfg = kv.TablesCfgByLabel(label)
	}
	return &DB{
		label:     label,
		cfg:       cfg,
		tables:    make(map[string]*table),
		sequences: make(map[string]uint64),
	}
}

// registry deduplicates DB instances by an arbitrary key (typically the
// filesystem path that MDBX would have used). Multiple OpenForPath calls with
// the same key return the same in-memory backend, matching the on-disk
// semantic that opening the same path returns the same data.
var (
	registryMu sync.Mutex
	registry   = map[string]*DB{}
)

// OpenForPath returns the shared DB for `path`, creating it on first use.
// A subsequent OpenForPath against the same path returns the same DB,
// mirroring MDBX where reopening the same file restores its data.
func OpenForPath(path string, label kv.Label, cfg kv.TableCfg) *DB {
	registryMu.Lock()
	defer registryMu.Unlock()
	if db, ok := registry[path]; ok {
		db.closed.Store(false)
		return db
	}
	db := New(label, cfg)
	registry[path] = db
	return db
}

// table is the per-table btree.
type table struct {
	tree    *btree2.BTreeG[entry]
	dupSort bool
}

// entry is the (key, value) pair stored in the btree.
type entry struct {
	k []byte
	v []byte
}

func lessKey(a, b entry) bool { return bytes.Compare(a.k, b.k) < 0 }

func lessKeyValue(a, b entry) bool {
	c := bytes.Compare(a.k, b.k)
	if c != 0 {
		return c < 0
	}
	return bytes.Compare(a.v, b.v) < 0
}

func newTable(dupSort bool) *table {
	less := lessKey
	if dupSort {
		less = lessKeyValue
	}
	// NoLocks: concurrency is handled at the DB / tx level.
	return &table{
		tree:    btree2.NewBTreeGOptions(less, btree2.Options{NoLocks: true}),
		dupSort: dupSort,
	}
}

// cloneShallow returns a COW snapshot of the table — O(1) via btree Copy.
func (t *table) cloneShallow() *table {
	return &table{tree: t.tree.Copy(), dupSort: t.dupSort}
}

func (db *DB) isDupSort(name string) bool {
	c, ok := db.cfg[name]
	if !ok {
		// Fall back to chaindata defaults for tables created on the fly,
		// matching memStore's historical behaviour.
		c, ok = kv.ChaindataTablesCfg[name]
		if !ok {
			return false
		}
	}
	return c.Flags&kv.DupSort != 0
}

// kv.RoDB interface.

func (db *DB) AllTables() kv.TableCfg      { return db.cfg }
func (db *DB) PageSize() datasize.ByteSize { return 4096 }
func (db *DB) CHandle() unsafe.Pointer     { return nil }
func (db *DB) Path() string                { return "memstoredb:" + string(db.label) }
func (db *DB) ReadOnly() bool              { return false }

// Close marks the handle closed. Mirrors MDBX, where on-disk data outlives
// the handle: a subsequent OpenForPath against the same path returns the same
// data. Tests that want a clean slate use WipePath.
func (db *DB) Close() { db.closed.Store(true) }

// WipePath fully removes the DB associated with `path` from the registry,
// discarding its in-memory data. Use only in tests that need a hard reset.
func WipePath(path string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, path)
}

func (db *DB) BeginRo(_ context.Context) (kv.Tx, error) {
	return db.beginRoTx(), nil
}

func (db *DB) View(_ context.Context, f func(tx kv.Tx) error) error {
	tx := db.beginRoTx()
	defer tx.Rollback()
	return f(tx)
}

// kv.RwDB interface.

func (db *DB) BeginRw(_ context.Context) (kv.RwTx, error) {
	return db.beginRwTx(), nil
}

func (db *DB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.BeginRw(ctx)
}

// BeginRwTry attempts to acquire the write lock without blocking. Returns
// syscall.EBUSY if another writer is active. Mirrors MDBX's MDBX_TXN_TRY.
func (db *DB) BeginRwTry(_ context.Context) (kv.RwTx, error) {
	if !db.writeMu.TryLock() {
		return nil, syscall.EBUSY
	}
	db.mu.RLock()
	t := &tx{
		db:        db,
		rw:        true,
		tables:    make(map[string]*table, len(db.tables)),
		sequences: maps.Clone(db.sequences),
	}
	for name, tab := range db.tables {
		t.tables[name] = tab.cloneShallow()
	}
	if t.sequences == nil {
		t.sequences = make(map[string]uint64)
	}
	db.mu.RUnlock()
	return t, nil
}

func (db *DB) Update(_ context.Context, f func(tx kv.RwTx) error) error {
	tx := db.beginRwTx()
	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	return db.Update(ctx, f)
}

// beginRoTx snapshots all tables at the moment of begin. Subsequent writes
// in concurrent RwTx are invisible to this RoTx.
func (db *DB) beginRoTx() *tx {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t := &tx{
		db:        db,
		rw:        false,
		tables:    make(map[string]*table, len(db.tables)),
		sequences: maps.Clone(db.sequences),
	}
	for name, tab := range db.tables {
		t.tables[name] = tab.cloneShallow()
	}
	if t.sequences == nil {
		t.sequences = make(map[string]uint64)
	}
	return t
}

// beginRwTx takes the exclusive write lock (held until Commit/Rollback)
// and snapshots all tables. Writes go to the snapshot; Commit swaps master.
func (db *DB) beginRwTx() *tx {
	db.writeMu.Lock()
	db.mu.RLock()
	t := &tx{
		db:        db,
		rw:        true,
		tables:    make(map[string]*table, len(db.tables)),
		sequences: maps.Clone(db.sequences),
	}
	for name, tab := range db.tables {
		t.tables[name] = tab.cloneShallow()
	}
	if t.sequences == nil {
		t.sequences = make(map[string]uint64)
	}
	db.mu.RUnlock()
	return t
}
