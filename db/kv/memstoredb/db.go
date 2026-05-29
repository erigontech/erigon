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

	// persistFile is the dump path written on Close. Empty if the DB wasn't
	// opened via OpenForPath. See persist.go for the rationale.
	persistFile string

	// writeMu serialises RwTx: only one writer at a time, matching MDBX.
	writeMu sync.Mutex

	// mu protects the master tables / sequences map pointers themselves.
	// Cloning takes RLock briefly; Commit takes Lock briefly to swap.
	mu        sync.RWMutex
	tables    map[string]*table
	sequences map[string]uint64

	// viewIDSeq is a monotonic counter assigned to each tx; mirrors MDBX's
	// transaction ID. Must be unique across distinct Begin* calls.
	viewIDSeq atomic.Uint64

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
// mirroring MDBX where reopening the same file restores its data. If a dump
// file exists alongside the path (`<path>.mem`), its contents are restored —
// this lets the multi-process CLI tooling (erigon init / import / daemon)
// round-trip chaindata through separate processes.
func OpenForPath(path string, label kv.Label, cfg kv.TableCfg) *DB {
	registryMu.Lock()
	defer registryMu.Unlock()
	if db, ok := registry[path]; ok {
		db.closed.Store(false)
		return db
	}
	db := New(label, cfg)
	if path != "" {
		db.persistFile = path + ".mem"
		if err := db.loadFromFile(db.persistFile); err != nil {
			// Don't crash on a corrupt dump — fall back to a fresh DB and
			// log via the caller's path. A loud no-op is preferable to a
			// startup panic when the dump file was written by an older
			// version or got truncated.
			db.tables = make(map[string]*table)
			db.sequences = make(map[string]uint64)
		}
	}
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
func (db *DB) Close() {
	if !db.closed.CompareAndSwap(false, true) {
		return
	}
	// Best-effort dump to disk so a subsequent OpenForPath against the same
	// path restores chaindata (see persist.go for why this is needed). A
	// failed save is non-fatal: the DB is volatile by design and any caller
	// that depends on durability is misusing the in-mem backend.
	if db.persistFile != "" {
		_ = db.saveToFile(db.persistFile)
	}
}

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
	return db.newRwTxLocked(), nil
}

// newRwTxLocked builds a write tx. The caller must hold writeMu.
// Tables are clone-on-first-write: reads go through the master pointer,
// the first mutation triggers a COW clone owned by this tx.
func (db *DB) newRwTxLocked() *tx {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t := &tx{
		db:            db,
		rw:            true,
		viewID:        db.viewIDSeq.Add(1),
		tables:        maps.Clone(db.tables),
		privateTables: make(map[string]struct{}),
		sequences:     maps.Clone(db.sequences),
	}
	if t.tables == nil {
		t.tables = make(map[string]*table)
	}
	if t.sequences == nil {
		t.sequences = make(map[string]uint64)
	}
	return t
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

// beginRoTx shares master's tables map by reference — no clone. Safe because
// RoTx never mutates the map (writes-via-cursor for read-only tx panic; reads
// of missing tables return a per-tx empty stub instead of inserting into the
// master). RwTx commits swap the master map with a fresh one, so a RoTx
// holding the old reference observes its pre-commit data via the still-valid
// old btree pointers.
func (db *DB) beginRoTx() *tx {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t := &tx{
		db:        db,
		rw:        false,
		viewID:    db.viewIDSeq.Add(1),
		tables:    db.tables,
		sequences: db.sequences,
	}
	if t.tables == nil {
		t.tables = emptyTables
	}
	if t.sequences == nil {
		t.sequences = emptySequences
	}
	return t
}

// emptyTables / emptySequences are the read-only fallbacks for RoTx when the
// master maps are nil. They MUST NOT be mutated.
var (
	emptyTables    = map[string]*table{}
	emptySequences = map[string]uint64{}
)

// privateTables is not allocated for RoTx — only RwTx clones-on-write.

// beginRwTx takes the exclusive write lock (held until Commit/Rollback) and
// captures the master tables map. Tables are clone-on-first-write.
func (db *DB) beginRwTx() *tx {
	db.writeMu.Lock()
	return db.newRwTxLocked()
}
