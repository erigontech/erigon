// Package hybridkv routes every kv.RwTx operation to one of two backing
// kv.RwDB instances based on a per-table classifier.
//
// Motivation: a pure in-memory kv.RwDB (db/kv/memstoredb) erases the MDBX cgo
// boundary on the newPayload hot path but at the cost of holding the entire
// chaindata in RAM and needing on-Close persistence to survive restarts. The
// hybrid split keeps the bulky durable tables (block bodies, headers,
// receipts, stage progress, …) in MDBX and the hot delta tables
// (temporal/domain values + histories + indices) in memstoredb. On restart
// the in-memory half is wiped and the existing aggregator startup logic
// re-derives it from the .kv snapshots + the persistent MDBX tables.
//
// Commit semantics: a hybrid RwTx commits the MDBX side first; only if that
// succeeds does it commit the memstore side. memstore commits do not perform
// I/O and cannot fail. If the process crashes between the MDBX commit and
// the memstore commit (or vice versa during normal Rollback), the
// memstore-only state is lost on the next restart by design — the aggregator
// re-derives it.
package hybridkv

import (
	"context"
	"errors"
	"sort"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/db/kv"
)

// DB wraps a "durable" backend (typically MDBX) and a "delta" backend
// (typically memstoredb). Each table is owned by exactly one of them per the
// isMem classifier passed at construction.
type DB struct {
	durable kv.RwDB
	delta   kv.RwDB
	isMem   func(table string) bool
	label   kv.Label
}

// Compile-time check.
var _ kv.RwDB = (*DB)(nil)

// New constructs a hybrid DB. `durable` holds tables for which isMem(name)
// returns false; `delta` holds tables for which it returns true. The
// classifier is consulted once per operation, so callers may pass a closure
// over a precomputed set.
func New(label kv.Label, durable, delta kv.RwDB, isMem func(table string) bool) *DB {
	if durable == nil {
		panic("hybridkv: durable backend is nil")
	}
	if delta == nil {
		panic("hybridkv: delta backend is nil")
	}
	if isMem == nil {
		panic("hybridkv: isMem classifier is nil")
	}
	return &DB{durable: durable, delta: delta, isMem: isMem, label: label}
}

// Backends exposes the underlying durable/delta DBs for callers that need to
// reach past the wrapper (migrations, backups, low-level admin tooling). New
// code should prefer the kv.RwDB / kv.RwTx surface.
func (db *DB) Backends() (durable, delta kv.RwDB) { return db.durable, db.delta }

// kv.RoDB / kv.RwDB top-level methods --------------------------------------

func (db *DB) AllTables() kv.TableCfg      { return db.durable.AllTables() }
func (db *DB) PageSize() datasize.ByteSize { return db.durable.PageSize() }
func (db *DB) CHandle() unsafe.Pointer     { return db.durable.CHandle() }
func (db *DB) Path() string                { return db.durable.Path() }
func (db *DB) ReadOnly() bool              { return db.durable.ReadOnly() }

func (db *DB) Close() {
	db.durable.Close()
	db.delta.Close()
}

func (db *DB) BeginRo(ctx context.Context) (kv.Tx, error) {
	durTx, err := db.durable.BeginRo(ctx) //nolint:gocritic // returned to caller as part of the hybrid Tx
	if err != nil {
		return nil, err
	}
	memTx, err := db.delta.BeginRo(ctx) //nolint:gocritic // returned to caller as part of the hybrid Tx
	if err != nil {
		durTx.Rollback()
		return nil, err
	}
	return &Tx{db: db, durTx: durTx, memTx: memTx}, nil
}

func (db *DB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (db *DB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.beginRw(ctx, false)
}

func (db *DB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.beginRw(ctx, true)
}

// BeginRwTry mirrors MDBX's MDBX_TXN_TRY. Returns syscall.EBUSY if the
// durable backend cannot acquire its write lock immediately. The delta
// backend uses the same try-acquire semantic; if either rejects, both are
// released and the caller sees the error.
type beginRwTryer interface {
	BeginRwTry(ctx context.Context) (kv.RwTx, error)
}

func (db *DB) BeginRwTry(ctx context.Context) (kv.RwTx, error) {
	durTryer, ok := db.durable.(beginRwTryer)
	if !ok {
		return nil, errors.New("hybridkv: durable backend does not support BeginRwTry")
	}
	memTryer, ok := db.delta.(beginRwTryer)
	if !ok {
		return nil, errors.New("hybridkv: delta backend does not support BeginRwTry")
	}
	durTx, err := durTryer.BeginRwTry(ctx) //nolint:gocritic // returned to caller as part of the hybrid RwTx
	if err != nil {
		return nil, err
	}
	memTx, err := memTryer.BeginRwTry(ctx) //nolint:gocritic // returned to caller as part of the hybrid RwTx
	if err != nil {
		durTx.Rollback()
		return nil, err
	}
	return &RwTx{Tx: Tx{db: db, durTx: durTx, memTx: memTx}, durRw: durTx, memRw: memTx}, nil
}

func (db *DB) beginRw(ctx context.Context, nosync bool) (kv.RwTx, error) {
	var (
		durTx kv.RwTx
		err   error
	)
	if nosync {
		durTx, err = db.durable.BeginRwNosync(ctx) //nolint:gocritic // returned to caller as part of the hybrid RwTx
	} else {
		durTx, err = db.durable.BeginRw(ctx) //nolint:gocritic // returned to caller as part of the hybrid RwTx
	}
	if err != nil {
		return nil, err
	}
	memTx, err := db.delta.BeginRw(ctx) //nolint:gocritic // returned to caller as part of the hybrid RwTx
	if err != nil {
		durTx.Rollback()
		return nil, err
	}
	return &RwTx{Tx: Tx{db: db, durTx: durTx, memTx: memTx}, durRw: durTx, memRw: memTx}, nil
}

func (db *DB) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginRwNosync(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// MergedListTables returns the union of table names across both backends,
// sorted. Used by Tx.ListTables.
func (db *DB) MergedListTables(durList, memList []string) []string {
	seen := make(map[string]struct{}, len(durList)+len(memList))
	out := make([]string, 0, len(durList)+len(memList))
	for _, n := range durList {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	for _, n := range memList {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// joinErrors returns nil if both nil, the non-nil one if one is nil, or
// errors.Join otherwise.
func joinErrors(a, b error) error {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return errors.Join(a, b)
	}
}
