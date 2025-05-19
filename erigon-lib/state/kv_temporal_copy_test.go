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

package state

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
)

var ( // Compile time interface checks
	_ kv.TemporalRwDB    = (*DB)(nil)
	_ kv.TemporalRwTx    = (*Tx)(nil)
	_ kv.TemporalDebugTx = (*Tx)(nil)
)

//Variables Naming:
//  tx - Database Transaction
//  txn - Ethereum Transaction (and TxNum - is also number of Ethereum Transaction)
//  RoTx - Read-Only Database Transaction. RwTx - read-write
//  k, v - key, value
//  ts - TimeStamp. Usually it's Ethereum's TransactionNumber (auto-increment ID). Or BlockNumber.
//  Cursor - low-level mdbx-tide api to navigate over Table
//  Iter - high-level iterator-like api over Table/InvertedIndex/History/Domain. Server-side-streaming friendly - less methods than Cursor, but constructor is powerful as `SELECT key, value FROM table WHERE key BETWEEN x1 AND x2 ORDER DESC LIMIT n`.

//Methods Naming:
//  Get: exact match of criterias
//  Range: [from, to). from=nil means StartOfTable, to=nil means EndOfTable, rangeLimit=-1 means Unlimited
//  Prefix: `Range(Table, prefix, kv.NextSubtree(prefix))`

//Abstraction Layers:
// LowLevel:
//      1. DB/Tx - low-level key-value database
//      2. Snapshots/Freeze - immutable files with historical data. May be downloaded at first App
//              start or auto-generate by moving old data from DB to Snapshots.
// MediumLevel:
//      1. TemporalDB - abstracting DB+Snapshots. Target is:
//              - provide 'time-travel' API for data: consistent snapshot of data as of given Timestamp.
//              - to keep DB small - only for Hot/Recent data (can be update/delete by re-org).
//              - using next entities:
//                      - InvertedIndex: supports range-scans
//                      - History: can return value of key K as of given TimeStamp. Doesn't know about latest/current
//                          value of key K. Returns NIL if K not changed after TimeStamp.
//                      - Domain: as History but also aware about latest/current value of key K. Can move
//                          cold (updated long time ago) parts of state from db to snapshots.

// HighLevel:
//      1. Application - rely on TemporalDB (Ex: ExecutionLayer) or just DB (Ex: TxPool, Sentry, Downloader).

type DB struct {
	kv.RwDB
	agg *Aggregator
}

func New(db kv.RwDB, agg *Aggregator) (*DB, error) {
	return &DB{RwDB: db, agg: agg}, nil
}
func (db *DB) Agg() any                  { return db.agg }
func (db *DB) InternalDB() kv.RwDB       { return db.RwDB }
func (db *DB) Debug() kv.TemporalDebugDB { return kv.TemporalDebugDB(db) }

func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.aggtx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *DB) ViewTemporal(ctx context.Context, f func(tx kv.TemporalTx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// TODO: it's temporary method, allowing inject TemproalTx without changing code. But it's not type-safe.
func (db *DB) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.BeginTemporalRo(ctx)
}
func (db *DB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (db *DB) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	kvTx, err := db.RwDB.BeginRw(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.aggtx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *DB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRw(ctx)
}
func (db *DB) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) UpdateTemporal(ctx context.Context, f func(tx kv.TemporalRwTx) error) error {
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) BeginTemporalRwNosync(ctx context.Context) (kv.RwTx, error) {
	kvTx, err := db.RwDB.BeginRwNosync(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.aggtx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *DB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRwNosync(ctx) //nolint:gocritic
}
func (db *DB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginTemporalRwNosync(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Close() {
	db.RwDB.Close()
	db.agg.Close()
}

func (db *DB) OnFilesChange(f kv.OnFilesChange) { db.agg.OnFilesChange(f) }

type Tx struct {
	*mdbx.MdbxTx
	db               *DB
	aggtx            *AggregatorRoTx
	resourcesToClose []kv.Closer
	ctx              context.Context
}

func (tx *Tx) ForceReopenAggCtx() {
	tx.aggtx.Close()
	tx.aggtx = tx.Agg().BeginFilesRo()
}
func (tx *Tx) FreezeInfo() kv.FreezeInfo { return tx.aggtx }
func (tx *Tx) Debug() kv.TemporalDebugTx { return tx }

func (tx *Tx) WarmupDB(force bool) error { return tx.MdbxTx.WarmupDB(force) }
func (tx *Tx) LockDBInRam() error        { return tx.MdbxTx.LockDBInRam() }
func (tx *Tx) AggTx() any                { return tx.aggtx }
func (tx *Tx) Agg() *Aggregator          { return tx.db.agg }
func (tx *Tx) Rollback() {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	mdbxTx.Rollback()
}
func (tx *Tx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.aggtx.Close()
}
func (tx *Tx) Commit() error {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return nil
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	return mdbxTx.Commit()
}

func (tx *Tx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.aggtx.HistoryStartFrom(name)
}

func (tx *Tx) RangeAsOf(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.aggtx.RangeAsOf(tx.ctx, tx.MdbxTx, name, fromKey, toKey, asOfTs, asc, limit)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

func (tx *Tx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, bool, error) {
	it, err := tx.Debug().RangeLatest(name, prefix, nil, 1)
	if err != nil {
		return nil, false, err
	}

	defer it.Close()
	if !it.HasNext() {
		return nil, false, nil
	}

	k, _, err := it.Next()
	if err != nil {
		return nil, false, err
	}

	return k, true, nil
}

func (tx *Tx) GetLatest(name kv.Domain, k []byte) (v []byte, step uint64, err error) {
	v, step, ok, err := tx.aggtx.GetLatest(name, k, tx.MdbxTx)
	if err != nil {
		return nil, step, err
	}
	if !ok {
		return nil, step, nil
	}
	return v, step, nil
}
func (tx *Tx) GetAsOf(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.aggtx.GetAsOf(name, k, ts, tx.MdbxTx)
}

func (tx *Tx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.aggtx.HistorySeek(name, key, ts, tx.MdbxTx)
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	timestamps, err = tx.aggtx.IndexRange(name, k, fromTs, toTs, asc, limit, tx.MdbxTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, timestamps)
	return timestamps, nil
}

func (tx *Tx) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.aggtx.HistoryRange(name, fromTs, toTs, asc, limit, tx.MdbxTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

// Write methods

func (tx *Tx) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	panic("implement me pls. or use SharedDomains")
}
func (tx *Tx) DomainDel(domain kv.Domain, k []byte, prevVal []byte, prevStep uint64) error {
	panic("implement me pls. or use SharedDomains")
}
func (tx *Tx) DomainDelPrefix(domain kv.Domain, prefix []byte) error {
	panic("implement me pls. or use SharedDomains")
}

// Debug methods

func (tx *Tx) RangeLatest(domain kv.Domain, from, to []byte, limit int) (stream.KV, error) {
	return tx.aggtx.DebugRangeLatest(tx.MdbxTx, domain, from, to, limit)
}
func (tx *Tx) GetLatestFromDB(domain kv.Domain, k []byte) (v []byte, step uint64, found bool, err error) {
	return tx.aggtx.DebugGetLatestFromDB(domain, k, tx.MdbxTx)
}
func (tx *Tx) GetLatestFromFiles(domain kv.Domain, k []byte, maxTxNum uint64) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	return tx.aggtx.DebugGetLatestFromFiles(domain, k, maxTxNum)
}
func (db *DB) DomainTables(domain ...kv.Domain) []string { return db.agg.DomainTables(domain...) }
func (db *DB) ReloadSalt() error                         { return db.agg.ReloadSalt() }
func (tx *Tx) DomainFiles(domain ...kv.Domain) kv.VisibleFiles {
	return tx.aggtx.DomainFiles(domain...)
}
func (db *DB) InvertedIdxTables(domain ...kv.InvertedIdx) []string {
	return db.agg.InvertedIdxTables(domain...)
}
func (tx *Tx) TxNumsInFiles(domains ...kv.Domain) (minTxNum uint64) {
	return tx.aggtx.TxNumsInFiles(domains...)
}
func (tx *Tx) PruneSmallBatches(ctx context.Context, timeout time.Duration) (haveMore bool, err error) {
	return tx.aggtx.PruneSmallBatches(ctx, timeout, tx.MdbxTx)
}
func (tx *Tx) GreedyPruneHistory(ctx context.Context, domain kv.Domain) error {
	return tx.aggtx.GreedyPruneHistory(ctx, domain, tx.MdbxTx)
}

func (tx *Tx) Unwind(ctx context.Context, txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	return tx.aggtx.Unwind(ctx, tx.MdbxTx, txNumUnwindTo, changeset)
}
