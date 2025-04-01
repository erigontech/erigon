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

package temporal

import (
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/state"
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
	agg *state.Aggregator
}

func New(db kv.RwDB, agg *state.Aggregator) (*DB, error) {
	return &DB{RwDB: db, agg: agg}, nil
}
func (db *DB) Agg() any            { return db.agg }
func (db *DB) InternalDB() kv.RwDB { return db.RwDB }

func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, tx: tx{db: db, ctx: ctx}}

	tx.filesTx = db.agg.BeginFilesRo()
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
	tx := &RwTx{RwTx: kvTx, tx: tx{db: db, ctx: ctx}}

	tx.filesTx = db.agg.BeginFilesRo()
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

func (db *DB) BeginTemporalRwNosync(ctx context.Context) (kv.TemporalRwTx, error) {
	kvTx, err := db.RwDB.BeginRwNosync(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &RwTx{RwTx: kvTx, tx: tx{db: db, ctx: ctx}}

	tx.filesTx = db.agg.BeginFilesRo()
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

func (db *DB) OnFreeze(f kv.OnFreezeFunc) { db.agg.OnFreeze(f) }

type tx struct {
	db               *DB
	filesTx          *state.AggregatorRoTx
	resourcesToClose []kv.Closer
	ctx              context.Context
	mu               sync.RWMutex
}

type Tx struct {
	kv.Tx
	tx
}

type RwTx struct {
	kv.RwTx
	tx
}

func (tx *tx) ForceReopenAggCtx() {
	tx.filesTx.Close()
	tx.filesTx = tx.Agg().BeginFilesRo()
}

func (tx *tx) AggTx() *state.AggregatorRoTx { return tx.filesTx }
func (tx *tx) Agg() *state.Aggregator       { return tx.db.agg }
func (tx *tx) Rollback() {
	tx.autoClose()
}

var str string

func (tx *Tx) Rollback() {
	if tx == nil {
		return
	}
	fmt.Printf("%s%p:rollback-ac\n", str, tx)
	tx.autoClose()
	if tx.Tx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	fmt.Printf("%s%p:rollback-reset\n", str, tx)
	tx.mu.Lock()
	rb := tx.Tx
	tx.Tx = nil
	tx.mu.Unlock()
	fmt.Printf("%s%p:rollback-rb\n", str, tx)
	str += " "
	rb.Rollback()
	if len(str) > 0 {
		str = str[1:]
	}
	fmt.Printf("%s%p:rollback-done\n", str, tx)
}

func (tx *Tx) Apply(f func(tx kv.Tx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.Tx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return fmt.Errorf("can't apply: transaction closed")
	}
	return applyTx.Apply(f)
}

func (tx *RwTx) Apply(f func(tx kv.Tx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.RwTx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return fmt.Errorf("can't apply: transaction closed")
	}
	return applyTx.Apply(f)
}

func (tx *RwTx) ApplyRW(f func(tx kv.RwTx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.RwTx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return fmt.Errorf("can't apply: transaction closed")
	}
	return applyTx.ApplyRw(f)
}

func (tx *RwTx) Rollback() {
	if tx == nil {
		return
	}
	tx.autoClose()
	if tx.RwTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	rb := tx.RwTx
	tx.RwTx = nil
	rb.Rollback()
}

type asyncClone struct {
	RwTx
}

// this is needed to create a clone that can be passed
// to external go rooutines - they are intended as slaves
// so should never commit or rollback the master transaction
func (rwtx *RwTx) AsyncClone(asyncTx kv.RwTx) *asyncClone {
	return &asyncClone{
		RwTx{
			RwTx: asyncTx,
			tx: tx{
				db:               rwtx.db,
				filesTx:          rwtx.filesTx,
				resourcesToClose: nil,
				ctx:              rwtx.ctx,
			}}}
}

func (tx *asyncClone) ApplyChan() mdbx.TxApplyChan {
	return tx.RwTx.RwTx.(mdbx.TxApplySource).ApplyChan()
}

func (tx *asyncClone) Commit() error {
	return fmt.Errorf("can't commit cloned tx")
}
func (tx *asyncClone) Rollback() {
}

func (tx *tx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.filesTx.Close()
}

func (tx *RwTx) Commit() error {
	if tx == nil {
		return nil
	}
	tx.autoClose()
	if tx.RwTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return nil
	}
	t := tx.RwTx
	tx.RwTx = nil
	return t.Commit()
}

func (tx *tx) historyStartFrom(name kv.Domain) uint64 {
	return tx.filesTx.HistoryStartFrom(name)
}

func (tx *Tx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.historyStartFrom(name)
}

func (tx *RwTx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.historyStartFrom(name)
}

func (tx *tx) rangeAsOf(name kv.Domain, rtx kv.Tx, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.filesTx.RangeAsOf(tx.ctx, rtx, name, fromKey, toKey, asOfTs, asc, limit)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

func (tx *Tx) RangeAsOf(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	return tx.rangeAsOf(name, tx.Tx, fromKey, toKey, asOfTs, asc, limit)
}

func (tx *RwTx) RangeAsOf(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	return tx.rangeAsOf(name, tx.RwTx, fromKey, toKey, asOfTs, asc, limit)
}

func (tx *tx) getLatest(name kv.Domain, dbTx kv.Tx, k []byte) (v []byte, step uint64, err error) {
	v, step, ok, err := tx.filesTx.GetLatest(name, k, dbTx)
	if err != nil {
		return nil, step, err
	}
	if !ok {
		return nil, step, nil
	}
	return v, step, nil
}

func (tx *Tx) GetLatest(name kv.Domain, k []byte) (v []byte, step uint64, err error) {
	return tx.getLatest(name, tx.Tx, k)
}

func (tx *RwTx) GetLatest(name kv.Domain, k []byte) (v []byte, step uint64, err error) {
	return tx.getLatest(name, tx.RwTx, k)
}

func (tx *tx) getAsOf(name kv.Domain, gtx kv.Tx, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.filesTx.GetAsOf(gtx, name, key, ts)
}

func (tx *Tx) GetAsOf(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.getAsOf(name, tx.Tx, key, ts)
}

func (tx *RwTx) GetAsOf(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.getAsOf(name, tx.RwTx, key, ts)
}

func (tx *tx) historySeek(name kv.Domain, dbTx kv.Tx, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.filesTx.HistorySeek(name, key, ts, dbTx)
}

func (tx *Tx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.historySeek(name, tx.Tx, key, ts)
}

func (tx *RwTx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.historySeek(name, tx.RwTx, key, ts)
}

func (tx *tx) indexRange(name kv.InvertedIdx, dbTx kv.Tx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	timestamps, err = tx.filesTx.IndexRange(name, k, fromTs, toTs, asc, limit, dbTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, timestamps)
	return timestamps, nil
}

func (tx *Tx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	return tx.indexRange(name, tx.Tx, k, fromTs, toTs, asc, limit)
}

func (tx *RwTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	return tx.indexRange(name, tx.RwTx, k, fromTs, toTs, asc, limit)
}

func (tx *tx) historyRange(name kv.Domain, dbTx kv.Tx, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.filesTx.HistoryRange(name, fromTs, toTs, asc, limit, dbTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

func (tx *Tx) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	return tx.historyRange(name, tx.Tx, fromTs, toTs, asc, limit)
}

func (tx *RwTx) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	return tx.historyRange(name, tx.RwTx, fromTs, toTs, asc, limit)
}
