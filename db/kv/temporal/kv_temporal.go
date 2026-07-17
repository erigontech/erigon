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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
)

var ( // Compile time interface checks
	_ kv.TemporalRwDB    = (*DB)(nil)
	_ kv.TemporalRwTx    = (*RwTx)(nil)
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
//  Get: exact match of criteria
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
	stateFiles *state.Aggregator
	// blockFiles: block snapshots, the peer of stateFiles. Optional; nil for
	// state-only tools, in which case block reads fall back to their own view.
	blockFiles *blocksnapshots.RoSnapshots
}

// New wires the temporal DB over a raw kv.RwDB, its state aggregator, and the
// (optional) block snapshots — the block-data peer of stateFiles. Pass nil
// blockSnaps for state-only tools.
func New(db kv.RwDB, agg *state.Aggregator, blockSnaps *blocksnapshots.RoSnapshots) (*DB, error) {
	return &DB{RwDB: db, stateFiles: agg, blockFiles: blockSnaps}, nil
}

func (db *DB) Agg() any                                     { return db.stateFiles }
func (db *DB) DebugBlockFiles() *blocksnapshots.RoSnapshots { return db.blockFiles }

// beginBlockFilesRo pins the block-files view for a tx, or nil if unset.
func (db *DB) beginBlockFilesRo() *blocksnapshots.View {
	if db.blockFiles == nil {
		return nil
	}
	return db.blockFiles.View()
}
func (db *DB) InternalDB() kv.RwDB       { return db.RwDB }
func (db *DB) Debug() kv.TemporalDebugDB { return kv.TemporalDebugDB(db) }

func (db *DB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &Tx{Tx: kvTx, tx: tx{db: db, ctx: ctx}}

	tx.aggtx = db.stateFiles.BeginFilesRo()
	tx.blocktx = db.beginBlockFilesRo()

	return tx, nil
}

// temporalFilesPin implements kv.TemporalFilesPin: it holds a consistent
// aggregator file snapshot and opens read txns bound to it.
type temporalFilesPin struct {
	db  *DB
	agg *state.AggregatorFilesPin
}

// Pin returns a kv.TemporalFilesPin holding this tx's aggregator file snapshot;
// read txns opened from it stay on that generation. Independent of this tx's
// lifetime — release with Close.
func (tx *tx) Pin() kv.TemporalFilesPin {
	return &temporalFilesPin{db: tx.db, agg: tx.aggtx.Pin()}
}

func (p *temporalFilesPin) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := p.db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	// Commitment workers read only state domains through aggtx, never forkable
	// data, so the worker tx needs the pinned file snapshot and nothing else.
	tx := &Tx{Tx: kvTx, tx: tx{db: p.db, ctx: ctx}}
	tx.aggtx = p.agg.BeginFilesRo()
	return tx, nil
}

func (p *temporalFilesPin) Close() { p.agg.Close() }

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

func (db *DB) newRwTx(kvTx kv.RwTx, ctx context.Context) *RwTx {
	tx := &RwTx{RwTx: kvTx, tx: tx{db: db, ctx: ctx}}
	tx.aggtx = db.stateFiles.BeginFilesRo()
	tx.blocktx = db.beginBlockFilesRo()
	return tx
}

func (db *DB) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	kvTx, err := db.RwDB.BeginRw(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return db.newRwTx(kvTx, ctx), nil
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

func (db *DB) BeginTemporalRwNosync(ctx context.Context) (kv.TemporalRwTx, error) {
	kvTx, err := db.RwDB.BeginRwNosync(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return db.newRwTx(kvTx, ctx), nil
}
func (db *DB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRwNosync(ctx) //nolint:gocritic
}

// rwTry is satisfied by DBs whose write-transaction open can be made non-blocking.
type rwTry interface {
	BeginRwTry(ctx context.Context) (kv.RwTx, error)
}

// BeginTemporalRwTry opens a write transaction without blocking. Returns
// syscall.EBUSY if another write transaction is already open.
func (db *DB) BeginTemporalRwTry(ctx context.Context) (kv.TemporalRwTx, error) {
	tryDB, ok := db.RwDB.(rwTry)
	if !ok {
		return nil, errors.New("underlying db does not support non-blocking write tx")
	}
	kvTx, err := tryDB.BeginRwTry(ctx)
	if err != nil {
		return nil, err
	}
	return db.newRwTx(kvTx, ctx), nil
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
	if db.stateFiles != nil {
		db.stateFiles.Close()
	}
	db.RwDB.Close()
}

func (db *DB) OnFilesChange(onChange, onDel kv.OnFilesChange) {
	db.stateFiles.OnFilesChange(onChange, onDel)
}

type tx struct {
	db               *DB
	aggtx            *state.AggregatorRoTx
	blocktx          *blocksnapshots.View
	resourcesToClose []kv.Closer
	ctx              context.Context
	mu               sync.RWMutex
}

type Tx struct {
	kv.Tx
	tx
	visibleEnds domainVisibleEnds
}

type RwTx struct {
	kv.RwTx
	tx
}

type domainVisibleEnds struct {
	ends  [kv.DomainLen]uint64
	mu    sync.Mutex
	state atomic.Uint32
}

// state packs a loaded and an available bit per domain — compile-time capacity check.
var _ [32 - 2*int(kv.DomainLen)]struct{}

func (v *domainVisibleEnds) get(tx *Tx, domain kv.Domain) (uint64, bool) {
	bit := uint32(1) << uint32(domain)
	state := v.state.Load()
	if state&bit != 0 {
		return v.ends[domain], state&(bit<<uint32(kv.DomainLen)) != 0
	}
	return v.load(tx, domain, bit)
}

func (v *domainVisibleEnds) load(tx *Tx, domain kv.Domain, bit uint32) (uint64, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	state := v.state.Load()
	availableBit := bit << uint32(kv.DomainLen)
	if state&bit == 0 {
		end, ok := tx.aggtx.DomainVisibleEnd(domain, tx.Tx)
		v.ends[domain] = end
		state |= bit
		if ok {
			state |= availableBit
		}
		v.state.Store(state)
	}
	return v.ends[domain], state&availableBit != 0
}

// reset takes mu so an in-flight load can't re-store pre-reset bits.
func (v *domainVisibleEnds) reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.state.Store(0)
}

func (tx *tx) ForceReopenUnderlyingFilesTx() {
	if tx.blocktx != nil {
		tx.blocktx.Close()
	}
	tx.blocktx = tx.db.beginBlockFilesRo()
	if tx.aggtx != nil {
		tx.aggtx.Close()
	}
	tx.aggtx = tx.Agg().BeginFilesRo()
}

// ForceReopenUnderlyingFilesTx swaps in a fresh files view, which can extend
// the visible frontier — drop the memoized ends so they are re-derived.
func (tx *Tx) ForceReopenUnderlyingFilesTx() {
	tx.tx.ForceReopenUnderlyingFilesTx()
	tx.visibleEnds.reset()
}
func (tx *tx) FreezeInfo() kv.FreezeInfo { return tx.aggtx }

func (tx *tx) AggTx() any             { return tx.aggtx }
func (tx *tx) Agg() *state.Aggregator { return tx.db.stateFiles }

// BlockFilesRoTx returns the tx's pinned block-files view, or nil if unset.
func (tx *tx) BlockFilesRoTx() *blocksnapshots.View { return tx.blocktx }
func (tx *tx) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return tx.aggtx.StepsInFiles(entitySet...)
}
func (tx *tx) Retire(ctx context.Context, cutoffs kv.RetireCutoffs) (int, error) {
	return tx.aggtx.Retire(ctx, cutoffs)
}

func (tx *tx) Rollback() {
	tx.autoClose()
}
func (tx *Tx) Rollback() {
	if tx == nil {
		return
	}
	tx.autoClose()
	if tx.Tx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	tx.mu.Lock()
	rb := tx.Tx
	tx.Tx = nil
	tx.mu.Unlock()
	rb.Rollback()
}

func (tx *Tx) WarmupDB(force bool) error {
	if mdbxTx, ok := tx.Tx.(*mdbx.MdbxTx); ok {
		return mdbxTx.WarmupDB(force)
	}
	return nil
}

func (tx *Tx) LockDBInRam() error {
	if mdbxTx, ok := tx.Tx.(*mdbx.MdbxTx); ok {
		return mdbxTx.LockDBInRam()
	}
	return nil
}

// DistributeCursors forwards to the underlying mdbx engine. Like DeleteRange, it
// fails loud on a non-mdbx inner rather than silently degrading (none exists today).
func (tx *Tx) DistributeCursors(table string, from []byte, n int) ([][]byte, error) {
	if s, ok := tx.Tx.(kv.DBWithDistributionSupport); ok {
		return s.DistributeCursors(table, from, n)
	}
	return nil, fmt.Errorf("DistributeCursors not supported by %T", tx.Tx)
}

func (tx *RwTx) DistributeCursors(table string, from []byte, n int) ([][]byte, error) {
	if s, ok := tx.RwTx.(kv.DBWithDistributionSupport); ok {
		return s.DistributeCursors(table, from, n)
	}
	return nil, fmt.Errorf("DistributeCursors not supported by %T", tx.RwTx)
}

func (tx *Tx) Apply(ctx context.Context, f func(tx kv.Tx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.Tx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return errors.New("can't apply: transaction closed")
	}
	return applyTx.Apply(ctx, f)
}

func (tx *RwTx) WarmupDB(force bool) error {
	if mdbxTx, ok := tx.RwTx.(*mdbx.MdbxTx); ok {
		return mdbxTx.WarmupDB(force)
	}
	return nil
}

func (tx *RwTx) DeleteRange(table string, from, to []byte) (uint64, error) {
	if dr, ok := tx.RwTx.(kv.HasDeleteRange); ok {
		return dr.DeleteRange(table, from, to)
	}
	// No non-mdbx temporal backend exists (mdbx's native range-delete is used
	// above); fail loud rather than carry an unexercised, DupSort-unsafe emulation.
	return 0, fmt.Errorf("DeleteRange not supported by %T", tx.RwTx)
}

func (tx *RwTx) LockDBInRam() error {
	if mdbxTx, ok := tx.RwTx.(*mdbx.MdbxTx); ok {
		return mdbxTx.LockDBInRam()
	}
	return nil
}

func (tx *RwTx) Debug() kv.TemporalDebugTx { return tx }
func (tx *Tx) Debug() kv.TemporalDebugTx   { return tx }

func (tx *RwTx) NewMemBatch(ioMetrics any) kv.TemporalMemBatch {
	return state.NewTemporalMemBatch(tx, ioMetrics)
}
func (tx *Tx) NewMemBatch(ioMetrics any) kv.TemporalMemBatch {
	return state.NewTemporalMemBatch(tx, ioMetrics)
}

func (tx *RwTx) Apply(ctx context.Context, f func(tx kv.Tx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.RwTx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return errors.New("can't apply: transaction closed")
	}
	return applyTx.Apply(ctx, f)
}

func (tx *RwTx) ApplyRW(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx.tx.mu.RLock()
	applyTx := tx.RwTx
	tx.tx.mu.RUnlock()
	if applyTx == nil {
		return errors.New("can't apply: transaction closed")
	}
	return applyTx.ApplyRw(ctx, f)
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
				aggtx:            rwtx.aggtx,
				blocktx:          rwtx.blocktx,
				resourcesToClose: nil,
				ctx:              rwtx.ctx,
			}}}
}

func (tx *asyncClone) ApplyChan() mdbx.TxApplyChan {
	return tx.RwTx.RwTx.(mdbx.TxApplySource).ApplyChan()
}

func (tx *asyncClone) Commit() error {
	return errors.New("can't commit cloned tx")
}
func (tx *asyncClone) Rollback() {
}

func (tx *tx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.aggtx.Close()
	if tx.blocktx != nil {
		tx.blocktx.Close()
	}
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

func (tx *tx) rangeAsOf(name kv.Domain, rtx kv.Tx, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.aggtx.RangeAsOf(tx.ctx, rtx, name, fromKey, toKey, asOfTs, asc, limit)
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

func (tx *tx) getLatest(name kv.Domain, dbTx kv.Tx, k []byte) (v []byte, step kv.Step, err error) {
	v, step, ok, err := tx.aggtx.GetLatest(name, k, dbTx)
	if err != nil {
		return nil, step, err
	}
	if !ok {
		return nil, step, nil
	}
	return v, step, err
}

func (tx *Tx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return tx.hasPrefix(name, tx.Tx, prefix)
}

func (tx *RwTx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return tx.hasPrefix(name, tx.RwTx, prefix)
}

func (tx *tx) hasPrefix(name kv.Domain, dbTx kv.Tx, prefix []byte) ([]byte, []byte, bool, error) {
	to, ok := kv.NextSubtree(prefix)
	if !ok {
		to = nil
	}

	it, err := tx.rangeLatest(name, dbTx, prefix, to, 1)
	if err != nil {
		return nil, nil, false, err
	}

	defer it.Close()
	if !it.HasNext() {
		return nil, nil, false, nil
	}

	k, v, err := it.Next()
	if err != nil {
		return nil, nil, false, err
	}

	return k, v, true, nil
}

func (tx *Tx) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return tx.getLatest(name, tx.Tx, k)
}

func (tx *RwTx) GetLatest(name kv.Domain, k []byte) (v []byte, step kv.Step, err error) {
	return tx.getLatest(name, tx.RwTx, k)
}

func (tx *tx) getAsOf(name kv.Domain, gtx kv.Tx, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.aggtx.GetAsOf(name, key, ts, gtx)
}

func (tx *Tx) GetAsOf(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.getAsOf(name, tx.Tx, key, ts)
}

func (tx *RwTx) GetAsOf(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.getAsOf(name, tx.RwTx, key, ts)
}

func (tx *tx) historySeek(name kv.Domain, dbTx kv.Tx, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.aggtx.HistorySeek(name, key, ts, dbTx)
}

func (tx *Tx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.historySeek(name, tx.Tx, key, ts)
}

func (tx *RwTx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.historySeek(name, tx.RwTx, key, ts)
}

func (tx *tx) indexRange(name kv.InvertedIdx, dbTx kv.Tx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	timestamps, err = tx.aggtx.IndexRange(name, k, fromTs, toTs, asc, limit, dbTx)
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
	it, err := tx.aggtx.HistoryRange(name, fromTs, toTs, asc, limit, dbTx)
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

func (tx *tx) historyKeyTxNumRange(name kv.Domain, dbTx kv.Tx, fromTs, toTs int, asc order.By, limit int) (stream.KU64, error) {
	it, err := tx.aggtx.HistoryKeyTxNumRange(name, fromTs, toTs, asc, limit, dbTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

func (tx *Tx) HistoryKeyTxNumRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KU64, error) {
	return tx.historyKeyTxNumRange(name, tx.Tx, fromTs, toTs, asc, limit)
}

func (tx *RwTx) HistoryKeyTxNumRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KU64, error) {
	return tx.historyKeyTxNumRange(name, tx.RwTx, fromTs, toTs, asc, limit)
}

// Write methods

func (tx *tx) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte) error {
	panic("implement me pls. or use SharedDomains")
}
func (tx *tx) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte) error {
	panic("implement me pls. or use SharedDomains")
}
func (tx *tx) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	panic("implement me pls. or use SharedDomains")
}

// Debug methods

func (tx *Tx) RangeLatest(domain kv.Domain, from, to []byte, limit int) (stream.KV, error) {
	return tx.rangeLatest(domain, tx.Tx, from, to, limit)
}

func (tx *RwTx) RangeLatest(domain kv.Domain, from, to []byte, limit int) (stream.KV, error) {
	return tx.rangeLatest(domain, tx.RwTx, from, to, limit)
}

func (tx *tx) rangeLatest(domain kv.Domain, dbTx kv.Tx, from, to []byte, limit int) (stream.KV, error) {
	return tx.aggtx.DebugRangeLatest(dbTx, domain, from, to, limit)
}

func (tx *Tx) GetLatestFromDB(domain kv.Domain, k []byte) (v []byte, step kv.Step, found bool, err error) {
	return tx.getLatestFromDB(domain, tx.Tx, k)
}

func (tx *RwTx) GetLatestFromDB(domain kv.Domain, k []byte) (v []byte, step kv.Step, found bool, err error) {
	return tx.getLatestFromDB(domain, tx.RwTx, k)
}

func (tx *RwTx) TraceKey(domain kv.Domain, k []byte, fromTxNum, toTxNum uint64) (stream.U64V, error) {
	return tx.aggtx.DebugTraceKey(tx.ctx, domain, k, fromTxNum, toTxNum, tx.RwTx)
}

func (tx *tx) getLatestFromDB(domain kv.Domain, dbTx kv.Tx, k []byte) (v []byte, step kv.Step, found bool, err error) {
	return tx.aggtx.DebugGetLatestFromDB(domain, k, dbTx)
}

func (tx *tx) GetLatestFromFiles(domain kv.Domain, k []byte, maxTxNum uint64) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	return tx.aggtx.DebugGetLatestFromFiles(domain, k, maxTxNum)
}

func (tx *Tx) TraceKey(domain kv.Domain, k []byte, fromTxNum, toTxNum uint64) (stream.U64V, error) {
	return tx.aggtx.DebugTraceKey(tx.ctx, domain, k, fromTxNum, toTxNum, tx.Tx)
}

func (db *DB) DomainTables(domain ...kv.Domain) []string {
	return db.stateFiles.DomainTables(domain...)
}
func (db *DB) InvertedIdxTables(domain ...kv.InvertedIdx) []string {
	return db.stateFiles.InvertedIdxTables(domain...)
}
func (db *DB) BuildMissedAccessors(ctx context.Context, workers int) (err error) {
	return db.stateFiles.BuildMissedAccessors(ctx, workers)
}
func (db *DB) EnableReadAhead() kv.TemporalDebugDB {
	db.stateFiles.MadvNormal()
	return db
}

func (db *DB) DisableReadAhead() {
	db.stateFiles.DisableReadAhead()
}

func (db *DB) Files() []string {
	return db.stateFiles.Files()
}

func (db *DB) MergeLoop(ctx context.Context) error {
	return db.stateFiles.MergeLoop(ctx)
}

func (tx *Tx) DomainFiles(domain ...kv.Domain) kv.VisibleFiles {
	return tx.aggtx.DomainFiles(domain...)
}
func (tx *Tx) CurrentDomainVersion(domain kv.Domain) version.Version {
	return tx.aggtx.CurrentDomainVersion(domain)
}
func (tx *tx) TxNumsInFiles(domains ...kv.Domain) (minTxNum uint64) {
	return tx.aggtx.TxNumsInFiles(domains...)
}

func (tx *RwTx) DomainFiles(domain ...kv.Domain) kv.VisibleFiles {
	return tx.aggtx.DomainFiles(domain...)
}
func (tx *RwTx) CurrentDomainVersion(domain kv.Domain) version.Version {
	return tx.aggtx.CurrentDomainVersion(domain)
}
func (tx *RwTx) PruneSmallBatches(ctx context.Context, timeout time.Duration) (haveMore bool, err error) {
	return tx.aggtx.PruneSmallBatches(ctx, timeout, tx.RwTx)
}
func (tx *RwTx) Unwind(ctx context.Context, txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	return tx.aggtx.Unwind(ctx, tx.RwTx, txNumUnwindTo, changeset)
}

func (tx *tx) historyStartFrom(name kv.Domain, roTx kv.Tx) uint64 {
	return tx.aggtx.HistoryStartFrom(name, roTx)
}
func (tx *Tx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.historyStartFrom(name, tx.Tx)
}
func (tx *RwTx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.historyStartFrom(name, tx.RwTx)
}
func (tx *Tx) DomainProgress(domain kv.Domain) uint64 {
	return tx.aggtx.DomainProgress(domain, tx.Tx)
}
func (tx *RwTx) DomainProgress(domain kv.Domain) uint64 {
	return tx.aggtx.DomainProgress(domain, tx.RwTx)
}
func (tx *Tx) DomainVisibleEnd(domain kv.Domain) (uint64, bool) {
	return tx.visibleEnds.get(tx, domain)
}
func (tx *RwTx) DomainVisibleEnd(domain kv.Domain) (uint64, bool) {
	return tx.aggtx.DomainVisibleEnd(domain, tx.RwTx)
}
func (tx *Tx) IIProgress(domain kv.InvertedIdx) uint64 {
	return tx.aggtx.IIProgress(domain, tx.Tx)
}
func (tx *RwTx) IIProgress(domain kv.InvertedIdx) uint64 {
	return tx.aggtx.IIProgress(domain, tx.RwTx)
}

func (tx *tx) dirs() datadir.Dirs   { return tx.aggtx.Dirs() }
func (tx *Tx) Dirs() datadir.Dirs   { return tx.dirs() }
func (tx *RwTx) Dirs() datadir.Dirs { return tx.dirs() }

func (tx *tx) stepSize() uint64 {
	return tx.aggtx.StepSize()
}
func (tx *Tx) StepSize() uint64 { return tx.stepSize() }
func (tx *RwTx) StepSize() uint64 {
	return tx.stepSize()
}
