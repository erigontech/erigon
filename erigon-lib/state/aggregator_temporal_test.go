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

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
)

func wrapTxWithCtx(tx kv.Tx, ctx *AggregatorRoTx) *txWithCtx {
	return &txWithCtx{MdbxTx: tx.(*mdbx.MdbxTx), filesTx: ctx}
}

type dbWithCtx struct {
	kv.RwDB
	agg *Aggregator
}

func New(db kv.RwDB, agg *Aggregator) (*dbWithCtx, error) {
	return &dbWithCtx{RwDB: db, agg: agg}, nil
}
func (db *dbWithCtx) Agg() any            { return db.agg }
func (db *dbWithCtx) InternalDB() kv.RwDB { return db.RwDB }

func (db *dbWithCtx) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	kvTx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &txWithCtx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.filesTx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *dbWithCtx) ViewTemporal(ctx context.Context, f func(tx kv.TemporalTx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// TODO: it's temporary method, allowing inject TemproalTx without changing code. But it's not type-safe.
func (db *dbWithCtx) BeginRo(ctx context.Context) (kv.Tx, error) {
	return db.BeginTemporalRo(ctx)
}
func (db *dbWithCtx) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (db *dbWithCtx) BeginTemporalRw(ctx context.Context) (kv.TemporalRwTx, error) {
	kvTx, err := db.RwDB.BeginRw(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &txWithCtx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.filesTx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *dbWithCtx) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRw(ctx)
}
func (db *dbWithCtx) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
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

func (db *dbWithCtx) BeginTemporalRwNosync(ctx context.Context) (kv.RwTx, error) {
	kvTx, err := db.RwDB.BeginRwNosync(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	tx := &txWithCtx{MdbxTx: kvTx.(*mdbx.MdbxTx), db: db, ctx: ctx}

	tx.filesTx = db.agg.BeginFilesRo()
	return tx, nil
}
func (db *dbWithCtx) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return db.BeginTemporalRwNosync(ctx) //nolint:gocritic
}
func (db *dbWithCtx) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
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

func (db *dbWithCtx) Close() {
	db.RwDB.Close()
	db.agg.Close()
}

func (db *dbWithCtx) OnFreeze(f kv.OnFreezeFunc) { db.agg.OnFreeze(f) }

type txWithCtx struct {
	*mdbx.MdbxTx
	db               *dbWithCtx
	filesTx          *AggregatorRoTx
	resourcesToClose []kv.Closer
	ctx              context.Context
}

func (tx *txWithCtx) ForceReopenAggCtx() {
	tx.filesTx.Close()
	tx.filesTx = tx.Agg().BeginFilesRo()
}
func (tx *txWithCtx) FreezeInfo() kv.FreezeInfo { return tx.filesTx }
func (tx *txWithCtx) Debug() kv.TemporalDebugTx { return tx }

func (tx *txWithCtx) WarmupDB(force bool) error { return tx.MdbxTx.WarmupDB(force) }
func (tx *txWithCtx) LockDBInRam() error        { return tx.MdbxTx.LockDBInRam() }
func (tx *txWithCtx) AggTx() any                { return tx.filesTx }
func (tx *txWithCtx) Agg() *Aggregator          { return tx.db.agg }
func (tx *txWithCtx) Rollback() {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	mdbxTx.Rollback()
}
func (tx *txWithCtx) autoClose() {
	for _, closer := range tx.resourcesToClose {
		closer.Close()
	}
	tx.filesTx.Close()
}
func (tx *txWithCtx) Commit() error {
	tx.autoClose()
	if tx.MdbxTx == nil { // invariant: it's safe to call Commit/Rollback multiple times
		return nil
	}
	mdbxTx := tx.MdbxTx
	tx.MdbxTx = nil
	return mdbxTx.Commit()
}

func (tx *txWithCtx) HistoryStartFrom(name kv.Domain) uint64 {
	return tx.filesTx.HistoryStartFrom(name)
}

func (tx *txWithCtx) RangeAsOf(name kv.Domain, fromKey, toKey []byte, asOfTs uint64, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.filesTx.RangeAsOf(tx.ctx, tx.MdbxTx, name, fromKey, toKey, asOfTs, asc, limit)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

func (tx *txWithCtx) GetLatest(name kv.Domain, k []byte) (v []byte, step uint64, err error) {
	v, step, ok, err := tx.filesTx.GetLatest(name, k, tx.MdbxTx)
	if err != nil {
		return nil, step, err
	}
	if !ok {
		return nil, step, nil
	}
	return v, step, nil
}
func (tx *txWithCtx) GetAsOf(name kv.Domain, k []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.filesTx.GetAsOf(name, k, ts, tx.MdbxTx)
}

func (tx *txWithCtx) HistorySeek(name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	return tx.filesTx.HistorySeek(name, key, ts, tx.MdbxTx)
}

func (tx *txWithCtx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps stream.U64, err error) {
	timestamps, err = tx.filesTx.IndexRange(name, k, fromTs, toTs, asc, limit, tx.MdbxTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, timestamps)
	return timestamps, nil
}

func (tx *txWithCtx) HistoryRange(name kv.Domain, fromTs, toTs int, asc order.By, limit int) (stream.KV, error) {
	it, err := tx.filesTx.HistoryRange(name, fromTs, toTs, asc, limit, tx.MdbxTx)
	if err != nil {
		return nil, err
	}
	tx.resourcesToClose = append(tx.resourcesToClose, it)
	return it, nil
}

// Debug methods

func (tx *txWithCtx) RangeLatest(domain kv.Domain, from, to []byte, limit int) (stream.KV, error) {
	return tx.filesTx.DebugRangeLatest(tx.MdbxTx, domain, from, to, limit)
}
