// Copyright 2021 The Erigon Authors
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

package memdb

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

func New(tmpDir string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem(tmpDir).MustOpen()
}

func NewStateDB(tmpDir string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem(tmpDir).GrowthStep(32 * datasize.MB).
		MapSize(2 * datasize.GB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
}

func NewPoolDB(tmpDir string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.TxPoolDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).MustOpen()
}
func NewDownloaderDB(tmpDir string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.DownloaderDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).MustOpen()
}
func NewSentryDB(tmpDir string) kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem(tmpDir).Label(kv.SentryDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.SentryTablesCfg }).MustOpen()
}

func NewTestDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	tb.Helper()
	db := New(tmpDir)
	tb.Cleanup(db.Close)
	return db
}

func BeginRw(tb testing.TB, db kv.RwDB) kv.RwTx {
	tb.Helper()
	tx, err := db.BeginRw(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return tx
}

func BeginRo(tb testing.TB, db kv.RoDB) kv.Tx {
	tb.Helper()
	tx, err := db.BeginRo(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return tx
}

func NewTestPoolDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := NewPoolDB(tmpDir)
	tb.Cleanup(db.Close)
	return db
}

func NewTestDownloaderDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := NewDownloaderDB(tmpDir)
	tb.Cleanup(db.Close)
	return db
}

func NewTestSentrylDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := NewPoolDB(tmpDir)
	tb.Cleanup(db.Close)
	return db
}

func NewTestTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir)
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}

func NewTestPoolTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	db := NewTestPoolDB(tb)
	tx, err := db.BeginRw(context.Background()) //nolint
	if err != nil {
		tb.Fatal(err)
	}
	if tb != nil {
		tb.Cleanup(tx.Rollback)
	}
	return db, tx
}

func NewTestSentryTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	db := NewTestSentrylDB(tb)
	tx, err := db.BeginRw(context.Background()) //nolint
	if err != nil {
		tb.Fatal(err)
	}
	if tb != nil {
		tb.Cleanup(tx.Rollback)
	}
	return db, tx
}
