/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package memdb

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func New() kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem().MustOpen()
}

func NewPoolDB() kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem().Label(kv.TxPoolDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).MustOpen()
}
func NewDownloaderDB() kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem().Label(kv.DownloaderDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).MustOpen()
}
func NewSentryDB() kv.RwDB {
	return mdbx.NewMDBX(log.New()).InMem().Label(kv.SentryDB).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.SentryTablesCfg }).MustOpen()
}

func NewTestDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	db := New()
	tb.Cleanup(db.Close)
	return db
}

func NewTestPoolDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	db := NewPoolDB()
	tb.Cleanup(db.Close)
	return db
}

func NewTestDownloaderDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	db := NewDownloaderDB()
	tb.Cleanup(db.Close)
	return db
}

func NewTestSentrylDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	db := NewPoolDB()
	tb.Cleanup(db.Close)
	return db
}

func NewTestTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	db := New()
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background())
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
