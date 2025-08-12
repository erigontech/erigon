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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func New(tmpDir string, label kv.Label) kv.RwDB {
	return mdbx.New(label, log.New()).InMem(tmpDir).MustOpen()
}

func NewStateDB(tmpDir string) kv.RwDB {
	return mdbx.New(kv.ChainDB, log.New()).InMem(tmpDir).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
}

func NewWithLabel(tmpDir string, label kv.Label) kv.RwDB {
	return mdbx.New(label, log.New()).InMem(tmpDir).MustOpen()
}

func NewTestDB(tb testing.TB, label kv.Label) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir, label)
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

func NewTestPoolDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir, kv.TxPoolDB)
	tb.Cleanup(db.Close)
	return db
}

func NewTestDownloaderDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir, kv.DownloaderDB)
	tb.Cleanup(db.Close)
	return db
}

func NewTestTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tmpDir, kv.ChainDB)
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}
