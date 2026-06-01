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
	"os"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/hybridkv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/memstoredb"
)

// wrapHybrid wraps the MDBX durable backend with a memstoredb delta backend
// when the in-memory flag is enabled and the label is ChainDB. Non-ChainDB
// labels (TxPool, Polygon-bridge, …) keep pure MDBX so that consumer code
// holding hard *mdbx.MdbxTx assertions keeps working in tests.
func wrapHybrid(durable kv.RwDB, label kv.Label) kv.RwDB {
	if !dbg.UseInMemoryKV || label != dbcfg.ChainDB {
		return durable
	}
	return hybridkv.New(label, durable, memstoredb.New(label, nil), kv.IsInMemoryTable)
}

func New(tb testing.TB, tmpDir string, label kv.Label) kv.RwDB {
	durable := mdbx.New(label, log.New()).InMem(tb, tmpDir).MustOpen()
	return wrapHybrid(durable, label)
}

func NewChainDB(tb testing.TB, tmpDir string) kv.RwDB {
	durable := mdbx.New(dbcfg.ChainDB, log.New()).InMem(tb, tmpDir).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	return wrapHybrid(durable, dbcfg.ChainDB)
}

func NewTestDB(tb testing.TB, label kv.Label) kv.RwDB {
	tb.Helper()
	// we can't use tb.TempDir() here because some tests produce names long
	// enough to cause 'file name too long' errors when reused as paths
	dirname, err := os.MkdirTemp("", "testdb-"+string(label)+"-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { dir.RemoveAll(dirname) })
	db := New(tb, dirname, label)
	tb.Cleanup(func() { db.Close() })
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
	db := New(tb, tmpDir, dbcfg.TxPoolDB)
	tb.Cleanup(db.Close)
	return db
}

func NewTestDownloaderDB(tb testing.TB) kv.RwDB {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tb, tmpDir, dbcfg.DownloaderDB)
	tb.Cleanup(db.Close)
	return db
}

func NewTestTx(tb testing.TB) (kv.RwDB, kv.RwTx) {
	tb.Helper()
	tmpDir := tb.TempDir()
	db := New(tb, tmpDir, dbcfg.ChainDB)
	tb.Cleanup(db.Close)
	tx, err := db.BeginRw(context.Background()) //nolint:gocritic
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(tx.Rollback)
	return db, tx
}
