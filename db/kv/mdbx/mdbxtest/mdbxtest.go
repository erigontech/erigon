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

package mdbxtest

import (
	"context"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

// InMem is InMem tuned for tests: the temp dir is left to the testing framework,
// and the map size is capped because parallel unit tests pile 16GB VA
// reservations into the Go race heap window ("too many address space collisions
// for -race mode"). Benchmarks run sequentially and can need the full map.
func InMem(tb testing.TB, opts mdbx.MdbxOpts, tmpDir string) mdbx.MdbxOpts {
	tb.Helper()
	opts = opts.InMem(tmpDir).AutoRemove(false).DirtySpace(uint64(2 * datasize.MB))
	if _, isBench := tb.(*testing.B); !isBench {
		opts = opts.MapSize(1 * datasize.GB)
	}
	return opts
}

func inMemOpts(tb testing.TB, tmpDir string, label kv.Label) mdbx.MdbxOpts {
	opts := mdbx.New(label, log.New())
	if tb == nil {
		return opts.InMem(tmpDir)
	}
	return InMem(tb, opts, tmpDir)
}

func New(tb testing.TB, tmpDir string, label kv.Label) kv.RwDB {
	return inMemOpts(tb, tmpDir, label).MustOpen()
}

func NewChainDB(tb testing.TB, tmpDir string) kv.RwDB {
	return inMemOpts(tb, tmpDir, dbcfg.ChainDB).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
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
