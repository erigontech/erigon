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

//go:build !windows

package mdbx_test

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	db, tx := memdb.NewTestTx(t)

	normalBucket := kv.ChaindataTables[15]
	deprecatedBucket := "none"
	if len(kv.ChaindataDeprecatedTables) > 0 {
		deprecatedBucket = kv.ChaindataDeprecatedTables[0]
	}
	migrator := tx

	// check thad buckets have unique DBI's
	uniquness := map[kv.DBI]bool{}
	castedKv, ok := db.(*mdbx.MdbxKV)
	if !ok {
		t.Skip()
	}
	for _, dbi := range castedKv.AllDBI() {
		if dbi == mdbx.NonExistingDBI {
			continue
		}
		_, ok := uniquness[dbi]
		require.False(ok)
		uniquness[dbi] = true
	}

	require.True(migrator.ExistsTable(normalBucket))
	require.ErrorIs(migrator.DropTable(normalBucket), kv.ErrAttemptToDeleteNonDeprecatedBucket)

	require.False(migrator.ExistsTable(deprecatedBucket))
	require.NoError(migrator.CreateTable(deprecatedBucket))
	require.True(migrator.ExistsTable(deprecatedBucket))

	if deprecatedBucket != "none" {
		require.NoError(migrator.DropTable(deprecatedBucket))
		require.False(migrator.ExistsTable(deprecatedBucket))
	}

	require.NoError(migrator.CreateTable(deprecatedBucket))
	require.True(migrator.ExistsTable(deprecatedBucket))

	c, err := tx.RwCursor(deprecatedBucket)
	require.NoError(err)
	err = c.Put([]byte{1}, []byte{1})
	require.NoError(err)
	v, err := tx.GetOne(deprecatedBucket, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	buckets, err := migrator.ListTables()
	require.NoError(err)
	require.Greater(len(buckets), 10)

	// check thad buckets have unique DBI's
	uniquness = map[kv.DBI]bool{}
	for _, dbi := range castedKv.AllDBI() {
		if dbi == mdbx.NonExistingDBI {
			continue
		}
		_, ok := uniquness[dbi]
		require.False(ok)
		uniquness[dbi] = true
	}
}

func TestReadOnlyMode(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	db1 := mdbx.New(kv.ChainDB, logger).Path(path).MapSize(16 * datasize.MB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableCfgItem{},
		}
	}).MustOpen()
	db1.Close()
	time.Sleep(10 * time.Millisecond) // win sometime need time to close file

	db2 := mdbx.New(kv.ChainDB, logger).Readonly(true).Path(path).MapSize(16 * datasize.MB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableCfgItem{},
		}
	}).MustOpen()
	defer db2.Close()

	tx, err := db2.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.Cursor(kv.Headers)
	require.NoError(t, err)
	defer c.Close()
	_, _, err = c.Seek([]byte("some prefix"))
	require.NoError(t, err)
}
