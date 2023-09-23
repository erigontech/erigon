//go:build !windows

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
package mdbx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	db, tx := memdb.NewTestTx(t)

	normalBucket := kv.ChaindataTables[15]
	deprecatedBucket := kv.ChaindataDeprecatedTables[0]
	migrator, ok := tx.(kv.BucketMigrator)
	if !ok {
		return
	}

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

	require.True(migrator.ExistsBucket(normalBucket))
	require.True(errors.Is(migrator.DropBucket(normalBucket), kv.ErrAttemptToDeleteNonDeprecatedBucket))

	require.False(migrator.ExistsBucket(deprecatedBucket))
	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.DropBucket(deprecatedBucket))
	require.False(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	c, err := tx.RwCursor(deprecatedBucket)
	require.NoError(err)
	err = c.Put([]byte{1}, []byte{1})
	require.NoError(err)
	v, err := tx.GetOne(deprecatedBucket, []byte{1})
	require.NoError(err)
	require.Equal([]byte{1}, v)

	buckets, err := migrator.ListBuckets()
	require.NoError(err)
	require.True(len(buckets) > 10)

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
	db1 := mdbx.NewMDBX(logger).Path(path).MapSize(16 * datasize.MB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableCfgItem{},
		}
	}).MustOpen()
	db1.Close()
	time.Sleep(10 * time.Millisecond) // win sometime need time to close file

	db2 := mdbx.NewMDBX(logger).Readonly().Path(path).MapSize(16 * datasize.MB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
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
