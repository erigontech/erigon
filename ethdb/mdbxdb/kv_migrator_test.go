package mdbx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon/ethdb/kv"
	mdbx "github.com/ledgerwatch/erigon/ethdb/mdbxdb"
	"github.com/ledgerwatch/erigon/ethdb/memdb"
	"github.com/stretchr/testify/require"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	db := memdb.NewMemKV()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	normalBucket := kv.Buckets[15]
	deprecatedBucket := kv.DeprecatedBuckets[0]
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
	db1 := mdbx.NewMDBX().Path(path).WithBucketsConfig(func(defaultBuckets kv.BucketsCfg) kv.BucketsCfg {
		return kv.BucketsCfg{
			kv.HeadersBucket: kv.BucketConfigItem{},
		}
	}).MustOpen()
	db1.Close()

	db2 := mdbx.NewMDBX().Readonly().Path(path).WithBucketsConfig(func(defaultBuckets kv.BucketsCfg) kv.BucketsCfg {
		return kv.BucketsCfg{
			kv.HeadersBucket: kv.BucketConfigItem{},
		}
	}).MustOpen()
	defer db2.Close()

	tx, err := db2.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.Cursor(kv.HeadersBucket)
	require.NoError(t, err)
	_, _, err = c.Seek([]byte("some prefix"))
	require.NoError(t, err)
}
