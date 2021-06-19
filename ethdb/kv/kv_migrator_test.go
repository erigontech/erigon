package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/stretchr/testify/require"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	kv := NewMemKV()
	defer kv.Close()

	ctx := context.Background()
	tx, err := kv.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	normalBucket := dbutils.Buckets[15]
	deprecatedBucket := dbutils.DeprecatedBuckets[0]
	migrator, ok := tx.(ethdb.BucketMigrator)
	if !ok {
		return
	}

	// check thad buckets have unique DBI's
	uniquness := map[dbutils.DBI]bool{}
	castedKv, ok := kv.(*MdbxKV)
	if !ok {
		t.Skip()
	}
	for _, bucketCfg := range castedKv.buckets {
		if bucketCfg.DBI == NonExistingDBI {
			continue
		}
		_, ok := uniquness[bucketCfg.DBI]
		require.False(ok)
		uniquness[bucketCfg.DBI] = true
	}

	require.True(migrator.ExistsBucket(normalBucket))
	require.True(errors.Is(migrator.DropBucket(normalBucket), ethdb.ErrAttemptToDeleteNonDeprecatedBucket))

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

	buckets, err := migrator.ExistingBuckets()
	require.NoError(err)
	require.True(len(buckets) > 10)

	// check thad buckets have unique DBI's
	uniquness = map[dbutils.DBI]bool{}
	for _, bucketCfg := range castedKv.buckets {
		if bucketCfg.DBI == NonExistingDBI {
			continue
		}
		_, ok := uniquness[bucketCfg.DBI]
		require.False(ok)
		uniquness[bucketCfg.DBI] = true
	}
}

func TestReadOnlyMode(t *testing.T) {
	path := t.TempDir()
	db1 := NewMDBX().Path(path).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketConfigItem{},
		}
	}).MustOpen()
	db1.Close()

	db2 := NewMDBX().Readonly().Path(path).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket: dbutils.BucketConfigItem{},
		}
	}).MustOpen()
	defer db2.Close()

	tx, err := db2.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.Cursor(dbutils.HeadersBucket)
	require.NoError(t, err)
	_, _, err = c.Seek([]byte("some prefix"))
	require.NoError(t, err)
}
