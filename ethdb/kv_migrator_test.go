package ethdb

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/require"
)

func TestBucketCRUD(t *testing.T) {
	require := require.New(t)
	kv := NewLMDB().InMem().MustOpen()
	defer kv.Close()

	ctx := context.Background()
	tx, err := kv.Begin(ctx, nil, RW)
	require.NoError(err)
	defer tx.Rollback()

	normalBucket := dbutils.Buckets[15]
	deprecatedBucket := dbutils.DeprecatedBuckets[0]
	migrator, ok := tx.(BucketMigrator)
	if !ok {
		return
	}

	// check thad buckets have unique DBI's
	uniquness := map[dbutils.DBI]bool{}
	castedKv := kv.(*LmdbKV)
	for _, bucketCfg := range castedKv.buckets {
		if bucketCfg.DBI == NonExistingDBI {
			continue
		}
		_, ok := uniquness[bucketCfg.DBI]
		require.False(ok)
		uniquness[bucketCfg.DBI] = true
	}

	require.True(migrator.ExistsBucket(normalBucket))
	require.True(errors.Is(migrator.DropBucket(normalBucket), ErrAttemptToDeleteNonDeprecatedBucket))

	require.False(migrator.ExistsBucket(deprecatedBucket))
	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.DropBucket(deprecatedBucket))
	require.False(migrator.ExistsBucket(deprecatedBucket))

	require.NoError(migrator.CreateBucket(deprecatedBucket))
	require.True(migrator.ExistsBucket(deprecatedBucket))

	err = tx.Cursor(deprecatedBucket).Put([]byte{1}, []byte{1})
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
	path := os.TempDir() + "/tm1"
	err := os.RemoveAll(path)
	if err != nil {
		t.Fatal(err)
	}
	db1 := NewLMDB().Path(path).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).MustOpen()
	db1.Close()

	db2 := NewLMDB().Path(path).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).ReadOnly().MustOpen()

	tx, err := db2.Begin(context.Background(), nil, RO)
	if err != nil {
		t.Fatal(err)
	}

	c := tx.Cursor(dbutils.HeaderPrefix)
	_, _, err = c.Seek([]byte("some prefix"))
	if err != nil {
		t.Fatal(err)
	}
}
