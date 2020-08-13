package ethdb

import (
	"context"
	"errors"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBucketCRUD(t *testing.T) {
	kv := NewLMDB().InMem().MustOpen()
	defer kv.Close()

	ctx := context.Background()
	if err := kv.Update(ctx, func(tx Tx) error {
		normalBucketMigrator, ok := tx.Bucket(dbutils.Buckets[15]).(BucketMigrator)
		if !ok {
			return nil
		}
		deprecatedBucket := tx.Bucket(dbutils.DeprecatedBuckets[0])
		deprecatedBucketMigrator, ok := deprecatedBucket.(BucketMigrator)
		if !ok {
			return nil
		}

		require := require.New(t)

		// check thad buckets have unique DBI's
		uniquness := map[lmdb.DBI]bool{}
		castedKv := kv.(*LmdbKV)
		for _, dbi := range castedKv.buckets {
			if dbi == NonExistingDBI {
				continue
			}
			_, ok := uniquness[dbi]
			require.False(ok)
			uniquness[dbi] = true
		}

		require.True(normalBucketMigrator.Exists())
		require.True(errors.Is(normalBucketMigrator.Drop(), ErrAttemptToDeleteNonDeprecatedBucket))

		require.False(deprecatedBucketMigrator.Exists())
		require.NoError(deprecatedBucketMigrator.Create())
		require.True(deprecatedBucketMigrator.Exists())

		require.NoError(deprecatedBucketMigrator.Drop())
		require.False(deprecatedBucketMigrator.Exists())

		require.NoError(deprecatedBucketMigrator.Create())
		require.True(deprecatedBucketMigrator.Exists())

		err := deprecatedBucket.Put([]byte{1}, []byte{1})
		require.NoError(err)
		v, err := deprecatedBucket.Get([]byte{1})
		require.NoError(err)
		require.Equal([]byte{1}, v)

		// check thad buckets have unique DBI's
		uniquness = map[lmdb.DBI]bool{}
		for _, dbi := range castedKv.buckets {
			if dbi == NonExistingDBI {
				continue
			}
			_, ok := uniquness[dbi]
			require.False(ok)
			uniquness[dbi] = true
		}
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}
