package ethdb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestManagedTx(t *testing.T) {
	defaultConfig := dbutils.BucketsConfigs
	defer func() {
		dbutils.BucketsConfigs = defaultConfig
	}()

	bucketID := 0
	bucket1 := dbutils.Buckets[bucketID]
	bucket2 := dbutils.Buckets[bucketID+1]
	writeDBs, readDBs, closeAll := setupDatabases(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return map[string]dbutils.BucketConfigItem{
			bucket1: {
				Flags:                     dbutils.DupSort,
				AutoDupSortKeysConversion: true,
				DupToLen:                  4,
				DupFromLen:                6,
			},
			bucket2: {
				Flags: 0,
			},
		}
	})
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		if err := db.Update(ctx, func(tx ethdb.Tx) error {
			c := tx.Cursor(bucket1)
			c1 := tx.Cursor(bucket2)
			require.NoError(t, c.Append([]byte{0}, []byte{1}))
			require.NoError(t, c1.Append([]byte{0}, []byte{1}))
			require.NoError(t, c.Append([]byte{0, 0, 0, 0, 0, 1}, []byte{1})) // prefixes of len=FromLen for DupSort test (other keys must be <ToLen)
			require.NoError(t, c1.Append([]byte{0, 0, 0, 0, 0, 1}, []byte{1}))
			require.NoError(t, c.Append([]byte{0, 0, 0, 0, 0, 2}, []byte{1}))
			require.NoError(t, c1.Append([]byte{0, 0, 0, 0, 0, 2}, []byte{1}))
			require.NoError(t, c.Append([]byte{0, 0, 1}, []byte{1}))
			require.NoError(t, c1.Append([]byte{0, 0, 1}, []byte{1}))
			for i := uint8(1); i < 10; i++ {
				require.NoError(t, c.Append([]byte{i}, []byte{1}))
				require.NoError(t, c1.Append([]byte{i}, []byte{1}))
			}
			require.NoError(t, c.Put([]byte{0, 0, 0, 0, 0, 1}, []byte{2}))
			require.NoError(t, c1.Put([]byte{0, 0, 0, 0, 0, 1}, []byte{2}))
			return nil
		}); err != nil {
			require.NoError(t, err)
		}
	}

	for _, db := range readDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		switch db.(type) {
		case *ethdb.RemoteKV:
		default:
			continue
		}

		t.Run("ctx cancel "+msg, func(t *testing.T) {
			t.Skip("probably need enable after go 1.4")
			testCtxCancel(t, db, bucket1)
		})
		t.Run("filter "+msg, func(t *testing.T) {
			//testPrefixFilter(t, db, bucket1)
		})
		t.Run("multiple cursors "+msg, func(t *testing.T) {
			testMultiCursor(t, db, bucket1, bucket2)
		})
	}
}

func setupDatabases(f ethdb.BucketConfigsFunc) (writeDBs []ethdb.KV, readDBs []ethdb.KV, close func()) {
	writeDBs = []ethdb.KV{
		ethdb.NewLMDB().InMem().WithBucketsConfig(f).MustOpen(),
		ethdb.NewMDBX().InMem().WithBucketsConfig(f).MustOpen(),
		ethdb.NewLMDB().InMem().WithBucketsConfig(f).MustOpen(), // for remote db
	}

	conn := bufconn.Listen(1024 * 1024)

	rdb, _ := ethdb.NewRemote().InMem(conn).MustOpen()
	readDBs = []ethdb.KV{
		writeDBs[0],
		writeDBs[1],
		rdb,
	}

	grpcServer := grpc.NewServer()
	go func() {
		remote.RegisterKVServer(grpcServer, remotedbserver.NewKvServer(writeDBs[1]))
		if err := grpcServer.Serve(conn); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	return writeDBs, readDBs, func() {
		grpcServer.Stop()

		if err := conn.Close(); err != nil {
			panic(err)
		}

		for _, db := range readDBs {
			db.Close()
		}

		for _, db := range writeDBs {
			db.Close()
		}
	}
}

func testPrefixFilter(t *testing.T, db ethdb.KV, bucket1 string) {
	assert := assert.New(t)

	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(bucket1).Prefix([]byte{2})
		counter := 0
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(1, counter)

		counter = 0
		if err := ethdb.ForEach(c, func(k, _ []byte) (bool, error) {
			counter++
			return true, nil
		}); err != nil {
			return err
		}
		assert.Equal(1, counter)

		k2, _, err2 := c.Seek([]byte{2})
		assert.NoError(err2)
		assert.Equal([]byte{2}, k2)

		c = tx.Cursor(bucket1)
		counter = 0
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(13, counter)

		counter = 0
		if err := ethdb.ForEach(c, func(_, _ []byte) (bool, error) {
			counter++
			return true, nil
		}); err != nil {
			return err
		}
		assert.Equal(13, counter)

		k2, _, err2 = c.Seek([]byte{2})
		assert.NoError(err2)
		assert.Equal([]byte{2}, k2)
		return nil
	}); err != nil {
		assert.NoError(err)
	}

}
func testCtxCancel(t *testing.T, db ethdb.KV, bucket1 string) {
	assert := assert.New(t)
	cancelableCtx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	if err := db.View(cancelableCtx, func(tx ethdb.Tx) error {
		c := tx.Cursor(bucket1)
		for {
			for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
			}
		}
	}); err != nil {
		assert.True(errors.Is(context.DeadlineExceeded, err))
	}
}

func testMultiCursor(t *testing.T, db ethdb.KV, bucket1, bucket2 string) {
	assert, ctx := assert.New(t), context.Background()

	if err := db.View(ctx, func(tx ethdb.Tx) error {
		c1 := tx.Cursor(bucket1)
		c2 := tx.Cursor(bucket2)

		k1, v1, err := c1.First()
		assert.NoError(err)
		k2, v2, err := c2.First()
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		assert.NoError(err)
		k2, v2, err = c2.Next()
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0, 0, 0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0, 0, 0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		assert.NoError(err)
		k2, v2, err = c2.Next()
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0, 0, 0})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0, 0, 0})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		assert.NoError(err)
		k2, v2, err = c2.Next()
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)
		k1, v1, err = c1.Seek([]byte{2})
		assert.NoError(err)
		k2, v2, err = c2.Seek([]byte{2})
		assert.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		return nil
	}); err != nil {
		assert.NoError(err)
	}
}

func TestMultipleBuckets(t *testing.T) {
	writeDBs, readDBs, closeAll := setupDatabases(ethdb.DefaultBucketConfigs)
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		t.Run("FillBuckets "+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				c := tx.Cursor(dbutils.Buckets[0])
				for i := uint8(0); i < 10; i++ {
					require.NoError(t, c.Put([]byte{i}, []byte{i}))
				}
				c2 := tx.Cursor(dbutils.Buckets[1])
				for i := uint8(0); i < 12; i++ {
					require.NoError(t, c2.Put([]byte{i}, []byte{i}))
				}

				// delete from first bucket key 5, then will seek on it and expect to see key 6
				if err := c.Delete([]byte{5}); err != nil {
					return err
				}
				// delete non-existing key
				if err := c.Delete([]byte{6, 1}); err != nil {
					return err
				}

				return nil
			}); err != nil {
				require.NoError(t, err)
			}
		})
	}

	for _, db := range readDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		t.Run("MultipleBuckets "+msg, func(t *testing.T) {
			counter2, counter := 0, 0
			var key, value []byte
			err := db.View(ctx, func(tx ethdb.Tx) error {
				c := tx.Cursor(dbutils.Buckets[0])
				for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
					if err != nil {
						return err
					}
					counter++
				}

				c2 := tx.Cursor(dbutils.Buckets[1])
				for k, _, err := c2.First(); k != nil; k, _, err = c2.Next() {
					if err != nil {
						return err
					}
					counter2++
				}

				c3 := tx.Cursor(dbutils.Buckets[0])
				k, v, err := c3.Seek([]byte{5})
				if err != nil {
					return err
				}
				key = common.CopyBytes(k)
				value = common.CopyBytes(v)

				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, 9, counter)
			assert.Equal(t, 12, counter2)
			assert.Equal(t, []byte{6}, key)
			assert.Equal(t, []byte{6}, value)
		})
	}
}

func TestReadAfterPut(t *testing.T) {
	writeDBs, _, closeAll := setupDatabases(ethdb.DefaultBucketConfigs)
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		t.Run("GetAfterPut "+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				c := tx.Cursor(dbutils.Buckets[0])
				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
					require.NoError(t, c.Put([]byte{i}, []byte{i}))
				}

				for i := uint8(0); i < 10; i++ {
					v, err := c.SeekExact([]byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				c2 := tx.Cursor(dbutils.Buckets[1])
				for i := uint8(0); i < 12; i++ {
					require.NoError(t, c2.Put([]byte{i}, []byte{i}))
				}

				for i := uint8(0); i < 12; i++ {
					v, err := c2.SeekExact([]byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				{
					require.NoError(t, c2.Delete([]byte{5}))
					v, err := c2.SeekExact([]byte{5})
					require.NoError(t, err)
					require.Nil(t, v)

					require.NoError(t, c2.Delete([]byte{255})) // delete non-existing key
				}

				return nil
			}); err != nil {
				require.NoError(t, err)
			}
		})

		t.Run("cursor put and delete"+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				c3 := tx.Cursor(dbutils.Buckets[2])
				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
					require.NoError(t, c3.Put([]byte{i}, []byte{i}))
				}
				for i := uint8(0); i < 10; i++ {
					v, err := tx.GetOne(dbutils.Buckets[2], []byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				require.NoError(t, c3.Delete([]byte{255})) // delete non-existing key
				return nil
			}); err != nil {
				t.Error(err)
			}

			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				c3 := tx.Cursor(dbutils.Buckets[2])
				require.NoError(t, c3.Delete([]byte{5}))
				v, err := tx.GetOne(dbutils.Buckets[2], []byte{5})
				require.NoError(t, err)
				require.Nil(t, v)
				return nil
			}); err != nil {
				t.Error(err)
			}
		})
	}
}
