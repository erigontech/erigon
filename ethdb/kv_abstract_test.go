package ethdb_test

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defaultConfig := dbutils.BucketsCfg
	defer func() {
		dbutils.BucketsCfg = defaultConfig
	}()

	bucketID := 0
	bucket1 := dbutils.Buckets[bucketID]
	bucket2 := dbutils.Buckets[bucketID+1]
	dbutils.BucketsCfg[string(bucket1)].IsDupsort = true
	dbutils.BucketsCfg[string(bucket1)].DupFromLen = 6
	dbutils.BucketsCfg[string(bucket1)].DupToLen = 4
	dbutils.BucketsCfg[string(bucket2)].IsDupsort = false

	writeDBs, readDBs, closeAll := setupDatabases()
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		if err := db.Update(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(bucket1)
			b1 := tx.Bucket(bucket2)
			c := b.Cursor()
			c1 := b1.Cursor()
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

		t.Run("NoValues iterator "+msg, func(t *testing.T) {
			testNoValuesIterator(t, db, bucket1)
		})
		t.Run("ctx cancel "+msg, func(t *testing.T) {
			t.Skip("probably need enable after go 1.4")
			testCtxCancel(t, db, bucket1)
		})
		t.Run("filter "+msg, func(t *testing.T) {
			testPrefixFilter(t, db, bucket1)
		})
		t.Run("multiple cursors "+msg, func(t *testing.T) {
			testMultiCursor(t, db, bucket1, bucket2)
		})
	}
}

func setupDatabases() (writeDBs []ethdb.KV, readDBs []ethdb.KV, close func()) {
	writeDBs = []ethdb.KV{
		ethdb.NewBolt().InMem().MustOpen(),
		ethdb.NewLMDB().InMem().MustOpen(), // for remote db
		ethdb.NewBadger().InMem().MustOpen(),
		ethdb.NewLMDB().InMem().MustOpen(),
		ethdb.NewLMDB().InMem().MustOpen(), // for remote2 db
	}

	serverIn, clientOut := io.Pipe()
	clientIn, serverOut := io.Pipe()
	conn := bufconn.Listen(1024 * 1024)

	readDBs = []ethdb.KV{
		writeDBs[0],
		ethdb.NewRemote().InMem(clientIn, clientOut).MustOpen(),
		writeDBs[2],
		writeDBs[3],
		ethdb.NewRemote2().InMem(conn).MustOpen(),
	}

	serverCtx, serverCancel := context.WithCancel(context.Background())
	go func() {
		_ = remotedbserver.Server(serverCtx, writeDBs[1], serverIn, serverOut, nil)
	}()
	grpcServer := grpc.NewServer()
	go func() {
		remote.RegisterKVServer(grpcServer, remotedbserver.NewKvServer(writeDBs[4]))
		if err := grpcServer.Serve(conn); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	return writeDBs, readDBs, func() {
		serverIn.Close()
		serverOut.Close()
		clientIn.Close()
		clientOut.Close()
		grpcServer.Stop()

		conn.Close()

		serverCancel()

		for _, db := range readDBs {
			db.Close()
		}

		for _, db := range writeDBs {
			db.Close()
		}
	}
}

func testPrefixFilter(t *testing.T, db ethdb.KV, bucket1 []byte) {
	assert := assert.New(t)

	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(bucket1)
		c := b.Cursor().Prefix([]byte{2})
		counter := 0
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(1, counter)

		counter = 0
		if err := c.Walk(func(_, _ []byte) (bool, error) {
			counter++
			return true, nil
		}); err != nil {
			return err
		}
		assert.Equal(1, counter)

		k2, _, err2 := c.Seek([]byte{2})
		assert.NoError(err2)
		assert.Equal([]byte{2}, k2)

		c = b.Cursor()
		counter = 0
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(13, counter)

		counter = 0
		if err := c.Walk(func(_, _ []byte) (bool, error) {
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
func testCtxCancel(t *testing.T, db ethdb.KV, bucket1 []byte) {
	assert := assert.New(t)
	cancelableCtx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	if err := db.View(cancelableCtx, func(tx ethdb.Tx) error {
		c := tx.Bucket(bucket1).Cursor()
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

func testNoValuesIterator(t *testing.T, db ethdb.KV, bucket1 []byte) {
	assert, ctx := assert.New(t), context.Background()

	if err := db.View(ctx, func(tx ethdb.Tx) error {
		b := tx.Bucket(bucket1)
		c := b.Cursor()

		k, _, err := c.First()
		assert.NoError(err)
		assert.Equal([]byte{0}, k)
		k, _, err = c.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 0, 0, 0, 1}, k)
		k, _, err = c.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 0, 0, 0, 2}, k)
		k, _, err = c.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 1}, k)
		k, _, err = c.Next()
		assert.NoError(err)
		assert.Equal([]byte{1}, k)
		k, _, err = c.Seek([]byte{0, 1})
		assert.NoError(err)
		assert.Equal([]byte{1}, k)
		k, _, err = c.Seek([]byte{2})
		assert.NoError(err)
		assert.Equal([]byte{2}, k)
		k, _, err = c.Seek([]byte{99})
		assert.NoError(err)
		assert.Nil(k)

		k, _, err = c.First()
		assert.NoError(err)
		assert.Equal([]byte{0}, k)
		k, _, err = c.SeekTo([]byte{0, 0, 0, 0})
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 0, 0, 0, 1}, k)
		k, _, err = c.SeekTo([]byte{2})
		assert.NoError(err)
		assert.Equal([]byte{2}, k)
		k, _, err = c.SeekTo([]byte{99})
		assert.NoError(err)
		assert.Nil(k)
		c2 := b.Cursor().NoValues()

		k, _, err = c2.First()
		assert.NoError(err)
		assert.Equal([]byte{0}, k)
		k, _, err = c2.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 0, 0, 0, 1}, k)
		k, _, err = c2.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 0, 0, 0, 2}, k)
		k, _, err = c2.Next()
		assert.NoError(err)
		assert.Equal([]byte{0, 0, 1}, k)
		k, _, err = c2.Next()
		assert.NoError(err)
		assert.Equal([]byte{1}, k)
		k, _, err = c2.Seek([]byte{0, 1})
		assert.NoError(err)
		assert.Equal([]byte{1}, k)
		k, _, err = c2.Seek([]byte{0})
		assert.NoError(err)
		assert.Equal([]byte{0}, k)
		k, _, err = c.Seek([]byte{2})
		assert.NoError(err)
		assert.Equal([]byte{2}, k)
		k, _, err = c.Seek([]byte{99})
		assert.NoError(err)
		assert.Nil(k)

		return nil
	}); err != nil {
		assert.NoError(err)
	}
}

func testMultiCursor(t *testing.T, db ethdb.KV, bucket1, bucket2 []byte) {
	assert, ctx := assert.New(t), context.Background()

	if err := db.View(ctx, func(tx ethdb.Tx) error {
		c1 := tx.Bucket(bucket1).Cursor()
		c2 := tx.Bucket(bucket2).Cursor()

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
	writeDBs, readDBs, closeAll := setupDatabases()
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		t.Run("FillBuckets "+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				b := tx.Bucket(dbutils.Buckets[0])
				for i := uint8(0); i < 10; i++ {
					require.NoError(t, b.Put([]byte{i}, []byte{i}))
				}
				b2 := tx.Bucket(dbutils.Buckets[1])
				for i := uint8(0); i < 12; i++ {
					require.NoError(t, b2.Put([]byte{i}, []byte{i}))
				}

				// delete from first bucket key 5, then will seek on it and expect to see key 6
				if err := b.Delete([]byte{5}); err != nil {
					return err
				}
				// delete non-existing key
				if err := b.Delete([]byte{6, 1}); err != nil {
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
				c := tx.Bucket(dbutils.Buckets[0]).Cursor()
				for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
					if err != nil {
						return err
					}
					counter++
				}

				c2 := tx.Bucket(dbutils.Buckets[1]).Cursor()
				for k, _, err := c2.First(); k != nil; k, _, err = c2.Next() {
					if err != nil {
						return err
					}
					counter2++
				}

				c3 := tx.Bucket(dbutils.Buckets[0]).Cursor()
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
	writeDBs, _, closeAll := setupDatabases()
	defer closeAll()

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		t.Run("GetAfterPut "+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				b := tx.Bucket(dbutils.Buckets[0])
				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
					require.NoError(t, b.Put([]byte{i}, []byte{i}))
				}

				for i := uint8(0); i < 10; i++ {
					v, err := b.Get([]byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				b2 := tx.Bucket(dbutils.Buckets[1])
				for i := uint8(0); i < 12; i++ {
					require.NoError(t, b2.Put([]byte{i}, []byte{i}))
				}

				for i := uint8(0); i < 12; i++ {
					v, err := b2.Get([]byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				{
					require.NoError(t, b2.Delete([]byte{5}))
					v, err := b2.Get([]byte{5})
					require.NoError(t, err)
					require.Nil(t, v)

					require.NoError(t, b2.Delete([]byte{255})) // delete non-existing key
				}

				return nil
			}); err != nil {
				require.NoError(t, err)
			}
		})

		t.Run("cursor put and delete"+msg, func(t *testing.T) {
			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				b3 := tx.Bucket(dbutils.Buckets[2])
				c3 := b3.Cursor()
				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
					require.NoError(t, c3.Put([]byte{i}, []byte{i}))
				}
				for i := uint8(0); i < 10; i++ {
					v, err := b3.Get([]byte{i})
					require.NoError(t, err)
					require.Equal(t, []byte{i}, v)
				}

				require.NoError(t, c3.Delete([]byte{255})) // delete non-existing key
				return nil
			}); err != nil {
				t.Error(err)
			}

			if err := db.Update(ctx, func(tx ethdb.Tx) error {
				b3 := tx.Bucket(dbutils.Buckets[2])
				require.NoError(t, b3.Cursor().Delete([]byte{5}))
				v, err := b3.Get([]byte{5})
				require.NoError(t, err)
				require.Nil(t, v)
				return nil
			}); err != nil {
				t.Error(err)
			}
		})
	}
}
