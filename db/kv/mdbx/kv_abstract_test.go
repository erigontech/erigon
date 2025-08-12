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

package mdbx_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
)

func TestSequence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	writeDBs, _ := setupDatabases(t, log.New(), func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return defaultBuckets
	})
	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		i, err := tx.ReadSequence(kv.ChaindataTables[0])
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[0], 1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[0], 6)
		require.NoError(t, err)
		require.Equal(t, uint64(1), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[0], 1)
		require.NoError(t, err)
		require.Equal(t, uint64(7), i)

		i, err = tx.ReadSequence(kv.ChaindataTables[1])
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[1], 1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[1], 6)
		require.NoError(t, err)
		require.Equal(t, uint64(1), i)
		i, err = tx.IncrementSequence(kv.ChaindataTables[1], 1)
		require.NoError(t, err)
		require.Equal(t, uint64(7), i)
		tx.Rollback()
	}
}

func TestManagedTx(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	logger := log.New()
	defaultConfig := kv.ChaindataTablesCfg
	defer func() {
		kv.ChaindataTablesCfg = defaultConfig
	}()

	bucketID := 0
	bucket1 := kv.ChaindataTables[bucketID]
	bucket2 := kv.ChaindataTables[bucketID+1]
	writeDBs, readDBs := setupDatabases(t, logger, func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return map[string]kv.TableCfgItem{
			bucket1: {
				//TODO: maybe it is bad to remove both flags but tests fail in another way
				Flags: 0,
			},
			bucket2: {
				Flags: 0,
			},
		}
	})

	ctx := context.Background()

	for _, db := range writeDBs {
		db := db
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		c, err := tx.RwCursor(bucket1)
		require.NoError(t, err)
		c1, err := tx.RwCursor(bucket2)
		require.NoError(t, err)
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
		err = tx.Commit()
		require.NoError(t, err)
	}

	for _, db := range readDBs {
		db := db
		msg := fmt.Sprintf("%T", db)
		switch db.(type) {
		case *remotedb.DB:
		default:
			continue
		}

		t.Run("filter "+msg, func(t *testing.T) {
			//testPrefixFilter(t, db, bucket1)
		})
		t.Run("multiple cursors "+msg, func(t *testing.T) {
			testMultiCursor(t, db, bucket1, bucket2)
		})
	}
}

func TestRemoteKvVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	ctx := context.Background()
	logger := log.New()
	writeDB := mdbx.New(kv.ChainDB, logger).InMem("").MustOpen()
	defer writeDB.Close()
	conn := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	go func() {
		remote.RegisterKVServer(grpcServer, remotedbserver.NewKvServer(ctx, writeDB, nil, nil, nil, logger))
		if err := grpcServer.Serve(conn); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()
	v := gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion)
	// Different Major versions
	v1 := v
	v1.Major++

	cc, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) { return conn.Dial() }))
	require.NoError(t, err)
	a, err := remotedb.NewRemote(v1, logger, remote.NewKVClient(cc)).Open()
	if err != nil {
		t.Fatalf("%v", err)
	}
	require.False(t, a.EnsureVersionCompatibility())
	// Different Minor versions
	v2 := v
	v2.Minor++
	a, err = remotedb.NewRemote(v2, logger, remote.NewKVClient(cc)).Open()
	if err != nil {
		t.Fatalf("%v", err)
	}
	require.False(t, a.EnsureVersionCompatibility())
	// Different Patch versions
	v3 := v
	v3.Patch++
	a, err = remotedb.NewRemote(v3, logger, remote.NewKVClient(cc)).Open()
	require.NoError(t, err)
	require.True(t, a.EnsureVersionCompatibility())
}

func TestRemoteKvRange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	logger := log.New()
	ctx, writeDB := context.Background(), memdb.NewTestDB(t, kv.ChainDB)
	grpcServer, conn := grpc.NewServer(), bufconn.Listen(1024*1024)
	go func() {
		kvServer := remotedbserver.NewKvServer(ctx, writeDB, nil, nil, nil, logger)
		remote.RegisterKVServer(grpcServer, kvServer)
		if err := grpcServer.Serve(conn); err != nil {
			log.Error("private RPC server fail", "err", err)
		}
	}()

	cc, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) { return conn.Dial() }))
	require.NoError(t, err)
	db, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, remote.NewKVClient(cc)).Open()
	require.NoError(t, err)
	require.True(t, db.EnsureVersionCompatibility())

	require := require.New(t)
	require.NoError(writeDB.Update(ctx, func(tx kv.RwTx) error {
		wc, err := tx.RwCursorDupSort(kv.TblAccountVals)
		require.NoError(err)
		require.NoError(wc.Append([]byte{1}, []byte{1}))
		require.NoError(wc.Append([]byte{1}, []byte{2}))
		require.NoError(wc.Append([]byte{2}, []byte{1}))
		require.NoError(wc.Append([]byte{3}, []byte{1}))
		return nil
	}))

	require.NoError(db.View(ctx, func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.TblAccountVals)
		require.NoError(err)

		k, v, err := c.First()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		// it must be possible to Stream and manipulate cursors in same time
		cnt := 0
		require.NoError(tx.ForEach(kv.TblAccountVals, nil, func(_, _ []byte) error {
			if cnt == 0 {
				k, v, err = c.Next()
				require.NoError(err)
				require.Equal([]byte{1}, k)
				require.Equal([]byte{2}, v)
			}
			cnt++
			return nil
		}))
		require.Equal(4, cnt)

		// remote Tx must provide Snapshots-Isolation-Level: new updates are not visible for old readers
		require.NoError(writeDB.Update(ctx, func(tx kv.RwTx) error {
			require.NoError(tx.Put(kv.TblAccountVals, []byte{4}, []byte{1}))
			return nil
		}))

		k, v, err = c.Last()
		require.NoError(err)
		require.Equal([]byte{3}, k)
		require.Equal([]byte{1}, v)
		return nil
	}))

	err = db.View(ctx, func(tx kv.Tx) error {
		cntRange := func(from, to []byte) (i int) {
			it, err := tx.Range(kv.TblAccountVals, from, to, order.Asc, kv.Unlim)
			require.NoError(err)
			for it.HasNext() {
				_, _, err = it.Next()
				require.NoError(err)
				i++
			}
			return i
		}

		require.Equal(2, cntRange([]byte{2}, []byte{4}))
		require.Equal(4, cntRange(nil, []byte{4}))
		require.Equal(3, cntRange([]byte{2}, nil))
		require.Equal(5, cntRange(nil, nil))
		return nil
	})
	require.NoError(err)

	// Limit
	err = db.View(ctx, func(tx kv.Tx) error {
		cntRange := func(from, to []byte) (i int) {
			it, err := tx.Range(kv.TblAccountVals, from, to, order.Asc, 2)
			require.NoError(err)
			for it.HasNext() {
				_, _, err := it.Next()
				require.NoError(err)
				i++
			}
			return i
		}

		require.Equal(2, cntRange([]byte{2}, []byte{4}))
		require.Equal(2, cntRange(nil, []byte{4}))
		require.Equal(2, cntRange([]byte{2}, nil))
		require.Equal(2, cntRange(nil, nil))
		return nil
	})
	require.NoError(err)

	err = db.View(ctx, func(tx kv.Tx) error {
		cntRange := func(from, to []byte) (i int) {
			it, err := tx.Range(kv.TblAccountVals, from, to, order.Desc, 2)
			require.NoError(err)
			for it.HasNext() {
				_, _, err := it.Next()
				require.NoError(err)
				i++
			}
			return i
		}

		require.Equal(2, cntRange([]byte{4}, []byte{2}))
		require.Equal(0, cntRange(nil, []byte{4}))
		require.Equal(2, cntRange([]byte{2}, nil))
		require.Equal(2, cntRange(nil, nil))
		return nil
	})
	require.NoError(err)
}

func setupDatabases(t *testing.T, logger log.Logger, f mdbx.TableCfgFunc) (writeDBs []kv.RwDB, readDBs []kv.RwDB) {
	t.Helper()
	ctx := context.Background()
	writeDBs = []kv.RwDB{
		mdbx.New(kv.ChainDB, logger).InMem("").WithTableCfg(f).MustOpen(),
		mdbx.New(kv.ChainDB, logger).InMem("").WithTableCfg(f).MustOpen(), // for remote db
	}

	conn := bufconn.Listen(1024 * 1024)

	grpcServer := grpc.NewServer()
	f2 := func() {
		remote.RegisterKVServer(grpcServer, remotedbserver.NewKvServer(ctx, writeDBs[1], nil, nil, nil, logger))
		if err := grpcServer.Serve(conn); err != nil {
			logger.Error("private RPC server fail", "err", err)
		}
	}
	go f2()
	v := gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion)
	cc, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) { return conn.Dial() }))
	require.NoError(t, err)
	rdb, err := remotedb.NewRemote(v, logger, remote.NewKVClient(cc)).Open()
	require.NoError(t, err)
	readDBs = []kv.RwDB{
		writeDBs[0],
		writeDBs[1],
		rdb,
	}

	t.Cleanup(func() {
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

	})
	return writeDBs, readDBs
}

func testMultiCursor(t *testing.T, db kv.RwDB, bucket1, bucket2 string) {
	t.Helper()
	assert, ctx := assert.New(t), context.Background()
	require := require.New(t)

	if err := db.View(ctx, func(tx kv.Tx) error {
		c1, err := tx.Cursor(bucket1)
		if err != nil {
			return err
		}
		c2, err := tx.Cursor(bucket2)
		if err != nil {
			return err
		}

		k1, v1, err := c1.First()
		require.NoError(err)
		k2, v2, err := c2.First()
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		require.NoError(err)
		k2, v2, err = c2.Next()
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0, 0, 0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0, 0, 0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		require.NoError(err)
		k2, v2, err = c2.Next()
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Seek([]byte{0, 0, 0, 0})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{0, 0, 0, 0})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		k1, v1, err = c1.Next()
		require.NoError(err)
		k2, v2, err = c2.Next()
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)
		k1, v1, err = c1.Seek([]byte{2})
		require.NoError(err)
		k2, v2, err = c2.Seek([]byte{2})
		require.NoError(err)
		assert.Equal(k1, k2)
		assert.Equal(v1, v2)

		return nil
	}); err != nil {
		require.NoError(err)
	}
}

//func TestMultipleBuckets(t *testing.T) {
//	writeDBs, readDBs, closeAll := setupDatabases(ethdb.WithChaindataTables)
//	defer closeAll()
//
//	ctx := context.Background()
//
//	for _, db := range writeDBs {
//		db := db
//		msg := fmt.Sprintf("%T", db)
//		t.Run("FillBuckets "+msg, func(t *testing.T) {
//			if err := db.Update(ctx, func(tx ethdb.Tx) error {
//				c := tx.Cursor(dbutils.ChaindataTables[0])
//				for i := uint8(0); i < 10; i++ {
//					require.NoError(t, c.Put([]byte{i}, []byte{i}))
//				}
//				c2 := tx.Cursor(dbutils.ChaindataTables[1])
//				for i := uint8(0); i < 12; i++ {
//					require.NoError(t, c2.Put([]byte{i}, []byte{i}))
//				}
//
//				// delete from first bucket key 5, then will seek on it and expect to see key 6
//				if err := c.Delete([]byte{5}, nil); err != nil {
//					return err
//				}
//				// delete non-existing key
//				if err := c.Delete([]byte{6, 1}, nil); err != nil {
//					return err
//				}
//
//				return nil
//			}); err != nil {
//				require.NoError(t, err)
//			}
//		})
//	}
//
//	for _, db := range readDBs {
//		db := db
//		msg := fmt.Sprintf("%T", db)
//		t.Run("MultipleBuckets "+msg, func(t *testing.T) {
//			counter2, counter := 0, 0
//			var key, value []byte
//			err := db.View(ctx, func(tx ethdb.Tx) error {
//				c := tx.Cursor(dbutils.ChaindataTables[0])
//				for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
//					if err != nil {
//						return err
//					}
//					counter++
//				}
//
//				c2 := tx.Cursor(dbutils.ChaindataTables[1])
//				for k, _, err := c2.First(); k != nil; k, _, err = c2.Next() {
//					if err != nil {
//						return err
//					}
//					counter2++
//				}
//
//				c3 := tx.Cursor(dbutils.ChaindataTables[0])
//				k, v, err := c3.seekInFiles([]byte{5})
//				if err != nil {
//					return err
//				}
//				key = common.CopyBytes(k)
//				value = common.CopyBytes(v)
//
//				return nil
//			})
//			require.NoError(t, err)
//			assert.Equal(t, 9, counter)
//			assert.Equal(t, 12, counter2)
//			assert.Equal(t, []byte{6}, key)
//			assert.Equal(t, []byte{6}, value)
//		})
//	}
//}

//func TestReadAfterPut(t *testing.T) {
//	writeDBs, _, closeAll := setupDatabases(ethdb.WithChaindataTables)
//	defer closeAll()
//
//	ctx := context.Background()
//
//	for _, db := range writeDBs {
//		db := db
//		msg := fmt.Sprintf("%T", db)
//		t.Run("GetAfterPut "+msg, func(t *testing.T) {
//			if err := db.Update(ctx, func(tx ethdb.Tx) error {
//				c := tx.Cursor(dbutils.ChaindataTables[0])
//				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
//					require.NoError(t, c.Put([]byte{i}, []byte{i}))
//				}
//
//				for i := uint8(0); i < 10; i++ {
//					v, err := c.SeekExact([]byte{i})
//					require.NoError(t, err)
//					require.Equal(t, []byte{i}, v)
//				}
//
//				c2 := tx.Cursor(dbutils.ChaindataTables[1])
//				for i := uint8(0); i < 12; i++ {
//					require.NoError(t, c2.Put([]byte{i}, []byte{i}))
//				}
//
//				for i := uint8(0); i < 12; i++ {
//					v, err := c2.SeekExact([]byte{i})
//					require.NoError(t, err)
//					require.Equal(t, []byte{i}, v)
//				}
//
//				{
//					require.NoError(t, c2.Delete([]byte{5}, nil))
//					v, err := c2.SeekExact([]byte{5})
//					require.NoError(t, err)
//					require.Nil(t, v)
//
//					require.NoError(t, c2.Delete([]byte{255}, nil)) // delete non-existing key
//				}
//
//				return nil
//			}); err != nil {
//				require.NoError(t, err)
//			}
//		})
//
//		t.Run("cursor put and delete"+msg, func(t *testing.T) {
//			if err := db.Update(ctx, func(tx ethdb.Tx) error {
//				c3 := tx.Cursor(dbutils.ChaindataTables[2])
//				for i := uint8(0); i < 10; i++ { // don't read in same loop to check that writes don't affect each other (for example by sharing bucket.prefix buffer)
//					require.NoError(t, c3.Put([]byte{i}, []byte{i}))
//				}
//				for i := uint8(0); i < 10; i++ {
//					v, err := tx.GetOne(dbutils.ChaindataTables[2], []byte{i})
//					require.NoError(t, err)
//					require.Equal(t, []byte{i}, v)
//				}
//
//				require.NoError(t, c3.Delete([]byte{255}, nil)) // delete non-existing key
//				return nil
//			}); err != nil {
//				t.Error(err)
//			}
//
//			if err := db.Update(ctx, func(tx ethdb.Tx) error {
//				c3 := tx.Cursor(dbutils.ChaindataTables[2])
//				require.NoError(t, c3.Delete([]byte{5}, nil))
//				v, err := tx.GetOne(dbutils.ChaindataTables[2], []byte{5})
//				require.NoError(t, err)
//				require.Nil(t, v)
//				return nil
//			}); err != nil {
//				t.Error(err)
//			}
//		})
//	}
//}
