package abstractbench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var boltOriginDb *bolt.DB
var badgerOriginDb *badger.DB
var boltDb ethdb.KV
var badgerDb ethdb.KV

func TestMain(m *testing.M) {
	setupDatabases()
	result := m.Run()
	os.Remove("test")
	os.RemoveAll("test2")
	os.Remove("test3")
	os.RemoveAll("test4")
	os.Exit(result)
}
func setupDatabases() {
	vsize := 100
	keysAmount := 1_000
	ctx := context.Background()
	var errOpen error

	boltDb, errOpen = ethdb.NewBolt().Path("test").Open(ctx)
	if errOpen != nil {
		panic(errOpen)
	}
	badgerDb, errOpen = ethdb.NewBadger().Path("test2").Open(ctx)
	if errOpen != nil {
		panic(errOpen)
	}

	boltOriginDb, errOpen = bolt.Open("test3", 0600, &bolt.Options{})
	if errOpen != nil {
		panic(errOpen)
	}

	badgerOriginDb, errOpen = badger.Open(badger.DefaultOptions("test4"))
	if errOpen != nil {
		panic(errOpen)
	}

	_ = boltOriginDb.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(dbutils.AccountsBucket, false)
		return nil
	})

	now := time.Now()
	if err := boltOriginDb.Update(func(tx *bolt.Tx) error {
		defer fmt.Println("origin bolt filled: ", time.Since(now))
		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket := tx.Bucket(dbutils.AccountsBucket)
			if err := bucket.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	now = time.Now()

	if err := boltDb.Update(ctx, func(tx ethdb.Tx) error {
		defer fmt.Println("abstract bolt filled: ", time.Since(now))

		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket := tx.Bucket(dbutils.AccountsBucket)

			if err := bucket.Put(k, v); err != nil {
				panic(err)
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}
	now = time.Now()

	if err := badgerDb.Update(ctx, func(tx ethdb.Tx) error {
		defer fmt.Println("abstract badger filled: ", time.Since(now))

		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket := tx.Bucket(dbutils.AccountsBucket)
			if err := bucket.Put(k, v); err != nil {
				panic(err)
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}
	now = time.Now()

	if err := badgerOriginDb.Update(func(tx *badger.Txn) error {
		defer fmt.Println("pure badger filled: ", time.Since(now))

		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			_ = tx.Set(append(dbutils.AccountsBucket, k...), v)
		}

		return nil
	}); err != nil {
		panic(err)
	}
}

func BenchmarkCursor(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.Run("abstract bolt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := boltDb.View(ctx, func(tx ethdb.Tx) error {
				c := tx.Bucket(dbutils.AccountsBucket).Cursor()
				for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
					if err != nil {
						return err
					}
					_ = v
				}

				return nil
			}); err != nil {
				panic(err)
			}
		}
	})
	//b.Run("abstract badger", func(b *testing.B) {
	//	for i := 0; i < b.N; i++ {
	//		if err := badgerDb.View(ctx, func(tx *ethdb.Tx) error {
	//			bucket, err := tx.Bucket(dbutils.AccountsBucket)
	//			if err != nil {
	//				return err
	//			}
	//
	//			c, err := bucket.CursorOpts().Cursor()
	//			if err != nil {
	//				return err
	//			}
	//
	//			for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
	//				if err != nil {
	//					return err
	//				}
	//				_ = v
	//			}
	//
	//			return nil
	//		}); err != nil {
	//			panic(err)
	//		}
	//	}
	//})
	b.Run("pure bolt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {

			if err := boltOriginDb.View(func(tx *bolt.Tx) error {
				c := tx.Bucket(dbutils.AccountsBucket).Cursor()

				for k, v := c.First(); k != nil; k, v = c.Next() {
					_ = v
				}

				return nil
			}); err != nil {
				panic(err)
			}
		}
	})
	//b.Run("pure badger", func(b *testing.B) {
	//	valFunc := func(val []byte) error {
	//		_ = val
	//		return nil
	//	}
	//	for i := 0; i < b.N; i++ {
	//		if err := badgerOriginDb.View(func(tx *badger.Txn) error {
	//			opts := badger.DefaultIteratorOptions
	//			opts.Prefix = dbutils.AccountsBucket
	//			it := tx.NewIterator(opts)
	//
	//			for it.Rewind(); it.Valid(); it.Next() {
	//				item := it.Item()
	//				_ = item.Key()
	//				_ = item.Value(valFunc)
	//			}
	//			it.Close()
	//			return nil
	//		}); err != nil {
	//			panic(err)
	//		}
	//	}
	//})
}

func BenchmarkGet(b *testing.B) {
}
