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
var boltDb *ethdb.DB
var badgerDb *ethdb.DB

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
	keysAmount := 1_000_0
	ctx := context.Background()
	var err error
	boltDb, err = ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).Path("test"))
	if err != nil {
		panic(err)
	}
	badgerDb, err = ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Badger).Path("test2"))
	if err != nil {
		panic(err)
	}

	boltOriginDb, err = bolt.Open("test3", 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}

	badgerOriginDb, err = badger.Open(badger.DefaultOptions("test4"))
	if err != nil {
		panic(err)
	}

	boltOriginDb.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(dbutils.IntermediateTrieHashBucket, false)
		return nil
	})

	now := time.Now()
	if err = boltOriginDb.Update(func(tx *bolt.Tx) error {
		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)

			err1 := bucket.Put(k, v)
			if err1 != nil {
				panic(err)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Println("origin bolt filled: ", time.Since(now))
	now = time.Now()

	if err = boltDb.Update(ctx, func(tx *ethdb.Tx) error {
		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket, err1 := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err1 != nil {
				panic(err)
			}

			err1 = bucket.Put(k, v)
			if err1 != nil {
				panic(err)
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Println("abstract bolt filled: ", time.Since(now))
	now = time.Now()

	if err = badgerDb.Update(ctx, func(tx *ethdb.Tx) error {
		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			bucket, err1 := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err1 != nil {
				panic(err)
			}

			err1 = bucket.Put(k, v)
			if err1 != nil {
				panic(err)
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Println("abstract badger filled: ", time.Since(now))
	now = time.Now()

	if err = badgerOriginDb.Update(func(tx *badger.Txn) error {
		v := make([]byte, vsize)
		for i := 0; i < keysAmount; i++ {
			k := common.FromHex(fmt.Sprintf("%064x", i))
			tx.Set(append(dbutils.IntermediateTrieHashBucket, k...), v)
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
			if err := boltDb.View(ctx, func(tx *ethdb.Tx) error {
				bucket, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
				if err != nil {
					return err
				}

				c, err := bucket.CursorOpts().Cursor()
				if err != nil {
					return err
				}

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
	//			bucket, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
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
				c := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()

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
	//			opts.Prefix = dbutils.IntermediateTrieHashBucket
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
