package abstractbench

import (
	"context"
	"os"
	"testing"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var boltOriginDb *bolt.DB

//var badgerOriginDb *badger.DB
var boltKV *ethdb.BoltKV

//var badgerDb ethdb.KV
var lmdbKV *ethdb.LmdbKV

var keysAmount = 100_000

func setupDatabases() func() {
	//vsize, ctx := 10, context.Background()

	clean := func() {
		os.Remove("test")
		os.RemoveAll("test2")
		os.Remove("test3")
		os.RemoveAll("test4")
		os.RemoveAll("test5")
	}
	//boltKV = ethdb.NewBolt().Path("/Users/alex.sharov/Library/Ethereum/geth-remove-me2/geth/chaindata").ReadOnly().MustOpen().(*ethdb.BoltKV)
	boltKV = ethdb.NewBolt().Path("test1").MustOpen().(*ethdb.BoltKV)
	//badgerDb = ethdb.NewBadger().Path("test2").MustOpen()
	//lmdbKV = ethdb.NewLMDB().Path("/Users/alex.sharov/Library/Ethereum/geth-remove-me4/geth/chaindata_lmdb").ReadOnly().MustOpen().(*ethdb.LmdbKV)
	lmdbKV = ethdb.NewLMDB().Path("test4").MustOpen().(*ethdb.LmdbKV)
	var errOpen error
	o := bolt.DefaultOptions
	o.KeysPrefixCompressionDisable = true
	boltOriginDb, errOpen = bolt.Open("test3", 0600, o)
	if errOpen != nil {
		panic(errOpen)
	}

	//badgerOriginDb, errOpen = badger.Open(badger.DefaultOptions("test4"))
	//if errOpen != nil {
	//	panic(errOpen)
	//}

	if err := boltOriginDb.Update(func(tx *bolt.Tx) error {
		for _, name := range dbutils.Buckets {
			_, createErr := tx.CreateBucketIfNotExists(name, false)
			if createErr != nil {
				return createErr
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}

	//if err := boltOriginDb.Update(func(tx *bolt.Tx) error {
	//	defer func(t time.Time) { fmt.Println("origin bolt filled:", time.Since(t)) }(time.Now())
	//	for i := 0; i < keysAmount; i++ {
	//		v := make([]byte, vsize)
	//		k := make([]byte, 8)
	//		binary.BigEndian.PutUint64(k, uint64(i))
	//		bucket := tx.Bucket(dbutils.CurrentStateBucket)
	//		if err := bucket.Put(k, common.CopyBytes(v)); err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}
	//
	//if err := boltKV.Update(ctx, func(tx ethdb.Tx) error {
	//	defer func(t time.Time) { fmt.Println("abstract bolt filled:", time.Since(t)) }(time.Now())
	//
	//	for i := 0; i < keysAmount; i++ {
	//		v := make([]byte, vsize)
	//		k := make([]byte, 8)
	//		binary.BigEndian.PutUint64(k, uint64(i))
	//		bucket := tx.Bucket(dbutils.CurrentStateBucket)
	//		if err := bucket.Put(k, common.CopyBytes(v)); err != nil {
	//			panic(err)
	//		}
	//	}
	//
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}
	//
	//if err := badgerDb.Update(ctx, func(tx ethdb.Tx) error {
	//	defer func(t time.Time) { fmt.Println("abstract badger filled:", time.Since(t)) }(time.Now())
	//
	//	//for i := 0; i < keysAmount; i++ {
	//	//	v := make([]byte, vsize)
	//	//	k := make([]byte, 8)
	//	//	binary.BigEndian.PutUint64(k, uint64(i))
	//	//	bucket := tx.Bucket(dbutils.CurrentStateBucket)
	//	//	if err := bucket.Put(k, common.CopyBytes(v)); err != nil {
	//	//		panic(err)
	//	//	}
	//	//}
	//
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}
	//
	//if err := badgerOriginDb.Update(func(tx *badger.Txn) error {
	//	defer func(t time.Time) { fmt.Println("pure badger filled:", time.Since(t)) }(time.Now())
	//
	//	for i := 0; i < keysAmount; i++ {
	//		v := make([]byte, vsize)
	//		k := make([]byte, 8)
	//		binary.BigEndian.PutUint64(k, uint64(i))
	//		_ = tx.Set(append(dbutils.CurrentStateBucket, k...), common.CopyBytes(v))
	//	}
	//
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}
	//
	//if err := lmdbKV.Update(ctx, func(tx ethdb.Tx) error {
	//	defer func(t time.Time) { fmt.Println("abstract lmdb filled:", time.Since(t)) }(time.Now())
	//
	//	bucket := tx.Bucket(dbutils.CurrentStateBucket)
	//	for i := 0; i < keysAmount; i++ {
	//		v := make([]byte, vsize)
	//		k := make([]byte, 8)
	//		binary.BigEndian.PutUint64(k, uint64(i))
	//		if err := bucket.Put(k, common.CopyBytes(v)); err != nil {
	//			panic(err)
	//		}
	//	}
	//
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}

	return clean
}

func BenchmarkCursor(b *testing.B) {
	clean := setupDatabases()
	defer clean()

	ctx := context.Background()

	b.ResetTimer()
	b.Run("abstract bolt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := boltKV.View(ctx, func(tx ethdb.Tx) error {
				c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()
				for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
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
	b.Run("abstract lmdb", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := boltKV.View(ctx, func(tx ethdb.Tx) error {
				c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()
				for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
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
	//			for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
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
				c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()

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
