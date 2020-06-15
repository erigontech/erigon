package abstractbench

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var boltOriginDb *bolt.DB

//var badgerOriginDb *badger.DB
var boltKV *ethdb.BoltKV

//var badgerDb ethdb.KV
var lmdbKV *ethdb.LmdbKV

var keysAmount = 1_000_000

func setupDatabases() func() {
	//vsize, ctx := 10, context.Background()

	clean := func() {
		os.Remove("test")
		os.RemoveAll("test2")
		os.Remove("test3")
		os.RemoveAll("test4")
		os.RemoveAll("test5")
	}
	boltKV = ethdb.NewBolt().Path("/Users/alex.sharov/Library/Ethereum/geth-remove-me2/geth/chaindata").ReadOnly().MustOpen().(*ethdb.BoltKV)
	//badgerDb = ethdb.NewBadger().Path("test2").MustOpen()
	lmdbKV = ethdb.NewLMDB().Path("/Users/alex.sharov/Library/Ethereum/geth-remove-me4/geth/chaindata_lmdb").ReadOnly().MustOpen().(*ethdb.LmdbKV)
	//lmdbKV = ethdb.NewLMDB().Path("test4").MustOpen().(*ethdb.LmdbKV)
	var errOpen error
	boltOriginDb, errOpen = bolt.Open("test3", 0600, &bolt.Options{KeysPrefixCompressionDisable: true})
	if errOpen != nil {
		panic(errOpen)
	}

	//badgerOriginDb, errOpen = badger.Open(badger.DefaultOptions("test4"))
	//if errOpen != nil {
	//	panic(errOpen)
	//}

	_ = boltOriginDb.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(dbutils.CurrentStateBucket, false)
		return nil
	})

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

func BenchmarkGet(b *testing.B) {
	clean := setupDatabases()
	defer clean()
	//b.Run("badger", func(b *testing.B) {
	//	db := ethdb.NewObjectDatabase(badgerDb)
	//	for i := 0; i < b.N; i++ {
	//		_, _ = db.Get(dbutils.CurrentStateBucket, k)
	//	}
	//})
	ctx := context.Background()

	rand.Seed(time.Now().Unix())
	b.Run("lmdb1", func(b *testing.B) {
		k := make([]byte, 9)
		k[8] = dbutils.HeaderHashSuffix[0]
		//k1 := make([]byte, 8+32)
		j := rand.Uint64() % 1
		binary.BigEndian.PutUint64(k, j)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalHash, _ := lmdbKV.Get(ctx, dbutils.HeaderPrefix, k)
			_ = canonicalHash
			//copy(k1[8:], canonicalHash)
			//binary.BigEndian.PutUint64(k1, uint64(j))
			//v1, _ := lmdbKV.Get1(ctx, dbutils.HeaderPrefix, k1)
			//v2, _ := lmdbKV.Get1(ctx, dbutils.BlockBodyPrefix, k1)
			//_, _, _ = len(canonicalHash), len(v1), len(v2)
		}
	})

	b.Run("lmdb2", func(b *testing.B) {
		db := ethdb.NewObjectDatabase(lmdbKV)
		k := make([]byte, 9)
		k[8] = dbutils.HeaderHashSuffix[0]
		//k1 := make([]byte, 8+32)
		j := rand.Uint64() % 1
		binary.BigEndian.PutUint64(k, j)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalHash, _ := db.Get(dbutils.HeaderPrefix, k)
			_ = canonicalHash
			//copy(k1[8:], canonicalHash)
			//binary.BigEndian.PutUint64(k1, uint64(j))
			//v1, _ := lmdbKV.Get1(ctx, dbutils.HeaderPrefix, k1)
			//v2, _ := lmdbKV.Get1(ctx, dbutils.BlockBodyPrefix, k1)
			//_, _, _ = len(canonicalHash), len(v1), len(v2)
		}
	})

	//b.Run("bolt", func(b *testing.B) {
	//	k := make([]byte, 9)
	//	k[8] = dbutils.HeaderHashSuffix[0]
	//	//k1 := make([]byte, 8+32)
	//	j := rand.Uint64() % 1
	//	binary.BigEndian.PutUint64(k, j)
	//	b.ResetTimer()
	//	for i := 0; i < b.N; i++ {
	//		canonicalHash, _ := boltKV.Get(ctx, dbutils.HeaderPrefix, k)
	//		_ = canonicalHash
	//		//binary.BigEndian.PutUint64(k1, uint64(j))
	//		//copy(k1[8:], canonicalHash)
	//		//v1, _ := boltKV.Get(ctx, dbutils.HeaderPrefix, k1)
	//		//v2, _ := boltKV.Get(ctx, dbutils.BlockBodyPrefix, k1)
	//		//_, _, _ = len(canonicalHash), len(v1), len(v2)
	//	}
	//})
}

func BenchmarkPut(b *testing.B) {
	clean := setupDatabases()
	defer clean()
	tuples := make(ethdb.MultiPutTuples, 0, keysAmount*3)
	for i := 0; i < keysAmount; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		v := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		tuples = append(tuples, dbutils.CurrentStateBucket, k, v)
	}
	sort.Sort(tuples)

	b.Run("bolt", func(b *testing.B) {
		db := ethdb.NewWrapperBoltDatabase(boltOriginDb).NewBatch()
		for i := 0; i < b.N; i++ {
			_, _ = db.MultiPut(tuples...)
			_, _ = db.Commit()
		}
	})
	//b.Run("badger", func(b *testing.B) {
	//	db := ethdb.NewObjectDatabase(badgerDb)
	//	for i := 0; i < b.N; i++ {
	//		_, _ = db.MultiPut(tuples...)
	//	}
	//})
	b.Run("lmdb", func(b *testing.B) {
		var kv ethdb.KV = lmdbKV
		db := ethdb.NewObjectDatabase(kv).NewBatch()
		for i := 0; i < b.N; i++ {
			_, _ = db.MultiPut(tuples...)
			_, _ = db.Commit()
		}
	})
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
