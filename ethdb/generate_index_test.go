package ethdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"log"
	"sort"
	"testing"
	"time"
)

func TestName1(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.AccountChangeSetBucket,
		bucketToWrite: []byte("hAT29"),
		fixedBits:     32,
		csWalker: func(cs []byte) Walker {
			fmt.Println("cs count", binary.BigEndian.Uint32(cs[0:4]))
			return changeset.AccountChangeSetBytes(cs)
		},
		cache: nil,
	}
	err = ig.generateIndex()
	if err != nil {
		t.Fatal(err)
	}
}

func TestName11(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.StorageChangeSetBucket,
		bucketToWrite: []byte("hST29"),
		fixedBits:     72,
		csWalker: func(cs []byte) Walker {
			fmt.Println("cs count", binary.BigEndian.Uint32(cs[0:4]))
			return changeset.StorageChangeSetBytes(cs)
		},
		cache: nil,
	}
	err = ig.generateIndex()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegenerateIndexes(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.AccountChangeSetBucket,
		bucketToWrite: []byte("hAT31"),
		fixedBits:     32,
		csWalker: func(cs []byte) Walker {
			fmt.Println("cs count", binary.BigEndian.Uint32(cs[0:4]))
			return changeset.AccountChangeSetBytes(cs)
		},
		cache: nil,
	}
	err = ig.generateIndex()
	if err != nil {
		t.Fatal(err)
	}

	ig2 := IndexGenerator{
		db:            db,
		csBucket:      dbutils.StorageChangeSetBucket,
		bucketToWrite: []byte("hST31"),
		fixedBits:     72,
		csWalker: func(cs []byte) Walker {
			fmt.Println("cs count", binary.BigEndian.Uint32(cs[0:4]))
			return changeset.StorageChangeSetBytes(cs)
		},
		cache: nil,
	}
	err = ig2.generateIndex()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckIndexes(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()

	aib := []byte("hAT31")
	sib := []byte("hST31")
	err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		fmt.Println("process", blockNum, string(k))
		err := changeset.AccountChangeSetBytes(v).Walk(func(key, val []byte) error {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, ^(blockNum))
			find := append(common.CopyBytes(key), b...)
			err := db.Walk(aib, find, 32, func(kk []byte, vv []byte) (b bool, e error) {
				index := dbutils.WrapHistoryIndex(common.CopyBytes(vv))
				if findVal, ok := index.Search(blockNum); ok == false {
					t.Error(blockNum, findVal, common.Bytes2Hex(find))
				}
				return false, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			return nil
		})

		if err != nil {
			t.Fatal("AccountChangeSetBytes Walk err", err)
		}
		return true, nil
	})

	if err != nil {
		t.Fatal("AccountChangeSetBytes Walk err", err)
	}
	err = db.Walk(dbutils.StorageChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		fmt.Println("process", blockNum, string(k))
		err := changeset.StorageChangeSetBytes(v).Walk(func(key, val []byte) error {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, ^(blockNum))
			find := append(key, b...)
			err := db.Walk(sib, find, 72, func(kk []byte, vv []byte) (b bool, e error) {
				index := dbutils.WrapHistoryIndex(common.CopyBytes(vv))
				if findVal, ok := index.Search(blockNum); ok == false {
					t.Error(blockNum, findVal, common.Bytes2Hex(find))
				}
				return false, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			return nil
		})
		if err != nil {
			t.Fatal("storageChangeSetBytes Walk err", err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal("storageChangeSetBytes Walk err", err)
	}

}

func TestName2(t *testing.T) {
	db := NewMemDatabase()
	bucket := []byte("hAT")
	key := []byte("key")
	for i := 0; i < 20; i += 2 {
		err := db.Put(bucket, append(key, ^uint8(i)), []byte{uint8(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	db.Walk(bucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		fmt.Println(string(k), k[3], v)
		return true, nil
	})
	fmt.Println("-------------------------------")
	db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		c := b.Cursor()
		k, v := c.Seek(append(key, ^uint8(11)))
		fmt.Println(string(k), v)
		return nil
	})
	fmt.Println("-------------------------------")

	db.Walk(bucket, append(key, ^uint8(11)), 0, func(k, v []byte) (b bool, e error) {
		fmt.Println(string(k), k[3], v)
		return false, nil
	})
}

func generateIndexesDB() {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()
	tuplesSize := 100000

	mp := make(map[string]*dbutils.HistoryIndexBytes)
	accInd := []byte("hAT22")
	currentKey := []byte{}
	for {
		stop := true
		err := db.Walk(dbutils.AccountChangeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)

			currentKey = common.CopyBytes(k)
			fmt.Println("next key", blockNum, string(k))
			if len(mp) > tuplesSize {
				stop = false
				return false, nil
			}
			fmt.Println("process", blockNum, string(k))
			err := changeset.AccountChangeSetBytes(v).Walk(func(k, v []byte) error {
				index, ok := mp[string(k)]
				if !ok {
					indexBytes, err := db.Get(accInd, k)
					if err != nil && err != ErrKeyNotFound {
						log.Fatal("err ", err)
					}
					indexBytes2 := common.CopyBytes(indexBytes)
					index = dbutils.WrapHistoryIndex(indexBytes2)
				}
				index.Append(blockNum)
				mp[string(k)] = index

				return nil
			})
			if err != nil {
				log.Fatal("AccountChangeSetBytes Walk err", err)
			}

			return true, nil
		})

		if err != nil {
			log.Fatal("DB Walk err", err)
		}

		if len(mp) > 0 {

			tuples := common.NewTuples(len(mp), 3, 1)
			for key, val := range mp {
				if err := tuples.Append(accInd, []byte(key), *val); err != nil {
					log.Fatal("tuple append", err)
				}
			}
			sort.Sort(tuples)
			fmt.Println("Commit", string(currentKey), len(mp))
			//for i:=0; i<len(tuples)-3; i++{
			//	fmt.Println(i, "PUT", string(tuples[i]), string(tuples[i+1]), string(tuples[i+2]))
			//	err:=db.Put(tuples[i], tuples[i+1],tuples[i+2])
			//	if err!=nil {
			//		log.Fatal("Multiput err", err)
			//	}
			//}
			_, err := db.MultiPut(tuples.Values...)
			if err != nil {
				log.Fatal("Multiput err", err)
			}
			mp = make(map[string]*dbutils.HistoryIndexBytes)
		}
		if stop {
			fmt.Println("Accont changeset finished")
			break
		}
	}
}

const statePath = "/media/b00ris/nvme/thin_last/geth/chaindata"

func generateSTIndexesDB() {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()
	tuplesSize := 100000

	mp := make(map[string]*dbutils.HistoryIndexBytes)
	stInd := []byte("hST22")
	currentKey := []byte{}
	for {
		stop := true
		err := db.Walk(dbutils.StorageChangeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)

			currentKey = common.CopyBytes(k)
			fmt.Println("next key", blockNum, string(k))
			if len(mp) > tuplesSize {
				stop = false
				return false, nil
			}
			fmt.Println("process", blockNum, string(k))
			err := changeset.StorageChangeSetBytes(v).Walk(func(k, v []byte) error {
				index, ok := mp[string(k)]
				if !ok {
					indexBytes, err := db.Get(stInd, k)
					if err != nil && err != ErrKeyNotFound {
						log.Fatal("err ", err)
					}
					indexBytes2 := common.CopyBytes(indexBytes)
					index = dbutils.WrapHistoryIndex(indexBytes2)
				}
				index.Append(blockNum)
				mp[string(k)] = index

				return nil
			})
			if err != nil {
				log.Fatal("AccountChangeSetBytes Walk err", err)
			}

			return true, nil
		})

		if err != nil {
			log.Fatal("DB Walk err", err)
		}

		if len(mp) > 0 {

			tuples := common.NewTuples(len(mp), 3, 1)
			for key, val := range mp {
				if err := tuples.Append(stInd, []byte(key), *val); err != nil {
					log.Fatal("tuple append", err)
				}
			}
			sort.Sort(tuples)
			fmt.Println("Commit", string(currentKey), len(mp))
			_, err := db.MultiPut(tuples.Values...)
			if err != nil {
				log.Fatal("Multiput err", err)
			}
			mp = make(map[string]*dbutils.HistoryIndexBytes)
		}
		if stop {
			fmt.Println("Accont changeset finished")
			break
		}
	}
}
