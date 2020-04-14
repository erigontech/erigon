package ethdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"log"
	"os"
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
		fmt.Println("end:", time.Since(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.AccountChangeSetBucket,
		bucketToWrite: []byte("hAT29"),
		fixedBits:     32,
		csWalker: func(cs []byte) ChangesetWalker {
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
		fmt.Println("end:", time.Since(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.StorageChangeSetBucket,
		bucketToWrite: []byte("hST29"),
		fixedBits:     72,
		csWalker: func(cs []byte) ChangesetWalker {
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
		fmt.Println("end:", time.Since(ts))
	}()

	ig := IndexGenerator{
		db:            db,
		csBucket:      dbutils.AccountChangeSetBucket,
		bucketToWrite: []byte("hAT31"),
		fixedBits:     32,
		csWalker: func(cs []byte) ChangesetWalker {
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
		csWalker: func(cs []byte) ChangesetWalker {
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
		fmt.Println("end:", time.Since(ts))
	}()

	aib := []byte("hAT31")
	sib := []byte("hST31")
	err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		fmt.Println("process", blockNum, string(k))
		err = changeset.AccountChangeSetBytes(v).Walk(func(key, val []byte) error {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, ^(blockNum))
			find := append(common.CopyBytes(key), b...)
			err = db.Walk(aib, find, 32, func(kk []byte, vv []byte) (b bool, e error) {
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
		err = changeset.StorageChangeSetBytes(v).Walk(func(key, val []byte) error {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, ^(blockNum))
			find := append(key, b...)
			err = db.Walk(sib, find, 72, func(kk []byte, vv []byte) (b bool, e error) {
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
func TestCheckContracts(t *testing.T) {

}

func TestMore1000Calc(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Since(ts))
	}()

	aib := []byte("hAT31")
	//sib := []byte("hST31")

	more1index := 0
	more10index := make(map[string]uint64)
	more100index := make(map[string]uint64)

	prevKey := []byte{}
	count := uint64(1)
	added := false
	i := uint64(0)

	err = db.Walk(aib, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		if i%100000 == 0 {
			fmt.Println(i, common.Bytes2Hex(k), len(v), " --- ", more1index, len(more10index), len(more100index))
		}

		i++
		if bytes.Equal(k[:common.HashLength], prevKey) {
			count++
			if count > 1 && !added {
				more1index++
				added = true
			}
			if count > 10 {
				more10index[string(common.CopyBytes(k[:common.HashLength]))] = count
			}
			if count > 100 {
				more100index[string(common.CopyBytes(k[:common.HashLength]))] = count
			}
		} else {
			added = false
			count = 1
			prevKey = common.CopyBytes(k[:common.HashLength])
		}

		return true, nil
	})
	if err != nil {
		t.Fatal("AccountChangeSetBytes Walk err", err)
	}
	fmt.Println("more100", len(more100index))
	fmt.Println("more1", more1index)
	fmt.Println("more10", len(more10index))

	save100 := make([]struct {
		Address      string
		Hash         string
		NumOfIndexes uint64
	}, 0, len(more100index))
	for hash, v := range more100index {
		p, innerErr := db.Get(dbutils.PreimagePrefix, []byte(hash))
		if innerErr != nil {
			t.Error(err)
		}
		if len(p) == 0 {
			t.Error("empty")
		}

		save100 = append(save100, struct {
			Address      string
			Hash         string
			NumOfIndexes uint64
		}{
			Address:      common.BytesToAddress(p).String(),
			NumOfIndexes: v,
			Hash:         common.Bytes2Hex([]byte(hash)),
		})

	}
	sort.Slice(save100, func(i, j int) bool {
		return save100[i].NumOfIndexes > save100[j].NumOfIndexes
	})
	f, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/turbo-geth/debug/more100.json")
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f).Encode(save100)
	if err != nil {
		t.Fatal(err)
	}

	save10 := make([]struct {
		Address      string
		Hash         string
		NumOfIndexes uint64
	}, 0, len(more10index))
	for hash, v := range more10index {
		p, innerErr := db.Get(dbutils.PreimagePrefix, []byte(hash))
		if innerErr != nil {
			t.Error(innerErr)
		}
		if len(p) == 0 {
			t.Error("empty")
		}
		save10 = append(save10, struct {
			Address      string
			Hash         string
			NumOfIndexes uint64
		}{
			Address:      common.BytesToAddress(p).String(),
			NumOfIndexes: v,
			Hash:         common.Bytes2Hex([]byte(hash)),
		})

	}
	sort.Slice(save10, func(i, j int) bool {
		return save10[i].NumOfIndexes > save10[j].NumOfIndexes
	})

	f2, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/turbo-geth/debug/more10.json")
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f2).Encode(save10)
	if err != nil {
		t.Fatal(err)
	}

}
func TestMore1000CalcStr(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Since(ts))
	}()

	sib := []byte("hST31")

	more1index := 0
	more10index := make(map[string]uint64)
	more100index := make(map[string]uint64)

	prevKey := []byte{}
	count := uint64(1)
	added := false
	i := uint64(0)

	keyLen := common.HashLength*2 + common.IncarnationLength
	err = db.Walk(sib, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		if i%100000 == 0 {
			fmt.Println(i, common.Bytes2Hex(k), len(v), " --- ", more1index, len(more10index), len(more100index))
		}

		i++
		if bytes.Equal(k[:keyLen], prevKey) {
			count++
			if count > 1 && !added {
				more1index++
				added = true
			}
			if count > 10 {
				more10index[string(common.CopyBytes(k[:keyLen]))] = count
			}
			if count > 100 {
				more100index[string(common.CopyBytes(k[:keyLen]))] = count
			}
		} else {
			added = false
			count = 1
			prevKey = common.CopyBytes(k[:keyLen])
		}

		return true, nil
	})
	if err != nil {
		t.Fatal("AccountChangeSetBytes Walk err", err)
	}
	fmt.Println("more100", len(more100index))
	fmt.Println("more1", more1index)
	fmt.Println("more10", len(more10index))

	save100 := make([]struct {
		Hash         string
		NumOfIndexes uint64
	}, 0, len(more100index))
	for hash, v := range more100index {
		save100 = append(save100, struct {
			Hash         string
			NumOfIndexes uint64
		}{
			NumOfIndexes: v,
			Hash:         common.Bytes2Hex([]byte(hash)),
		})

	}
	sort.Slice(save100, func(i, j int) bool {
		return save100[i].NumOfIndexes > save100[j].NumOfIndexes
	})
	f, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/turbo-geth/debug/more100str.json")
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f).Encode(save100)
	if err != nil {
		t.Fatal(err)
	}

	save10 := make([]struct {
		Hash         string
		NumOfIndexes uint64
	}, 0, len(more10index))
	for hash, v := range more10index {
		save10 = append(save10, struct {
			Hash         string
			NumOfIndexes uint64
		}{
			NumOfIndexes: v,
			Hash:         common.Bytes2Hex([]byte(hash)),
		})

	}
	sort.Slice(save10, func(i, j int) bool {
		return save10[i].NumOfIndexes > save10[j].NumOfIndexes
	})

	f2, err := os.Create("/home/b00ris/go/src/github.com/ledgerwatch/turbo-geth/debug/more10str.json")
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f2).Encode(save10)
	if err != nil {
		t.Fatal(err)
	}

}

func TestCheckAcc(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	bucket := []byte("hAT31")
	key := common.Hex2Bytes("002afdbfc7f15d5b2f6df268e8b4b26ce02e297e53503d0f056c065e980ebda1")
	fmt.Println(len(key))
	i := 0
	err = db.Walk(bucket, key, 32, func(k, v []byte) (b bool, e error) {
		fmt.Println(i, len(v), common.Bytes2Hex(k[:common.HashLength]), ^binary.BigEndian.Uint64(k[common.HashLength:]))
		i++
		return true, nil
	})
	t.Log(err)
}

func TestSecondTypeIndex(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	db2, err := NewBoltDatabase("/media/b00ris/nvme/bucket_index_exp")
	if err != nil {
		log.Fatal(err)
	}
	bucket := []byte("hAT31")
	numOfMultiput := 0
	i := 0
	tuples := common.NewTuples(db2.IdealBatchSize(), 3, 1)
	sort.Sort(tuples)
	err = db.Walk(bucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		if i%10000 == 0 {
			fmt.Println(i)
		}
		i++
		ui, innerErr := dbutils.WrapHistoryIndex(v).Decode()
		if innerErr != nil {
			return false, innerErr
		}
		for _, val := range ui {
			numOfMultiput++
			key := make([]byte, common.HashLength+8)
			copy(key[0:common.HashLength], k[0:common.HashLength])
			binary.BigEndian.PutUint64(key[common.HashLength:], val)
			if innerErr = tuples.Append(bucket, key, []byte{}); innerErr != nil {
				t.Fatal("tuple append", innerErr)
			}
		}
		if numOfMultiput > db2.IdealBatchSize() {
			fmt.Println("Multiput", len(tuples.Values)/3, db2.IdealBatchSize())
			sort.Sort(tuples)
			if _, innerErr = db2.MultiPut(tuples.Values...); innerErr != nil {
				t.Fatal(innerErr)
			}
			numOfMultiput = 0
			tuples = common.NewTuples(db2.IdealBatchSize(), 3, 1)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples.Values) > 0 {
		if _, err := db2.MultiPut(tuples.Values...); err != nil {
			t.Fatal(err)
		}
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
	db.Walk(bucket, []byte{}, 0, func(k, v []byte) (b bool, e error) { //nolint
		fmt.Println(string(k), k[3], v)
		return true, nil
	})
	fmt.Println("-------------------------------")
	db.db.View(func(tx *bolt.Tx) error { // nolint
		b := tx.Bucket(bucket)
		c := b.Cursor()
		k, v := c.Seek(append(key, ^uint8(11)))
		fmt.Println(string(k), v)
		return nil
	})
	fmt.Println("-------------------------------")

	db.Walk(bucket, append(key, ^uint8(11)), 0, func(k, v []byte) (b bool, e error) {  //nolint
		fmt.Println(string(k), k[3], v)
		return false, nil
	})
}

func TestGenerateIndexesDB(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Since(ts))
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

func TestGenerateSTIndexesDB(t *testing.T) {
	db, err := NewBoltDatabase(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Since(ts))
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
