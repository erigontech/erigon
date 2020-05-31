package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestIndexGenerator_GenerateIndex_SimpleCase(t *testing.T) {
	test := func(blocksNum int, csBucket []byte) func(t *testing.T) {
		return func(t *testing.T) {
			db := ethdb.NewMemDatabase()
			ig := NewIndexGenerator(db, make(chan struct{}))
			log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
			csInfo, ok := mapper[string(csBucket)]
			if !ok {
				t.Fatal("incorrect cs bucket")
			}
			addrs, expecedIndexes := generateTestData(t, db, csBucket, blocksNum)

			ig.ChangeSetBufSize = 16 * 1024
			err := ig.GenerateIndex(0, csBucket)
			if err != nil {
				t.Fatal(err)
			}

			checkIndex(t, db, csInfo.IndexBucket, addrs[0], 0, expecedIndexes[string(addrs[0])][0])
			checkIndex(t, db, csInfo.IndexBucket, addrs[0], 999, expecedIndexes[string(addrs[0])][0])
			checkIndex(t, db, csInfo.IndexBucket, addrs[0], 1000, expecedIndexes[string(addrs[0])][1])
			checkIndex(t, db, csInfo.IndexBucket, addrs[0], 1999, expecedIndexes[string(addrs[0])][1])
			checkIndex(t, db, csInfo.IndexBucket, addrs[0], 2000, expecedIndexes[string(addrs[0])][2])
			checkIndex(t, db, csInfo.IndexBucket, addrs[1], 0, expecedIndexes[string(addrs[1])][0])
			checkIndex(t, db, csInfo.IndexBucket, addrs[1], 2000, expecedIndexes[string(addrs[1])][1])
			checkIndex(t, db, csInfo.IndexBucket, addrs[2], 0, expecedIndexes[string(addrs[2])][0])

			//check last chunk
			lastChunkCheck(t, db, csInfo.IndexBucket, addrs[0], expecedIndexes[string(addrs[0])][2])
			lastChunkCheck(t, db, csInfo.IndexBucket, addrs[1], expecedIndexes[string(addrs[1])][1])
			lastChunkCheck(t, db, csInfo.IndexBucket, addrs[2], expecedIndexes[string(addrs[2])][0])
		}
	}

	t.Run("account hashed state", test(2100, dbutils.AccountChangeSetBucket))
	t.Run("account plain state", test(2100, dbutils.PlainAccountChangeSetBucket))
	t.Run("storage hashed state", test(2100, dbutils.StorageChangeSetBucket))
	t.Run("storage plain state", test(2100, dbutils.PlainStorageChangeSetBucket))

}

func TestIndexGenerator_Truncate(t *testing.T) {
	buckets := [][]byte{dbutils.StorageChangeSetBucket}
	//buckets:=[][]byte{dbutils.AccountChangeSetBucket, dbutils.StorageChangeSetBucket, dbutils.PlainAccountChangeSetBucket, dbutils.PlainStorageChangeSetBucket}
	for i := range buckets {
		csbucket := buckets[i]
		db := ethdb.NewMemDatabase()
		hashes, expected := generateTestData(t, db, csbucket, 2100)
		mp := mapper[string(csbucket)]
		indexBucket := mp.IndexBucket
		ig := NewIndexGenerator(db, make(chan struct{}))
		err := ig.GenerateIndex(0, csbucket)
		if err != nil {
			t.Fatal(err)
		}

		reduceSlice := func(arr []uint64, timestamtTo uint64) []uint64 {
			pos := sort.Search(len(arr), func(i int) bool {
				return arr[i] > timestamtTo
			})
			return arr[:pos]
		}

		t.Run("truncate to 2050 "+string(csbucket), func(t *testing.T) {
			expected[string(hashes[0])][2] = reduceSlice(expected[string(hashes[0])][2], 2050)
			expected[string(hashes[1])][1] = reduceSlice(expected[string(hashes[1])][1], 2050)
			expected[string(hashes[2])][0] = reduceSlice(expected[string(hashes[2])][0], 2050)

			err = ig.Truncate(2050, csbucket)
			if err != nil {
				t.Fatal(err)
			}

			checkIndex(t, db, indexBucket, hashes[0], 2030, expected[string(hashes[0])][2])
			checkIndex(t, db, indexBucket, hashes[1], 2030, expected[string(hashes[1])][1])
			checkIndex(t, db, indexBucket, hashes[2], 2030, expected[string(hashes[2])][0])
			checkIndex(t, db, indexBucket, hashes[0], 1999, expected[string(hashes[0])][1])
			checkIndex(t, db, indexBucket, hashes[1], 999, expected[string(hashes[1])][0])
		})

		t.Run("truncate to 2000 "+string(csbucket), func(t *testing.T) {
			expected[string(hashes[0])][2] = reduceSlice(expected[string(hashes[0])][2], 2000)
			expected[string(hashes[1])][1] = reduceSlice(expected[string(hashes[1])][1], 2000)
			expected[string(hashes[2])][0] = reduceSlice(expected[string(hashes[2])][0], 2000)

			err = ig.Truncate(2000, csbucket)
			if err != nil {
				t.Fatal(err)
			}

			checkIndex(t, db, indexBucket, hashes[0], 2000, expected[string(hashes[0])][2])
			checkIndex(t, db, indexBucket, hashes[1], 2000, expected[string(hashes[1])][1])
			checkIndex(t, db, indexBucket, hashes[2], expected[string(hashes[2])][0][len(expected[string(hashes[2])][0])-1], expected[string(hashes[2])][0])
		})

		t.Run("truncate to 1999 "+string(csbucket), func(t *testing.T) {
			err = ig.Truncate(1999, csbucket)
			if err != nil {
				t.Fatal(err)
			}
			checkIndex(t, db, indexBucket, hashes[0], 1999, expected[string(hashes[0])][1])
			checkIndex(t, db, indexBucket, hashes[1], 1998, expected[string(hashes[1])][0])
			checkIndex(t, db, indexBucket, hashes[2], 1998, expected[string(hashes[2])][0])
			_, err = db.GetIndexChunk(csbucket, hashes[0], 2000)
			if err != ethdb.ErrKeyNotFound {
				t.Fatal()
			}
			_, err = db.GetIndexChunk(csbucket, hashes[1], 2000)
			if err != ethdb.ErrKeyNotFound {
				t.Fatal()
			}
		})

		t.Run("truncate to 999 "+string(csbucket), func(t *testing.T) {
			expected[string(hashes[1])][0] = reduceSlice(expected[string(hashes[1])][0], 999)
			expected[string(hashes[2])][0] = reduceSlice(expected[string(hashes[2])][0], 999)

			err = ig.Truncate(999, csbucket)
			if err != nil {
				t.Fatal(err)
			}
			checkIndex(t, db, indexBucket, hashes[0], 999, expected[string(hashes[0])][0])
			checkIndex(t, db, indexBucket, hashes[1], 998, expected[string(hashes[1])][0])
			checkIndex(t, db, indexBucket, hashes[2], 999, expected[string(hashes[2])][0])
			_, err = db.GetIndexChunk(csbucket, hashes[0], 1000)
			if err != ethdb.ErrKeyNotFound {
				t.Fatal()
			}
			_, err = db.GetIndexChunk(csbucket, hashes[1], 1000)
			if err != ethdb.ErrKeyNotFound {
				t.Fatal()
			}
		})
	}

}

func generateTestData(t *testing.T, db ethdb.Database, csBucket []byte, numOfBlocks int) ([][]byte, map[string][][]uint64) { //nolint
	csInfo, ok := mapper[string(csBucket)]
	if !ok {
		t.Fatal("incorrect cs bucket")
	}
	var isPlain bool
	if bytes.Equal(dbutils.PlainStorageChangeSetBucket, csBucket) || bytes.Equal(dbutils.PlainAccountChangeSetBucket, csBucket) {
		isPlain = true
	}
	addrs, err := generateAddrs(3, isPlain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(dbutils.StorageChangeSetBucket, csBucket) || bytes.Equal(dbutils.PlainStorageChangeSetBucket, csBucket) {
		keys, innerErr := generateAddrs(3, false)
		if innerErr != nil {
			t.Fatal(innerErr)
		}

		defaultIncarnation := make([]byte, 8)
		binary.BigEndian.PutUint64(defaultIncarnation, ^uint64(1))
		for i := range addrs {
			addrs[i] = append(addrs[i], defaultIncarnation...)
			addrs[i] = append(addrs[i], keys[i]...)
		}
	}

	expected1 := make([][]uint64, 0)
	expected1 = append(expected1, make([]uint64, 0))
	expected2 := make([][]uint64, 0)
	expected2 = append(expected2, make([]uint64, 0))
	expected3 := make([][]uint64, 0)
	expected3 = append(expected3, make([]uint64, 0))

	for i := 0; i < numOfBlocks; i++ {
		cs := csInfo.New()
		err = cs.Add(addrs[0], []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

		if len(expected1[len(expected1)-1]) == dbutils.MaxChunkSize {
			expected1 = append(expected1, make([]uint64, 0))
		}
		expected1[len(expected1)-1] = append(expected1[len(expected1)-1], uint64(i))

		if i%2 == 0 {
			err = cs.Add(addrs[1], []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}

			if len(expected2[len(expected2)-1]) == dbutils.MaxChunkSize {
				expected2 = append(expected2, make([]uint64, 0))
			}
			expected2[len(expected2)-1] = append(expected2[len(expected2)-1], uint64(i))
		}
		if i%3 == 0 {
			err = cs.Add(addrs[2], []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			if len(expected3[len(expected3)-1]) == dbutils.MaxChunkSize {
				expected3 = append(expected3, make([]uint64, 0))
			}
			expected3[len(expected3)-1] = append(expected3[len(expected3)-1], uint64(i))

		}
		v, err := csInfo.Encode(cs)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put(csBucket, dbutils.EncodeTimestamp(uint64(i)), v)
		if err != nil {
			t.Fatal(err)
		}
	}
	return addrs, map[string][][]uint64{
		string(addrs[0]): expected1,
		string(addrs[1]): expected2,
		string(addrs[2]): expected3,
	}
}

func checkIndex(t *testing.T, db ethdb.Database, bucket, addrHash []byte, chunkBlock uint64, expected []uint64) {
	t.Helper()
	b, err := db.GetIndexChunk(bucket, addrHash, chunkBlock)
	if err != nil {
		t.Fatal(err, common.Bytes2Hex(addrHash), chunkBlock)
	}
	val, _, err := dbutils.HistoryIndexBytes(b).Decode()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(val, expected) {
		fmt.Println("get", val)
		fmt.Println("expected", expected)
		t.Fatal()
	}
}

func lastChunkCheck(t *testing.T, db ethdb.Database, bucket, key []byte, expected []uint64) {
	t.Helper()
	v, err := db.Get(bucket, dbutils.CurrentChunkKey(key))
	if err != nil {
		t.Fatal(err, dbutils.CurrentChunkKey(key))
	}

	val, _, err := dbutils.HistoryIndexBytes(v).Decode()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(val, expected) {
		fmt.Println(val)
		fmt.Println(expected)
		t.Fatal()
	}
}

func generateAddrs(numOfAddrs int, isPlain bool) ([][]byte, error) {
	addrs := make([][]byte, numOfAddrs)
	for i := 0; i < numOfAddrs; i++ {
		key1, err := crypto.GenerateKey()
		if err != nil {
			return nil, err
		}
		addr := crypto.PubkeyToAddress(key1.PublicKey)
		if isPlain {
			addrs[i] = addr.Bytes()
			continue
		}
		hash, err := common.HashData(addr.Bytes())
		if err != nil {
			return nil, err
		}
		addrs[i] = hash.Bytes()
	}
	return addrs, nil
}
