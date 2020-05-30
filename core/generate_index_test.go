package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func TestIndexGenerator_GenerateIndex_SimpleCase(t *testing.T) {
	db := ethdb.NewMemDatabase()

	ig := NewIndexGenerator(db)
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	hashes, expecedIndexes, err := generateTestData(t, db)
	if err != nil {
		t.Fatal(err)
	}

	err = ig.GenerateIndex(0, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
		return changeset.AccountChangeSetBytes(bytes)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	checkIndex(t, db, hashes[0].Bytes(), 0, expecedIndexes[hashes[0]][0])
	checkIndex(t, db, hashes[0].Bytes(), 999, expecedIndexes[hashes[0]][0])
	checkIndex(t, db, hashes[0].Bytes(), 1000, expecedIndexes[hashes[0]][1])
	checkIndex(t, db, hashes[0].Bytes(), 1999, expecedIndexes[hashes[0]][1])
	checkIndex(t, db, hashes[0].Bytes(), 2000, expecedIndexes[hashes[0]][2])
	checkIndex(t, db, hashes[1].Bytes(), 0, expecedIndexes[hashes[1]][0])
	checkIndex(t, db, hashes[1].Bytes(), 2000, expecedIndexes[hashes[1]][1])
	checkIndex(t, db, hashes[2].Bytes(), 0, expecedIndexes[hashes[2]][0])

	//check last chunk
	lastChunkCheck(t, db, hashes[0].Bytes(), expecedIndexes[hashes[0]][2])
	lastChunkCheck(t, db, hashes[1].Bytes(), expecedIndexes[hashes[1]][1])
	lastChunkCheck(t, db, hashes[2].Bytes(), expecedIndexes[hashes[2]][0])

}

func TestIndexGenerator_Truncate(t *testing.T) {
	//don't run it parallel
	db := ethdb.NewMemDatabase()
	hashes, expected, err := generateTestData(t, db)
	if err != nil {
		t.Fatal(err)
	}

	ig := NewIndexGenerator(db)
	err = ig.GenerateIndex(0, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
		return changeset.AccountChangeSetBytes(bytes)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	reduceSlice := func(arr []uint64, timestamtTo uint64) []uint64 {
		pos := sort.Search(len(arr), func(i int) bool {
			return arr[i] > timestamtTo
		})
		return arr[:pos]
	}

	t.Run("truncate to 2050", func(t *testing.T) {
		expected[hashes[0]][2] = reduceSlice(expected[hashes[0]][2], 2050)
		expected[hashes[1]][1] = reduceSlice(expected[hashes[1]][1], 2050)
		expected[hashes[2]][0] = reduceSlice(expected[hashes[2]][0], 2050)

		err = ig.Truncate(2050, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(bytes)
		})
		if err != nil {
			t.Fatal(err)
		}

		checkIndex(t, db, hashes[0].Bytes(), 2030, expected[hashes[0]][2])
		checkIndex(t, db, hashes[1].Bytes(), 2030, expected[hashes[1]][1])
		checkIndex(t, db, hashes[2].Bytes(), 2030, expected[hashes[2]][0])
		checkIndex(t, db, hashes[0].Bytes(), 1999, expected[hashes[0]][1])
		checkIndex(t, db, hashes[1].Bytes(), 999, expected[hashes[1]][0])
	})

	t.Run("truncate to 2000", func(t *testing.T) {
		expected[hashes[0]][2] = reduceSlice(expected[hashes[0]][2], 2000)
		expected[hashes[1]][1] = reduceSlice(expected[hashes[1]][1], 2000)
		expected[hashes[2]][0] = reduceSlice(expected[hashes[2]][0], 2000)

		err = ig.Truncate(2000, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(bytes)
		})
		if err != nil {
			t.Fatal(err)
		}

		checkIndex(t, db, hashes[0].Bytes(), 2000, expected[hashes[0]][2])
		checkIndex(t, db, hashes[1].Bytes(), 2000, expected[hashes[1]][1])
		checkIndex(t, db, hashes[2].Bytes(), expected[hashes[2]][0][len(expected[hashes[2]][0])-1], expected[hashes[2]][0])
	})

	t.Run("truncate to 1999", func(t *testing.T) {
		err = ig.Truncate(1999, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(bytes)
		})
		if err != nil {
			t.Fatal(err)
		}
		checkIndex(t, db, hashes[0].Bytes(), 1999, expected[hashes[0]][1])
		checkIndex(t, db, hashes[1].Bytes(), 1998, expected[hashes[1]][0])
		checkIndex(t, db, hashes[2].Bytes(), 1998, expected[hashes[2]][0])
		_, err = db.GetIndexChunk(dbutils.AccountsHistoryBucket, hashes[0].Bytes(), 2000)
		if err != ethdb.ErrKeyNotFound {
			t.Fatal()
		}
		_, err = db.GetIndexChunk(dbutils.AccountsHistoryBucket, hashes[1].Bytes(), 2000)
		if err != ethdb.ErrKeyNotFound {
			t.Fatal()
		}
	})

	t.Run("truncate to 999", func(t *testing.T) {
		expected[hashes[1]][0] = reduceSlice(expected[hashes[1]][0], 999)
		expected[hashes[2]][0] = reduceSlice(expected[hashes[2]][0], 999)

		err = ig.Truncate(999, dbutils.AccountChangeSetBucket, dbutils.AccountsHistoryBucket, func(bytes []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(bytes)
		})
		if err != nil {
			t.Fatal(err)
		}
		checkIndex(t, db, hashes[0].Bytes(), 999, expected[hashes[0]][0])
		checkIndex(t, db, hashes[1].Bytes(), 998, expected[hashes[1]][0])
		checkIndex(t, db, hashes[2].Bytes(), 999, expected[hashes[2]][0])
		_, err = db.GetIndexChunk(dbutils.AccountsHistoryBucket, hashes[0].Bytes(), 1000)
		if err != ethdb.ErrKeyNotFound {
			t.Fatal()
		}
		_, err = db.GetIndexChunk(dbutils.AccountsHistoryBucket, hashes[1].Bytes(), 1000)
		if err != ethdb.ErrKeyNotFound {
			t.Fatal()
		}
	})
}

func TestIndexGenerator_GenerateIndexStorage(t *testing.T) {
	db := ethdb.NewMemDatabase()
	key1, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	addrHash1 := crypto.PubkeyToAddress(key1.PublicKey).Hash()
	key2, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	addrHash2 := crypto.PubkeyToAddress(key2.PublicKey).Hash()
	key3, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	addrHash3 := crypto.PubkeyToAddress(key3.PublicKey).Hash()

	compositeKey1 := dbutils.GenerateCompositeStorageKey(addrHash1, ^uint64(1), common.Hash{111})
	compositeKey2 := dbutils.GenerateCompositeStorageKey(addrHash2, ^uint64(2), common.Hash{111})
	compositeKey3 := dbutils.GenerateCompositeStorageKey(addrHash3, ^uint64(3), common.Hash{111})
	expected11 := make([]uint64, 0)
	expected12 := make([]uint64, 0)
	expected13 := make([]uint64, 0)
	expected21 := make([]uint64, 0)
	expected22 := make([]uint64, 0)
	expected3 := make([]uint64, 0)
	const numOfBlocks = 2100

	for i := 0; i < numOfBlocks; i++ {
		cs := changeset.NewStorageChangeSet()
		err = cs.Add(compositeKey1, []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

		if i < 1000 {
			expected11 = append(expected11, uint64(i))
		} else if i < 2000 {
			expected12 = append(expected12, uint64(i))
		} else {
			expected13 = append(expected13, uint64(i))
		}

		if i%2 == 0 {
			err = cs.Add(compositeKey2, []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}

			if i < 2000 {
				expected21 = append(expected21, uint64(i))
			} else {
				expected22 = append(expected22, uint64(i))
			}
		}
		if i%3 == 0 {
			err = cs.Add(compositeKey3, []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			expected3 = append(expected3, uint64(i))
		}
		v, innerErr := changeset.EncodeStorage(cs)
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		err = db.Put(dbutils.StorageChangeSetBucket, dbutils.EncodeTimestamp(uint64(i)), v)
		if err != nil {
			t.Fatal(err)
		}
	}

	ig := NewIndexGenerator(db)
	err = ig.GenerateIndex(0, dbutils.StorageChangeSetBucket, dbutils.StorageHistoryBucket, func(bytes []byte) changeset.Walker {
		return changeset.StorageChangeSetBytes(bytes)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	check := func(compositeKey []byte, chunkBlock uint64, expected []uint64) {
		t.Helper()
		b, err := db.GetIndexChunk(dbutils.StorageHistoryBucket, compositeKey, chunkBlock)
		if err != nil {
			t.Fatal(err)
		}
		val, _, err := dbutils.HistoryIndexBytes(b).Decode()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(val, expected) {
			fmt.Println(val)
			fmt.Println(expected)
			t.Fatal()
		}
	}

	check(compositeKey1, 0, expected11)
	check(compositeKey1, 999, expected11)
	check(compositeKey1, 1000, expected12)
	check(compositeKey1, 1999, expected12)
	check(compositeKey1, 2000, expected13)
	check(compositeKey2, 0, expected21)
	check(compositeKey2, 2000, expected22)
	check(compositeKey3, 0, expected3)
}

func generateTestData(t *testing.T, db ethdb.Database) ([]common.Hash, map[common.Hash][][]uint64, error) { //nolint
	key1, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}
	addrHash1 := crypto.PubkeyToAddress(key1.PublicKey).Hash()
	key2, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}

	addrHash2 := crypto.PubkeyToAddress(key2.PublicKey).Hash()
	key3, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}

	addrHash3 := crypto.PubkeyToAddress(key3.PublicKey).Hash()

	expected11 := make([]uint64, 0)
	expected12 := make([]uint64, 0)
	expected13 := make([]uint64, 0)
	expected21 := make([]uint64, 0)
	expected22 := make([]uint64, 0)
	expected3 := make([]uint64, 0)
	const numOfBlocks = 2100

	for i := 0; i < numOfBlocks; i++ {
		cs := changeset.NewAccountChangeSet()
		err = cs.Add(addrHash1.Bytes(), []byte(strconv.Itoa(i)))
		if err != nil {
			return nil, nil, err
		}

		if i < 1000 {
			expected11 = append(expected11, uint64(i))
		} else if i < 2000 {
			expected12 = append(expected12, uint64(i))
		} else {
			expected13 = append(expected13, uint64(i))
		}

		if i%2 == 0 {
			err = cs.Add(addrHash2.Bytes(), []byte(strconv.Itoa(i)))
			if err != nil {
				return nil, nil, err
			}

			if i < 2000 {
				expected21 = append(expected21, uint64(i))
			} else {
				expected22 = append(expected22, uint64(i))
			}
		}
		if i%3 == 0 {
			err = cs.Add(addrHash3.Bytes(), []byte(strconv.Itoa(i)))
			if err != nil {
				return nil, nil, err
			}
			expected3 = append(expected3, uint64(i))
		}
		v, err := changeset.EncodeAccounts(cs)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put(dbutils.AccountChangeSetBucket, dbutils.EncodeTimestamp(uint64(i)), v)
		if err != nil {
			t.Fatal(err)
		}
	}
	return []common.Hash{addrHash1, addrHash2, addrHash3}, map[common.Hash][][]uint64{
		addrHash1: {expected11, expected12, expected13},
		addrHash2: {expected21, expected22},
		addrHash3: {expected3},
	}, nil
}

func checkIndex(t *testing.T, db ethdb.Database, addrHash []byte, chunkBlock uint64, expected []uint64) {
	t.Helper()
	b, err := db.GetIndexChunk(dbutils.AccountsHistoryBucket, addrHash, chunkBlock)
	if err != nil {
		t.Fatal(err)
	}
	val, _, err := dbutils.HistoryIndexBytes(b).Decode()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(val, expected) {
		fmt.Println(val)
		fmt.Println(expected)
		t.Fatal()
	}
}

func lastChunkCheck(t *testing.T, db ethdb.Database, key []byte, expected []uint64) {
	t.Helper()
	v, err := db.Get(dbutils.AccountsHistoryBucket, dbutils.CurrentChunkKey(key))
	if err != nil {
		t.Fatal(err)
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
func debugIndexes(db ethdb.Database, bucket []byte) { //nolint
	l := common.HashLength
	if bytes.Equal(dbutils.StorageHistoryBucket, bucket) {
		l = common.HashLength * 2
	}
	db.Walk(bucket, []byte{}, 0, func(k []byte, v []byte) (bool, error) { //nolint
		fmt.Println(common.Bytes2Hex(k), binary.BigEndian.Uint64(k[l:]))
		return true, nil
	})
}
