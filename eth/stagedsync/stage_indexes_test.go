package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func TestIndexGenerator_GenerateIndex_SimpleCase(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	test := func(blocksNum int, csBucket string) func(t *testing.T) {
		return func(t *testing.T) {
			db := ethdb.NewMemDatabase()
			defer db.Close()
			tx, err := db.Begin(context.Background(), ethdb.RW)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			csInfo, ok := changeset.Mapper[csBucket]
			if !ok {
				t.Fatal("incorrect cs bucket")
			}
			addrs, expecedIndexes := generateTestData(t, tx, csBucket, blocksNum)
			err = promoteHistory("logPrefix", tx, csBucket, 0, uint64(blocksNum/2), 10, time.Millisecond, getTmpDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			err = promoteHistory("logPrefix", tx, csBucket, uint64(blocksNum/2), uint64(blocksNum), 10, time.Millisecond, getTmpDir(), nil)
			if err != nil {
				t.Fatal(err)
			}

			checkIndex(t, tx, csInfo.IndexBucket, addrs[0], expecedIndexes[string(addrs[0])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[1], expecedIndexes[string(addrs[1])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[2], expecedIndexes[string(addrs[2])])
		}
	}

	t.Run("account plain state", test(2100, dbutils.PlainAccountChangeSetBucket))
	t.Run("storage plain state", test(2100, dbutils.PlainStorageChangeSetBucket))

}

func TestIndexGenerator_Truncate(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	buckets := []string{dbutils.PlainAccountChangeSetBucket, dbutils.PlainStorageChangeSetBucket}
	for i := range buckets {
		csbucket := buckets[i]
		db := ethdb.NewMemDatabase()
		defer db.Close()
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		hashes, expected := generateTestData(t, tx, csbucket, 2100)
		mp := changeset.Mapper[csbucket]
		indexBucket := mp.IndexBucket
		err = promoteHistory("logPrefix", tx, csbucket, 0, uint64(2100), 10, time.Millisecond, getTmpDir(), nil)
		if err != nil {
			t.Fatal(err)
		}

		reduceSlice := func(arr []uint64, timestamtTo uint64) []uint64 {
			pos := sort.Search(len(arr), func(i int) bool {
				return arr[i] > timestamtTo
			})
			return arr[:pos]
		}

		//t.Run("truncate to 2050 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2050)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2050)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2050)

		err = unwindHistory("logPrefix", tx, csbucket, 2050, nil)
		if err != nil {
			t.Fatal(err)
		}

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 2000 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2000)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2000)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2000)

		err = unwindHistory("logPrefix", tx, csbucket, 2000, nil)
		if err != nil {
			t.Fatal(err)
		}

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 1999 "+csbucket, func(t *testing.T) {
		err = unwindHistory("logPrefix", tx, csbucket, 1999, nil)
		if err != nil {
			t.Fatal(err)
		}
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 1999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 1999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 1999)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		bm, err := bitmapdb.Get64(tx, indexBucket, hashes[0], 1999, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal(bm.Maximum())
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 1999, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal()
		}
		//})

		//t.Run("truncate to 999 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 999)

		err = unwindHistory("logPrefix", tx, csbucket, 999, nil)
		if err != nil {
			t.Fatal(err)
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[0], 999, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 999, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})
		tx.Rollback()
		db.Close()
	}
}

func generateTestData(t *testing.T, db ethdb.Database, csBucket string, numOfBlocks int) ([][]byte, map[string][]uint64) { //nolint
	csInfo, ok := changeset.Mapper[csBucket]
	if !ok {
		t.Fatal("incorrect cs bucket")
	}
	var isPlain bool
	if dbutils.PlainStorageChangeSetBucket == csBucket || dbutils.PlainAccountChangeSetBucket == csBucket {
		isPlain = true
	}
	addrs, err := generateAddrs(3, isPlain)
	if err != nil {
		t.Fatal(err)
	}
	if dbutils.PlainStorageChangeSetBucket == csBucket {
		keys, innerErr := generateAddrs(3, false)
		if innerErr != nil {
			t.Fatal(innerErr)
		}

		defaultIncarnation := make([]byte, 8)
		binary.BigEndian.PutUint64(defaultIncarnation, uint64(1))
		for i := range addrs {
			addrs[i] = append(addrs[i], defaultIncarnation...)
			addrs[i] = append(addrs[i], keys[i]...)
		}
	}

	res := make([]uint64, 0)
	res2 := make([]uint64, 0)
	res3 := make([]uint64, 0)

	for i := 0; i < numOfBlocks; i++ {
		cs := csInfo.New()
		err = cs.Add(addrs[0], []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

		res = append(res, uint64(i))

		if i%2 == 0 {
			err = cs.Add(addrs[1], []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			res2 = append(res2, uint64(i))
		}
		if i%3 == 0 {
			err = cs.Add(addrs[2], []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
			res3 = append(res3, uint64(i))
		}
		err = csInfo.Encode(uint64(i), cs, func(k, v []byte) error {
			return db.Put(csBucket, k, v)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	return addrs, map[string][]uint64{
		string(addrs[0]): res,
		string(addrs[1]): res2,
		string(addrs[2]): res3,
	}
}

func checkIndex(t *testing.T, db ethdb.Getter, bucket string, k []byte, expected []uint64) {
	t.Helper()
	k = dbutils.CompositeKeyWithoutIncarnation(k)
	m, err := bitmapdb.Get64(db, bucket, k, 0, math.MaxUint32)
	if err != nil {
		t.Fatal(err, common.Bytes2Hex(k))
	}
	val := m.ToArray()
	if !reflect.DeepEqual(val, expected) {
		fmt.Printf("get     : %v\n", val)
		fmt.Printf("expected: %v\n", toU32(expected))
		t.Fatal()
	}
}

func toU32(in []uint64) []uint32 {
	out := make([]uint32, len(in))
	for i := range in {
		out[i] = uint32(in[i])
	}
	return out
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
