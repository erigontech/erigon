package changeset

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
)

const (
	defaultIncarnation = 1
)

var numOfChanges = []int{1, 3, 10, 100}

func getDefaultIncarnation() uint64 { return defaultIncarnation }
func getRandomIncarnation() uint64  { return rand.Uint64() }

func hashValueGenerator(j int) []byte {
	val, _ := common.HashData([]byte("val" + strconv.Itoa(j)))
	return val.Bytes()
}

func emptyValueGenerator(j int) []byte {
	return []byte{}
}

func getTestDataAtIndex(i, j int, inc uint64, generator func(common.Address, uint64, common.Hash) []byte) []byte {
	address := common.HexToAddress(fmt.Sprintf("0xBe828AD8B538D1D691891F6c725dEdc5989abBc%d", i))
	key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
	return generator(address, inc, key)
}

func hashKeyGenerator(address common.Address, inc uint64, key common.Hash) []byte {
	addrHash, _ := common.HashData(address[:])
	return dbutils.GenerateCompositeStorageKey(addrHash, inc, key)
}

func plainKeyGenerator(address common.Address, inc uint64, key common.Hash) []byte {
	return dbutils.PlainGenerateCompositeStorageKey(address, inc, key)
}

func TestEncodingStorageNewWithRandomIncarnationHashed(t *testing.T) {
	m := Mapper[dbutils.StorageChangeSetBucket]
	doTestEncodingStorageNew(t, hashKeyGenerator, getRandomIncarnation, hashValueGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithRandomIncarnationPlain(t *testing.T) {
	m := Mapper[dbutils.PlainStorageChangeSetBucket]
	doTestEncodingStorageNew(t, plainKeyGenerator, getRandomIncarnation, hashValueGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithDefaultIncarnationHashed(t *testing.T) {
	m := Mapper[dbutils.StorageChangeSetBucket]
	doTestEncodingStorageNew(t, hashKeyGenerator, getDefaultIncarnation, hashValueGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithDefaultIncarnationPlain(t *testing.T) {
	m := Mapper[dbutils.PlainStorageChangeSetBucket]
	doTestEncodingStorageNew(t, plainKeyGenerator, getDefaultIncarnation, hashValueGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithDefaultIncarnationAndEmptyValueHashed(t *testing.T) {
	m := Mapper[dbutils.StorageChangeSetBucket]
	doTestEncodingStorageNew(t, hashKeyGenerator, getDefaultIncarnation, emptyValueGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithDefaultIncarnationAndEmptyValuePlain(t *testing.T) {
	m := Mapper[dbutils.PlainStorageChangeSetBucket]
	doTestEncodingStorageNew(t, plainKeyGenerator, getDefaultIncarnation, emptyValueGenerator, m.New, m.Encode, m.Decode)
}

func doTestEncodingStorageNew(
	t *testing.T,
	keyGen func(common.Address, uint64, common.Hash) []byte,
	incarnationGenerator func() uint64,
	valueGenerator func(int) []byte,
	newFunc func() *ChangeSet,
	encodeFunc Encoder,
	decodeFunc Decoder,
) {
	f := func(t *testing.T, numOfElements int, numOfKeys int) {
		var err error
		ch := newFunc()
		for i := 0; i < numOfElements; i++ {
			inc := incarnationGenerator()
			for j := 0; j < numOfKeys; j++ {
				key := getTestDataAtIndex(i, j, inc, keyGen)
				val := valueGenerator(j)
				err = ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}

			}
		}
		ch2 := newFunc()
		err = encodeFunc(0, ch, func(k, v []byte) error {
			_, k, v = decodeFunc(k, v)
			return ch2.Add(k, v)
		})
		if err != nil {
			t.Fatal(err)
		}

		for i := range ch.Changes {
			if !bytes.Equal(ch.Changes[i].Key, ch2.Changes[i].Key) {
				t.Log(common.Bytes2Hex(ch.Changes[i].Key))
				t.Log(common.Bytes2Hex(ch2.Changes[i].Key))
				t.Error("not equal", i)
			}
		}
		for i := range ch.Changes {
			if !bytes.Equal(ch.Changes[i].Value, ch2.Changes[i].Value) {
				t.Log(common.Bytes2Hex(ch.Changes[i].Value))
				t.Log(common.Bytes2Hex(ch2.Changes[i].Value))
				t.Fatal("not equal", i)
			}
		}

		if !reflect.DeepEqual(ch, ch2) {
			for i, v := range ch.Changes {
				if !bytes.Equal(v.Key, ch2.Changes[i].Key) || !bytes.Equal(v.Value, ch2.Changes[i].Value) {
					fmt.Println("Diff ", i)
					fmt.Println("k1", common.Bytes2Hex(v.Key), len(v.Key))
					fmt.Println("k2", common.Bytes2Hex(ch2.Changes[i].Key))
					fmt.Println("v1", common.Bytes2Hex(v.Value))
					fmt.Println("v2", common.Bytes2Hex(ch2.Changes[i].Value))
				}
			}
			t.Error("not equal")
		}
	}

	for _, v := range numOfChanges {
		v := v
		t.Run(formatTestName(v, 1), func(t *testing.T) {
			f(t, v, 1)
		})
	}

	for _, v := range numOfChanges {
		v := v
		t.Run(formatTestName(v, 5), func(t *testing.T) {
			f(t, v, 5)
		})
	}

	t.Run(formatTestName(10, 10), func(t *testing.T) {
		f(t, 10, 10)
	})
	t.Run(formatTestName(50, 1000), func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run(formatTestName(100, 1000), func(t *testing.T) {
		f(t, 100, 1000)
	})
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationWalkHashed(t *testing.T) {
	m := Mapper[dbutils.StorageChangeSetBucket]
	doTestWalk(t, hashKeyGenerator, m.New, m.Encode, m.Decode)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationWalkPlain(t *testing.T) {
	m := Mapper[dbutils.PlainStorageChangeSetBucket]
	doTestWalk(t, plainKeyGenerator, m.New, m.Encode, m.Decode)
}

func doTestWalk(
	t *testing.T,
	generator func(common.Address, uint64, common.Hash) []byte,
	newfunc func() *ChangeSet,
	encodeFunc Encoder,
	decodeFunc Decoder,
) {
	ch := newfunc()
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		for i := 0; i < numOfElements; i++ {
			for j := 0; j < numOfKeys; j++ {
				val := hashValueGenerator(j)
				key := getTestDataAtIndex(i, j, defaultIncarnation, generator)
				err := ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		i := 0
		err := encodeFunc(0, ch, func(k, v []byte) error {
			_, k, v = decodeFunc(k, v)
			if !bytes.Equal(k, ch.Changes[i].Key) {
				t.Log(common.Bytes2Hex(ch.Changes[i].Key))
				t.Log(common.Bytes2Hex(k))
				t.Error(i, "key was incorrect", common.Bytes2Hex(k), common.Bytes2Hex(ch.Changes[i].Key))
			}
			if !bytes.Equal(v, ch.Changes[i].Value) {
				t.Log(common.Bytes2Hex(ch.Changes[i].Value))
				t.Log(common.Bytes2Hex(v))
				t.Error(i, "val is incorrect", v, ch.Changes[i].Value)
			}
			i++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, v := range numOfChanges {
		v := v
		t.Run(fmt.Sprintf("elements: %d keys: %d", v, 1), func(t *testing.T) {
			f(t, v, 1)
		})
	}

	for _, v := range numOfChanges {
		v := v
		t.Run(fmt.Sprintf("elements: %d keys: %d", v, 5), func(t *testing.T) {
			f(t, v, 5)
		})
	}

	t.Run(formatTestName(50, 1000), func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run(formatTestName(5, 1000), func(t *testing.T) {
		f(t, 5, 1000)
	})
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindHashed(t *testing.T) {
	bkt := dbutils.StorageChangeSetBucket
	m := Mapper[bkt]
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	cs := m.WalkerAdapter(tx.CursorDupSort(bkt)).(StorageChangeSet)

	clear := func() {
		c := tx.Cursor(bkt)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.First() {
			if err != nil {
				t.Fatal(err)
			}
			err = c.DeleteCurrent()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	doTestFind(t, tx, bkt, hashKeyGenerator, m.New, m.Encode, m.Decode, cs.FindWithIncarnation, clear)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindPlain(t *testing.T) {
	bkt := dbutils.PlainStorageChangeSetBucket
	m := Mapper[bkt]
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	cs := m.WalkerAdapter(tx.CursorDupSort(bkt)).(StorageChangeSetPlain)

	clear := func() {
		c := tx.Cursor(bkt)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.First() {
			if err != nil {
				t.Fatal(err)
			}
			err = c.DeleteCurrent()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	doTestFind(t, tx, bkt, plainKeyGenerator, m.New, m.Encode, m.Decode, cs.FindWithIncarnation, clear)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindWithoutIncarnationHashed(t *testing.T) {
	bkt := dbutils.StorageChangeSetBucket
	m := Mapper[bkt]
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	cs := m.WalkerAdapter(tx.CursorDupSort(bkt)).(StorageChangeSet)

	clear := func() {
		c := tx.Cursor(bkt)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.First() {
			if err != nil {
				t.Fatal(err)
			}
			err = c.DeleteCurrent()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	doTestFind(t, tx, bkt, hashKeyGenerator, m.New, m.Encode, m.Decode, findWithoutIncarnationFunc(cs.FindWithoutIncarnation), clear)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindWithoutIncarnationPlain(t *testing.T) {
	bkt := dbutils.PlainStorageChangeSetBucket
	m := Mapper[bkt]
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	cs := m.WalkerAdapter(tx.CursorDupSort(bkt)).(StorageChangeSetPlain)

	clear := func() {
		c := tx.Cursor(bkt)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.First() {
			if err != nil {
				t.Fatal(err)
			}
			err = c.DeleteCurrent()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	doTestFind(t, tx, bkt, plainKeyGenerator, m.New, m.Encode, m.Decode, findWithoutIncarnationFunc(cs.FindWithoutIncarnation), clear)
}

func findWithoutIncarnationFunc(f func(blockNumber uint64, addrHashToFind []byte, keyHashToFind []byte) ([]byte, error)) func(blockN uint64, k []byte) ([]byte, error) {
	return func(blockN uint64, k []byte) ([]byte, error) {
		isHashed := len(k) == 2*common.HashLength+common.IncarnationLength
		var addrBytes []byte
		var keyBytes []byte
		if isHashed {
			addrHash, _, key := dbutils.ParseCompositeStorageKey(k)
			addrBytes = addrHash[:]
			keyBytes = key[:]
		} else {
			addr, _, key := dbutils.PlainParseCompositeStorageKey(k)
			addrBytes = addr[:]
			keyBytes = key[:]
		}
		return f(blockN, addrBytes, keyBytes)
	}
}

func doTestFind(
	t *testing.T,
	tx ethdb.Tx,
	bucket string,
	generator func(common.Address, uint64, common.Hash) []byte,
	newFunc func() *ChangeSet,
	encodeFunc Encoder,
	_ Decoder,
	findFunc func(uint64, []byte) ([]byte, error),
	clear func(),
) {
	t.Helper()
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		defer clear()
		ch := newFunc()
		for i := 0; i < numOfElements; i++ {
			for j := 0; j < numOfKeys; j++ {
				val := hashValueGenerator(j)
				key := getTestDataAtIndex(i, j, defaultIncarnation, generator)
				err := ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		c := tx.Cursor(bucket)
		defer c.Close()
		err := encodeFunc(1, ch, func(k, v []byte) error {
			if err2 := c.Append(common.CopyBytes(k), common.CopyBytes(v)); err2 != nil {
				return err2
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range ch.Changes {
			val, err := findFunc(1, v.Key)
			if err != nil {
				t.Error(err, i)
			}
			if !bytes.Equal(val, v.Value) {
				t.Fatal("value not equal for ", v.Value, val)
			}
		}
	}

	for _, v := range numOfChanges[:len(numOfChanges)-2] {
		v := v
		t.Run(fmt.Sprintf("elements: %d keys: %d", v, 1), func(t *testing.T) {
			f(t, v, 1)
		})
	}

	for _, v := range numOfChanges[:len(numOfChanges)-2] {
		v := v
		t.Run(fmt.Sprintf("elements: %d keys: %d", v, 5), func(t *testing.T) {
			f(t, v, 5)
		})
	}

	t.Run(formatTestName(50, 1000), func(t *testing.T) {
		f(t, 50, 1000)
	})

	t.Run(formatTestName(100, 1000), func(t *testing.T) {
		f(t, 100, 1000)
	})
}

func BenchmarkDecodeNewStorage(t *testing.B) {
	numOfElements := 10
	// empty StorageChangeSet first
	ch := NewStorageChangeSet()
	var err error
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, rand.Uint64(), key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	dec := FromDBFormat(common.HashLength)

	t.ResetTimer()
	var ch2 *ChangeSet
	for i := 0; i < t.N; i++ {
		err := EncodeStorage(1, ch, func(k, v []byte) error {
			_, _, _ = dec(k, v)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = ch2
}

func BenchmarkEncodeNewStorage(t *testing.B) {
	numOfElements := 10
	// empty StorageChangeSet first
	ch := NewStorageChangeSet()
	var err error
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, rand.Uint64(), key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		err := EncodeStorage(1, ch, func(k, v []byte) error {
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func formatTestName(elements, keys int) string {
	return fmt.Sprintf("elements: %d keys: %d", elements, keys)
}

func TestMultipleIncarnationsOfTheSameContract(t *testing.T) {
	bkt := dbutils.PlainStorageChangeSetBucket
	m := Mapper[bkt]
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.KV().Begin(context.Background(), nil, ethdb.RW)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	cs := m.WalkerAdapter(tx.CursorDupSort(bkt)).(StorageChangeSetPlain)

	contractA := common.HexToAddress("0x6f0e0cdac6c716a00bd8db4d0eee4f2bfccf8e6a")
	contractB := common.HexToAddress("0xc5acb79c258108f288288bc26f7820d06f45f08c")
	contractC := common.HexToAddress("0x1cbdd8336800dc3fe27daf5fb5188f0502ac1fc7")
	contractD := common.HexToAddress("0xd88eba4c93123372a9f67215f80477bc3644e6ab")

	key1 := common.HexToHash("0xa4e69cebbf4f8f3a1c6e493a6983d8a5879d22057a7c73b00e105d7c7e21efbc")
	key2 := common.HexToHash("0x0bece5a88f7b038f806dbef77c0b462506e4b566c5be7dd44e8e2fc7b1f6a99c")
	key3 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	key4 := common.HexToHash("0x4fdf6c1878d2469b49684effe69db8689d88a4f1695055538501ff197bc9e30e")
	key5 := common.HexToHash("0xaa2703c3ae5d0024b2c3ab77e5200bb2a8eb39a140fad01e89a495d73760297c")
	key6 := common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000df77")
	key7 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")

	val1 := common.FromHex("0x33bf0d0c348a2ef1b3a12b6a535e1e25a56d3624e45603e469626d80fd78c762")
	val2 := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000459")
	val3 := common.FromHex("0x0000000000000000000000000000002506e4b566c5be7dd44e8e2fc7b1f6a99c")
	val4 := common.FromHex("0x207a386cdf40716455365db189633e822d3a7598558901f2255e64cb5e424714")
	val5 := common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000")
	val6 := common.FromHex("0xec89478783348038046b42cc126a3c4e351977b5f4cf5e3c4f4d8385adbf8046")

	c := tx.CursorDupSort(bkt)

	ch := NewStorageChangeSetPlain()
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA, 2, key1), val1))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA, 1, key5), val5))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA, 2, key6), val6))

	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractB, 1, key2), val2))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractB, 1, key3), val3))

	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractC, 5, key4), val4))

	assert.NoError(t, EncodeStoragePlain(1, ch, func(k, v []byte) error {
		return c.Append(k, v)
	}))

	data1, err1 := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractA, 2, key1))
	assert.NoError(t, err1)
	assert.Equal(t, data1, val1)

	data3, err3 := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractB, 1, key3))
	assert.NoError(t, err3)
	assert.Equal(t, data3, val3)

	data5, err5 := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractA, 1, key5))
	assert.NoError(t, err5)
	assert.Equal(t, data5, val5)

	_, errA := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractA, 1, key1))
	assert.Error(t, errA)

	_, errB := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractD, 2, key1))
	assert.Error(t, errB)

	_, errC := cs.FindWithIncarnation(1, dbutils.PlainGenerateCompositeStorageKey(contractB, 1, key7))
	assert.Error(t, errC)
}
