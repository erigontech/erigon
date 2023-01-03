package changeset

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	historyv22 "github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	storageTable       = kv.StorageChangeSet
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
	return nil
}

func getTestDataAtIndex(i, j int, inc uint64) []byte {
	address := common.HexToAddress(fmt.Sprintf("0xBe828AD8B538D1D691891F6c725dEdc5989abBc%d", i))
	key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
	return dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), inc, key.Bytes())
}

func TestEncodingStorageNewWithRandomIncarnation(t *testing.T) {
	doTestEncodingStorageNew(t, getRandomIncarnation, hashValueGenerator)
}

func TestEncodingStorageNewWithDefaultIncarnation(t *testing.T) {
	doTestEncodingStorageNew(t, getDefaultIncarnation, hashValueGenerator)
}

func TestEncodingStorageNewWithDefaultIncarnationAndEmptyValue(t *testing.T) {
	doTestEncodingStorageNew(t, getDefaultIncarnation, emptyValueGenerator)
}

func doTestEncodingStorageNew(
	t *testing.T,
	incarnationGenerator func() uint64,
	valueGenerator func(int) []byte,
) {
	m := historyv22.Mapper[storageTable]

	f := func(t *testing.T, numOfElements int, numOfKeys int) {
		var err error
		ch := m.New()
		for i := 0; i < numOfElements; i++ {
			inc := incarnationGenerator()
			for j := 0; j < numOfKeys; j++ {
				key := getTestDataAtIndex(i, j, inc)
				val := valueGenerator(j)
				err = ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}

			}
		}
		ch2 := m.New()
		err = m.Encode(0, ch, func(k, v []byte) error {
			var err error
			_, k, v, err = m.Decode(k, v)
			if err != nil {
				return err
			}
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

func TestEncodingStorageNewWithoutNotDefaultIncarnationWalk(t *testing.T) {
	m := historyv22.Mapper[storageTable]

	ch := m.New()
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		for i := 0; i < numOfElements; i++ {
			for j := 0; j < numOfKeys; j++ {
				val := hashValueGenerator(j)
				key := getTestDataAtIndex(i, j, defaultIncarnation)
				err := ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		i := 0
		err := m.Encode(0, ch, func(k, v []byte) error {
			var err error
			_, k, v, err = m.Decode(k, v)
			assert.NoError(t, err)
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

func TestEncodingStorageNewWithoutNotDefaultIncarnationFind(t *testing.T) {
	m := historyv22.Mapper[storageTable]
	_, tx := memdb.NewTestTx(t)

	clear := func() {
		c, err := tx.RwCursor(storageTable)
		require.NoError(t, err)
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

	doTestFind(t, tx, m.Find, clear)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindWithoutIncarnation(t *testing.T) {
	bkt := storageTable
	m := historyv22.Mapper[bkt]
	_, tx := memdb.NewTestTx(t)

	clear := func() {
		c, err := tx.RwCursor(bkt)
		require.NoError(t, err)
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

	doTestFind(t, tx, m.Find, clear)
}

func doTestFind(
	t *testing.T,
	tx kv.RwTx,
	findFunc func(kv.CursorDupSort, uint64, []byte) ([]byte, error),
	clear func(),
) {
	m := historyv22.Mapper[storageTable]
	t.Helper()
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		defer clear()
		ch := m.New()
		for i := 0; i < numOfElements; i++ {
			for j := 0; j < numOfKeys; j++ {
				val := hashValueGenerator(j)
				key := getTestDataAtIndex(i, j, defaultIncarnation)
				err := ch.Add(key, val)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		c, err := tx.RwCursorDupSort(storageTable)
		require.NoError(t, err)

		err = m.Encode(1, ch, func(k, v []byte) error {
			if err2 := c.Put(libcommon.Copy(k), libcommon.Copy(v)); err2 != nil {
				return err2
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range ch.Changes {
			val, err := findFunc(c, 1, v.Key)
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
		f(t, v, 1)
	}

	for _, v := range numOfChanges[:len(numOfChanges)-2] {
		v := v
		f(t, v, 5)
	}

	f(t, 50, 1000)
	f(t, 100, 1000)
}

func BenchmarkDecodeNewStorage(t *testing.B) {
	numOfElements := 10
	// empty StorageChangeSet first
	ch := historyv22.NewStorageChangeSet()
	var err error
	for i := 0; i < numOfElements; i++ {
		address := []byte("0xa4e69cebbf4f8f3a1c6e493a6983d8a5879d22057a7c73b00e105d7c7e21ef" + strconv.Itoa(i))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.PlainGenerateCompositeStorageKey(address, rand.Uint64(), key[:]), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	t.ResetTimer()
	var ch2 *historyv22.ChangeSet
	for i := 0; i < t.N; i++ {
		err := historyv22.EncodeStorage(1, ch, func(k, v []byte) error {
			var err error
			_, _, _, err = historyv22.DecodeStorage(k, v)
			return err
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
	ch := historyv22.NewStorageChangeSet()
	var err error
	for i := 0; i < numOfElements; i++ {
		address := []byte("0xa4e69cebbf4f8f3a1c6e493a6983d8a5879d22057a7c73b00e105d7c7e21ef" + strconv.Itoa(i))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.PlainGenerateCompositeStorageKey(address, rand.Uint64(), key[:]), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		err := historyv22.EncodeStorage(1, ch, func(k, v []byte) error {
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
	bkt := kv.StorageChangeSet
	m := historyv22.Mapper[bkt]
	_, tx := memdb.NewTestTx(t)

	c1, err := tx.CursorDupSort(bkt)
	require.NoError(t, err)
	defer c1.Close()

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

	c, err := tx.RwCursorDupSort(bkt)
	require.NoError(t, err)

	ch := historyv22.NewStorageChangeSet()
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 2, key1.Bytes()), val1))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 1, key5.Bytes()), val5))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 2, key6.Bytes()), val6))

	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractB.Bytes(), 1, key2.Bytes()), val2))
	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractB.Bytes(), 1, key3.Bytes()), val3))

	assert.NoError(t, ch.Add(dbutils.PlainGenerateCompositeStorageKey(contractC.Bytes(), 5, key4.Bytes()), val4))

	assert.NoError(t, historyv22.EncodeStorage(1, ch, func(k, v []byte) error {
		return c.Put(k, v)
	}))

	data1, err1 := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 2, key1.Bytes()))
	assert.NoError(t, err1)
	assert.Equal(t, data1, val1)

	data3, err3 := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractB.Bytes(), 1, key3.Bytes()))
	assert.NoError(t, err3)
	assert.Equal(t, data3, val3)

	data5, err5 := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 1, key5.Bytes()))
	assert.NoError(t, err5)
	assert.Equal(t, data5, val5)

	_, errA := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractA.Bytes(), 1, key1.Bytes()))
	assert.Error(t, errA)

	_, errB := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractD.Bytes(), 2, key1.Bytes()))
	assert.Error(t, errB)

	_, errC := m.Find(c, 1, dbutils.PlainGenerateCompositeStorageKey(contractB.Bytes(), 1, key7.Bytes()))
	assert.Error(t, errC)
}
