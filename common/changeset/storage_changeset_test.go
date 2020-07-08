package changeset

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
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

type csStorageBytes interface {
	Walk(func([]byte, []byte) error) error
	Find([]byte) ([]byte, error)
	FindWithoutIncarnation([]byte, []byte) ([]byte, error)
	FindWithIncarnation([]byte) ([]byte, error)
}

func getHashedBytes(b []byte) csStorageBytes {
	return StorageChangeSetBytes(b)
}

func getPlainBytes(b []byte) csStorageBytes {
	return StorageChangeSetPlainBytes(b)
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

type encodeFunc func(*ChangeSet) ([]byte, error)
type decodeFunc func([]byte) (*ChangeSet, error)

func TestEncodingStorageNewWithRandomIncarnationHashed(t *testing.T) {
	ch := NewStorageChangeSet()
	doTestEncodingStorageNew(t, ch, hashKeyGenerator, getRandomIncarnation, hashValueGenerator, EncodeStorage, DecodeStorage)
}

func TestEncodingStorageNewWithRandomIncarnationPlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestEncodingStorageNew(t, ch, plainKeyGenerator, getRandomIncarnation, hashValueGenerator, EncodeStoragePlain, DecodeStoragePlain)
}

func TestEncodingStorageNewWithDefaultIncarnationHashed(t *testing.T) {
	ch := NewStorageChangeSet()
	doTestEncodingStorageNew(t, ch, hashKeyGenerator, getDefaultIncarnation, hashValueGenerator, EncodeStorage, DecodeStorage)
}

func TestEncodingStorageNewWithDefaultIncarnationPlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestEncodingStorageNew(t, ch, plainKeyGenerator, getDefaultIncarnation, hashValueGenerator, EncodeStoragePlain, DecodeStoragePlain)
}

func TestEncodingStorageNewWithDefaultIncarnationAndEmptyValueHashed(t *testing.T) {
	ch := NewStorageChangeSet()
	doTestEncodingStorageNew(t, ch, hashKeyGenerator, getDefaultIncarnation, emptyValueGenerator, EncodeStorage, DecodeStorage)
}

func TestEncodingStorageNewWithDefaultIncarnationAndEmptyValuePlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestEncodingStorageNew(t, ch, plainKeyGenerator, getDefaultIncarnation, emptyValueGenerator, EncodeStoragePlain, DecodeStoragePlain)
}

func doTestEncodingStorageNew(
	t *testing.T,
	ch *ChangeSet,
	keyGen func(common.Address, uint64, common.Hash) []byte,
	incarnationGenerator func() uint64,
	valueGenerator func(int) []byte,
	encodeFunc encodeFunc,
	decodeFunc decodeFunc,
) {
	f := func(t *testing.T, numOfElements int, numOfKeys int) {
		var err error
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

		b, err := encodeFunc(ch)
		if err != nil {
			t.Fatal(err)
		}

		ch2, err := decodeFunc(b)
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
	ch := NewStorageChangeSet()
	doTestWalk(t, ch, hashKeyGenerator, EncodeStorage, getHashedBytes)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationWalkPlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestWalk(t, ch, plainKeyGenerator, EncodeStoragePlain, getPlainBytes)
}

func doTestWalk(
	t *testing.T,
	ch *ChangeSet,
	generator func(common.Address, uint64, common.Hash) []byte,
	encodeFunc encodeFunc,
	csStorageBytes func([]byte) csStorageBytes,
) {
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

		b, err := encodeFunc(ch)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		err = csStorageBytes(b).Walk(func(k, v []byte) error {
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
	ch := NewStorageChangeSet()
	doTestFind(t, ch, hashKeyGenerator, EncodeStorage, getHashedBytes, regularFindFunc)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindPlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestFind(t, ch, plainKeyGenerator, EncodeStoragePlain, getPlainBytes, regularFindFunc)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindWithoutIncarnationHashed(t *testing.T) {
	ch := NewStorageChangeSet()
	doTestFind(t, ch, hashKeyGenerator, EncodeStorage, getHashedBytes, findWithoutIncarnationFunc)
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFindWithoutIncarnationPlain(t *testing.T) {
	ch := NewStorageChangeSetPlain()
	doTestFind(t, ch, plainKeyGenerator, EncodeStoragePlain, getPlainBytes, findWithoutIncarnationFunc)
}

func regularFindFunc(b csStorageBytes, k []byte) ([]byte, error) {
	return b.FindWithIncarnation(k)
}

func findWithoutIncarnationFunc(b csStorageBytes, k []byte) ([]byte, error) {
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
	return b.FindWithoutIncarnation(addrBytes, keyBytes)
}

func doTestFind(
	t *testing.T,
	ch *ChangeSet,
	generator func(common.Address, uint64, common.Hash) []byte,
	encodeFunc encodeFunc,
	csStorageBytes func([]byte) csStorageBytes,
	findFunc func(csStorageBytes, []byte) ([]byte, error),
) {
	t.Helper()
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

		b, err := encodeFunc(ch)
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range ch.Changes {
			val, err := findFunc(csStorageBytes(b), v.Key)
			if err != nil {
				t.Error(err, i)
			}
			if !bytes.Equal(val, v.Value) {
				t.Fatal("value not equal for ", v, val)
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

	b, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()
	var ch2 *ChangeSet
	for i := 0; i < t.N; i++ {
		ch2, err = DecodeStorage(b)
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

	var b []byte
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		b, err = EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = b
}

func BenchmarkFindStorage(t *testing.B) {
	numOfElements := 1000
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

	var v []byte
	b, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}

	finder := StorageChangeSetBytes(b)
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		v, err = finder.Find(ch.Changes[10].Key)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = b
	_ = v
}

func BenchmarkWalkStorage(t *testing.B) {
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

	var v, k []byte
	b, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}

	finder := StorageChangeSetBytes(b)
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		err = finder.Walk(func(kk, vv []byte) error {
			v = vv
			k = kk
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = b
	_ = v
	_ = k
}

func formatTestName(elements, keys int) string {
	return fmt.Sprintf("elements: %d keys: %d", elements, keys)
}

// TestDefaultIncarnationCompress is a encoding-specific test that may need to be
// adjusted if the encoding changes. This tests checks that default incarnations are
// getting compressed
func TestDefaultIncarnationCompressHashed(t *testing.T) {
	ch1 := NewStorageChangeSet()
	ch2 := NewStorageChangeSet()
	doTestDefaultIncarnationCompress(t, ch1, ch2, hashKeyGenerator, EncodeStorage)
}

func TestDefaultIncarnationCompressPlain(t *testing.T) {
	ch1 := NewStorageChangeSetPlain()
	ch2 := NewStorageChangeSetPlain()
	doTestDefaultIncarnationCompress(t, ch1, ch2, plainKeyGenerator, EncodeStoragePlain)
}

func doTestDefaultIncarnationCompress(t *testing.T,
	ch1 *ChangeSet,
	ch2 *ChangeSet,
	generator func(common.Address, uint64, common.Hash) []byte,
	encodeFunc encodeFunc,
) {
	// We create two changsets, with the same data, except for the incarnation
	// First changeset has incarnation == defaultIncarnation, which should be compressed
	// Second changeset has incarnation == defautIncarnation+1, which would not be compressed
	key1 := getTestDataAtIndex(0, 0, defaultIncarnation, generator)
	key2 := getTestDataAtIndex(0, 0, defaultIncarnation+1, generator)
	val := hashValueGenerator(0)
	err := ch1.Add(key1, val)
	if err != nil {
		t.Fatal(err)
	}
	b1, err1 := encodeFunc(ch1)
	if err1 != nil {
		t.Fatal(err1)
	}
	err = ch2.Add(key2, val)
	if err != nil {
		t.Fatal(err)
	}
	b2, err2 := encodeFunc(ch2)
	if err2 != nil {
		t.Fatal(err2)
	}
	if len(b1) >= len(b2) {
		t.Errorf("first encoding should be shorter than the second, got %d >= %d", len(b1), len(b2))
	}
}
