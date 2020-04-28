package changeset

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

const (
	defaultIncarnation = 1
)

var numOfChanges = []int{1, 3, 10, 100, 1000, 10000}

func TestEncodingStorageNewWithRandomIncarnation(t *testing.T) {
	f := func(t *testing.T, numOfElements int, numOfKeys int) {
		// empty StorageChangeSet first
		ch := NewStorageChangeSet()
		var err error
		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			inc := rand.Uint64()
			for j := 0; j < numOfKeys; j++ {
				key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
				val, _ := common.HashData([]byte("val" + strconv.Itoa(j)))
				err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, inc, key), val.Bytes())
				if err != nil {
					t.Fatal(err)
				}

			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		ch2, err := DecodeStorage(b)
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

	t.Run("3.3", func(t *testing.T) {
		f(t, 3, 3)
	})
	t.Run("10.10", func(t *testing.T) {
		f(t, 10, 10)
	})
	t.Run("50.1000", func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run("5.10000", func(t *testing.T) {
		f(t, 5, 10000)
	})
}

func TestEncodingStorageNewWithDefaultIncarnation(t *testing.T) {
	f := func(t *testing.T, numOfElements int, numOfKeys int) {
		// empty StorageChangeSet first
		ch := NewStorageChangeSet()
		var err error
		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			for j := 0; j < numOfKeys; j++ {
				key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
				val, _ := common.HashData([]byte("val" + strconv.Itoa(j)))
				err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
				if err != nil {
					t.Fatal(err)
				}

			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		ch2, err := DecodeStorage(b)
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

	t.Run("3.3", func(t *testing.T) {
		f(t, 3, 3)
	})
	t.Run("10.10", func(t *testing.T) {
		f(t, 10, 10)
	})
	t.Run("50.1000", func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run("5.10000", func(t *testing.T) {
		f(t, 5, 10000)
	})
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationWalk(t *testing.T) {
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		ch := NewStorageChangeSet()
		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			for j := 0; j < numOfKeys; j++ {
				key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
				val, _ := common.HashData([]byte("val" + strconv.Itoa(j)))
				err := ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		err = StorageChangeSetBytes(b).Walk(func(k, v []byte) error {
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

	t.Run("3.3", func(t *testing.T) {
		f(t, 3, 3)
	})
	t.Run("10.10", func(t *testing.T) {
		f(t, 10, 10)
	})
	t.Run("50.1000", func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run("5.10000", func(t *testing.T) {
		f(t, 5, 10000)
	})
}

func TestEncodingStorageNewWithoutNotDefaultIncarnationFind(t *testing.T) {
	f := func(t *testing.T, numOfElements, numOfKeys int) {
		ch := NewStorageChangeSet()

		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			for j := 0; j < numOfKeys; j++ {
				key, _ := common.HashData([]byte("key" + strconv.Itoa(j)))
				val, _ := common.HashData([]byte("val" + strconv.Itoa(j)))
				err := ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range ch.Changes {
			val, err := StorageChangeSetBytes(b).Find(v.Key)
			if err != nil {
				t.Error(err, i)
			}
			if !bytes.Equal(val, v.Value) {
				t.Error("value not equal for ") //, v, val)
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

	t.Run("3.3", func(t *testing.T) {
		f(t, 3, 3)
	})
	t.Run("10.10", func(t *testing.T) {
		f(t, 10, 10)
	})
	t.Run("50.1000", func(t *testing.T) {
		f(t, 50, 1000)
	})
	t.Run("5.10000", func(t *testing.T) {
		f(t, 5, 10000)
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
