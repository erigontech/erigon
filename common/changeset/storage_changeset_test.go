package changeset

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/assert"
)

const (
	defaultIncarnation = 1
)

func TestEncodingStorageWithoutNotDefaultIncarnation1(t *testing.T) {
	f := func(t *testing.T, numOfElements int) {
		// empty StorageChangeSset first
		ch := NewStorageChangeSet()
		_, err := EncodeStorage(ch)
		assert.NoError(t, err)

		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
			val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
			err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
			if err != nil {
				t.Fatal(err)
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
			t.Fatal("not equal")
		}
	}

	t.Run("10", func(t *testing.T) {
		f(t, 10)
	})
	t.Run("100", func(t *testing.T) {
		f(t, 100)
	})
	t.Run("1000", func(t *testing.T) {
		f(t, 1000)
	})
	t.Run("10000", func(t *testing.T) {
		f(t, 10000)
	})
}

func TestEncodingStorageWithtRandomIncarnation(t *testing.T) {

	f := func(t *testing.T, numOfElements int) {
		// empty StorageChangeSet first
		ch := NewStorageChangeSet()
		_, err := EncodeStorage(ch)
		assert.NoError(t, err)

		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
			val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
			err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, rand.Uint64(), key), val.Bytes())
			if err != nil {
				t.Error(err)
			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Error(err)
		}

		ch2, err := DecodeStorage(b)
		if err != nil {
			t.Error(err)
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

	t.Run("10", func(t *testing.T) {
		f(t, 10)
	})
	t.Run("100", func(t *testing.T) {
		f(t, 100)
	})
	t.Run("1000", func(t *testing.T) {
		f(t, 1000)
	})
	t.Run("10000", func(t *testing.T) {
		f(t, 10000)
	})
}

func TestEncodingStorageWithoutNotDefaultIncarnationWalk(t *testing.T) {
	f := func(t *testing.T, numOfElements int) {
		ch := NewStorageChangeSet()
		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
			val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
			err := ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		sort.Sort(ch)
		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		err = StorageChangeSetBytes(b).Walk(func(k, v []byte) error {
			if !bytes.Equal(k, ch.Changes[i].Key) {
				t.Errorf("%d key was incorrect %x %x", i, k, ch.Changes[i].Key)
			}
			if !bytes.Equal(v, ch.Changes[i].Value) {
				t.Errorf("%d val is incorrect %x %x", i, v, ch.Changes[i].Value)
			}
			i++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("10", func(t *testing.T) {
		f(t, 10)
	})
	t.Run("100", func(t *testing.T) {
		f(t, 100)
	})
	t.Run("1000", func(t *testing.T) {
		f(t, 1000)
	})
	t.Run("10000", func(t *testing.T) {
		f(t, 10000)
	})
}

func TestEncodingStorageWithoutNotDefaultIncarnationFind(t *testing.T) {
	f := func(t *testing.T, numOfElements int) {
		ch := NewStorageChangeSet()

		for i := 0; i < numOfElements; i++ {
			addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
			key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
			val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
			err := ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		b, err := EncodeStorage(ch)
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range ch.Changes {
			val, err := StorageChangeSetBytes(b).Find(v.Key[:common.HashLength], v.Key[common.HashLength+common.IncarnationLength:])
			if err != nil {
				t.Error(err, i)
			}
			if !bytes.Equal(val, v.Value) {
				t.Errorf("not equal for %x %x", v, val)
			}
		}
	}

	t.Run("10", func(t *testing.T) {
		f(t, 10)
	})
	t.Run("100", func(t *testing.T) {
		f(t, 100)
	})
	t.Run("1000", func(t *testing.T) {
		f(t, 1000)
	})
	t.Run("3000", func(t *testing.T) {
		f(t, 3000)
	})
	t.Run("10000", func(t *testing.T) {
		f(t, 10000)
	})
}
