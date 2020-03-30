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
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/stretchr/testify/assert"
)

const (
	defaultIncarnation = 1
)

func TestEncodingStorageWithoutNotDefaultIncarnation(t *testing.T) {
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
				t.Error(i, "key was incorrect", k, ch.Changes[i].Key)
			}
			if !bytes.Equal(v, ch.Changes[i].Value) {
				t.Error(i, "val is incorrect", v, ch.Changes[i].Value)
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
			val, err := StorageChangeSetBytes(b).Find(v.Key)
			if err != nil {
				t.Error(err, i)
			}
			if !bytes.Equal(val, v.Value) {
				t.Error("not equal for ", v, val)
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

func TestFind(t *testing.T) {
	// storage changes at block 51385
	changes := hexutil.MustDecode("0x0000000a0002353e456a1b25b4640cbf753b6094458a4e38929a0c5bbe22904d9d08abc6d11adf396ae6730bdcd2e30c871da8978de3251900d45eaf15c0ba4d8a691c1d251300290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563003f4920f7f194a9a91a5d5422dc6313c329b82e533bce5e6614fbd13d4da7a32800b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf601290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e56301405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace0159bd035209cfbd05133d9c61cd860212636c4146228286761610b6e8811e537a018db697c2abd4284e7bb9aae7273fd67d061dd6ed4282b8382a3ed29d9cfaa1bb0198da4b407718e49fb0fe900da3b7fb2c3e0fed30f4148729225f24534e3e471b01b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf601c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b000a000000000101020a0b0b0b0b0c0d130e0429d069189e0013041115")
	key := hexutil.MustDecode("0xdf396ae6730bdcd2e30c871da8978de3251900d45eaf15c0ba4d8a691c1d2513fffffffffffffffeb10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6")
	val, err := StorageChangeSetBytes(changes).Find(key)
	assert.NoError(t, err)
	assert.Equal(t, hexutil.MustDecode("0x11"), val)
}
