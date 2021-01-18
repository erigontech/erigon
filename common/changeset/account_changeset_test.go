package changeset

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/assert"
)

func TestEncodingAccountPlain(t *testing.T) {
	bkt := dbutils.PlainAccountChangeSetBucket
	m := Mapper[bkt]
	runTestAccountEncoding(t, m.New, m.Encode, m.Decode)
}

func runTestAccountEncoding(t *testing.T, New func() *ChangeSet, enc Encoder, dec Decoder) {
	ch := New()
	// empty StorageChangeSset first
	err := enc(1, ch, func(k, v []byte) error {
		return fmt.Errorf("must never call")
	})
	assert.NoError(t, err)

	vals := [][]byte{
		common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"),
		common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"),
		common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"),
	}
	numOfElements := 3
	for i := 0; i < numOfElements; i++ {
		address := common.HexToAddress(fmt.Sprintf("0xBe828AD8B538D1D691891F6c725dEdc5989abBc%d", i))
		err := ch.Add(address[:], vals[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	ch2 := New()
	err = enc(1, ch, func(k, v []byte) error {
		_, k, v = dec(k, v)
		return ch2.Add(k, v)
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(ch, ch2) {
		fmt.Println("ch", len(ch.Changes), "ch2", len(ch2.Changes))
		for i, v := range ch.Changes {
			fmt.Println("Line ", i)

			if !bytes.Equal(v.Key, ch2.Changes[i].Key) || !bytes.Equal(v.Value, ch2.Changes[i].Value) {
				fmt.Println("Diff ", i)
				fmt.Println("k1", common.Bytes2Hex(v.Key), len(v.Key))
				fmt.Println("k2", common.Bytes2Hex(ch2.Changes[i].Key))
				fmt.Println("v1", common.Bytes2Hex(v.Value))
				fmt.Println("v2", common.Bytes2Hex(ch2.Changes[i].Value))
			}
		}
		fmt.Printf("%+v %+v\n", ch, ch2)
		t.Fatal("not equal")
	}
}
