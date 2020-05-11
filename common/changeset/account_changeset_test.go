package changeset

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

type csAccountBytes interface {
	Walk(func([]byte, []byte) error) error
	FindLast([]byte) ([]byte, error)
}

func TestEncodingAccountHashed(t *testing.T) {
	ch := NewAccountChangeSet()
	runTestAccountEncoding(t, ch, true /*isHashed*/)
}

func TestEncodingAccountPlain(t *testing.T) {
	ch := NewAccountChangeSetPlain()
	runTestAccountEncoding(t, ch, false /*isHashed*/)
}

func runTestAccountEncoding(t *testing.T, ch *ChangeSet, isHashed bool) {
	// empty StorageChangeSset first
	_, err := EncodeAccounts(ch)
	assert.NoError(t, err)

	vals := [][]byte{
		common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"),
		common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"),
		common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"),
	}
	numOfElements := 3
	for i := 0; i < numOfElements; i++ {
		address := common.HexToAddress(fmt.Sprintf("0xBe828AD8B538D1D691891F6c725dEdc5989abBc%d", i))
		if isHashed {
			addrHash, _ := common.HashData(address[:])
			err = ch.Add(addrHash.Bytes(), vals[i])
		} else {
			err = ch.Add(address[:], vals[i])
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	encFunc := EncodeAccountsPlain
	if isHashed {
		encFunc = EncodeAccounts
	}

	b, err := encFunc(ch)
	if err != nil {
		t.Fatal(err)
	}

	decFunc := DecodeAccountsPlain
	if isHashed {
		decFunc = DecodeAccounts
	}

	ch2, err := decFunc(b)
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

	var csBytes csAccountBytes

	if isHashed {
		csBytes = AccountChangeSetBytes(b)
	} else {
		csBytes = AccountChangeSetPlainBytes(b)
	}
	i := 0
	err = csBytes.Walk(func(k, v []byte) error {
		if !bytes.Equal(k, ch2.Changes[i].Key) || !bytes.Equal(v, ch2.Changes[i].Value) {
			fmt.Println("Diff ", i)
			fmt.Println("k1", common.Bytes2Hex(k), len(k))
			fmt.Println("k2", common.Bytes2Hex(ch2.Changes[i].Key))
			fmt.Println("v1", common.Bytes2Hex(v))
			fmt.Println("v2", common.Bytes2Hex(ch2.Changes[i].Value))
			t.Fatal("not equal line")
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range ch.Changes {
		val, err := csBytes.FindLast(v.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v.Value, val) {
			t.Fatal("not equal")
		}
	}
}
