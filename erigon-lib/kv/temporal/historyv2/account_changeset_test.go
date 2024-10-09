/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package historyv2

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func TestEncodingAccount(t *testing.T) {
	bkt := kv.AccountChangeSet
	m := Mapper[bkt]

	ch := m.New()
	// empty StorageChangeSset first
	err := m.Encode(1, ch, func(k, v []byte) error {
		return fmt.Errorf("must never call")
	})
	assert.NoError(t, err)

	vals := [][]byte{
		hexutility.MustDecodeHex("f7f6db1eb17c6d582078e0ffdd0c"),
		hexutility.MustDecodeHex("b1e9b5c16355eede662031dd621d08faf4ea"),
		hexutility.MustDecodeHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"),
	}
	numOfElements := 3
	for i := 0; i < numOfElements; i++ {
		address := hexutility.MustDecodeHex(fmt.Sprintf("0xBe828AD8B538D1D691891F6c725dEdc5989abBc%d", i))
		err2 := ch.Add(address, vals[i])
		if err2 != nil {
			t.Fatal(err)
		}
	}

	ch2 := m.New()
	err = m.Encode(1, ch, func(k, v []byte) error {
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

	if !reflect.DeepEqual(ch, ch2) {
		fmt.Println("ch", len(ch.Changes), "ch2", len(ch2.Changes))
		for i, v := range ch.Changes {
			fmt.Println("Line ", i)

			if !bytes.Equal(v.Key, ch2.Changes[i].Key) || !bytes.Equal(v.Value, ch2.Changes[i].Value) {
				fmt.Println("Diff ", i)
				fmt.Println("k1", hex.EncodeToString(v.Key), len(v.Key))
				fmt.Println("k2", hex.EncodeToString(ch2.Changes[i].Key))
				fmt.Println("v1", hex.EncodeToString(v.Value))
				fmt.Println("v2", hex.EncodeToString(ch2.Changes[i].Value))
			}
		}
		fmt.Printf("%+v %+v\n", ch, ch2)
		t.Fatal("not equal")
	}
}
