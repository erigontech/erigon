package hack

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestDecodeNewStorageDebug(t *testing.T) {
	t.Skip("debug test")
	pathToDB := ""
	db := ethdb.MustOpen(pathToDB)

	data, err := db.Get(dbutils.StorageChangeSetBucket2, dbutils.EncodeTimestamp(116526))
	if err != nil {
		t.Fatal(err)
	}

	cs, err := changeset.DecodeStorage(data)
	if err != nil {
		t.Fatal(err)
	}

	newData, err := changeset.EncodeStorage(cs)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range cs.Changes {
		fmt.Println(common.Bytes2Hex(v.Key), " - ", common.Bytes2Hex(v.Value))
	}
	j := 0
	fmt.Println()
	err = changeset.StorageChangeSetBytes(newData).Walk(func(kk, vv []byte) error {
		fmt.Println(common.Bytes2Hex(kk), " - ", common.Bytes2Hex(vv))
		if !bytes.Equal(kk, cs.Changes[j].Key) {
			t.Errorf("incorrect order. element: %v", j)
		}
		if !bytes.Equal(vv, cs.Changes[j].Value) {
			t.Errorf("incorrect value. key:%v", common.Bytes2Hex(cs.Changes[j].Key))
		}
		j++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
