package ethdb_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := kv.NewTestTx(t)
	sm, err := ethdb.GetStorageModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, ethdb.StorageMode{Initialised: true}) {
		t.Fatal()
	}

	err = ethdb.SetStorageModeIfNotExist(tx, ethdb.StorageMode{
		true,
		true,
		true,
		true,
		true,
		false,
	})
	if err != nil {
		t.Fatal(err)
	}

	sm, err = ethdb.GetStorageModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, ethdb.StorageMode{
		true,
		true,
		true,
		true,
		true,
		false,
	}) {
		spew.Dump(sm)
		t.Fatal("not equal")
	}
}
