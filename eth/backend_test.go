package eth

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	db := ethdb.NewMemDatabase()
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, ethdb.StorageMode{}) {
		t.Fatal()
	}

	err = setStorageModeIfNotExist(db, ethdb.StorageMode{
		true,
		true,
		true,
		true,
	})
	if err != nil {
		t.Fatal(err)
	}

	sm, err = ethdb.GetStorageModeFromDB(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, ethdb.StorageMode{
		true,
		true,
		true,
		true,
	}) {
		spew.Dump(sm)
		t.Fatal("not equal")
	}
}
