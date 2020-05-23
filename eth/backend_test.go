package eth

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	db := ethdb.NewMemDatabase()
	sm, err := GetStorageModeFromDB(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, StorageMode{}) {
		t.Fatal()
	}

	err = setStorageModeIfNotExist(db, StorageMode{
		true,
		true,
		true,
		true,
	})
	if err != nil {
		t.Fatal(err)
	}

	sm, err = GetStorageModeFromDB(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, StorageMode{
		true,
		true,
		true,
		true,
	}) {
		spew.Dump(sm)
		t.Fatal("not equal")
	}

}
