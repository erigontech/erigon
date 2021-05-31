package ethdb

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := NewTestTx(t)
	sm, err := GetStorageModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, StorageMode{Initialised: true}) {
		t.Fatal()
	}

	err = SetStorageModeIfNotExist(tx, StorageMode{
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

	sm, err = GetStorageModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, StorageMode{
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
