package ethdb

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	db := NewMemDatabase()
	sm, err := GetStorageModeFromDB(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sm, StorageMode{}) {
		t.Fatal()
	}

	err = SetStorageModeIfNotExist(db, StorageMode{
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
	}) {
		spew.Dump(sm)
		t.Fatal("not equal")
	}
}


func TestName22(t *testing.T) {
	os.RemoveAll(os.TempDir()+"/tm1")
	//os.RemoveAll(os.TempDir()+"/tm2")
	db1:=NewLMDB().Path(os.TempDir()+"/tm1").MustOpen()
	db2:=NewLMDB().Path(os.TempDir()+"/tm2").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).ReadOnly().MustOpen()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	tx,err:=db1.Begin(context.Background(), nil, true)
	if err!=nil {
		t.Fatal(err)
	}
	tx2,err:=db2.Begin(context.Background(), nil, false)
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println(db1.AllBuckets())
	fmt.Println(db2.AllBuckets())
	c:=tx.Cursor(dbutils.HeaderPrefix)
	c2:=tx2.Cursor(dbutils.HeaderPrefix)
	v,err:=tx.(*lmdbTx).ExistingBuckets()
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("db1",v)
	v,err=tx2.(*lmdbTx).ExistingBuckets()
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("db1",v)
	_,_,err1:=c.Seek([]byte("sa"))
	_,_,err2:=c2.Seek([]byte("sa"))
	t.Log(err)
	t.Log(err1)
	t.Log(err2)

}
