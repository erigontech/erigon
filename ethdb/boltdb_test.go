package ethdb

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"reflect"
	"strconv"
	"testing"
)

func TestBoltDB_WalkAsOf(t *testing.T) {
	db:=NewMemDatabase()

	block2Expected:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}

	block4Expected:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}

	block6Expected:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}

	//create state and history
	for i:=uint8(1); i<5;i++ {
		key:=dbutils.GenerateCompositeStorageKey(common.Hash{i}, uint64(1), common.Hash{i})
		val:=[]byte("state   "+ strconv.Itoa(int(i)))
		err:=db.Put(dbutils.StorageBucket, key, val)
		if err!=nil {
			t.Fatal(err)
		}
		block6Expected.Add(key,val)
		if i<=2 {
			block4Expected.Add(key,val)
		}
	}
	for i:=uint8(1); i<=7;i++ {
		key:=dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1,common.Hash{i})
		val:=[]byte("block 3 "+ strconv.Itoa(int(i)))
		err:=db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			3,
			false,
		)
		block2Expected = block2Expected.Add(key, val)
		if err!=nil {
			t.Fatal(err)
		}
	}
	for i:=uint8(3); i<=7;i++ {
		key:=dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1,common.Hash{i})
		val:=[]byte("block 5 "+ strconv.Itoa(int(i)))
		err:=db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			5,
			false,
		)
		if err!=nil {
			t.Fatal(err)
		}
		block4Expected=block4Expected.Add(key,val)
	}


	block2:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}

	block4:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}

	block6:= &dbutils.ChangeSet{
		Changes:make([]dbutils.Change,0),
	}


	//walk and collect walkAsOf result
	var err error
	var startKey [32]byte
	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		block2.Add(k,v)
		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket,  startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		block6.Add(k,v)
		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket,  startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		block4.Add(k,v)
		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(block2.Changes, block2Expected.Changes) {
		t.Fatal("block 2 result is incorrect")
	}
	if !reflect.DeepEqual(block4, block4Expected) {
		t.Fatal("block 4 result is incorrect")
	}
	if !reflect.DeepEqual(block6, block6Expected) {
		t.Fatal("block 6 result is incorrect")
	}

}