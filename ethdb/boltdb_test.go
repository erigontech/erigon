package ethdb

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"strconv"
	"testing"
)

func TestBoltDB_WalkAsOf(t *testing.T) {
	db:=NewMemDatabase()

	for i:=uint8(0); i<5;i++ {
		err:=db.Put(dbutils.StorageBucket, dbutils.GenerateCompositeStorageKey(common.Hash{i}, uint64(1), common.Hash{i}), []byte("state "+ strconv.Itoa(int(i))))
		if err!=nil {
			t.Fatal(err)
		}
	}
	for i:=uint8(0); i<7;i++ {
		err:=db.PutS(
			dbutils.StorageHistoryBucket,
			dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1,common.Hash{i}),
			[]byte("block 3 "+ strconv.Itoa(int(i))),
			3,
			false,
		)
		if err!=nil {
			t.Fatal(err)
		}
	}
	for i:=uint8(0); i<7;i++ {
		err:=db.PutS(
			dbutils.StorageHistoryBucket,
			dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1,common.Hash{i}),
			[]byte("block 5 "+ strconv.Itoa(int(i))),
			5,
			false,
		)
		if err!=nil {
			t.Fatal(err)
		}
	}

	fmt.Println()
	fmt.Println()
	var err error

	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, []byte{}, 0, 2, func(k []byte, v []byte) (b bool, e error) {
		fmt.Printf("%v - %v \n", k, string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}


	fmt.Println()
	fmt.Println()


	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, []byte{}, 0, 6, func(k []byte, v []byte) (b bool, e error) {
		fmt.Printf("%v - %v \n", k, string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println()
	fmt.Println()

	err=db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, []byte{}, 0, 3, func(k []byte, v []byte) (b bool, e error) {
		fmt.Printf("%v - %v \n", k, string(v))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}


}