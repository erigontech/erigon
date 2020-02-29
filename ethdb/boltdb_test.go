package ethdb

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"reflect"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
)

func TestBoltDB_WalkAsOf1(t *testing.T) {
	if debug.IsThinHistory() {
		t.Skip()
	}
	db := NewMemDatabase()

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//create state and history
	for i := uint8(1); i < 5; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, uint64(1), common.Hash{i})
		val := []byte("state   " + strconv.Itoa(int(i)))
		err := db.Put(dbutils.StorageBucket, key, val)
		if err != nil {
			t.Fatal(err)
		}
		err = block6Expected.Add(key, val)
		if err != nil {
			t.Fatal(err)
		}

		if i <= 2 {
			err = block4Expected.Add(key, val)
			if err != nil {
				t.Fatal(err)
			}

		}
	}
	for i := uint8(1); i <= 7; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1, common.Hash{i})
		val := []byte("block 3 " + strconv.Itoa(int(i)))
		err := db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			3,
			false,
		)
		if err != nil {
			t.Fatal(err)
		}

		err = block2Expected.Add(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := uint8(3); i <= 7; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1, common.Hash{i})
		val := []byte("block 5 " + strconv.Itoa(int(i)))
		err := db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			5,
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		err = block4Expected.Add(key, val)
		if err != nil {
			t.Fatal(err)
		}

	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	var err error
	var startKey [72]byte
	err = db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}

		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}

		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}

		//fmt.Printf("%v - %v \n", common.BytesToHash(k).String(), string(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(block2, block2Expected) {
		spew.Dump("expected", block2Expected)
		spew.Dump("current", block2)
		t.Fatal("block 2 result is incorrect")
	}
	if !reflect.DeepEqual(block4, block4Expected) {
		spew.Dump(block6)
		t.Fatal("block 4 result is incorrect")
	}
	if !reflect.DeepEqual(block6, block6Expected) {
		spew.Dump(block6)
		t.Fatal("block 6 result is incorrect")
	}

}

func TestBoltDB_MultiWalkAsOf(t *testing.T) {
	if debug.IsThinHistory() {
		t.Skip()
	}

	db := NewMemDatabase()

	block2Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{1}, 1, common.Hash{1}),
				Value: []byte("block 3 " + strconv.Itoa(1)),
			},
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{3}, 1, common.Hash{3}),
				Value: []byte("block 3 " + strconv.Itoa(3)),
			},
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{7}, 1, common.Hash{7}),
				Value: []byte("block 3 " + strconv.Itoa(7)),
			},
		},
	}

	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{1}, 1, common.Hash{1}),
				Value: []byte("state   " + strconv.Itoa(1)),
			},
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{3}, 1, common.Hash{3}),
				Value: []byte("block 5 " + strconv.Itoa(3)),
			},
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{7}, 1, common.Hash{7}),
				Value: []byte("block 5 " + strconv.Itoa(7)),
			},
		},
	}

	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{1}, 1, common.Hash{1}),
				Value: []byte("state   " + strconv.Itoa(1)),
			},
			{
				Key:   dbutils.GenerateCompositeStorageKey(common.Hash{3}, 1, common.Hash{3}),
				Value: []byte("state   " + strconv.Itoa(3)),
			},
		},
	}

	//create state and history
	for i := uint8(1); i < 5; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, uint64(1), common.Hash{i})
		val := []byte("state   " + strconv.Itoa(int(i)))
		err := db.Put(dbutils.StorageBucket, key, val)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := uint8(1); i <= 7; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1, common.Hash{i})
		val := []byte("block 3 " + strconv.Itoa(int(i)))
		err := db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			3,
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := uint8(3); i <= 7; i++ {
		key := dbutils.GenerateCompositeStorageKey(common.Hash{i}, 1, common.Hash{i})
		val := []byte("block 5 " + strconv.Itoa(int(i)))
		err := db.PutS(
			dbutils.StorageHistoryBucket,
			key,
			val,
			5,
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	//walk and collect walkAsOf result
	var err error
	startKeys := [][]byte{
		dbutils.GenerateCompositeStorageKey(common.Hash{1}, 1, common.Hash{1}),
		dbutils.GenerateCompositeStorageKey(common.Hash{3}, 1, common.Hash{3}),
		dbutils.GenerateCompositeStorageKey(common.Hash{7}, 1, common.Hash{7}),
	}
	fixedBits := []uint{
		60,
		60,
		60,
	}

	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	err = db.MultiWalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKeys, fixedBits, 2, func(idx int, k []byte, v []byte) error {
		fmt.Printf("%v - %s - %s\n", idx, string(k), string(v))
		err = block2.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.MultiWalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKeys, fixedBits, 4, func(idx int, k []byte, v []byte) error {
		fmt.Printf("%v - %s - %s\n", idx, string(k), string(v))
		err = block4.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.MultiWalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startKeys, fixedBits, 6, func(idx int, k []byte, v []byte) error {
		fmt.Printf("%v - %s - %s\n", idx, string(k), string(v))
		err = block6.Add(k, v)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(block2Expected, block2) {
		spew.Dump(block2)
		t.Fatal("block2")
	}
	if !reflect.DeepEqual(block4Expected, block4) {
		spew.Dump(block4)
		t.Fatal("block4")
	}
	if !reflect.DeepEqual(block6Expected, block6) {
		spew.Dump(block6)
		t.Fatal("block6")
	}
}
