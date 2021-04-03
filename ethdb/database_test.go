// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// +build !js

package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLmdb() *ObjectDatabase {
	return NewObjectDatabase(NewLMDB().InMem().MustOpen())
}

var testBucket = dbutils.HashedAccountsBucket
var testValues = []string{"a", "1251", "\x00123\x00"}

func TestMemoryDB_PutGet(t *testing.T) {
	db := NewMemDatabase()
	defer db.Close()
	testPutGet(db, t)
	testNoPanicAfterDbClosed(db, t)
}

func TestLMDB_PutGet(t *testing.T) {
	db := newTestLmdb()
	defer db.Close()
	testPutGet(db, t)
	testNoPanicAfterDbClosed(db, t)
}

func testPutGet(db MinDatabase, t *testing.T) {
	//for _, k := range testValues {
	//	err := db.Put(testBucket, []byte(k), []byte{})
	//	if err != nil {
	//		t.Fatalf("put failed: %v", err)
	//	}
	//}
	//
	//for _, k := range testValues {
	//	data, err := db.Get(testBucket, []byte(k))
	//	if err != nil {
	//		t.Fatalf("get failed: %v", err)
	//	}
	//	if len(data) != 0 {
	//		t.Fatalf("get returned wrong result, got %q expected nil", string(data))
	//	}
	//}

	_, err := db.Get(testBucket, []byte("non-exist-key"))
	if err == nil {
		t.Fatalf("expect to return a not found error")
	}

	for _, v := range testValues {
		err := db.Put(testBucket, []byte(v), []byte(v))
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	for _, v := range testValues {
		data, err := db.Get(testBucket, []byte(v))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte(v)) {
			t.Fatalf("get returned wrong result, got %q expected %q", string(data), v)
		}
	}

	for _, v := range testValues {
		err := db.Put(testBucket, []byte(v), []byte("?"))
		if err != nil {
			t.Fatalf("put override failed: %v", err)
		}
	}

	for _, v := range testValues {
		data, err := db.Get(testBucket, []byte(v))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte("?")) {
			t.Fatalf("get returned wrong result, got %q expected ?", string(data))
		}
	}

	for _, v := range testValues {
		orig, err := db.Get(testBucket, []byte(v))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		orig[0] = byte(0xff)
		data, err := db.Get(testBucket, []byte(v))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(data, []byte("?")) {
			fmt.Printf("Error: %s %s\n", v, data)
			t.Fatalf("get returned wrong result, got %s expected ?", string(data))
		}
	}

	for _, v := range testValues {
		err := db.Delete(testBucket, []byte(v), nil)
		if err != nil {
			t.Fatalf("delete %q failed: %v", v, err)
		}
	}

	for _, v := range testValues {
		_, err := db.Get(testBucket, []byte(v))
		if err == nil {
			t.Fatalf("got deleted value %q", v)
		}
	}
}

func testNoPanicAfterDbClosed(db Database, t *testing.T) {
	tx, err := db.(HasRwKV).RwKV().Begin(context.Background())
	require.NoError(t, err)
	writeTx, err := db.(HasRwKV).RwKV().BeginRw(context.Background())
	require.NoError(t, err)

	closeCh := make(chan struct{}, 1)
	go func() {
		require.NotPanics(t, func() {
			<-closeCh
			db.Close()
		})
	}()
	time.Sleep(time.Millisecond) // wait to check that db.Close doesn't panic, but wait when read tx finished
	err = writeTx.Put(dbutils.Buckets[0], []byte{1}, []byte{1})
	require.NoError(t, err)
	err = writeTx.Commit()
	require.NoError(t, err)
	_, err = tx.GetOne(dbutils.Buckets[0], []byte{1})
	require.NoError(t, err)
	tx.Rollback()

	db.Close() // close db from 2nd goroutine
	close(closeCh)

	// after db closed, methods must not panic but return some error
	require.NotPanics(t, func() {
		_, err := db.Get(testBucket, []byte{11})
		require.Error(t, err)
		err = db.Put(testBucket, []byte{11}, []byte{11})
		require.Error(t, err)
	})
}

func TestMemoryDB_ParallelPutGet(t *testing.T) {
	db := NewMemDatabase()
	defer db.Close()
	testParallelPutGet(db)
}

func TestLMDB_ParallelPutGet(t *testing.T) {
	db := newTestLmdb()
	defer db.Close()
	testParallelPutGet(db)
}

func testParallelPutGet(db MinDatabase) {
	const n = 8
	var pending sync.WaitGroup

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			err := db.Put(testBucket, []byte(key), []byte("v"+key))
			if err != nil {
				panic("put failed: " + err.Error())
			}
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			data, err := db.Get(testBucket, []byte(key))
			if err != nil {
				panic("get failed: " + err.Error())
			}
			if !bytes.Equal(data, []byte("v"+key)) {
				panic(fmt.Sprintf("get failed, got %q expected %q", data, []byte("v"+key)))
			}
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			err := db.Delete(testBucket, []byte(key), nil)
			if err != nil {
				panic("delete failed: " + err.Error())
			}
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			_, err := db.Get(testBucket, []byte(key))
			if err == nil {
				panic("get succeeded")
			}
		}(strconv.Itoa(i))
	}
	pending.Wait()
}

func TestMemoryDB_Walk(t *testing.T) {
	db := NewMemDatabase()
	defer db.Close()
	testWalk(db, t)
}

func TestLMDB_Walk(t *testing.T) {
	db := newTestLmdb()
	defer db.Close()
	testWalk(db, t)
}

var hexEntries = map[string]string{
	"6b": "89c6",
	"91": "c476",
	"a8": "0a514e",
	"bb": "7a",
	"bd": "fe76",
	"c0": "12",
}

var startKey = common.FromHex("a0")
var fixedBits = 3

var keysInRange = [][]byte{common.FromHex("a8"), common.FromHex("bb"), common.FromHex("bd")}

func testWalk(db Database, t *testing.T) {
	for k, v := range hexEntries {
		err := db.Put(testBucket, common.FromHex(k), common.FromHex(v))
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	var gotKeys [][]byte

	err := db.Walk(testBucket, startKey, fixedBits, func(key, val []byte) (bool, error) {
		gotKeys = append(gotKeys, common.CopyBytes(key))
		return true, nil
	})
	assert.NoError(t, err)

	assert.Equal(t, keysInRange, gotKeys)
}
