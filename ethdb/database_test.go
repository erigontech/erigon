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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

func newTestBoltDB() (*BoltDatabase, func()) {
	dirname, err := ioutil.TempDir(os.TempDir(), "ethdb_test_")
	if err != nil {
		panic("failed to create test file: " + err.Error())
	}
	db, err := NewBoltDatabase(path.Join(dirname, "db"))
	if err != nil {
		panic("failed to create test database: " + err.Error())
	}

	return db, func() {
		db.Close()
		os.RemoveAll(dirname)
	}
}

func newTestBadgerDB() (*BadgerDatabase, func()) {
	db, err := NewEphemeralBadger()
	if err != nil {
		panic("failed to create test database: " + err.Error())
	}

	return db, func() {
		db.Close()
	}
}

var testBucket = []byte("TestBucket")
var testValues = []string{"a", "1251", "\x00123\x00"}

func TestBoltDB_PutGet(t *testing.T) {
	db, remove := newTestBoltDB()
	defer remove()
	testPutGet(db, t)
}

func TestMemoryDB_PutGet(t *testing.T) {
	testPutGet(NewMemDatabase(), t)
}

func TestBadgerDB_PutGet(t *testing.T) {
	db, remove := newTestBadgerDB()
	defer remove()
	testPutGet(db, t)
}

func testPutGet(db MinDatabase, t *testing.T) {
	t.Parallel()

	for _, k := range testValues {
		err := db.Put(testBucket, []byte(k), nil)
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	for _, k := range testValues {
		data, err := db.Get(testBucket, []byte(k))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if len(data) != 0 {
			t.Fatalf("get returned wrong result, got %q expected nil", string(data))
		}
	}

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
			t.Fatalf("get returned wrong result, got %q expected ?", string(data))
		}
	}

	for _, v := range testValues {
		err := db.Delete(testBucket, []byte(v))
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

func TestBoltDB_ParallelPutGet(t *testing.T) {
	db, remove := newTestBoltDB()
	defer remove()
	testParallelPutGet(db)
}

func TestMemoryDB_ParallelPutGet(t *testing.T) {
	testParallelPutGet(NewMemDatabase())
}

func TestBadgerDB_ParallelPutGet(t *testing.T) {
	db, remove := newTestBadgerDB()
	defer remove()
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
			err := db.Delete(testBucket, []byte(key))
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
	testWalk(NewMemDatabase(), t)
}

func TestBoltDB_Walk(t *testing.T) {
	db, remove := newTestBoltDB()
	defer remove()
	testWalk(db, t)
}

func TestBadgerDB_Walk(t *testing.T) {
	db, remove := newTestBadgerDB()
	defer remove()
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
var fixedBits uint = 3

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
		gotKeys = append(gotKeys, key)
		return true, nil
	})
	assert.NoError(t, err)

	assert.Equal(t, keysInRange, gotKeys)
}
