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

//go:build !js

package membatch

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

var testBucket = kv.HashedAccounts
var testValues = []string{"a", "1251", "\x00123\x00"}

func TestPutGet(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

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

	_, err := tx.GetOne(testBucket, []byte("non-exist-key"))
	require.NoError(t, err)

	for _, v := range testValues {
		err := tx.Put(testBucket, []byte(v), []byte(v))
		require.NoError(t, err)
	}

	for _, v := range testValues {
		data, err := tx.GetOne(testBucket, []byte(v))
		require.NoError(t, err)
		if !bytes.Equal(data, []byte(v)) {
			t.Fatalf("get returned wrong result, got %q expected %q", string(data), v)
		}
	}

	for _, v := range testValues {
		err := tx.Put(testBucket, []byte(v), []byte("?"))
		require.NoError(t, err)
	}

	for _, v := range testValues {
		data, err := tx.GetOne(testBucket, []byte(v))
		require.NoError(t, err)
		if !bytes.Equal(data, []byte("?")) {
			t.Fatalf("get returned wrong result, got %q expected ?", string(data))
		}
	}

	for _, v := range testValues {
		err := tx.Delete(testBucket, []byte(v))
		require.NoError(t, err)
	}

	for _, v := range testValues {
		_, err := tx.GetOne(testBucket, []byte(v))
		require.NoError(t, err)
	}
}

func TestNoPanicAfterDbClosed(t *testing.T) {
	db := memdb.NewTestDB(t)
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	writeTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer writeTx.Rollback()

	closeCh := make(chan struct{}, 1)
	go func() {
		require.NotPanics(t, func() {
			<-closeCh
			db.Close()
		})
	}()
	time.Sleep(time.Millisecond) // wait to check that db.Close doesn't panic, but wait when read tx finished
	err = writeTx.Put(kv.ChaindataTables[0], []byte{1}, []byte{1})
	require.NoError(t, err)
	err = writeTx.Commit()
	require.NoError(t, err)
	_, err = tx.GetOne(kv.ChaindataTables[0], []byte{1})
	require.NoError(t, err)
	tx.Rollback()

	db.Close() // close db from 2nd goroutine
	close(closeCh)

	// after db closed, methods must not panic but return some error
	//require.NotPanics(t, func() {
	//	_, err := tx.GetOne(testBucket, []byte{11})
	//	require.Error(t, err)
	//	err = writeTx.Put(testBucket, []byte{11}, []byte{11})
	//	require.Error(t, err)
	//})
}

func TestParallelPutGet(t *testing.T) {
	db := memdb.NewTestDB(t)

	const n = 8
	var pending sync.WaitGroup

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			_ = db.Update(context.Background(), func(tx kv.RwTx) error {
				err := tx.Put(testBucket, []byte(key), []byte("v"+key))
				if err != nil {
					panic("put failed: " + err.Error())
				}
				return nil
			})
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			_ = db.View(context.Background(), func(tx kv.Tx) error {
				data, err := tx.GetOne(testBucket, []byte(key))
				if err != nil {
					panic("get failed: " + err.Error())
				}
				if !bytes.Equal(data, []byte("v"+key)) {
					panic(fmt.Sprintf("get failed, got %q expected %q", data, []byte("v"+key)))
				}
				return nil
			})
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			_ = db.Update(context.Background(), func(tx kv.RwTx) error {
				err := tx.Delete(testBucket, []byte(key))
				if err != nil {
					panic("delete failed: " + err.Error())
				}
				return nil
			})
		}(strconv.Itoa(i))
	}
	pending.Wait()

	pending.Add(n)
	for i := 0; i < n; i++ {
		go func(key string) {
			defer pending.Done()
			_ = db.Update(context.Background(), func(tx kv.RwTx) error {
				v, err := tx.GetOne(testBucket, []byte(key))
				if err != nil {
					panic(err)
				}
				if v != nil {
					panic("get returned something")
				}
				return nil
			})
		}(strconv.Itoa(i))
	}
	pending.Wait()
}
