// Copyright 2019 The go-ethereum Authors
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

package remotedbserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closerType struct {
}

func (c closerType) Close() error {
	return nil
}

var closer closerType

const (
	key1   = "key1"
	value1 = "value1"
	key2   = "key2"
	value2 = "value2"
	key3   = "key3"
	value3 = "value3"
)

func TestCmdVersion(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	assert.Nil(encoder.Encode(remote.CmdVersion), "Could not encode CmdVersion")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	var responseCode remote.ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdVersion")

	var v uint64
	assert.Nil(decoder.Decode(&v), "Could not decode version returned by CmdVersion")
	assert.Equal(remote.Version, v)
}

func TestCmdBeginEndError(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	// Send CmdBeginTx, followed by double CmdEndTx
	// followed by the CmdLastError
	assert.Nil(encoder.Encode(remote.CmdBeginTx), "Could not encode CmdBeginTx")

	// Call first CmdEndTx
	assert.Nil(encoder.Encode(remote.CmdEndTx), "Could not encode CmdEndTx")

	// Second CmdEndTx
	assert.Nil(encoder.Encode(remote.CmdEndTx), "Could not encode CmdEndTx")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	var responseCode remote.ResponseCode
	// Begin
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")

	// first End
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")

	// second End
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
}

func TestCmdBucket(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()
	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	var name = dbutils.Buckets[0]
	assert.Nil(encoder.Encode(remote.CmdBeginTx), "Could not encode CmdBegin")

	assert.Nil(encoder.Encode(remote.CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	var responseCode remote.ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")

	var bucketHandle uint64
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Could not decode response from CmdBucket")
}

func TestCmdGet(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = dbutils.Buckets[0]
	require.NoError(db.Put(name, []byte(key1), []byte(value1)))
	require.NoError(db.Put(name, []byte(key2), []byte(value2)))

	assert.Nil(encoder.Encode(remote.CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(remote.CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	// Issue CmdGet with existing key
	var bucketHandle uint64 = 1
	var key = []byte("key1")
	assert.Nil(encoder.Encode(remote.CmdGet), "Could not encode CmdGet")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(encoder.Encode(&key), "Could not encode key for CmdGet")
	// Issue CmdGet with non-existing key
	key = []byte("key3")
	assert.Nil(encoder.Encode(remote.CmdGet), "Could not encode CmdGet")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(encoder.Encode(&key), "Could not encode key for CmdGet")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode remote.ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdGet (for key1)
	var value []byte
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Equal("value1", string(value), "Wrong value from CmdGet")
	// Results of CmdGet (for key3)
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Nil(value, "Wrong value from CmdGet")
}

func TestCmdSeek(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = dbutils.Buckets[0]
	require.NoError(db.Put(name, []byte(key1), []byte(value1)))
	require.NoError(db.Put(name, []byte(key2), []byte(value2)))
	assert.Nil(encoder.Encode(remote.CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(remote.CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	var cursorPrefix []byte
	assert.Nil(encoder.Encode(remote.CmdCursor), "Could not encode CmdCursor")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")
	assert.Nil(encoder.Encode(cursorPrefix), "Could not encode prefix for CmdCursor")

	var cursorHandle uint64 = 2
	var seekKey = []byte("key15") // Should find key2
	assert.Nil(encoder.Encode(remote.CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode remote.ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(uint64(2), cursorHandle, "Unexpected cursorHandle")
	// Results of CmdCursorSeek
	var key, value []byte
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(key2, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(value2, string(value), "Unexpected value")
}

func TestCursorOperations(t *testing.T) {
	assert, require, parentCtx, db := assert.New(t), require.New(t), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = dbutils.Buckets[0]
	require.NoError(db.Put(name, []byte(key1), []byte(value1)))
	require.NoError(db.Put(name, []byte(key2), []byte(value2)))

	assert.Nil(encoder.Encode(remote.CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(remote.CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	var cursorPrefix []byte
	assert.Nil(encoder.Encode(remote.CmdCursor), "Could not encode CmdCursor")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")
	assert.Nil(encoder.Encode(cursorPrefix), "Could not encode cursorPrefix for CmdCursor")

	// Logic of test: .Seek(), .Next(), .First(), .Next()

	var cursorHandle uint64 = 2
	var seekKey = []byte("key1") // Should find key1
	assert.Nil(encoder.Encode(remote.CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")

	var numberOfKeys uint64 = 2 // Trying to get 2 keys, but will get 1 + nil
	// .Next()
	assert.Nil(encoder.Encode(remote.CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	// .First()
	assert.Nil(encoder.Encode(remote.CmdCursorFirst), "Could not encode CmdCursorFirst")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorFirst")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorFirst")

	// .Next()
	assert.Nil(encoder.Encode(remote.CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db.KV(), &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode remote.ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(uint64(2), cursorHandle, "Unexpected cursorHandle")

	var key, value []byte

	// Results of CmdCursorSeek
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	// first
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(key1, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(value1, string(value), "Unexpected value")

	// Results of CmdCursorNext
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdCursorNext")
	assert.Equal(remote.ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Equal(key2, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Equal(value2, string(value), "Unexpected value")

	// Results of last CmdCursorNext
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Nil(key, "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Nil(value, "Unexpected value")
}

func TestTxYield(t *testing.T) {
	assert, db := assert.New(t), ethdb.NewMemDatabase()
	defer db.Close()

	errors := make(chan error, 10)
	writeDoneNotify := make(chan struct{}, 1)
	defer close(writeDoneNotify)
	readDoneNotify := make(chan struct{}, 1)
	go func() {
		defer func() {
			readDoneNotify <- struct{}{}
			close(readDoneNotify)
			close(errors)
		}()

		// Long read-only transaction
		if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.Buckets[0])
			var keyBuf [8]byte
			var i uint64
			for {
				select { // do reads until write finish
				case <-writeDoneNotify:
					return nil
				default:
				}

				i++
				binary.BigEndian.PutUint64(keyBuf[:], i)
				_, err2 := b.Get(keyBuf[:])
				if err2 != nil {
					return err2
				}
				type Yieldable interface {
					Yield()
				}
				if casted, ok := tx.(Yieldable); ok {
					casted.Yield()
				}
			}
		}); err != nil {
			errors <- err
		}
	}()

	// Expand the database
	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.Buckets[0])
		var keyBuf, valBuf [8]byte
		for i := uint64(0); i < 10000; i++ {
			binary.BigEndian.PutUint64(keyBuf[:], i)
			binary.BigEndian.PutUint64(valBuf[:], i)
			if err2 := b.Put(keyBuf[:], valBuf[:]); err2 != nil {
				return err2
			}
		}
		return nil
	})
	assert.NoError(err, "Could not execute update")

	// write must finish before read
	assert.Equal(0, len(readDoneNotify), "Read should not finished here, if it did, it means the writes were blocked by it")
	writeDoneNotify <- struct{}{}
	<-readDoneNotify

	for err := range errors {
		assert.NoError(err)
	}
}

func BenchmarkRemoteCursorFirst(b *testing.B) {
	assert, require, parentCtx, db := assert.New(b), require.New(b), context.Background(), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code

	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = dbutils.Buckets[0]

	require.NoError(db.Put(name, []byte(key1), []byte(value1)))
	require.NoError(db.Put(name, []byte(key2), []byte(value2)))
	require.NoError(db.Put(name, []byte(key3), []byte(value3)))

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// By now we constructed all input requests, now we call the
	// Server to process them all
	go func() {
		require.NoError(Server(ctx, db.KV(), &inBuf, &outBuf, closer))
	}()

	var responseCode remote.ResponseCode
	var key, value []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Begin
		assert.Nil(encoder.Encode(remote.CmdBeginTx))
		assert.Nil(decoder.Decode(&responseCode))
		if responseCode != remote.ResponseOk {
			panic("not Ok")
		}

		// Bucket
		assert.Nil(encoder.Encode(remote.CmdBucket))
		assert.Nil(encoder.Encode(name))

		var bucketHandle uint64 = 0
		assert.Nil(decoder.Decode(&responseCode))
		if responseCode != remote.ResponseOk {
			panic("not Ok")
		}
		assert.Nil(decoder.Decode(&bucketHandle))

		// Cursor
		assert.Nil(encoder.Encode(remote.CmdCursor))
		assert.Nil(encoder.Encode(bucketHandle))
		var cursorHandle uint64 = 0
		assert.Nil(decoder.Decode(&cursorHandle))

		// .First()
		assert.Nil(encoder.Encode(remote.CmdCursorFirst))
		assert.Nil(encoder.Encode(cursorHandle))
		var numberOfFirstKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
		assert.Nil(encoder.Encode(numberOfFirstKeys))

		// .First()
		assert.Nil(decoder.Decode(&responseCode))
		assert.Nil(decoder.Decode(&key))
		assert.Nil(decoder.Decode(&value))
		// Results of CmdCursorNext
		assert.Nil(decoder.Decode(&key))
		assert.Nil(decoder.Decode(&value))
		// Results of last CmdCursorNext
		assert.Nil(decoder.Decode(&key))
		assert.Nil(decoder.Decode(&value))

		// .End()
		assert.Nil(encoder.Encode(remote.CmdEndTx))
		assert.Nil(decoder.Decode(&responseCode))
		assert.Equal(responseCode, remote.ResponseOk)
	}
}

func BenchmarkKVCursorFirst(b *testing.B) {
	_, require, db := assert.New(b), require.New(b), ethdb.NewMemDatabase()
	defer db.Close()

	// ---------- Start of boilerplate code
	var name = dbutils.Buckets[0]
	require.NoError(db.Put(name, []byte(key1), []byte(value1)))
	require.NoError(db.Put(name, []byte(key2), []byte(value2)))
	require.NoError(db.Put(name, []byte(key3), []byte(value3)))

	var k, v []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.KV().Begin(context.Background(), false)
		if err != nil {
			panic(err)
		}

		bucket := tx.Bucket(name)
		cursor := bucket.Cursor()
		i := 0
		for k, v, err = cursor.First(); k != nil; k, v, err = cursor.Next() {
			require.NoError(err)
			i++
			if i == 3 {
				break
			}
			_ = k
			_ = v
		}

		tx.Rollback()
	}

}
