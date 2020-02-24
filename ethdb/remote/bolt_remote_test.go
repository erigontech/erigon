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

package remote

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
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

func memBb(t require.TestingT) *bolt.DB {
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	require.NoError(t, err)
	return db
}

func TestCmdVersion(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	assert.Nil(encoder.Encode(CmdVersion), "Could not encode CmdVersion")

	err := Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	var responseCode ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdVersion")

	var v uint64
	assert.Nil(decoder.Decode(&v), "Could not decode version returned by CmdVersion")
	assert.Equal(Version, v)
}

func TestCmdBeginEndError(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	// Call first CmdEndTx
	assert.Nil(encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")

	// Second CmdEndTx
	assert.Nil(encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	var responseCode ResponseCode
	// Begin
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")

	// first End
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")

	// second End
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
}

func TestCmdBucket(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	// Create a bucket
	var name = []byte("testbucket")
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err1 := tx.CreateBucket(name, false)
		return err1
	}); err != nil {
		t.Errorf("Could not create and populate a bucket: %v", err)
	}
	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBegin")

	assert.Nil(encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	var responseCode ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")

	var bucketHandle uint64
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Could not decode response from CmdBucket")
}

func TestCmdGet(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	var name = []byte("testbucket")
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(name, false)
		if err1 != nil {
			return err1
		}
		if err1 = b.Put([]byte(key1), []byte(value1)); err1 != nil {
			return err1
		}
		if err1 = b.Put([]byte(key2), []byte(value2)); err1 != nil {
			return err1
		}
		return nil
	}); err != nil {
		t.Errorf("Could not create and populate a bucket: %v", err)
	}
	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	// Issue CmdGet with existing key
	var bucketHandle uint64 = 1
	var key = []byte("key1")
	assert.Nil(encoder.Encode(CmdGet), "Could not encode CmdGet")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(encoder.Encode(&key), "Could not encode key for CmdGet")
	// Issue CmdGet with non-existing key
	key = []byte("key3")
	assert.Nil(encoder.Encode(CmdGet), "Could not encode CmdGet")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(encoder.Encode(&key), "Could not encode key for CmdGet")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdGet (for key1)
	var value []byte
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Equal("value1", string(value), "Wrong value from CmdGet")
	// Results of CmdGet (for key3)
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Nil(value, "Wrong value from CmdGet")
}

func TestCmdSeek(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	var name = []byte("testbucket")
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(name, false)
		if err1 != nil {
			return err1
		}
		if err1 = b.Put([]byte(key1), []byte(value1)); err1 != nil {
			return err1
		}
		if err1 = b.Put([]byte(key2), []byte(value2)); err1 != nil {
			return err1
		}
		return nil
	}); err != nil {
		t.Errorf("Could not create and populate a bucket: %v", err)
	}
	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	assert.Nil(encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	var cursorHandle uint64 = 2
	var seekKey = []byte("key15") // Should find key2
	assert.Nil(encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")
	// By now we constructed all input requests, now we call the
	// Server to process them all
	err := Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(uint64(2), cursorHandle, "Unexpected cursorHandle")
	// Results of CmdCursorSeek
	var key, value []byte
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(key2, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(value2, string(value), "Unexpected value")
}

func TestCursorOperations(t *testing.T) {
	assert, require, ctx, db := assert.New(t), require.New(t), context.Background(), memBb(t)

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
	var name = []byte("testbucket")
	err := db.Update(func(tx *bolt.Tx) error {
		b, err1 := tx.CreateBucket(name, false)
		require.NoError(err1)
		err1 = b.Put([]byte(key1), []byte(value1))
		require.NoError(err1)
		err1 = b.Put([]byte(key2), []byte(value2))
		require.NoError(err1)
		return nil
	})
	require.NoError(err)

	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	assert.Nil(encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	assert.Nil(encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	// Logic of test: .Seek(), .Next(), .First(), .Next(), .FirstKey(), .NextKey()

	var cursorHandle uint64 = 2
	var seekKey = []byte("key1") // Should find key1
	assert.Nil(encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")

	var numberOfKeys uint64 = 2 // Trying to get 2 keys, but will get 1 + nil
	// .Next()
	assert.Nil(encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	// .First()
	assert.Nil(encoder.Encode(CmdCursorFirst), "Could not encode CmdCursorFirst")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorFirst")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorFirst")

	// .Next()
	assert.Nil(encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	// .FirstKey()
	assert.Nil(encoder.Encode(CmdCursorFirstKey), "Could not encode CmdCursorFirstKey")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorFirstKey")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorFirstKey")

	// .NextKey()
	assert.Nil(encoder.Encode(CmdCursorNextKey), "Could not encode CmdCursorNextKey")
	assert.Nil(encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNextKey")
	assert.Nil(encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNextKey")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	err = Server(ctx, db, &inBuf, &outBuf, closer)
	require.NoError(err, "Error while calling Server")

	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(uint64(2), cursorHandle, "Unexpected cursorHandle")

	var key, value []byte

	// Results of CmdCursorSeek
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	// first
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(key1, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(value1, string(value), "Unexpected value")

	// Results of CmdCursorNext
	assert.Nil(decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdCursorNext")
	assert.Equal(ResponseOk, responseCode, "unexpected response code")
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Equal(key2, string(key), "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Equal(value2, string(value), "Unexpected value")

	// Results of last CmdCursorNext
	assert.Nil(decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Nil(key, "Unexpected key")
	assert.Nil(decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Nil(value, "Unexpected value")

	assert.Nil(encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(encoder.Encode(&name), "Could not encode name for CmdBucket")
}

func TestTxYield(t *testing.T) {
	assert, db := assert.New(t), memBb(t)

	// Create bucket
	err := db.Update(func(tx *bolt.Tx) error {
		_, err1 := tx.CreateBucket([]byte("bucket"), false)
		return err1
	})
	assert.Nil(err, "Could not create bucket")

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
		if err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
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
				b.Get(keyBuf[:])
				tx.Yield()
			}
		}); err != nil {
			errors <- err
		}
	}()

	// Expand the database
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bucket"))
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
	assert.Nil(err, "Could not execute update")

	// write must finish before read
	assert.Equal(0, len(readDoneNotify), "Read should not finished here, if it did, it means the writes were blocked by it")
	writeDoneNotify <- struct{}{}
	<-readDoneNotify

	for err := range errors {
		assert.Nil(err)
	}
}

func BenchmarkRemoteCursorFirst(b *testing.B) {
	assert, require, ctx, db := assert.New(b), require.New(b), context.Background(), memBb(b)

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
	var name = []byte("testbucket")

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err1 := tx.CreateBucket(name, false)
		if err1 != nil {
			return err1
		}

		if err1 = bucket.Put([]byte(key1), []byte(value1)); err1 != nil {
			return err1
		}
		if err1 = bucket.Put([]byte(key2), []byte(value2)); err1 != nil {
			return err1
		}
		if err1 = bucket.Put([]byte(key3), []byte(value3)); err1 != nil {
			return err1
		}
		return nil
	})
	require.NoError(err, "Could not create and populate a bucket")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	go func() {
		require.NoError(Server(ctx, db, &inBuf, &outBuf, closer))
	}()

	var responseCode ResponseCode
	var key, value []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Begin
		assert.Nil(encoder.Encode(CmdBeginTx))
		assert.Nil(decoder.Decode(&responseCode))
		if responseCode != ResponseOk {
			panic("not Ok")
		}

		// Bucket
		assert.Nil(encoder.Encode(CmdBucket))
		assert.Nil(encoder.Encode(name))

		var bucketHandle uint64 = 0
		assert.Nil(decoder.Decode(&responseCode))
		if responseCode != ResponseOk {
			panic("not Ok")
		}
		assert.Nil(decoder.Decode(&bucketHandle))

		// Cursor
		assert.Nil(encoder.Encode(CmdCursor))
		assert.Nil(encoder.Encode(bucketHandle))
		var cursorHandle uint64 = 0
		assert.Nil(decoder.Decode(&cursorHandle))

		// .First()
		assert.Nil(encoder.Encode(CmdCursorFirst))
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
		assert.Nil(encoder.Encode(CmdEndTx))
		assert.Nil(decoder.Decode(&responseCode))
		assert.Equal(responseCode, ResponseOk)
	}
}

func BenchmarkBoltCursorFirst(b *testing.B) {
	assert, require, db := assert.New(b), require.New(b), memBb(b)

	// ---------- Start of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err1 := tx.CreateBucket(name, false)
		require.NoError(err1)

		require.NoError(bucket.Put([]byte(key1), []byte(value1)))
		require.NoError(bucket.Put([]byte(key2), []byte(value2)))
		require.NoError(bucket.Put([]byte(key3), []byte(value3)))
		return nil
	})
	require.NoError(err, "Could not create and populate a bucket")

	var k, v []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin(false)
		if err != nil {
			panic(err)
		}

		bucket := tx.Bucket(name)
		cursor := bucket.Cursor()
		i := 0
		for k, v = cursor.First(); k != nil; k, v = cursor.Next() {
			i++
			if i == 3 {
				break
			}
			_ = k
			_ = v
		}

		assert.Nil(b, tx.Rollback())
	}

}

func TestReconnect(t *testing.T) {
	assert := assert.New(t)
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)

	dialCallCounter := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pingCh := make(chan time.Time, ClientMaxConnections)
	db := &DB{
		dialFunc: func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error) {
			dialCallCounter++
			if dialCallCounter%2 == 0 {
				return &inBuf, &outBuf, nil, net.UnknownNetworkError("Oops")
			}
			return &inBuf, &outBuf, nil, nil
		},
		connectionPool: make(chan *conn, ClientMaxConnections),
		doDial:         make(chan struct{}, ClientMaxConnections),
		doPing:         pingCh,
		dialTimeout:    time.Second,
		pingTimeout:    time.Minute,
		retryDialAfter: 0 * time.Nanosecond,
		pingEvery:      time.Second,
	}

	// no open connections by default
	assert.Equal(0, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open 1 connection and wait for it
	db.doDial <- struct{}{}
	db.autoReconnect(ctx)
	<-db.connectionPool
	assert.Equal(1, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open 2nd connection - dialFunc will return err on 2nd call, but db must reconnect automatically
	db.doDial <- struct{}{}
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	<-db.connectionPool
	assert.Equal(3, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open conn and call ping on it
	db.doDial <- struct{}{}
	assert.Nil(encoder.Encode(ResponseOk))
	assert.Nil(encoder.Encode(Version))
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	assert.Equal(5, dialCallCounter)
	assert.Equal(1, len(db.connectionPool))
	pingCh <- time.Now()
	db.autoReconnect(ctx)
	var cmd Command
	assert.Nil(decoder.Decode(&cmd))
	assert.Equal(CmdVersion, cmd)

	// TODO: cover case when ping receive io.EOF
}
