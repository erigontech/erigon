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
	"github.com/stretchr/testify/assert"
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
	ctx := context.Background()

	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	assert.Nil(t, encoder.Encode(CmdVersion), "Could not encode CmdVersion")

	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}

	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdVersion")

	var v uint64
	assert.Nil(t, decoder.Decode(&v), "Could not decode version returned by CmdVersion")
	assert.Equal(t, Version, v)
}

func TestCmdBeginEndError(t *testing.T) {
	ctx := context.Background()
	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Send CmdBeginTx, followed by double CmdEndTx
	// followed by the CmdLastError
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	// Call first CmdEndTx
	assert.Nil(t, encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")

	// Second CmdEndTx
	assert.Nil(t, encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}

	var responseCode ResponseCode
	// Begin
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")

	// first End
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")

	// second End
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
}

func TestCmdBucket(t *testing.T) {
	ctx := context.Background()

	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Create a bucket
	var name = []byte("testbucket")
	if err = db.Update(func(tx *bolt.Tx) error {
		_, err1 := tx.CreateBucket(name, false)
		return err1
	}); err != nil {
		t.Errorf("Could not create and populate a bucket: %v", err)
	}
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBegin")

	assert.Nil(t, encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(t, encoder.Encode(&name), "Could not encode name for CmdBucket")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}

	// And then we interpret the results
	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")

	var bucketHandle uint64
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(1), bucketHandle, "Could not decode response from CmdBucket")
}

func TestCmdGet(t *testing.T) {
	ctx := context.Background()

	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")
	if err = db.Update(func(tx *bolt.Tx) error {
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
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(t, encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(t, encoder.Encode(&name), "Could not encode name for CmdBucket")

	// Issue CmdGet with existing key
	var bucketHandle uint64 = 1
	var key = []byte("key1")
	assert.Nil(t, encoder.Encode(CmdGet), "Could not encode CmdGet")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(t, encoder.Encode(&key), "Could not encode key for CmdGet")
	// Issue CmdGet with non-existing key
	key = []byte("key3")
	assert.Nil(t, encoder.Encode(CmdGet), "Could not encode CmdGet")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(t, encoder.Encode(&key), "Could not encode key for CmdGet")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdGet (for key1)
	var value []byte
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Equal(t, "value1", string(value), "Wrong value from CmdGet")
	// Results of CmdGet (for key3)
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&value), "Could not decode value from CmdGet")
	assert.Nil(t, value, "Wrong value from CmdGet")
}

func TestCmdSeek(t *testing.T) {
	ctx := context.Background()

	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")
	if err = db.Update(func(tx *bolt.Tx) error {
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
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(t, encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(t, encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	assert.Nil(t, encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	var cursorHandle uint64 = 2
	var seekKey = []byte("key15") // Should find key2
	assert.Nil(t, encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(t, encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(t, uint64(2), cursorHandle, "Unexpected cursorHandle")
	// Results of CmdCursorSeek
	var key, value []byte
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, key2, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, value2, string(value), "Unexpected value")
}

func TestCursorOperations(t *testing.T) {
	// ---------- Start of boilerplate code
	ctx := context.Background()
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")
	if err = db.Update(func(tx *bolt.Tx) error {
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
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(t, encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(t, encoder.Encode(&name), "Could not encode name for CmdBucket")

	var bucketHandle uint64 = 1
	assert.Nil(t, encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	// Logic of test: .Seek(), .Next(), .First(), .Next(), .FirstKey(), .NextKey()

	var cursorHandle uint64 = 2
	var seekKey = []byte("key1") // Should find key1
	assert.Nil(t, encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	assert.Nil(t, encoder.Encode(&seekKey), "Could not encode seekKey for CmdCursorSeek")

	var numberOfKeys uint64 = 2 // Trying to get 2 keys, but will get 1 + nil
	// .Next()
	assert.Nil(t, encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	// .First()
	assert.Nil(t, encoder.Encode(CmdCursorFirst), "Could not encode CmdCursorFirst")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorFirst")
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorFirst")

	// .Next()
	assert.Nil(t, encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")

	// .FirstKey()
	assert.Nil(t, encoder.Encode(CmdCursorFirstKey), "Could not encode CmdCursorFirstKey")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorFirstKey")
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorFirstKey")

	// .NextKey()
	assert.Nil(t, encoder.Encode(CmdCursorNextKey), "Could not encode CmdCursorNextKey")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNextKey")
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNextKey")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(1), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(t, uint64(2), cursorHandle, "Unexpected cursorHandle")

	var key, value []byte

	// Results of CmdCursorSeek
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	// first
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, key1, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, value1, string(value), "Unexpected value")

	// Results of CmdCursorNext
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdCursorNext")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Equal(t, key2, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Equal(t, value2, string(value), "Unexpected value")

	// Results of last CmdCursorNext
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Nil(t, key, "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Nil(t, value, "Unexpected value")

	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	assert.Nil(t, encoder.Encode(CmdBucket), "Could not encode CmdBucket")
	assert.Nil(t, encoder.Encode(&name), "Could not encode name for CmdBucket")
}

func TestTxYield(t *testing.T) {
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		t.Errorf("Could not create database: %v", err)
	}
	// Create bucket
	if err = db.Update(func(tx *bolt.Tx) error {
		_, err1 := tx.CreateBucket([]byte("bucket"), false)
		return err1
	}); err != nil {
		t.Errorf("Could not create bucket: %v", err)
	}
	var readFinished bool
	errors := make(chan error, 10)
	go func() {
		defer close(errors)
		// Long read-only transaction
		if err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			var keyBuf [8]byte
			for i := 0; i < 100000; i++ {
				binary.BigEndian.PutUint64(keyBuf[:], uint64(i))
				b.Get(keyBuf[:])
				tx.Yield()
			}
			return nil
		}); err != nil {
			errors <- err
		}
		readFinished = true
	}()

	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	// Expand the database
	if err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bucket"))
		var keyBuf, valBuf [8]byte
		for i := 0; i < 10000; i++ {
			binary.BigEndian.PutUint64(keyBuf[:], uint64(i))
			binary.BigEndian.PutUint64(valBuf[:], uint64(i))
			if err2 := b.Put(keyBuf[:], valBuf[:]); err2 != nil {
				return err2
			}
		}
		return nil
	}); err != nil {
		t.Errorf("Could not execute update: %v", err)
	}
	if readFinished {
		t.Errorf("Read should not finished here, if it did, it means the writes were blocked by it")
	}
}

func BenchmarkRemoteCursorFirst(b *testing.B) {
	// ---------- Start of boilerplate code
	ctx := context.Background()
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		b.Errorf("Could not create database: %v", err)
	}
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")

	if err = db.Update(func(tx *bolt.Tx) error {
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
	}); err != nil {
		b.Errorf("Could not create and populate a bucket: %v", err)
	}

	// By now we constructed all input requests, now we call the
	// Server to process them all
	go func() {
		assert.Nil(b, Server(ctx, db, &inBuf, &outBuf, closer))
	}()

	var responseCode ResponseCode
	var key, value []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Begin
		assert.Nil(b, encoder.Encode(CmdBeginTx))
		assert.Nil(b, decoder.Decode(&responseCode))
		if responseCode != ResponseOk {
			panic("not Ok")
		}

		// Bucket
		assert.Nil(b, encoder.Encode(CmdBucket))
		assert.Nil(b, encoder.Encode(name))

		var bucketHandle uint64 = 0
		assert.Nil(b, decoder.Decode(&responseCode))
		if responseCode != ResponseOk {
			panic("not Ok")
		}
		assert.Nil(b, decoder.Decode(&bucketHandle))

		// Cursor
		assert.Nil(b, encoder.Encode(CmdCursor))
		assert.Nil(b, encoder.Encode(bucketHandle))
		var cursorHandle uint64 = 0
		assert.Nil(b, decoder.Decode(&cursorHandle))

		// .First()
		assert.Nil(b, encoder.Encode(CmdCursorFirst))
		assert.Nil(b, encoder.Encode(cursorHandle))
		var numberOfFirstKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
		assert.Nil(b, encoder.Encode(numberOfFirstKeys))

		// .First()
		assert.Nil(b, decoder.Decode(&responseCode))
		assert.Nil(b, decoder.Decode(&key))
		assert.Nil(b, decoder.Decode(&value))
		// Results of CmdCursorNext
		assert.Nil(b, decoder.Decode(&key))
		assert.Nil(b, decoder.Decode(&value))
		// Results of last CmdCursorNext
		assert.Nil(b, decoder.Decode(&key))
		assert.Nil(b, decoder.Decode(&value))

		// .End()
		assert.Nil(b, encoder.Encode(CmdEndTx))
		assert.Nil(b, decoder.Decode(&responseCode))
		assert.Equal(b, responseCode, ResponseOk)
	}
}

func BenchmarkBoltCursorFirst(b *testing.B) {
	// ---------- Start of boilerplate code
	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		b.Errorf("Could not create database: %v", err)
	}
	// ---------- End of boilerplate code
	// Create a bucket and populate some values
	var name = []byte("testbucket")

	if err = db.Update(func(tx *bolt.Tx) error {
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
	}); err != nil {
		b.Errorf("Could not create and populate a bucket: %v", err)
	}

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
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := newEncoder(&inBuf)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := newDecoder(&outBuf)
	defer returnDecoderToPool(decoder)

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
	}

	// no open connections by default
	assert.Equal(t, 0, dialCallCounter)
	assert.Equal(t, 0, len(db.connectionPool))

	// open 1 connection and wait for it
	db.doDial <- struct{}{}
	db.autoReconnect(ctx)
	<-db.connectionPool
	assert.Equal(t, 1, dialCallCounter)
	assert.Equal(t, 0, len(db.connectionPool))

	// open 2nd connection - dialFunc will return err on 2nd call, but db must reconnect automatically
	db.doDial <- struct{}{}
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	<-db.connectionPool
	assert.Equal(t, 3, dialCallCounter)
	assert.Equal(t, 0, len(db.connectionPool))

	// open conn and call ping on it
	db.doDial <- struct{}{}
	assert.Nil(t, encoder.Encode(ResponseOk))
	assert.Nil(t, encoder.Encode(Version))
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	assert.Equal(t, 5, dialCallCounter)
	assert.Equal(t, 1, len(db.connectionPool))
	pingCh <- time.Now()
	db.autoReconnect(ctx)
	var cmd Command
	assert.Nil(t, decoder.Decode(&cmd))
	assert.Equal(t, CmdVersion, cmd)

	// TODO: cover case when ping receive io.EOF
	// TODO: <-time.After(time.Second) is too long for test - need to move it to configuration

}
