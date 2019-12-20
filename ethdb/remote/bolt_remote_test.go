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
	"testing"

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
	// Send CmdBeginTx, followed by CmdEndTx with the wrong txHandle, followed by CmdLastError, followed by CmdEndTx with the correct handle
	// followed by the CmdLastError
	assert.Nil(t, encoder.Encode(CmdBeginTx), "Could not encode CmdBeginTx")

	// CmdEnd with the wrong handle
	txHandle := 156
	assert.Nil(t, encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle")

	// Now we issue CmdEndTx with the correct tx handle
	txHandle = 1
	assert.Nil(t, encoder.Encode(CmdEndTx), "Could not encode CmdEndTx")
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}

	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	// And then we interpret the results
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBeginTx")

	// receive error message
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdEndTx")
	assert.Equal(t, ResponseErr, responseCode, "unexpected response code")
	var errorMessage string
	assert.Nil(t, decoder.Decode(&errorMessage), "Could not decode response")
	assert.Equal(t, "transaction not found: 156", errorMessage, "unexpected response code")

	// receive good response
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
	var txHandle uint64 = 1
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle for CmdBucket")
	assert.Nil(t, encoder.Encode(name), "Could not encode name for CmdBucket")

	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(ctx, db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}

	// And then we interpret the results
	var responseCode ResponseCode
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBeginTx")
	assert.Equal(t, uint64(1), txHandle, "Unexpected txHandle")

	var bucketHandle uint64
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(2), bucketHandle, "Could not decode response from CmdBucket")
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
	var txHandle uint64 = 1
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle for CmdBucket")
	assert.Nil(t, encoder.Encode(name), "Could not encode name for CmdBucket")

	// Issue CmdGet with existing key
	assert.Nil(t, encoder.Encode(CmdGet), "Could not encode CmdGet")
	var bucketHandle uint64 = 2
	var key = []byte("key1")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(t, encoder.Encode(key), "Could not encode key for CmdGet")
	// Issue CmdGet with non-existing key
	assert.Nil(t, encoder.Encode(CmdGet), "Could not encode CmdGet")
	key = []byte("key3")
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandle for CmdGet")
	assert.Nil(t, encoder.Encode(key), "Could not encode key for CmdGet")

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
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBegin")
	assert.Equal(t, uint64(1), txHandle, "Unexpected txHandle")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(2), bucketHandle, "Unexpected bucketHandle")
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
	var txHandle uint64 = 1
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle for CmdBucket")
	assert.Nil(t, encoder.Encode(name), "Could not encode name for CmdBucket")

	assert.Nil(t, encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	var bucketHandle uint64 = 2
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	assert.Nil(t, encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	var cursorHandle uint64 = 3
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	var seekKey = []byte("key15") // Should find key2
	assert.Nil(t, encoder.Encode(seekKey), "Could not encode seekKey for CmdCursorSeek")
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
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBegin")
	assert.Equal(t, uint64(1), txHandle, "Unexpected txHandle")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(2), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(t, uint64(3), cursorHandle, "Unexpected cursorHandle")
	// Results of CmdCursorSeek
	var key, value []byte
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, key2, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, value2, string(value), "Unexpected value")
}

func TestCmdNext(t *testing.T) {
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
	var txHandle uint64 = 1
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle for CmdBucket")
	assert.Nil(t, encoder.Encode(name), "Could not encode name for CmdBucket")
	assert.Nil(t, encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	var bucketHandle uint64 = 2
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")
	assert.Nil(t, encoder.Encode(CmdCursorSeek), "Could not encode CmdCursorSeek")
	var cursorHandle uint64 = 3
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorSeek")
	var seekKey = []byte("key1") // Should find key1
	assert.Nil(t, encoder.Encode(seekKey), "Could not encode seekKey for CmdCursorSeek")
	assert.Nil(t, encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	var numberOfKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")
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
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBegin")
	assert.Equal(t, uint64(1), txHandle, "Unexpected txHandle")
	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(2), bucketHandle, "Unexpected bucketHandle")
	// Results of CmdCursor
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(t, uint64(3), cursorHandle, "Unexpected cursorHandle")
	// Results of CmdCursorSeek
	var key, value []byte
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, key1, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorSeek")
	assert.Equal(t, value1, string(value), "Unexpected value")

	// Results of CmdCursorNext
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
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
}

func TestCmdFirst(t *testing.T) {
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

	var txHandle uint64 = 1
	assert.Nil(t, encoder.Encode(txHandle), "Could not encode txHandle for CmdBucket")
	assert.Nil(t, encoder.Encode(name), "Could not encode name for CmdBucket")
	assert.Nil(t, encoder.Encode(CmdCursor), "Could not encode CmdCursor")
	var bucketHandle uint64 = 2
	assert.Nil(t, encoder.Encode(bucketHandle), "Could not encode bucketHandler for CmdCursor")

	assert.Nil(t, encoder.Encode(CmdCursorFirst), "Could not encode CmdCursorFirst")
	var cursorHandle uint64 = 3
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandle for CmdCursorFirst")
	var numberOfFirstKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
	assert.Nil(t, encoder.Encode(numberOfFirstKeys), "Could not encode numberOfFirstKeys for CmdCursorFirst")

	assert.Nil(t, encoder.Encode(CmdCursorNext), "Could not encode CmdCursorNext")
	assert.Nil(t, encoder.Encode(cursorHandle), "Could not encode cursorHandler for CmdCursorNext")
	var numberOfKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
	assert.Nil(t, encoder.Encode(numberOfKeys), "Could not encode numberOfKeys for CmdCursorNext")
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
	assert.Nil(t, decoder.Decode(&txHandle), "Could not decode response from CmdBegin")
	assert.Equal(t, uint64(1), txHandle, "Unexpected txHandle")

	// Results of CmdBucket
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&bucketHandle), "Could not decode response from CmdBucket")
	assert.Equal(t, uint64(2), bucketHandle, "Unexpected bucketHandle")

	// Results of CmdCursor
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode ResponseCode returned by CmdBeginTx")
	assert.Equal(t, ResponseOk, responseCode, "unexpected response code")
	assert.Nil(t, decoder.Decode(&cursorHandle), "Could not decode response from CmdCursor")
	assert.Equal(t, uint64(3), cursorHandle, "Unexpected cursorHandle")

	// Results of CmdCursorFirst
	var key, value []byte
	assert.Nil(t, decoder.Decode(&responseCode), "Could not decode response from CmdCursorFirst")
	assert.Equal(t, ResponseOk, responseCode, "unexpected responseCode")
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorFirst")
	assert.Equal(t, key1, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorFirst")
	assert.Equal(t, value1, string(value), "Unexpected value")

	// Results of CmdCursorNext
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Equal(t, key2, string(key), "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Equal(t, value2, string(value), "Unexpected value")

	// Results of last CmdCursorNext
	assert.Nil(t, decoder.Decode(&key), "Could not decode response from CmdCursorNext")
	assert.Nil(t, key, "Unexpected key")
	assert.Nil(t, decoder.Decode(&value), "Could not decode response from CmdCursorNext")
	assert.Nil(t, value, "Unexpected value")
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
	go func() {
		// Long read-only transaction
		if err1 := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			var keyBuf [8]byte
			for i := 0; i < 100000; i++ {
				binary.BigEndian.PutUint64(keyBuf[:], uint64(i))
				b.Get(keyBuf[:])
				tx.Yield()
			}
			return nil
		}); err1 != nil {
			t.Fatal(err1)
		}
		readFinished = true
	}()
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
