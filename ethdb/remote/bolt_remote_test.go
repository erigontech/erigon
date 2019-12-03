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
	"testing"

	"github.com/ledgerwatch/bolt"
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
	var c = CmdVersion
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdVersion: %v", err)
	}
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	var v uint64
	if err = decoder.Decode(&v); err != nil {
		t.Errorf("Could not decode version returned by CmdVersion: %v", err)
	}
	if v != Version {
		t.Errorf("Returned version %d, expected %d", v, Version)
	}
}

func TestCmdBeginEndLastError(t *testing.T) {
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
	var c = CmdBeginTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBeginTx: %v", err)
	}
	// CmdEnd with the wrong handle
	var txHandle uint64 = 156
	c = CmdEndTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdEndTx: %v", err)
	}
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle: %v", err)
	}
	// CmdLastError to retrive the error related to the CmdEndTx with the wrong handle
	c = CmdLastError
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdLastError: %v", err)
	}
	// Now we issue CmdEndTx with the correct tx handle
	c = CmdEndTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdEndTx: %v", err)
	}
	txHandle = 1
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle: %v", err)
	}
	// Check that CmdLastError now returns empty string
	c = CmdLastError
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdLastError: %v", err)
	}
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	if err = decoder.Decode(&txHandle); err != nil {
		t.Errorf("Could not decode response from CmdBeginTx")
	}
	var lastErrorStr string
	if err = decoder.Decode(&lastErrorStr); err != nil {
		t.Errorf("Could not decode response from CmdLastError")
	}
	if lastErrorStr != "transaction not found" {
		t.Errorf("Wrong error message from CmdLastError: %s", lastErrorStr)
	}
	if err = decoder.Decode(&lastErrorStr); err != nil {
		t.Errorf("Could not decode response from CmdLastError")
	}
	if lastErrorStr != "<nil>" {
		t.Errorf("Wrong error message from CmdLastError: %s", lastErrorStr)
	}
}

func TestCmdBucket(t *testing.T) {
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
	var c = CmdBeginTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBegin: %v", err)
	}
	c = CmdBucket
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBucket: %v", err)
	}
	var txHandle uint64 = 1
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle for CmdBucket: %v", err)
	}
	if err = encoder.Encode(&name); err != nil {
		t.Errorf("Could not encode name for CmdBucket: %v", err)
	}
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	if err = decoder.Decode(&txHandle); err != nil {
		t.Errorf("Could not decode response from CmdBegin")
	}
	if txHandle != 1 {
		t.Errorf("Unexpected txHandle: %d", txHandle)
	}
	var bucketHandle uint64
	if err = decoder.Decode(&bucketHandle); err != nil {
		t.Errorf("Could not decode response from CmdBucket")
	}
	if bucketHandle != 2 {
		t.Errorf("Unexpected bucketHandle: %d", bucketHandle)
	}
}

func TestCmdGet(t *testing.T) {
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
	var c = CmdBeginTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBeginTx: %v", err)
	}
	c = CmdBucket
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBucket: %v", err)
	}
	var txHandle uint64 = 1
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle for CmdBucket: %v", err)
	}
	if err = encoder.Encode(&name); err != nil {
		t.Errorf("Could not encode name for CmdBucket: %v", err)
	}
	// Issue CmdGet with existing key
	c = CmdGet
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdGet: %v", err)
	}
	var bucketHandle uint64 = 2
	var key = []byte("key1")
	if err = encoder.Encode(&bucketHandle); err != nil {
		t.Errorf("Could not encode bucketHandle for CmdGet: %v", err)
	}
	if err = encoder.Encode(&key); err != nil {
		t.Errorf("Could not encode key for CmdGet: %v", err)
	}
	// Issue CmdGet with non-existing key
	c = CmdGet
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdGet: %v", err)
	}
	key = []byte("key3")
	if err = encoder.Encode(&bucketHandle); err != nil {
		t.Errorf("Could not encode bucketHandle for CmdGet: %v", err)
	}
	if err = encoder.Encode(&key); err != nil {
		t.Errorf("Could not encode key for CmdGet: %v", err)
	}
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	if err = decoder.Decode(&txHandle); err != nil {
		t.Errorf("Could not decode response from CmdBegin")
	}
	if txHandle != 1 {
		t.Errorf("Unexpected txHandle: %d", txHandle)
	}
	// Results of CmdBucket
	if err = decoder.Decode(&bucketHandle); err != nil {
		t.Errorf("Could not decode response from CmdBucket")
	}
	if bucketHandle != 2 {
		t.Errorf("Unexpected bucketHandle: %d", bucketHandle)
	}
	// Results of CmdGet (for key1)
	var value []byte
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode value from CmdGet: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Wrong value from CmdGet, expected: %x, got %x", "value1", value)
	}
	// Results of CmdGet (for key3)
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode value from CmdGet: %v", err)
	}
	if value != nil {
		t.Errorf("Wrong value from CmdGet, expected: %x, got %x", "value1", value)
	}
}

func TestCmdSeek(t *testing.T) {
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
	var c = CmdBeginTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBeginTx: %v", err)
	}
	c = CmdBucket
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBucket: %v", err)
	}
	var txHandle uint64 = 1
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle for CmdBucket: %v", err)
	}
	if err = encoder.Encode(&name); err != nil {
		t.Errorf("Could not encode name for CmdBucket: %v", err)
	}
	c = CmdCursor
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdCursor: %v", err)
	}
	var bucketHandle uint64 = 2
	if err = encoder.Encode(&bucketHandle); err != nil {
		t.Errorf("Could not encode bucketHandler for CmdCursor: %v", err)
	}
	c = CmdCursorSeek
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdCursorSeek: %v", err)
	}
	var cursorHandle uint64 = 3
	if err = encoder.Encode(&cursorHandle); err != nil {
		t.Errorf("Could not encode cursorHandle for CmdCursorSeek: %v", err)
	}
	var seekKey = []byte("key15") // Should find key2
	if err = encoder.Encode(&seekKey); err != nil {
		t.Errorf("Could not encode seekKey for CmdCursorSeek: %v", err)
	}
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	if err = decoder.Decode(&txHandle); err != nil {
		t.Errorf("Could not decode response from CmdBegin")
	}
	if txHandle != 1 {
		t.Errorf("Unexpected txHandle: %d", txHandle)
	}
	// Results of CmdBucket
	if err = decoder.Decode(&bucketHandle); err != nil {
		t.Errorf("Could not decode response from CmdBucket")
	}
	if bucketHandle != 2 {
		t.Errorf("Unexpected bucketHandle: %d", bucketHandle)
	}
	// Results of CmdCursor
	if err = decoder.Decode(&cursorHandle); err != nil {
		t.Errorf("Could not decode response from CmdCursor: %v", err)
	}
	if cursorHandle != 3 {
		t.Errorf("Unexpected cursorHandle: %d", cursorHandle)
	}
	// Results of CmdCursorSeek
	var key, value []byte
	if err = decoder.Decode(&key); err != nil {
		t.Errorf("Could not decode response from CmdCursorSeek: %v", err)
	}
	if string(key) != key2 {
		t.Errorf("Unexpected key: %s", key)
	}
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode response from CmdCursorSeek: %v", err)
	}
	if string(value) != value2 {
		t.Errorf("Unexpected value: %s", key)
	}
}

func TestCmdNext(t *testing.T) {
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
	var c = CmdBeginTx
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBeginTx: %v", err)
	}
	c = CmdBucket
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdBucket: %v", err)
	}
	var txHandle uint64 = 1
	if err = encoder.Encode(&txHandle); err != nil {
		t.Errorf("Could not encode txHandle for CmdBucket: %v", err)
	}
	if err = encoder.Encode(&name); err != nil {
		t.Errorf("Could not encode name for CmdBucket: %v", err)
	}
	c = CmdCursor
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdCursor: %v", err)
	}
	var bucketHandle uint64 = 2
	if err = encoder.Encode(&bucketHandle); err != nil {
		t.Errorf("Could not encode bucketHandler for CmdCursor: %v", err)
	}
	c = CmdCursorSeek
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdCursorSeek: %v", err)
	}
	var cursorHandle uint64 = 3
	if err = encoder.Encode(&cursorHandle); err != nil {
		t.Errorf("Could not encode cursorHandle for CmdCursorSeek: %v", err)
	}
	var seekKey = []byte("key1") // Should find key1
	if err = encoder.Encode(&seekKey); err != nil {
		t.Errorf("Could not encode seekKey for CmdCursorSeek: %v", err)
	}
	c = CmdCursorNext
	if err = encoder.Encode(&c); err != nil {
		t.Errorf("Could not encode CmdCursorNext: %v", err)
	}
	if err = encoder.Encode(&cursorHandle); err != nil {
		t.Errorf("Could not encode cursorHandler for CmdCursorNext: %v", err)
	}
	var numberOfKeys uint64 = 3 // Trying to get 3 keys, but will get 1 + nil
	if err = encoder.Encode(&numberOfKeys); err != nil {
		t.Errorf("Could not encode numberOfKeys for CmdNex: %v", err)
	}
	// By now we constructed all input requests, now we call the
	// Server to process them all
	if err = Server(db, &inBuf, &outBuf, closer); err != nil {
		t.Errorf("Error while calling Server: %v", err)
	}
	// And then we interpret the results
	// Results of CmdBeginTx
	if err = decoder.Decode(&txHandle); err != nil {
		t.Errorf("Could not decode response from CmdBegin")
	}
	if txHandle != 1 {
		t.Errorf("Unexpected txHandle: %d", txHandle)
	}
	// Results of CmdBucket
	if err = decoder.Decode(&bucketHandle); err != nil {
		t.Errorf("Could not decode response from CmdBucket")
	}
	if bucketHandle != 2 {
		t.Errorf("Unexpected bucketHandle: %d", bucketHandle)
	}
	// Results of CmdCursor
	if err = decoder.Decode(&cursorHandle); err != nil {
		t.Errorf("Could not decode response from CmdCursor: %v", err)
	}
	if cursorHandle != 3 {
		t.Errorf("Unexpected cursorHandle: %d", cursorHandle)
	}
	// Results of CmdCursorSeek
	var key, value []byte
	if err = decoder.Decode(&key); err != nil {
		t.Errorf("Could not decode response from CmdCursorSeek: %v", err)
	}
	if string(key) != key1 {
		t.Errorf("Unexpected key: %s", key)
	}
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode response from CmdCursorSeek: %v", err)
	}
	if string(value) != value1 {
		t.Errorf("Unexpected value: %s", value)
	}
	// Results of CmdCursorNext
	if err = decoder.Decode(&key); err != nil {
		t.Errorf("Could not decode response from CmdCursorNext: %v", err)
	}
	if string(key) != key2 {
		t.Errorf("Unexpected key: %s", key)
	}
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode response from CmdCursorNext: %v", err)
	}
	if string(value) != value2 {
		t.Errorf("Unexpected value: %s", value)
	}
	if err = decoder.Decode(&key); err != nil {
		t.Errorf("Could not decode response from CmdCursorNext: %v", err)
	}
	if key != nil {
		t.Errorf("Unexpected key: %s", key)
	}
	if err = decoder.Decode(&value); err != nil {
		t.Errorf("Could not decode response from CmdCursorNext: %v", err)
	}
	if value != nil {
		t.Errorf("Unexpected value: %s", value)
	}
}
