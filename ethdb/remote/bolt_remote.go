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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

// Version is the current version of the remote db protocol. If the protocol changes in a non backwards compatible way,
// this constant needs to be increased
const Version uint64 = 2

// Command is the type of command in the boltdb remote protocol
type Command uint8
type ResponseCode uint8

const (
	// ResponseOk
	// successful response to client's request
	ResponseOk ResponseCode = iota
	// ResponseErr : errorMessage
	// returns error to client
	ResponseErr
)

const (
	// CmdVersion : version
	// is sent from client to server to ask about the version of protocol the server supports
	// it is also to be used to be sent periodically to make sure the connection stays open
	CmdVersion Command = iota
	// CmdBeginTx
	// request starting a new transaction (read-only). It returns transaction's handle (uint64), or 0
	// if there was an error. If 0 is returned, the corresponding
	CmdBeginTx
	// CmdEndTx ()
	// request the end of the transaction (rollback)
	CmdEndTx
	// CmdBucket (name): bucketHandle
	// requests opening a bucket with given name. It returns bucket's handle (uint64)
	CmdBucket
	// CmdGet (bucketHandle, key): value
	// requests a value for a key from given bucket.
	CmdGet
	// CmdCursor (bucketHandle): cursorHandle
	// request creating a cursor for the given bucket. It returns cursor's handle (uint64)
	CmdCursor
	// CmdCursorSeek (cursorHandle, seekKey): (key, value)
	// Moves given cursor to the seekKey, or to the next key after seekKey
	CmdCursorSeek
	// CmdCursorNext (cursorHandle, number of keys): [(key, value)]
	// Moves given cursor over the next given number of keys and streams back the (key, value) pairs
	// Pair with key == nil signifies the end of the stream
	CmdCursorNext
	// CmdCursorFirst (cursorHandle, number of keys): [(key, value)]
	// Moves given cursor to bucket start and streams back the (key, value) pairs
	// Pair with key == nil signifies the end of the stream
	CmdCursorFirst
	// CmdCursorSeekTo (cursorHandle, seekKey): (key, value)
	// Moves given cursor to the seekKey, or to the next key after seekKey
	CmdCursorSeekTo
	// CmdGetAsOf (bucket, hBucket, key []byte, timestamp uint64): (value)
	CmdGetAsOf
	// CmdCursorFirstKey (cursorHandle, number of keys): [(key, valueIsEmpty)]
	// Moves given cursor to bucket start and streams back the (key, valueIsEmpty) pairs
	// Pair with key == nil signifies the end of the stream
	CmdCursorFirstKey
	// CmdCursorNextKey (cursorHandle, number of keys): [(key, valueIsEmpty)]
	// Moves given cursor over the next given number of keys and streams back the (key, valueIsEmpty) pairs
	// Pair with key == nil signifies the end of the stream
	CmdCursorNextKey
)

const DefaultCursorBatchSize uint64 = 1
const CursorMaxBatchSize uint64 = 1 * 1000 * 1000
const ServerMaxConnections uint64 = 2048
const ClientMaxConnections uint64 = 128

var logger = log.New("database", "remote")

func encodeKeyValue(encoder *codec.Encoder, key *[]byte, value *[]byte) error {
	if err := encoder.Encode(key); err != nil {
		return err
	}
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return nil
}

func decodeKeyValue(decoder *codec.Decoder, key *[]byte, value *[]byte) (err error) {
	if err := decoder.Decode(key); err != nil {
		return err
	}
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return nil
}

func encodeKey(encoder *codec.Encoder, key *[]byte, valueIsEmpty bool) error {
	if err := encoder.Encode(key); err != nil {
		return err
	}
	if err := encoder.Encode(valueIsEmpty); err != nil {
		return err
	}
	return nil
}

func decodeKey(decoder *codec.Decoder, key *[]byte, valueIsEmpty *bool) error {
	if err := decoder.Decode(key); err != nil {
		return err
	}
	if err := decoder.Decode(valueIsEmpty); err != nil {
		return err
	}
	return nil
}

func encodeErr(encoder *codec.Encoder, mainError error) {
	if err := encoder.Encode(ResponseErr); err != nil {
		logger.Error("could not encode ResponseErr", "err", err)
		return
	}
	if err := encoder.Encode(mainError.Error()); err != nil {
		logger.Error("could not encode errCode", "err", err)
		return
	}
}

func decodeErr(decoder *codec.Decoder, responseCode ResponseCode) error {
	if responseCode != ResponseErr {
		return fmt.Errorf("unknown response code: %d", responseCode)
	}

	var errorMessage string
	if err := decoder.Decode(&errorMessage); err != nil {
		return fmt.Errorf("can't decode errorMessage: %w", err)
	}

	return errors.New(errorMessage)
}

// Server is to be called as a go-routine, one per every client connection.
// It runs while the connection is active and keep the entire connection's context
// in the local variables
// For tests, bytes.Buffer can be used for both `in` and `out`
func Server(ctx context.Context, db *bolt.DB, in io.Reader, out io.Writer, closer io.Closer) error {
	defer func() {
		if err1 := closer.Close(); err1 != nil {
			logger.Error("Could not close connection", "err", err1)
		}
	}()

	decoder := codecpool.Decoder(in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(out)
	defer codecpool.Return(encoder)
	// Server is passive - it runs a loop what reads commands (and their arguments) and attempts to respond
	var lastHandle uint64
	// Read-only transactions opened by the client
	var tx *bolt.Tx

	// We do Rollback and never Commit, because the remote transactions are always read-only, and must never change
	// anything
	defer func() {
		if tx != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logger.Error("could not roll back", "err", rollbackErr)
			}
			tx = nil
		}
	}()

	// Buckets opened by the client
	buckets := make(map[uint64]*bolt.Bucket, 2)
	// List of buckets opened in each transaction
	//bucketsByTx := make(map[uint64][]uint64, 10)
	// Cursors opened by the client
	cursors := make(map[uint64]*bolt.Cursor, 2)
	// List of cursors opened in each bucket
	cursorsByBucket := make(map[uint64][]uint64, 2)

	var c Command
	var bucketHandle uint64
	var cursorHandle uint64

	var name []byte
	var seekKey []byte

	for {
		// Make sure we are not blocking the resizing of the memory map
		if tx != nil {
			tx.Yield()
		}

		if err := decoder.Decode(&c); err != nil {
			if err == io.EOF {
				// Graceful termination when the end of the input is reached
				break
			}
			return fmt.Errorf("could not decode command: %w", err)
		}
		switch c {
		case CmdVersion:
			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code to CmdVersion: %w", err)
			}
			if err := encoder.Encode(Version); err != nil {
				return fmt.Errorf("could not encode response to CmdVersion: %w", err)
			}
		case CmdBeginTx:
			var err error
			tx, err = db.Begin(false)
			if err != nil {
				err2 := fmt.Errorf("could not start transaction for CmdBeginTx: %w", err)
				encodeErr(encoder, err2)
				return err2
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdBeginTx: %w", err)
			}
		case CmdEndTx:
			// Remove all the buckets
			for bucketHandle := range buckets {
				if cursorHandles, ok2 := cursorsByBucket[bucketHandle]; ok2 {
					for _, cursorHandle := range cursorHandles {
						delete(cursors, cursorHandle)
					}
					delete(cursorsByBucket, bucketHandle)
				}
				delete(buckets, bucketHandle)
			}

			if tx != nil {
				if err := tx.Rollback(); err != nil {
					return fmt.Errorf("could not end transaction: %w", err)
				}
				tx = nil
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdEndTx: %w", err)
			}
		case CmdBucket:
			// Read the name of the bucket
			if err := decoder.Decode(&name); err != nil {
				return fmt.Errorf("could not decode name for CmdBucket: %w", err)
			}

			// Open the bucket
			if tx == nil {
				err := fmt.Errorf("send CmdBucket before CmdBeginTx")
				encodeErr(encoder, err)
				return err
			}

			bucket := tx.Bucket(name)
			if bucket == nil {
				err := fmt.Errorf("bucket not found: %s", name)
				encodeErr(encoder, err)
				continue
			}

			lastHandle++
			buckets[lastHandle] = bucket
			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdBucket: %w", err)
			}

			if err := encoder.Encode(lastHandle); err != nil {
				return fmt.Errorf("could not encode bucketHandle in response to CmdBucket: %w", err)
			}

		case CmdGet:
			var k, v []byte
			if err := decoder.Decode(&bucketHandle); err != nil {
				return fmt.Errorf("could not decode bucketHandle for CmdGet: %w", err)
			}
			if err := decoder.Decode(&k); err != nil {
				return fmt.Errorf("could not decode key for CmdGet: %w", err)
			}
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found for CmdGet: %d", bucketHandle))
				continue
			}
			v, _ = bucket.Get(k)

			if err := encoder.Encode(ResponseOk); err != nil {
				err = fmt.Errorf("could not encode response code for CmdGet: %w", err)
				return err
			}

			if err := encoder.Encode(&v); err != nil {
				return fmt.Errorf("could not encode value in response for CmdGet: %w", err)
			}

		case CmdCursor:
			if err := decoder.Decode(&bucketHandle); err != nil {
				return fmt.Errorf("could not decode bucketHandle for CmdCursor: %w", err)
			}
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found for CmdCursor: %d", bucketHandle))
				continue
			}

			cursor := bucket.Cursor()
			lastHandle++
			cursorHandle = lastHandle
			cursors[cursorHandle] = cursor
			if cursorHandles, ok1 := cursorsByBucket[bucketHandle]; ok1 {
				cursorHandles = append(cursorHandles, cursorHandle)
				cursorsByBucket[bucketHandle] = cursorHandles
			} else {
				cursorsByBucket[bucketHandle] = []uint64{cursorHandle}
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response for CmdCursor: %w", err)
			}

			if err := encoder.Encode(cursorHandle); err != nil {
				return fmt.Errorf("could not cursor handle in response to CmdCursor: %w", err)
			}
		case CmdCursorSeek:
			var k, v []byte

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not encode (key,value) for CmdCursorSeek: %w", err)
			}
			if err := decoder.Decode(&seekKey); err != nil {
				return fmt.Errorf("could not encode (key,value) for CmdCursorSeek: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}
			k, v = cursor.Seek(seekKey)
			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode (key,value) for CmdCursorSeek: %w", err)
			}
			if err := encodeKeyValue(encoder, &k, &v); err != nil {
				return fmt.Errorf("could not encode (key,value) for CmdCursorSeek: %w", err)
			}
		case CmdCursorSeekTo:
			var k, v []byte

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdCursorSeekTo: %w", err)
			}
			if err := decoder.Decode(&seekKey); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdCursorSeekTo: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			k, v = cursor.SeekTo(seekKey)

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdCursorSeek: %w", err)
			}

			if err := encodeKeyValue(encoder, &k, &v); err != nil {
				return fmt.Errorf("could not encode (key,value) in response to CmdCursorSeekTo: %w", err)
			}
		case CmdCursorNext:
			var k = &[]byte{}
			var v = &[]byte{}

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for CmdCursorNext: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for CmdCursorNext: %w", err)
			}

			if numberOfKeys > CursorMaxBatchSize {
				encodeErr(encoder, fmt.Errorf("requested numberOfKeys is too large: %d", numberOfKeys))
				continue
			}

			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdCursorNext: %w", err)
			}

			for *k, *v = cursor.Next(); numberOfKeys > 0; *k, *v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) in response to CmdCursorNext: %w", err)
				}

				numberOfKeys--
				if *k == nil {
					break
				}
			}

		case CmdCursorFirst:
			var k = &[]byte{}
			var v = &[]byte{}

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for CmdCursorFirst: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for CmdCursorFirst: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code for CmdCursorFirst: %w", err)
			}

			for *k, *v = cursor.First(); numberOfKeys > 0; *k, *v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) for CmdCursorFirst: %w", err)
				}

				numberOfKeys--
				if *k == nil {
					break
				}
			}
		case CmdCursorNextKey:
			var k = &[]byte{}
			var v = &[]byte{}

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for CmdCursorNextKey: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for CmdCursorNextKey: %w", err)
			}

			if numberOfKeys > CursorMaxBatchSize {
				encodeErr(encoder, fmt.Errorf("requested numberOfKeys is too large: %d", numberOfKeys))
				continue
			}

			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdCursorNextKey: %w", err)
			}

			for *k, *v = cursor.Next(); numberOfKeys > 0; *k, *v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKey(encoder, k, len(*v) == 0); err != nil {
					return fmt.Errorf("could not encode (key,valueIsEmpty) in response to CmdCursorNextKey: %w", err)
				}

				numberOfKeys--
				if *k == nil {
					break
				}
			}
		case CmdCursorFirstKey:
			var k = &[]byte{}
			var v = &[]byte{}

			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for CmdCursorFirstKey: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for CmdCursorFirstKey: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code for CmdCursorFirstKey: %w", err)
			}

			for *k, *v = cursor.First(); numberOfKeys > 0; *k, *v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKey(encoder, k, len(*v) == 0); err != nil {
					return fmt.Errorf("could not encode (key,valueIsEmpty) for CmdCursorFirstKey: %w", err)
				}

				numberOfKeys--
				if k == nil {
					break
				}
			}
		case CmdGetAsOf:
			var bucket, hBucket, key, v []byte
			var timestamp uint64
			if err := decoder.Decode(&bucket); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdGetAsOf: %w", err)
			}
			if err := decoder.Decode(&hBucket); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdGetAsOf: %w", err)
			}
			if err := decoder.Decode(&key); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdGetAsOf: %w", err)
			}
			if err := decoder.Decode(&timestamp); err != nil {
				return fmt.Errorf("could not decode seekKey for CmdGetAsOf: %w", err)
			}

			d := ethdb.NewWrapperBoltDatabase(db)

			var err error
			v, err = d.GetAsOf(bucket, hBucket, key, timestamp)
			if err != nil {
				encodeErr(encoder, err)
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to CmdGetAsOf: %w", err)
			}

			if err := encoder.Encode(&v); err != nil {
				return fmt.Errorf("could not encode response to CmdGetAsOf: %w", err)
			}
		default:
			logger.Error("unknown", "command", c)
			return fmt.Errorf("unknown command %d", c)
		}
	}

	return nil
}

// Listener starts listener that for each incoming connection
// spawn a go-routine invoking Server
func Listener(ctx context.Context, db *bolt.DB, address string) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		logger.Error("Could not create listener", "address", address, "err", err)
		return
	}
	defer func() {
		if err = ln.Close(); err != nil {
			logger.Error("Could not close listener", "err", err)
		}
	}()
	logger.Info("Listening on", "address", address)

	ch := make(chan bool, ServerMaxConnections)
	defer close(ch)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Trace("connections", "amount", len(ch))
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		conn, err1 := ln.Accept()
		if err1 != nil {
			logger.Error("Could not accept connection", "err", err1)
			continue
		}

		go func() {
			ch <- true
			defer func() {
				<-ch
			}()

			err := Server(ctx, db, conn, conn, conn)
			if err != nil {
				logger.Warn("server error", "err", err)
			}
		}()
	}
}

type conn struct {
	in     io.Reader
	out    io.Writer
	closer io.Closer
}

// DB mimicks the interface of the bolt.DB,
// but it works via a pair (Reader, Writer)
type DB struct {
	dialFunc       DialFunc
	connectionPool chan *conn
	dialTimeout    time.Duration
	pingTimeout    time.Duration
	retryDialAfter time.Duration
	pingEvery      time.Duration
	doDial         chan struct{}
	doPing         <-chan time.Time
}

type DialFunc func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error)

// Pool of connections to server
func (db *DB) getConnection(ctx context.Context) (io.Reader, io.Writer, io.Closer, error) {
	select {
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	case conn := <-db.connectionPool:
		return conn.in, conn.out, conn.closer, nil
	}
}

func (db *DB) returnConn(ctx context.Context, in io.Reader, out io.Writer, closer io.Closer) {
	select {
	case db.connectionPool <- &conn{in: in, out: out, closer: closer}:
	case <-ctx.Done():
	}
}

func (db *DB) ping(ctx context.Context) (err error) {
	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if closeErr := closer.Close(); closeErr != nil {
				logger.Error("can't close connection", "err", closeErr)
			}
			return
		}

		db.returnConn(ctx, in, out, closer)
	}()

	decoder := codecpool.Decoder(in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(out)
	defer codecpool.Return(encoder)
	// Check version
	if err := encoder.Encode(CmdVersion); err != nil {
		return fmt.Errorf("could not encode CmdVersion: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return fmt.Errorf("could not decode ResponseCode of CmdVersion: %w", err)
	}

	if responseCode != ResponseOk {
		return decodeErr(decoder, responseCode)
	}

	var v uint64
	if err := decoder.Decode(&v); err != nil {
		return err
	}
	if v != Version {
		return fmt.Errorf("server protocol version %d, expected %d", v, Version)
	}

	return nil
}

type notifyOnClose struct {
	internal io.Closer
	notifyCh chan struct{}
}

func (closer notifyOnClose) Close() error {
	closer.notifyCh <- struct{}{}
	if closer.internal == nil {
		return nil
	}

	return closer.internal.Close()
}

// NewDB creates a new instance of DB
func NewDB(parentCtx context.Context, dialFunc DialFunc) (*DB, error) {
	db := &DB{
		dialFunc:       dialFunc,
		connectionPool: make(chan *conn, ClientMaxConnections),
		doDial:         make(chan struct{}, ClientMaxConnections),
		dialTimeout:    3 * time.Second,
		pingTimeout:    500 * time.Millisecond,
		retryDialAfter: 1 * time.Second,
		pingEvery:      1 * time.Second,
	}

	for i := uint64(0); i < ClientMaxConnections; i++ {
		db.doDial <- struct{}{}
	}

	ctx, cancelConnections := context.WithCancel(context.Background())
	go func() {
		<-parentCtx.Done()
		cancelConnections()
	}()

	traceTicker := time.NewTicker(3 * time.Second)
	pingTicker := time.NewTicker(db.pingEvery)
	db.doPing = pingTicker.C
	go func() {
		defer pingTicker.Stop()
		defer traceTicker.Stop()

		for {
			select {
			default:
			case <-ctx.Done():
				return
			case <-traceTicker.C:
				logger.Trace("connections in pool", "amount", len(db.connectionPool))
			}

			db.autoReconnect(ctx)
		}
	}()

	return db, nil
}

func (db *DB) autoReconnect(ctx context.Context) {
	select {
	case <-db.doDial:
		dialCtx, cancel := context.WithTimeout(ctx, db.dialTimeout)
		defer cancel()
		newIn, newOut, newCloser, err := db.dialFunc(dialCtx)
		if err != nil {
			logger.Warn("dial failed", "err", err)
			db.doDial <- struct{}{}
			time.Sleep(db.retryDialAfter)
			return
		}

		notifyCloser := notifyOnClose{notifyCh: db.doDial, internal: newCloser}
		db.returnConn(ctx, newIn, newOut, notifyCloser)
	case <-db.doPing:
		// periodically ping to close broken connections
		pingCtx, cancel := context.WithTimeout(ctx, db.pingTimeout)
		defer cancel()
		if err := db.ping(pingCtx); err != nil {
			if !errors.Is(err, io.EOF) { // io.EOF means server gone
				logger.Warn("ping failed", "err", err)
				return
			}

			// if server gone, then need re-check all connections by ping. It will remove broken connections from pool.
			for i := uint64(0); i < ClientMaxConnections-1; i++ {
				pingCtx, cancel := context.WithTimeout(ctx, db.pingTimeout)
				_ = db.ping(pingCtx)
				cancel()
			}
		}
	}
}

// Close closes DB by using the closer field
func (db *DB) Close() error {
	return nil
}

// Tx mimicks the interface of bolt.Tx
type Tx struct {
	ctx context.Context
	in  io.Reader
	out io.Writer
}

func (db *DB) endTx(ctx context.Context, encoder *codec.Encoder, decoder *codec.Decoder) error {
	_ = ctx
	var responseCode ResponseCode

	if err := encoder.Encode(CmdEndTx); err != nil {
		return fmt.Errorf("could not encode CmdEndTx: %w", err)
	}

	if err := decoder.Decode(&responseCode); err != nil {
		return fmt.Errorf("could not decode ResponseCode for CmdEndTx: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return fmt.Errorf("could not decode errorMessage for CmdEndTx: %w", err)
		}
	}
	return nil
}

func (db *DB) CmdGetAsOf(ctx context.Context, bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	var err error
	var responseCode ResponseCode
	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			closer.Close()
			return
		}
		db.returnConn(ctx, in, out, closer)
	}()

	decoder := codecpool.Decoder(in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(out)
	defer codecpool.Return(encoder)

	err = encoder.Encode(CmdGetAsOf)
	if err != nil {
		return nil, fmt.Errorf("could not encode CmdGetAsOf: %w", err)
	}
	err = encoder.Encode(&bucket)
	if err != nil {
		return nil, fmt.Errorf("could not encode CmdGetAsOf: %w", err)
	}
	err = encoder.Encode(&hBucket)
	if err != nil {
		return nil, fmt.Errorf("could not encode CmdGetAsOf: %w", err)
	}
	err = encoder.Encode(&key)
	if err != nil {
		return nil, fmt.Errorf("could not encode CmdGetAsOf: %w", err)
	}
	err = encoder.Encode(timestamp)
	if err != nil {
		return nil, fmt.Errorf("could not encode CmdGetAsOf: %w", err)
	}

	err = decoder.Decode(&responseCode)
	if err != nil {
		return nil, err
	}

	if responseCode != ResponseOk {
		err = decodeErr(decoder, responseCode)
		return nil, err
	}

	var val []byte
	err = decoder.Decode(&val)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// View performs read-only transaction on the remote database
// NOTE: not thread-safe
func (db *DB) View(ctx context.Context, f func(tx *Tx) error) (err error) {
	var opErr error
	var endTxErr error

	var responseCode ResponseCode

	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil || endTxErr != nil || opErr != nil {
			if closeErr := closer.Close(); closeErr != nil {
				logger.Error("can't close connection", "err", closeErr)
			}
			return
		}
		db.returnConn(ctx, in, out, closer)
	}()

	decoder := codecpool.Decoder(in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(out)
	defer codecpool.Return(encoder)

	if err = encoder.Encode(CmdBeginTx); err != nil {
		return fmt.Errorf("could not encode CmdBeginTx: %w", err)
	}

	if err = decoder.Decode(&responseCode); err != nil {
		return fmt.Errorf("could not decode response code of CmdBeginTx: %w", err)
	}

	if responseCode != ResponseOk {
		return decodeErr(decoder, responseCode)
	}

	tx := &Tx{ctx: ctx, in: in, out: out}
	opErr = f(tx)

	endTxErr = db.endTx(ctx, encoder, decoder)
	if endTxErr != nil {
		logger.Warn("could not finish tx", "err", err)
	}

	return opErr
}

// Bucket mimicks the interface of bolt.Bucket
type Bucket struct {
	ctx          context.Context
	in           io.Reader
	out          io.Writer
	bucketHandle uint64
}

type Cursor struct {
	ctx          context.Context
	in           io.Reader
	out          io.Writer
	cursorHandle uint64

	cacheLastIdx      uint64
	cacheIdx          uint64
	batchSize         uint64
	cacheKeys         [][]byte
	cacheValues       [][]byte
	cacheValueIsEmpty []bool
}

// Bucket returns the handle to the bucket in remote DB
func (tx *Tx) Bucket(name []byte) (*Bucket, error) {
	select {
	default:
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	}

	decoder := codecpool.Decoder(tx.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(tx.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(CmdBucket); err != nil {
		return nil, fmt.Errorf("could not encode CmdBucket: %w", err)
	}
	if err := encoder.Encode(&name); err != nil {
		return nil, fmt.Errorf("could not encode name for CmdBucket: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return nil, fmt.Errorf("could not decode ResponseCode for CmdBucket: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return nil, fmt.Errorf("could not decode errorMessage for CmdBucket: %w", err)
		}
	}

	var bucketHandle uint64
	if err := decoder.Decode(&bucketHandle); err != nil {
		return nil, fmt.Errorf("could not decode bucketHandle for CmdBucket: %w", err)
	}
	if bucketHandle == 0 {
		return nil, fmt.Errorf("unexpected bucketHandle: 0")
	}

	bucket := &Bucket{ctx: tx.ctx, bucketHandle: bucketHandle, in: tx.in, out: tx.out}
	return bucket, nil
}

// Get reads a value corresponding to the given key, from the bucket
// return nil if they key is not present
func (b *Bucket) Get(key []byte) ([]byte, error) {
	select {
	default:
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}

	decoder := codecpool.Decoder(b.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(b.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(CmdGet); err != nil {
		return nil, fmt.Errorf("could not encode CmdGet: %w", err)
	}
	if err := encoder.Encode(b.bucketHandle); err != nil {
		return nil, fmt.Errorf("could not encode bucketHandle for CmdGet: %w", err)
	}
	if err := encoder.Encode(&key); err != nil {
		return nil, fmt.Errorf("could not encode key for CmdGet: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return nil, fmt.Errorf("could not decode ResponseCode for CmdGet: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return nil, fmt.Errorf("could not decode errorMessage for CmdGet: %w", err)
		}
	}

	var value []byte
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("could not decode value for CmdGet: %w", err)
	}
	return value, nil
}

func (b *Bucket) BatchCursor(batchSize uint64) (*Cursor, error) {
	c, err := b.Cursor()
	if err != nil {
		return nil, err
	}
	c.SetBatchSize(batchSize)
	return c, nil
}

// Cursor iterating over bucket keys
func (b *Bucket) Cursor() (*Cursor, error) {
	select {
	default:
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}

	decoder := codecpool.Decoder(b.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(b.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(CmdCursor); err != nil {
		return nil, fmt.Errorf("could not encode CmdCursor: %w", err)
	}
	if err := encoder.Encode(b.bucketHandle); err != nil {
		return nil, fmt.Errorf("could not encode bucketHandle for CmdCursor: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return nil, fmt.Errorf("could not decode ResponseCode for CmdCursor: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return nil, fmt.Errorf("could not decode errorMessage for CmdCursor: %w", err)
		}
	}

	var cursorHandle uint64
	if err := decoder.Decode(&cursorHandle); err != nil {
		return nil, fmt.Errorf("could not decode cursorHandle for CmdCursor: %w", err)
	}

	if cursorHandle == 0 { // Retrieve the error
		return nil, fmt.Errorf("unexpected bucketHandle: 0")
	}

	cursor := &Cursor{
		ctx:          b.ctx,
		in:           b.in,
		out:          b.out,
		cursorHandle: cursorHandle,

		batchSize: DefaultCursorBatchSize,
	}

	return cursor, nil
}

func (c *Cursor) SetBatchSize(batchSize uint64) {
	c.batchSize = batchSize
	c.cacheKeys = nil
	c.cacheValues = nil
}

func (c *Cursor) First() (key []byte, value []byte, err error) {
	if err := c.fetchPage(CmdCursorFirst); err != nil {
		return nil, nil, err
	}
	c.cacheIdx = 0

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValues[c.cacheIdx]
	c.cacheIdx++
	return k, v, nil

}

func (c *Cursor) FirstKey() (key []byte, vIsEmpty bool, err error) {
	if err := c.fetchPage(CmdCursorFirstKey); err != nil {
		return nil, false, err
	}
	c.cacheIdx = 0

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValueIsEmpty[c.cacheIdx]
	c.cacheIdx++

	return k, v, nil
}

func (c *Cursor) SeekKey(seek []byte) (key []byte, vIsEmpty bool, err error) {
	key, v, err := c.Seek(seek)
	vIsEmpty = len(v) == 0
	return key, vIsEmpty, err
}

func (c *Cursor) Seek(seek []byte) (key []byte, value []byte, err error) {
	c.cacheLastIdx = 0 // .Next() cache is invalid after .Seek() and .SeekTo() calls

	select {
	default:
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	}

	decoder := codecpool.Decoder(c.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(c.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(CmdCursorSeek); err != nil {
		return nil, nil, fmt.Errorf("could not encode CmdCursorSeek: %w", err)
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		return nil, nil, fmt.Errorf("could not encode cursorHandle for CmdCursorSeek: %w", err)
	}
	if err := encoder.Encode(&seek); err != nil {
		return nil, nil, fmt.Errorf("could not encode key for CmdCursorSeek: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return nil, nil, fmt.Errorf("could not decode ResponseCode for CmdCursorSeek: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return nil, nil, fmt.Errorf("could not decode errorMessage for CmdCursorSeek: %w", err)
		}
	}

	if err := decoder.Decode(&key); err != nil {
		return nil, nil, fmt.Errorf("could not decode key for CmdCursorSeek: %w", err)
	}

	if err := decoder.Decode(&value); err != nil {
		return nil, nil, fmt.Errorf("could not decode value for CmdCursorSeek: %w", err)
	}

	return key, value, nil
}

func (c *Cursor) SeekTo(seek []byte) (key []byte, value []byte, err error) {
	c.cacheLastIdx = 0 // .Next() cache is invalid after .Seek() and .SeekTo() calls

	select {
	default:
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	}

	decoder := codecpool.Decoder(c.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(c.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(CmdCursorSeekTo); err != nil {
		return nil, nil, fmt.Errorf("could not encode CmdCursorSeekTo: %w", err)
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		return nil, nil, fmt.Errorf("could not encode cursorHandle for CmdCursorSeekTo: %w", err)
	}
	if err := encoder.Encode(&seek); err != nil {
		return nil, nil, fmt.Errorf("could not encode key for CmdCursorSeekTo: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return nil, nil, fmt.Errorf("could not decode ResponseCode for CmdCursorSeekTo: %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return nil, nil, fmt.Errorf("could not decode errorMessage for CmdCursorSeekTo: %w", err)
		}
	}

	if err := decodeKeyValue(decoder, &key, &value); err != nil {
		return nil, nil, fmt.Errorf("could not decode (key, value) for CmdCursorSeekTo: %w", err)
	}

	return key, value, nil
}

func (c *Cursor) needFetchNextPage() bool {
	res := c.cacheLastIdx == 0 || // cache is empty
		c.cacheIdx == c.cacheLastIdx // all cache read
	return res
}

func (c *Cursor) Next() (keys []byte, values []byte, err error) {
	if c.needFetchNextPage() {
		err := c.fetchPage(CmdCursorNext)
		if err != nil {
			return nil, nil, err
		}
		c.cacheIdx = 0
	}

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValues[c.cacheIdx]
	c.cacheIdx++

	return k, v, nil
}

func (c *Cursor) NextKey() (keys []byte, vIsEmpty bool, err error) {
	if c.needFetchNextPage() {
		err := c.fetchPage(CmdCursorNextKey)
		if err != nil {
			return nil, false, err
		}
		c.cacheIdx = 0
	}

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValueIsEmpty[c.cacheIdx]
	c.cacheIdx++

	return k, v, nil
}

func (c *Cursor) fetchPage(cmd Command) error {
	if c.cacheKeys == nil {
		c.cacheKeys = make([][]byte, c.batchSize)
		c.cacheValues = make([][]byte, c.batchSize)
		c.cacheValueIsEmpty = make([]bool, c.batchSize)
	}

	decoder := codecpool.Decoder(c.in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(c.out)
	defer codecpool.Return(encoder)

	if err := encoder.Encode(cmd); err != nil {
		return fmt.Errorf("could not encode command %d. %w", cmd, err)
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		return fmt.Errorf("could not encode cursorHandle. %w", err)
	}

	if err := encoder.Encode(c.batchSize); err != nil {
		return fmt.Errorf("could not encode c.batchSize. %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return fmt.Errorf("could not decode ResponseCode. %w", err)
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			return fmt.Errorf("could not decode errorMessage. %w", err)
		}
	}

	for c.cacheLastIdx = uint64(0); c.cacheLastIdx < c.batchSize; c.cacheLastIdx++ {
		select {
		default:
		case <-c.ctx.Done():
			return c.ctx.Err()
		}

		switch cmd {
		case CmdCursorFirst, CmdCursorNext:
			if err := decodeKeyValue(decoder, &c.cacheKeys[c.cacheLastIdx], &c.cacheValues[c.cacheLastIdx]); err != nil {
				return fmt.Errorf("could not decode (key, value) for cmd %d: %w", cmd, err)
			}
		case CmdCursorFirstKey, CmdCursorNextKey:
			if err := decodeKey(decoder, &c.cacheKeys[c.cacheLastIdx], &c.cacheValueIsEmpty[c.cacheLastIdx]); err != nil {
				return fmt.Errorf("could not decode (key, valueIsEmpty) for cmd %d: %w", cmd, err)
			}
		}

		if c.cacheKeys[c.cacheLastIdx] == nil {
			break
		}
	}
	return nil
}
