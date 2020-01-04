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
	"os"
	"strings"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/ethdb"
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
const ClientMaxConnections uint64 = 32

// tracing enable by evn GODEBUG=remotedb.debug=1
var tracing bool

func init() {
	for _, f := range strings.Split(os.Getenv("GODEBUG"), ",") {
		if f == "remotedb.debug=1" {
			tracing = true
		}
	}
}

// Pool of decoders
var decoderPool = make(chan *codec.Decoder, 128)

func newDecoder(r io.Reader) *codec.Decoder {
	var d *codec.Decoder
	select {
	case d = <-decoderPool:
		d.Reset(r)
	default:
		{
			var handle codec.CborHandle
			handle.ReaderBufferSize = 64 * 1024
			d = codec.NewDecoder(r, &handle)
		}
	}
	return d
}

func returnDecoderToPool(d *codec.Decoder) {
	select {
	case decoderPool <- d:
	default:
		log.Warn("Allowing decoder to be garbage collected, pool is full")
	}
}

// Pool of encoders
var encoderPool = make(chan *codec.Encoder, 128)

func newEncoder(w io.Writer) *codec.Encoder {
	var e *codec.Encoder
	select {
	case e = <-encoderPool:
		e.Reset(w)
	default:
		{
			var handle codec.CborHandle
			handle.WriterBufferSize = 64 * 1024
			e = codec.NewEncoder(w, &handle)
		}
	}
	return e
}

func returnEncoderToPool(e *codec.Encoder) {
	select {
	case encoderPool <- e:
	default:
		log.Warn("Allowing encoder to be garbage collected, pool is full")
	}
}

func encodeKeyValue(encoder *codec.Encoder, key []byte, value []byte) error {
	if err := encoder.Encode(key); err != nil {
		return err
	}
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return nil
}

func decodeKeyValue(decoder *codec.Decoder, key *[]byte, value *[]byte) error {
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
		log.Error("could not encode ResponseErr", "error", err)
		return
	}
	if err := encoder.Encode(mainError.Error()); err != nil {
		log.Error("could not encode errCode", "error", err)
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
			log.Error("Could not close connection", "error", err1)
		}
	}()

	var err error

	decoder := newDecoder(in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(out)
	defer returnEncoderToPool(encoder)
	// Server is passive - it runs a loop what reads commands (and their arguments) and attempts to respond
	var lastHandle uint64
	// Read-only transactions opened by the client
	var tx *bolt.Tx

	// We do Rollback and never Commit, because the remote transactions are always read-only, and must never change
	// anything
	defer func() {
		if tx != nil {
			// nolint:errcheck
			tx.Rollback()
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

			if err := encoder.Encode(v); err != nil {
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
			if err := encodeKeyValue(encoder, k, v); err != nil {
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

			if err := encodeKeyValue(encoder, k, v); err != nil {
				return fmt.Errorf("could not encode (key,value) in response to CmdCursorSeekTo: %w", err)
			}
		case CmdCursorNext:
			var k, v []byte

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

			for k, v = cursor.Next(); numberOfKeys > 0; k, v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) in response to CmdCursorNext: %w", err)
				}

				numberOfKeys--
				if k == nil {
					break
				}
			}

		case CmdCursorFirst:
			var k, v []byte

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

			for k, v = cursor.First(); numberOfKeys > 0; k, v = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) for CmdCursorFirst: %w", err)
				}

				numberOfKeys--
				if k == nil {
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
			log.Error("unknown", "command", c)
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
		log.Error("Could not create listener", "address", address, "error", err)
		return
	}
	defer func() {
		if err = ln.Close(); err != nil {
			log.Error("Could not close listener", "error", err)
		}
	}()
	log.Info("Remote DB interface listening on", "address", address)

	ch := make(chan bool, ServerMaxConnections)
	defer close(ch)

	if tracing {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					log.Info("remote db: connections", "amount", len(ch))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for {
		conn, err1 := ln.Accept()
		if err1 != nil {
			log.Error("Could not accept connection", "err", err1)
			continue
		}

		go func() {
			ch <- true
			defer func() {
				<-ch
			}()

			//nolint:errcheck
			err := Server(ctx, db, conn, conn, conn)
			if err != nil {
				log.Warn("remote db server error", "err", err)
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
	reconnect      chan struct{}
}

type DialFunc func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error)

// Pool of connections to server
func (db *DB) getConnection(ctx context.Context) (io.Reader, io.Writer, io.Closer, error) {
	var in io.Reader
	var out io.Writer
	var closer io.Closer
	var err error

	select {
	case conn := <-db.connectionPool:
		in, out, closer = conn.in, conn.out, conn.closer
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	}

	return in, out, closer, err
}

func (db *DB) returnConn(ctx context.Context, in io.Reader, out io.Writer, closer io.Closer) {
	select {
	case db.connectionPool <- &conn{in: in, out: out, closer: closer}:
	case <-ctx.Done():
	}
}

func (db *DB) ping(ctx context.Context) error {
	var err error
	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return fmt.Errorf("remote db: ping failed: %w", err)
	}
	defer func() {
		if err != nil { // reconnect on error
			db.reconnect <- struct{}{}
			closer.Close()
			return
		}
		db.returnConn(ctx, in, out, closer)
	}()

	decoder := newDecoder(in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(out)
	defer returnEncoderToPool(encoder)
	// Check version
	if err := encoder.Encode(CmdVersion); err != nil {
		return err
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return err
	}

	if responseCode != ResponseOk {
		return decodeErr(decoder, responseCode)
	}

	var v uint64
	if err := decoder.Decode(&v); err != nil {
		return err
	}
	if v != Version {
		return fmt.Errorf("returned version %d, expected %d", v, Version)
	}

	return nil
}

// NewDB creates a new instance of DB
func NewDB(ctx context.Context, dialFunc DialFunc) (*DB, error) {
	db := &DB{
		dialFunc:       dialFunc,
		connectionPool: make(chan *conn, ClientMaxConnections),
		reconnect:      make(chan struct{}, ClientMaxConnections),
	}

	connectionCtx, connectionCtxCancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		connectionCtxCancel()
	}()

	for i := 0; i < cap(db.connectionPool); i++ {
		in, out, closer, err := db.dialFunc(connectionCtx)
		if err != nil {
			return nil, err
		}

		db.connectionPool <- &conn{in, out, closer}
	}

	go func() { // reconnect, regular ping
		pingTicker := time.NewTicker(10 * time.Second)
		defer pingTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-db.reconnect:
				// TODO: what to do if can't reconnect?
				newIn, newOut, newCloser, dialErr := db.dialFunc(ctx)
				if dialErr != nil {
					log.Error("could not create new connection", "error", dialErr)
					return
				}
				db.returnConn(ctx, newIn, newOut, newCloser)
			case <-pingTicker.C:
				if err := db.ping(ctx); err != nil {
					log.Error("remote db: ping failed", "err", err)
				}
			}
		}
	}()

	if err := db.ping(ctx); err != nil {
		return nil, err
	}

	if tracing {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					log.Info("remote db: connections in pool", "amount", len(db.connectionPool))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return db, nil
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
		err = fmt.Errorf("could not encode CmdEndTx: %w", err)
		return err
	}

	if err := decoder.Decode(&responseCode); err != nil {
		err = fmt.Errorf("could not decode ResponseCode for CmdEndTx: %w", err)
		return err
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			err = fmt.Errorf("could not decode errorMessage for CmdEndTx: %w", err)
			return err
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
			db.reconnect <- struct{}{}
			closer.Close()
			return
		}
		db.returnConn(ctx, in, out, closer)
	}()

	decoder := newDecoder(in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(out)
	defer returnEncoderToPool(encoder)

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
func (db *DB) View(ctx context.Context, f func(tx *Tx) error) error {
	var err error
	var opErr error
	var endTxErr error

	var responseCode ResponseCode

	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil || endTxErr != nil || opErr != nil {
			db.reconnect <- struct{}{}
			closer.Close()
			return
		}
		db.returnConn(ctx, in, out, closer)
	}()

	decoder := newDecoder(in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(out)
	defer returnEncoderToPool(encoder)

	err = encoder.Encode(CmdBeginTx)
	if err != nil {
		return fmt.Errorf("can't encode CmdBeginTx: %w", err)
	}
	err = decoder.Decode(&responseCode)
	if err != nil {
		return err
	}

	if responseCode != ResponseOk {
		err = decodeErr(decoder, responseCode)
		return err
	}

	tx := &Tx{ctx: ctx, in: in, out: out}
	opErr = f(tx)

	endTxErr = db.endTx(ctx, encoder, decoder)
	if endTxErr != nil {
		log.Error("remote db: could not finish tx", "err", err)
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

	decoder := newDecoder(tx.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(tx.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdBucket); err != nil {
		return nil, fmt.Errorf("could not encode CmdBucket: %w", err)
	}
	if err := encoder.Encode(name); err != nil {
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

	decoder := newDecoder(b.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(b.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdGet); err != nil {
		return nil, fmt.Errorf("could not encode CmdGet: %w", err)
	}
	if err := encoder.Encode(b.bucketHandle); err != nil {
		return nil, fmt.Errorf("could not encode bucketHandle for CmdGet: %w", err)
	}
	if err := encoder.Encode(key); err != nil {
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

	decoder := newDecoder(b.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(b.out)
	defer returnEncoderToPool(encoder)

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

	decoder := newDecoder(c.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(c.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdCursorSeek); err != nil {
		return nil, nil, fmt.Errorf("could not encode CmdCursorSeek: %w", err)
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		return nil, nil, fmt.Errorf("could not encode cursorHandle for CmdCursorSeek: %w", err)
	}
	if err := encoder.Encode(seek); err != nil {
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

	decoder := newDecoder(c.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(c.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdCursorSeekTo); err != nil {
		return nil, nil, fmt.Errorf("could not encode CmdCursorSeekTo: %w", err)
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		return nil, nil, fmt.Errorf("could not encode cursorHandle for CmdCursorSeekTo: %w", err)
	}
	if err := encoder.Encode(seek); err != nil {
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

	decoder := newDecoder(c.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(c.out)
	defer returnEncoderToPool(encoder)

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
