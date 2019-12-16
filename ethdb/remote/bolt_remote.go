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
	// CmdBeginTx : txHandle
	// request starting a new transaction (read-only). It returns transaction's handle (uint64), or 0
	// if there was an error. If 0 is returned, the corresponding
	CmdBeginTx
	// CmdEndTx (txHandle)
	// request the end of the transaction (rollback)
	CmdEndTx
	// CmdBucket (txHandle, name): bucketHandle
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
)

const DefaultCursorBatchSize uint64 = 100 * 1000
const CursorMaxBatchSize uint64 = 1 * 1000 * 1000
const ServerMaxConnections uint64 = 1024
const ClientMaxConnections uint64 = 128

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

	decoder := newDecoder(in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(out)
	defer returnEncoderToPool(encoder)
	// Server is passive - it runs a loop what reads commands (and their arguments) and attempts to respond
	var lastHandle uint64
	// Read-only transactions opened by the client
	transactions := make(map[uint64]*bolt.Tx)
	// Buckets opened by the client
	buckets := make(map[uint64]*bolt.Bucket)
	// List of buckets opened in each transaction
	bucketsByTx := make(map[uint64][]uint64)
	// Cursors opened by the client
	cursors := make(map[uint64]*bolt.Cursor)
	// List of cursors opened in each bucket
	cursorsByBucket := make(map[uint64][]uint64)
	var c Command
	for {
		if err := decoder.Decode(&c); err != nil {
			if err == io.EOF {
				// Graceful termination when the end of the input is reached
				break
			}
			log.Error("could not decode command", "error", err, "command", c)
			return err
		}
		switch c {
		case CmdVersion:
			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdVersion", "error", err)
				return err
			}
			if err := encoder.Encode(Version); err != nil {
				log.Error("could not encode response to CmdVersion", "error", err)
				return err
			}
		case CmdBeginTx:
			var txHandle uint64
			tx, err := db.Begin(false)
			if err != nil {
				encodeErr(encoder, fmt.Errorf("can't start transaction for CmdBeginTx: %w", err))
				return nil
			}

			// We do Rollback and never Commit, because the remote transactions are always read-only, and must never change
			// anything
			// nolint:errcheck
			defer func(tx *bolt.Tx) {
				tx.Rollback()
			}(tx)
			lastHandle++
			txHandle = lastHandle
			transactions[txHandle] = tx

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdBeginTx", "error", err)
				return err
			}

			if err := encoder.Encode(txHandle); err != nil {
				log.Error("could not encode txHandle in response to CmdBeginTx", "error", err)
				return err
			}
		case CmdEndTx:
			var txHandle uint64
			if err := decoder.Decode(&txHandle); err != nil {
				log.Error("could not decode txHandle for CmdEndTx", "error", err)
				return err
			}
			tx, ok := transactions[txHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("transaction not found: %d", txHandle))
				continue
			}

			// Remove all the buckets
			if bucketHandles, ok1 := bucketsByTx[txHandle]; ok1 {
				for _, bucketHandle := range bucketHandles {
					if cursorHandles, ok2 := cursorsByBucket[bucketHandle]; ok2 {
						for _, cursorHandle := range cursorHandles {
							delete(cursors, cursorHandle)
						}
						delete(cursorsByBucket, bucketHandle)
					}
					delete(buckets, bucketHandle)
				}
				delete(bucketsByTx, txHandle)
			}
			if err := tx.Rollback(); err != nil {
				log.Error("could not end transaction", "handle", txHandle, "error", err)
				return err
			}
			delete(transactions, txHandle)

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdEndTx", "error", err)
				return err
			}

		case CmdBucket:
			// Read the txHandle
			var txHandle uint64
			if err := decoder.Decode(&txHandle); err != nil {
				encodeErr(encoder, fmt.Errorf("could not decode txHandle for CmdBucket: %w", err))
				return err
			}
			// Read the name of the bucket
			var name []byte
			if err := decoder.Decode(&name); err != nil {
				log.Error("could not decode name for CmdBucket", "error", err)
				return err
			}
			var bucketHandle uint64
			tx, ok := transactions[txHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("transaction not found: %d", txHandle))
				continue
			}

			// Open the bucket
			var bucket *bolt.Bucket
			bucket = tx.Bucket(name)
			if bucket == nil {
				encodeErr(encoder, fmt.Errorf("bucket not found: %s", name))
				continue
			}

			lastHandle++
			bucketHandle = lastHandle
			buckets[bucketHandle] = bucket
			if bucketHandles, ok1 := bucketsByTx[txHandle]; ok1 {
				bucketHandles = append(bucketHandles, bucketHandle)
				bucketsByTx[txHandle] = bucketHandles
			} else {
				bucketsByTx[txHandle] = []uint64{bucketHandle}
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdBucket", "error", err)
				return err
			}

			if err := encoder.Encode(bucketHandle); err != nil {
				log.Error("could not encode bucketHandle in response to CmdBucket", "error", err)
				return err
			}
		case CmdGet:
			var bucketHandle uint64
			if err := decoder.Decode(&bucketHandle); err != nil {
				log.Error("could not decode bucketHandle for CmdGet", "error", err)
				return err
			}
			var key []byte
			if err := decoder.Decode(&key); err != nil {
				log.Error("could not decode key for CmdGet", "error", err)
				return err
			}
			var value []byte
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found: %d", bucketHandle))
				continue
			}
			value, _ = bucket.Get(key)

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdGet", "error", err)
				return err
			}

			if err := encoder.Encode(value); err != nil {
				log.Error("could not encode value in response to CmdGet", "error", err)
				return err
			}
		case CmdCursor:
			var bucketHandle uint64
			if err := decoder.Decode(&bucketHandle); err != nil {
				log.Error("could not decode bucketHandle for CmdCursor", "error", err)
				return err
			}
			var cursorHandle uint64
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found: %d", bucketHandle))
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
				log.Error("could not encode response to CmdCursor", "error", err)
				return err
			}

			if err := encoder.Encode(cursorHandle); err != nil {
				log.Error("could not cursor handle in response to CmdCursor", "error", err)
				return err
			}
		case CmdCursorSeek:
			var cursorHandle uint64
			if err := decoder.Decode(&cursorHandle); err != nil {
				log.Error("could not decode cursorHandle for CmdCursorSeek", "error", err)
				return err
			}
			var seekKey []byte
			if err := decoder.Decode(&seekKey); err != nil {
				log.Error("could not decode seekKey for CmdCursorSeek", "error", err)
				return err
			}
			var key, value []byte
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			key, value = cursor.Seek(seekKey)

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdCursorSeek", "error", err)
				return err
			}

			if err := encodeKeyValue(encoder, key, value); err != nil {
				log.Error("could not encode (key,value) in response to CmdCursorSeek", "error", err)
				return err
			}
		case CmdCursorSeekTo:
			var cursorHandle uint64
			if err := decoder.Decode(&cursorHandle); err != nil {
				log.Error("could not decode cursorHandle for CmdCursorSeekTo", "error", err)
				return err
			}
			var seekKey []byte
			if err := decoder.Decode(&seekKey); err != nil {
				log.Error("could not decode seekKey for CmdCursorSeekTo", "error", err)
				return err
			}
			var key, value []byte
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}
			key, value = cursor.SeekTo(seekKey)

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdCursorSeek", "error", err)
				return err
			}

			if err := encodeKeyValue(encoder, key, value); err != nil {
				log.Error("could not encode (key,value) in response to CmdCursorSeekTo", "error", err)
				return err
			}
		case CmdCursorNext:
			var cursorHandle uint64
			if err := decoder.Decode(&cursorHandle); err != nil {
				log.Error("could not decode cursorHandle for CmdCursorNext", "error", err)
				return err
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				log.Error("could not decode numberOfKeys for CmdCursorNext", "error", err)
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
				log.Error("could not encode response to CmdCursorNext", "error", err)
				return err
			}

			for key, value := cursor.Next(); numberOfKeys > 0; key, value = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, key, value); err != nil {
					log.Error("could not encode (key,value) in response to CmdCursorNext", "error", err)
					return err
				}

				numberOfKeys--
				if key == nil {
					break
				}
			}

		case CmdCursorFirst:
			var cursorHandle uint64
			if err := decoder.Decode(&cursorHandle); err != nil {
				log.Error("could not decode cursorHandle for CmdCursorFirst", "error", err)
				return err
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				log.Error("could not decode numberOfKeys for CmdCursorFirst", "error", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(ResponseOk); err != nil {
				log.Error("could not encode response to CmdCursorFirst", "error", err)
				return err
			}

			for key, value := cursor.First(); numberOfKeys > 0; key, value = cursor.Next() {
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, key, value); err != nil {
					log.Error("could not encode (key,value) in response to CmdCursorFirst", "error", err)
					return err
				}

				numberOfKeys--
				if key == nil {
					break
				}
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
		return fmt.Errorf("ping failed: %w", err)
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
					log.Error("ping failed", "err", err)
				}
			}
		}
	}()

	if err := db.ping(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes DB by using the closer field
func (db *DB) Close() error {
	return nil
}

// Tx mimicks the interface of bolt.Tx
type Tx struct {
	ctx      context.Context
	in       io.Reader
	out      io.Writer
	txHandle uint64
}

// View performs read-only transaction on the remote database
// NOTE: not thread-safe
func (db *DB) View(ctx context.Context, f func(tx *Tx) error) error {
	var err error
	in, out, closer, err := db.getConnection(ctx)
	if err != nil {
		return err
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
	if err := encoder.Encode(CmdBeginTx); err != nil {
		return fmt.Errorf("can't encode CmdBeginTx: %w", err)
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		return err
	}

	if responseCode != ResponseOk {
		return decodeErr(decoder, responseCode)
	}

	var txHandle uint64
	if err := decoder.Decode(&txHandle); err != nil {
		return fmt.Errorf("can't decode txHandle: %w", err)
	}
	if txHandle == 0 {
		return errors.New("got incorrect txHandle value from server")
	}
	tx := &Tx{ctx: ctx, in: in, out: out, txHandle: txHandle}
	opErr := f(tx)

	if err := encoder.Encode(CmdEndTx); err != nil {
		err = fmt.Errorf("could not encode CmdEndTx: %w", err)
		if opErr != nil {
			return fmt.Errorf("%w. %s", opErr, err)
		}
		return err
	}
	if err := encoder.Encode(txHandle); err != nil {
		err = fmt.Errorf("could not encode txHandle: %w", err)
		if opErr != nil {
			return fmt.Errorf("%w. %s", opErr, err)
		}
		return err
	}

	if err := decoder.Decode(&responseCode); err != nil {
		err = fmt.Errorf("could not decode ResponseCode for CmdEndTx: %w", err)
		if opErr != nil {
			return fmt.Errorf("%w. %s", opErr, err)
		}
		return err
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			err = fmt.Errorf("could not decode errorMessage for CmdEndTx: %w", err)
			if opErr != nil {
				return fmt.Errorf("%w. %s", opErr, err)
			}
			return err
		}
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
	ctx           context.Context
	in            io.Reader
	out           io.Writer
	cursorHandle  uint64
	errWasChecked bool
	err           error

	batchSize    uint64
	cacheKeys    [][]byte
	cacheValues  [][]byte
	cacheLastIdx uint64
	cacheIdx     uint64
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
	if err := encoder.Encode(tx.txHandle); err != nil {
		return nil, fmt.Errorf("could not encode txHandle for CmdBucket: %w", err)
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

		batchSize:   DefaultCursorBatchSize,
		cacheKeys:   make([][]byte, DefaultCursorBatchSize),
		cacheValues: make([][]byte, DefaultCursorBatchSize),
	}

	return cursor, nil
}

func (c *Cursor) Err() error {
	c.errWasChecked = true // TODO: check this flag inside .View()
	return c.err
}

func (c *Cursor) First() (key []byte, value []byte) {
	select {
	default:
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return nil, nil
	}

	err := c.fetchPage(CmdCursorFirst)
	if err != nil {
		c.err = err
		return nil, nil
	}
	c.cacheIdx = 0

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValues[c.cacheIdx]

	c.cacheIdx++

	return k, v

}

func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	select {
	default:
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return nil, nil
	}

	decoder := newDecoder(c.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(c.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdCursorSeek); err != nil {
		c.err = fmt.Errorf("could not encode CmdCursorSeek: %w", err)
		return nil, nil
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		c.err = fmt.Errorf("could not encode cursorHandle for CmdCursorSeek: %w", err)
		return nil, nil
	}
	if err := encoder.Encode(seek); err != nil {
		c.err = fmt.Errorf("could not encode key for CmdCursorSeek: %w", err)
		return nil, nil
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		c.err = fmt.Errorf("could not decode ResponseCode for CmdCursorSeek: %w", err)
		return nil, nil
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			c.err = fmt.Errorf("could not decode errorMessage for CmdCursorSeek: %w", err)
			return nil, nil
		}
	}

	if err := decoder.Decode(&key); err != nil {
		c.err = fmt.Errorf("could not decode key for CmdCursorSeek: %w", err)
		return nil, nil
	}

	if err := decoder.Decode(&value); err != nil {
		c.err = fmt.Errorf("could not decode value for CmdCursorSeek: %w", err)
		return nil, nil
	}

	return key, value
}

func (c *Cursor) SeekTo(seek []byte) (key []byte, value []byte) {
	select {
	default:
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return nil, nil
	}

	decoder := newDecoder(c.in)
	defer returnDecoderToPool(decoder)
	encoder := newEncoder(c.out)
	defer returnEncoderToPool(encoder)

	if err := encoder.Encode(CmdCursorSeekTo); err != nil {
		c.err = fmt.Errorf("could not encode CmdCursorSeekTo: %w", err)
		return nil, nil
	}
	if err := encoder.Encode(c.cursorHandle); err != nil {
		c.err = fmt.Errorf("could not encode cursorHandle for CmdCursorSeekTo: %w", err)
		return nil, nil
	}
	if err := encoder.Encode(seek); err != nil {
		c.err = fmt.Errorf("could not encode key for CmdCursorSeekTo: %w", err)
		return nil, nil
	}

	var responseCode ResponseCode
	if err := decoder.Decode(&responseCode); err != nil {
		c.err = fmt.Errorf("could not decode ResponseCode for CmdCursorSeekTo: %w", err)
		return nil, nil
	}

	if responseCode != ResponseOk {
		if err := decodeErr(decoder, responseCode); err != nil {
			c.err = fmt.Errorf("could not decode errorMessage for CmdCursorSeekTo: %w", err)
			return nil, nil
		}
	}

	if err := decodeKeyValue(decoder, &key, &value); err != nil {
		c.err = fmt.Errorf("could not decode (key, value) for CmdCursorSeekTo: %w", err)
		return nil, nil
	}

	return key, value
}

func (c *Cursor) needFetchNextPage() bool {
	res := c.cacheLastIdx == 0 || // cache is empty
		c.cacheIdx == c.cacheLastIdx // all cache read
	return res
}

func (c *Cursor) Next() (keys []byte, values []byte) {
	if c.needFetchNextPage() {
		err := c.fetchPage(CmdCursorNext)
		if err != nil {
			c.err = err
			return nil, nil
		}
		c.cacheIdx = 0
	}

	select {
	default:
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
		return nil, nil
	}

	k, v := c.cacheKeys[c.cacheIdx], c.cacheValues[c.cacheIdx]
	c.cacheIdx++

	return k, v
}

func (c *Cursor) fetchPage(cmd Command) error {
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

		if err := decodeKeyValue(decoder, &c.cacheKeys[c.cacheLastIdx], &c.cacheValues[c.cacheLastIdx]); err != nil {
			return fmt.Errorf("could not decode (key, value) for cmd %d: %w", cmd, err)
		}

		//time.Sleep(1 * time.Second)

		if c.cacheKeys[c.cacheLastIdx] == nil {
			break
		}
	}
	return nil
}
