package remotedbserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

// Version is the current version of the remote db protocol. If the protocol changes in a non backwards compatible way,
// this constant needs to be increased
const Version uint64 = 2

// Server is to be called as a go-routine, one per every client connection.
// It runs while the connection is active and keep the entire connection's context
// in the local variables
// For tests, bytes.Buffer can be used for both `in` and `out`
func Server(ctx context.Context, db ethdb.KV, in io.Reader, out io.Writer, closer io.Closer) error {
	defer func() {
		if closer != nil {
			if err1 := closer.Close(); err1 != nil {
				logger.Error("Could not close connection", "err", err1)
			}
		}
	}()

	decoder := codecpool.Decoder(in)
	defer codecpool.Return(decoder)
	encoder := codecpool.Encoder(out)
	defer codecpool.Return(encoder)

	// Server is passive - it runs a loop what reads remote.Commands (and their arguments) and attempts to respond
	var lastHandle uint64
	// Read-only transactions opened by the client
	var tx ethdb.Tx

	// We do Rollback and never Commit, because the remote transactions are always read-only, and must never change
	// anything
	defer func() {
		if tx != nil {
			tx.Rollback()
			tx = nil
		}
	}()

	// Buckets opened by the client
	buckets := make(map[uint64]ethdb.Bucket, 2)
	// List of buckets opened in each transaction
	//bucketsByTx := make(map[uint64][]uint64, 10)
	// Cursors opened by the client
	cursors := make(map[uint64]ethdb.Cursor, 2)
	// List of cursors opened in each bucket
	cursorsByBucket := make(map[uint64][]uint64, 2)

	var c remote.Command
	var bucketHandle uint64
	var cursorHandle uint64
	var cursorPrefix []byte

	var name []byte
	var seekKey []byte

	for {
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Make sure we are not blocking the resizing of the memory map
		if tx != nil {
			type Yieldable interface {
				Yield()
			}
			if casted, ok := tx.(Yieldable); ok {
				casted.Yield()
			}
		}

		if err := decoder.Decode(&c); err != nil {
			if err == io.EOF {
				// Graceful termination when the end of the input is reached
				break
			}
			return fmt.Errorf("could not decode remote.Command: %w", err)
		}
		switch c {
		case remote.CmdVersion:
			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code to remote.CmdVersion: %w", err)
			}
			if err := encoder.Encode(Version); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdVersion: %w", err)
			}
		case remote.CmdBeginTx:
			var err error
			tx, err = db.Begin(ctx, nil, false)
			if err != nil {
				err2 := fmt.Errorf("could not start transaction for remote.CmdBeginTx: %w", err)
				encodeErr(encoder, err2)
				return err2
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdBeginTx: %w", err)
			}
		case remote.CmdEndTx:
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
				tx.Rollback()
				tx = nil
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdEndTx: %w", err)
			}
		case remote.CmdBucket:
			// Read the name of the bucket
			if err := decoder.Decode(&name); err != nil {
				return fmt.Errorf("could not decode name for remote.CmdBucket: %w", err)
			}

			// Open the bucket
			if tx == nil {
				err := fmt.Errorf("send remote.CmdBucket before remote.CmdBeginTx")
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
			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdBucket: %w", err)
			}

			if err := encoder.Encode(lastHandle); err != nil {
				return fmt.Errorf("could not encode bucketHandle in response to remote.CmdBucket: %w", err)
			}

		case remote.CmdGet:
			var k []byte
			if err := decoder.Decode(&bucketHandle); err != nil {
				return fmt.Errorf("could not decode bucketHandle for remote.CmdGet: %w", err)
			}
			if err := decoder.Decode(&k); err != nil {
				return fmt.Errorf("could not decode key for remote.CmdGet: %w", err)
			}
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found for remote.CmdGet: %d", bucketHandle))
				continue
			}
			v, _ := bucket.Get(k)

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				err = fmt.Errorf("could not encode response code for remote.CmdGet: %w", err)
				return err
			}

			if err := encoder.Encode(&v); err != nil {
				return fmt.Errorf("could not encode value in response for remote.CmdGet: %w", err)
			}

		case remote.CmdCursor:
			if err := decoder.Decode(&bucketHandle); err != nil {
				return fmt.Errorf("could not decode bucketHandle for remote.CmdCursor: %w", err)
			}
			if err := decoder.Decode(&cursorPrefix); err != nil {
				return fmt.Errorf("could not decode prefix for remote.CmdCursor: %w", err)
			}
			bucket, ok := buckets[bucketHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("bucket not found for remote.CmdCursor: %d", bucketHandle))
				continue
			}

			cursor := bucket.Cursor().Prefix(cursorPrefix)
			lastHandle++
			cursorHandle = lastHandle
			cursors[cursorHandle] = cursor
			if cursorHandles, ok1 := cursorsByBucket[bucketHandle]; ok1 {
				cursorHandles = append(cursorHandles, cursorHandle)
				cursorsByBucket[bucketHandle] = cursorHandles
			} else {
				cursorsByBucket[bucketHandle] = []uint64{cursorHandle}
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response for remote.CmdCursor: %w", err)
			}

			if err := encoder.Encode(cursorHandle); err != nil {
				return fmt.Errorf("could not cursor handle in response to remote.CmdCursor: %w", err)
			}
		case remote.CmdCursorSeek:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not encode (key,value) for remote.CmdCursorSeek: %w", err)
			}
			if err := decoder.Decode(&seekKey); err != nil {
				return fmt.Errorf("could not encode (key,value) for remote.CmdCursorSeek: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}
			k, v, err := cursor.Seek(seekKey)
			if err != nil {
				return fmt.Errorf("in CmdCursorSeek: %w", err)
			}
			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode (key,value) for remote.CmdCursorSeek: %w", err)
			}
			if err := encodeKeyValue(encoder, k, v); err != nil {
				return fmt.Errorf("could not encode (key,value) for remote.CmdCursorSeek: %w", err)
			}
		case remote.CmdCursorSeekTo:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode seekKey for remote.CmdCursorSeekTo: %w", err)
			}
			if err := decoder.Decode(&seekKey); err != nil {
				return fmt.Errorf("could not decode seekKey for remote.CmdCursorSeekTo: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			k, v, err := cursor.Seek(seekKey)
			if err != nil {
				return fmt.Errorf("in CmdCursorSeekTo: %w", err)
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdCursorSeek: %w", err)
			}
			if err := encodeKeyValue(encoder, k, v); err != nil {
				return fmt.Errorf("could not encode (key,value) in response to remote.CmdCursorSeekTo: %w", err)
			}
		case remote.CmdCursorNext:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for remote.CmdCursorNext: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for remote.CmdCursorNext: %w", err)
			}

			if numberOfKeys > remote.CursorMaxBatchSize {
				encodeErr(encoder, fmt.Errorf("requested numberOfKeys is too large: %d", numberOfKeys))
				continue
			}

			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdCursorNext: %w", err)
			}

			for k, v, err := cursor.Next(); ; k, v, err = cursor.Next() {
				if err != nil {
					return fmt.Errorf("in CmdCursorNext: %w", err)
				}

				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) in response to remote.CmdCursorNext: %w", err)
				}

				numberOfKeys--
				if numberOfKeys == 0 {
					break
				}
				if k == nil {
					break
				}
			}

		case remote.CmdCursorFirst:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for remote.CmdCursorFirst: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for remote.CmdCursorFirst: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code for remote.CmdCursorFirst: %w", err)
			}

			for k, v, err := cursor.First(); ; k, v, err = cursor.Next() {
				if err != nil {
					return fmt.Errorf("in CmdCursorFirst: %w", err)
				}
				if err := encodeKeyValue(encoder, k, v); err != nil {
					return fmt.Errorf("could not encode (key,value) for remote.CmdCursorFirst: %w", err)
				}

				numberOfKeys--
				if numberOfKeys == 0 {
					break
				}
				if k == nil {
					break
				}
			}
		case remote.CmdCursorNextKey:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for remote.CmdCursorNextKey: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for remote.CmdCursorNextKey: %w", err)
			}

			if numberOfKeys > remote.CursorMaxBatchSize {
				encodeErr(encoder, fmt.Errorf("requested numberOfKeys is too large: %d", numberOfKeys))
				continue
			}

			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response to remote.CmdCursorNextKey: %w", err)
			}

			for k, v, err := cursor.Next(); ; k, v, err = cursor.Next() {
				if err != nil {
					return fmt.Errorf("in CmdCursorNextKey: %w", err)
				}

				if err := encodeKey(encoder, k, uint32(len(v))); err != nil {
					return fmt.Errorf("could not encode (key,vSize) in response to remote.CmdCursorNextKey: %w", err)
				}

				numberOfKeys--
				if numberOfKeys == 0 {
					break
				}
				if k == nil {
					break
				}
			}
		case remote.CmdCursorFirstKey:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for remote.CmdCursorFirstKey: %w", err)
			}
			var numberOfKeys uint64
			if err := decoder.Decode(&numberOfKeys); err != nil {
				return fmt.Errorf("could not decode numberOfKeys for remote.CmdCursorFirstKey: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}

			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode response code for remote.CmdCursorFirstKey: %w", err)
			}

			for k, v, err := cursor.First(); ; k, v, err = cursor.Next() {
				if err != nil {
					return fmt.Errorf("in CmdCursorFirstKey: %w", err)
				}

				if err := encodeKey(encoder, k, uint32(len(v))); err != nil {
					return fmt.Errorf("could not encode (key,vSize) for remote.CmdCursorFirstKey: %w", err)
				}

				numberOfKeys--
				if numberOfKeys == 0 {
					break
				}
				if k == nil {
					break
				}
			}
		case remote.CmdCursorSeekKey:
			if err := decoder.Decode(&cursorHandle); err != nil {
				return fmt.Errorf("could not decode cursorHandle for CmdCursorSeekKey: %w", err)
			}
			if err := decoder.Decode(&seekKey); err != nil {
				return fmt.Errorf("could not decode (key,vSize) for CmdCursorSeekKey: %w", err)
			}
			cursor, ok := cursors[cursorHandle]
			if !ok {
				encodeErr(encoder, fmt.Errorf("cursor not found: %d", cursorHandle))
				continue
			}
			k, v, err := cursor.Seek(seekKey)
			if err != nil {
				return fmt.Errorf("in CmdCursorSeek: %w", err)
			}
			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode (key,vSize) for CmdCursorSeekKey: %w", err)
			}
			if err := encodeKey(encoder, k, uint32(len(v))); err != nil {
				return fmt.Errorf("could not encode (key,vSize) for CmdCursorSeekKey: %w", err)
			}
		case remote.CmdDBDiskSize:
			size, err := db.(ethdb.HasStats).DiskSize(ctx)
			if err != nil {
				return fmt.Errorf("in CmdDBDiskSize: %w", err)
			}
			if err := encoder.Encode(remote.ResponseOk); err != nil {
				return fmt.Errorf("could not encode responseCode for CmdCursorSeekKey: %w", err)
			}
			if err := encoder.Encode(size); err != nil {
				return fmt.Errorf("could not encode remote.CmdDBDiskSize: %w", err)
			}
		default:
			logger.Error("unknown", "remote.Command", c)
			return fmt.Errorf("unknown remote.Command %d", c)
		}
	}

	return nil
}

const ServerMaxConnections uint64 = 2048

var logger = log.New("database", "remote")

func encodeKeyValue(encoder *codec.Encoder, key []byte, value []byte) error {
	if err := encoder.Encode(&key); err != nil {
		return err
	}
	if err := encoder.Encode(&value); err != nil {
		return err
	}
	return nil
}

func encodeKey(encoder *codec.Encoder, key []byte, valueSize uint32) error {
	if err := encoder.Encode(&key); err != nil {
		return err
	}
	if err := encoder.Encode(&valueSize); err != nil {
		return err
	}
	return nil
}

func encodeErr(encoder *codec.Encoder, mainError error) {
	if err := encoder.Encode(remote.ResponseErr); err != nil {
		logger.Error("could not encode remote.ResponseErr", "err", err)
		return
	}
	if err := encoder.Encode(mainError.Error()); err != nil {
		logger.Error("could not encode errCode", "err", err)
		return
	}
}

var netAddr string
var stopNetInterface context.CancelFunc

func StartDeprecated(db ethdb.KV, addr string) {
	if stopNetInterface != nil {
		stopNetInterface()
	}

	// TODO: implement node.Service, then Stop() will called on SIGINT | SIGTERM and we can call cancel() there
	tcpCtx, cancel := context.WithCancel(context.Background())
	if addr != "" {
		netAddr = addr
	}
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
			cancel()
		case <-tcpCtx.Done():
		}
	}()

	var lc net.ListenConfig
	ln, err := lc.Listen(tcpCtx, "tcp", netAddr)
	if err != nil {
		logger.Error("Could not create listener", "address", netAddr, "err", err)
		return
	}
	stopNetInterface = func() {
		cancel()
		ln.Close()
	}

	logger.Info("Listening on", "address", netAddr)
	go Listen(tcpCtx, ln, db)
}

// Listener starts listener that for each incoming connection
// spawn a go-routine invoking Server
func Listen(ctx context.Context, ln net.Listener, db ethdb.KV) {
	defer func() {
		if err := ln.Close(); err != nil {
			logger.Error("Could not close listener", "err", err)
		}
	}()

	ch := make(chan bool, ServerMaxConnections)
	defer close(ch)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Debug("connections", "amount", len(ch))
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		conn, err1 := ln.Accept()
		if err1 != nil {
			if netErr, ok := err1.(*net.OpError); ok && !netErr.Temporary() {
				return
			}
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
