package ethdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"
)

// generate the messages and services
//go:generate protoc --proto_path=../interfaces --go_out=. --go-grpc_out=. "remote/kv.proto" -I=. -I=./../build/include/google
//go:generate protoc --proto_path=../interfaces --go_out=. --go-grpc_out=. "remote/db.proto" -I=. -I=./../build/include/google
//go:generate protoc --proto_path=../interfaces --go_out=. --go-grpc_out=. "remote/ethbackend.proto" -I=. -I=./../build/include/google

type remoteOpts struct {
	DialAddress string
	inMemConn   *bufconn.Listener // for tests
	bucketsCfg  BucketConfigsFunc
}

type RemoteKV struct {
	opts     remoteOpts
	remoteKV remote.KVClient
	remoteDB remote.DBClient
	conn     *grpc.ClientConn
	log      log.Logger
	buckets  dbutils.BucketsCfg
}

type remoteTx struct {
	ctx                context.Context
	db                 *RemoteKV
	cursors            []*remoteCursor
	stream             remote.KV_TxClient
	streamCancelFn     context.CancelFunc
	streamingRequested bool
}

type remoteCursor struct {
	id         uint32
	ctx        context.Context
	stream     remote.KV_TxClient
	tx         *remoteTx
	bucketName string
	bucketCfg  dbutils.BucketConfigItem
}

type remoteCursorDupSort struct {
	*remoteCursor
}

func (opts remoteOpts) ReadOnly() remoteOpts {
	return opts
}

func (opts remoteOpts) Path(path string) remoteOpts {
	opts.DialAddress = path
	return opts
}

func (opts remoteOpts) WithBucketsConfig(f BucketConfigsFunc) remoteOpts {
	opts.bucketsCfg = f
	return opts
}

func (opts remoteOpts) InMem(listener *bufconn.Listener) remoteOpts {
	opts.inMemConn = listener
	return opts
}

func (opts remoteOpts) Open(certFile, keyFile, caCert string) (RwKV, error) {
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(5 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}
	if certFile == "" {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		var creds credentials.TransportCredentials
		var err error
		if caCert == "" {
			creds, err = credentials.NewClientTLSFromFile(certFile, "")

			if err != nil {
				return nil, err
			}
		} else {
			// load peer cert/key, ca cert
			peerCert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				log.Error("load peer cert/key error:%v", err)
				return nil, err
			}
			caCert, err := ioutil.ReadFile(caCert)
			if err != nil {
				log.Error("read ca cert file error:%v", err)
				return nil, err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			creds = credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{peerCert},
				ClientCAs:    caCertPool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
				//nolint:gosec
				InsecureSkipVerify: true, // This is to make it work when Common Name does not match - remove when procedure is updated for common name
			})
		}

		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}

	if opts.inMemConn != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, url string) (net.Conn, error) {
			return opts.inMemConn.Dial()
		}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, opts.DialAddress, dialOpts...)
	if err != nil {
		return nil, err
	}

	db := &RemoteKV{
		opts:     opts,
		conn:     conn,
		remoteKV: remote.NewKVClient(conn),
		remoteDB: remote.NewDBClient(conn),
		log:      log.New("remote_db", opts.DialAddress),
		buckets:  dbutils.BucketsCfg{},
	}
	customBuckets := opts.bucketsCfg(dbutils.BucketsConfigs)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	return db, nil
}

func (opts remoteOpts) MustOpen() RwKV {
	db, err := opts.Open("", "", "")
	if err != nil {
		panic(err)
	}
	return db
}

func NewRemote() remoteOpts {
	return remoteOpts{bucketsCfg: DefaultBucketConfigs}
}

func (db *RemoteKV) AllBuckets() dbutils.BucketsCfg {
	return db.buckets
}

func (db *RemoteKV) GrpcConn() *grpc.ClientConn {
	return db.conn
}

// Close
// All transactions must be closed before closing the database.
func (db *RemoteKV) Close() {
	if db.conn != nil {
		if err := db.conn.Close(); err != nil {
			db.log.Warn("failed to close remote DB", "err", err)
		} else {
			db.log.Info("remote database closed")
		}
		db.conn = nil
	}
}

func (db *RemoteKV) DiskSize(ctx context.Context) (uint64, error) {
	sizeReply, err := db.remoteDB.Size(ctx, &remote.SizeRequest{})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (db *RemoteKV) CollectMetrics() {}

func (db *RemoteKV) Begin(ctx context.Context) (Tx, error) {
	streamCtx, streamCancelFn := context.WithCancel(ctx) // We create child context for the stream so we can cancel it to prevent leak
	stream, err := db.remoteKV.Tx(streamCtx)
	if err != nil {
		streamCancelFn()
		return nil, err
	}
	return &remoteTx{ctx: ctx, db: db, stream: stream, streamCancelFn: streamCancelFn}, nil
}

func (db *RemoteKV) BeginRw(ctx context.Context) (RwTx, error) {
	return nil, fmt.Errorf("remote db provider doesn't support .BeginRw method")
}

func (db *RemoteKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *RemoteKV) Update(ctx context.Context, f func(tx RwTx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) Comparator(bucket string) dbutils.CmpFunc { panic("not implemented yet") }
func (tx *remoteTx) CHandle() unsafe.Pointer                  { panic("not implemented yet") }

func (tx *remoteTx) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	panic("not implemented yet")
}
func (tx *remoteTx) ReadSequence(bucket string) (uint64, error) {
	panic("not implemented yet")
}

func (tx *remoteTx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Rollback() {
	for _, c := range tx.cursors {
		c.Close()
	}
	tx.closeGrpcStream()
}

func (c *remoteCursor) Prefix(v []byte) Cursor {
	return c
}

func (c *remoteCursor) Prefetch(v uint) Cursor {
	return c
}

func (tx *remoteTx) BucketSize(name string) (uint64, error) {
	sizeReply, err := tx.db.remoteDB.BucketSize(tx.ctx, &remote.BucketSizeRequest{BucketName: name})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (tx *remoteTx) GetOne(bucket string, key []byte) (val []byte, err error) {
	c, _ := tx.Cursor(bucket)
	defer c.Close()
	_, val, err = c.SeekExact(key)
	return val, err
}

func (tx *remoteTx) HasOne(bucket string, key []byte) (bool, error) {
	c, _ := tx.Cursor(bucket)
	defer c.Close()
	k, _, err := c.Seek(key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, k), nil
}

func (c *remoteCursor) SeekExact(key []byte) (k, val []byte, err error) {
	return c.seekExact(key)
}

func (c *remoteCursor) Prev() ([]byte, []byte, error) {
	return c.prev()
}

func (tx *remoteTx) Cursor(bucket string) (Cursor, error) {
	b := tx.db.buckets[bucket]
	c := &remoteCursor{tx: tx, ctx: tx.ctx, bucketName: bucket, bucketCfg: b, stream: tx.stream}
	tx.cursors = append(tx.cursors, c)
	if err := c.stream.Send(&remote.Cursor{Op: remote.Op_OPEN, BucketName: c.bucketName}); err != nil {
		return nil, err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	c.id = msg.CursorID
	return c, nil
}

func (c *remoteCursor) Put(key []byte, value []byte) error            { panic("not supported") }
func (c *remoteCursor) PutNoOverwrite(key []byte, value []byte) error { panic("not supported") }
func (c *remoteCursor) Append(key []byte, value []byte) error         { panic("not supported") }
func (c *remoteCursor) Delete(k, v []byte) error                      { panic("not supported") }
func (c *remoteCursor) DeleteCurrent() error                          { panic("not supported") }
func (c *remoteCursor) Count() (uint64, error)                        { panic("not supported") }

func (c *remoteCursor) first() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remoteCursor) next() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) nextDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) nextNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prev() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prevDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) prevNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) last() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) setRange(k []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK, K: k}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) seekExact(k []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_EXACT, K: k}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) getBothRange(k, v []byte) ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH, K: k, V: v}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) seekBothExact(k, v []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH_EXACT, K: k, V: v}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remoteCursor) firstDup() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST_DUP}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) lastDup() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST_DUP}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remoteCursor) getCurrent() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CURRENT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remoteCursor) Current() ([]byte, []byte, error) {
	return c.getCurrent()
}

// Seek - doesn't start streaming (because much of code does only several .Seek calls without reading sequence of data)
// .Next() - does request streaming (if configured by user)
func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	return c.setRange(seek)
}

func (c *remoteCursor) First() ([]byte, []byte, error) {
	return c.first()
}

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remoteCursor) Next() ([]byte, []byte, error) {
	return c.next()
}

func (c *remoteCursor) Last() ([]byte, []byte, error) {
	return c.last()
}

func (tx *remoteTx) closeGrpcStream() {
	if tx.stream == nil {
		return
	}
	defer tx.streamCancelFn() // hard cancel stream if graceful wasn't successful

	if tx.streamingRequested {
		// if streaming is in progress, can't use `CloseSend` - because
		// server will not read it right not - it busy with streaming data
		// TODO: set flag 'tx.streamingRequested' to false when got terminator from server (nil key or os.EOF)
		tx.streamCancelFn()
	} else {
		// try graceful close stream
		err := tx.stream.CloseSend()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				log.Warn("couldn't send msg CloseSend to server", "err", err)
			}
		} else {
			_, err = tx.stream.Recv()
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				log.Warn("received unexpected error from server after CloseSend", "err", err)
			}
		}
	}
	tx.stream = nil
	tx.streamingRequested = false
}

func (c *remoteCursor) Close() {
	if c.stream == nil {
		return
	}
	st := c.stream
	c.stream = nil
	if err := st.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CLOSE}); err == nil {
		_, _ = st.Recv()
	}
}

func (tx *remoteTx) CursorDupSort(bucket string) (CursorDupSort, error) {
	c, err := tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	return &remoteCursorDupSort{remoteCursor: c.(*remoteCursor)}, nil
}

func (c *remoteCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	return c.seekBothExact(key, value)
}

func (c *remoteCursorDupSort) SeekBothRange(key, value []byte) ([]byte, error) {
	return c.getBothRange(key, value)
}

func (c *remoteCursorDupSort) DeleteExact(k1, k2 []byte) error      { panic("not supported") }
func (c *remoteCursorDupSort) AppendDup(k []byte, v []byte) error   { panic("not supported") }
func (c *remoteCursorDupSort) PutNoDupData(key, value []byte) error { panic("not supported") }
func (c *remoteCursorDupSort) DeleteCurrentDuplicates() error       { panic("not supported") }
func (c *remoteCursorDupSort) CountDuplicates() (uint64, error)     { panic("not supported") }

func (c *remoteCursorDupSort) FirstDup() ([]byte, error) {
	return c.firstDup()
}
func (c *remoteCursorDupSort) NextDup() ([]byte, []byte, error) {
	return c.nextDup()
}
func (c *remoteCursorDupSort) NextNoDup() ([]byte, []byte, error) {
	return c.nextNoDup()
}
func (c *remoteCursorDupSort) PrevDup() ([]byte, []byte, error) {
	return c.prevDup()
}
func (c *remoteCursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	return c.prevNoDup()
}
func (c *remoteCursorDupSort) LastDup() ([]byte, error) {
	return c.lastDup()
}
