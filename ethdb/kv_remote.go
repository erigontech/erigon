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

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/ledgerwatch/turbo-geth/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"
)

// generate the messages
//go:generate protoc --go_out=. "./remote/kv.proto" -I=. -I=./../build/include/google
//go:generate protoc --go_out=. "./remote/db.proto" -I=. -I=./../build/include/google
//go:generate protoc --go_out=. "./remote/ethbackend.proto" -I=. -I=./../build/include/google

// generate the services
//go:generate protoc --go-grpc_out=. "./remote/kv.proto" -I=. -I=./../build/include/google
//go:generate protoc --go-grpc_out=. "./remote/db.proto" -I=. -I=./../build/include/google
//go:generate protoc --go-grpc_out=. "./remote/ethbackend.proto" -I=. -I=./../build/include/google

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
	ctx     context.Context
	db      *RemoteKV
	cursors []*remoteCursor
}

type remoteCursor struct {
	initialized        bool
	streamingRequested bool
	prefetch           uint32
	ctx                context.Context
	prefix             []byte
	stream             remote.KV_SeekClient
	streamCancelFn     context.CancelFunc // this function needs to be called to close the stream
	tx                 *remoteTx
	bucketName         string
	bucketCfg          dbutils.BucketConfigItem
}

type remoteCursorDupSort struct {
	*remoteCursor
}

type RemoteBackend struct {
	opts             remoteOpts
	remoteEthBackend remote.ETHBACKENDClient
	conn             *grpc.ClientConn
	log              log.Logger
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

func (opts remoteOpts) Open(certFile, keyFile, caCert string) (KV, Backend, error) {
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(5 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Timeout: 10 * time.Minute,
		}),
	}
	if certFile == "" {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		var creds credentials.TransportCredentials
		var err error
		if caCert == "" {
			creds, err = credentials.NewClientTLSFromFile(certFile, "")

			if err != nil {
				return nil, nil, err
			}
		} else {
			// load peer cert/key, ca cert
			peerCert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				log.Error("load peer cert/key error:%v", err)
				return nil, nil, err
			}
			caCert, err := ioutil.ReadFile(caCert)
			if err != nil {
				log.Error("read ca cert file error:%v", err)
				return nil, nil, err
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
		return nil, nil, err
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

	eth := &RemoteBackend{
		opts:             opts,
		remoteEthBackend: remote.NewETHBACKENDClient(conn),
		conn:             conn,
		log:              log.New("remote_db", opts.DialAddress),
	}

	return db, eth, nil
}

func (opts remoteOpts) MustOpen() (KV, Backend) {
	db, txPool, err := opts.Open("", "", "")
	if err != nil {
		panic(err)
	}
	return db, txPool
}

func NewRemote() remoteOpts {
	return remoteOpts{bucketsCfg: DefaultBucketConfigs}
}

func (db *RemoteKV) AllBuckets() dbutils.BucketsCfg {
	return db.buckets
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

func (db *RemoteKV) Begin(ctx context.Context, parent Tx, writable bool) (Tx, error) {
	return &remoteTx{ctx: ctx, db: db}, nil
}

func (db *RemoteKV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	tx, err := db.Begin(ctx, nil, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *RemoteKV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remoteTx) Comparator(bucket string) dbutils.CmpFunc { panic("not implemented yet") }
func (tx *remoteTx) Cmp(bucket string, a, b []byte) int       { panic("not implemented yet") }
func (tx *remoteTx) DCmp(bucket string, a, b []byte) int      { panic("not implemented yet") }

func (tx *remoteTx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remoteTx) Rollback() {
	// signal server for graceful shutdown
	// after signaling need wait for .Recv() or cancel context to ensure that resources are free
	for _, c := range tx.cursors {
		c.Close()
	}
}

func (c *remoteCursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *remoteCursor) Prefetch(v uint) Cursor {
	c.prefetch = uint32(v)
	return c
}

func (tx *remoteTx) BucketSize(name string) (uint64, error) {
	sizeReply, err := tx.db.remoteDB.BucketSize(tx.ctx, &remote.BucketSizeRequest{BucketName: name})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (tx *remoteTx) Get(bucket string, key []byte) (val []byte, err error) {
	c := tx.Cursor(bucket)
	defer c.Close()
	return c.SeekExact(key)
}

func (c *remoteCursor) SeekExact(key []byte) (val []byte, err error) {
	k, v, err := c.Seek(key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, k) {
		return nil, nil
	}
	return v, nil
}

func (c *remoteCursor) Prev() ([]byte, []byte, error) {
	panic("not implemented")
}

func (tx *remoteTx) Cursor(bucket string) Cursor {
	b := tx.db.buckets[bucket]
	c := &remoteCursor{tx: tx, ctx: tx.ctx, bucketName: bucket, bucketCfg: b}
	tx.cursors = append(tx.cursors, c)
	return c
}

//func (c *remoteCursor) initCursor() error {
//	return nil
//}

func (c *remoteCursor) Current() ([]byte, []byte, error)              { panic("not supported") }
func (c *remoteCursor) Put(key []byte, value []byte) error            { panic("not supported") }
func (c *remoteCursor) PutNoOverwrite(key []byte, value []byte) error { panic("not supported") }
func (c *remoteCursor) PutCurrent(key, value []byte) error            { panic("not supported") }
func (c *remoteCursor) Append(key []byte, value []byte) error         { panic("not supported") }
func (c *remoteCursor) Delete(key []byte) error                       { panic("not supported") }
func (c *remoteCursor) DeleteCurrent() error                          { panic("not supported") }
func (c *remoteCursor) Count() (uint64, error)                        { panic("not supported") }
func (c *remoteCursor) Reserve(k []byte, n int) ([]byte, error)       { panic("not supported") }

func (c *remoteCursor) First() ([]byte, []byte, error) {
	return c.Seek(c.prefix)
}

// Seek - doesn't start streaming (because much of code does only several .Seek calls without reading sequence of data)
// .Next() - does request streaming (if configured by user)
func (c *remoteCursor) Seek(seek []byte) ([]byte, []byte, error) {
	c.closeGrpcStream()
	c.initialized = true

	var err error
	if c.stream == nil {
		var streamCtx context.Context
		streamCtx, c.streamCancelFn = context.WithCancel(c.ctx) // We create child context for the stream so we can cancel it to prevent leak
		c.stream, err = c.tx.db.remoteKV.Seek(streamCtx)
	}

	if err != nil {
		return []byte{}, nil, err
	}
	err = c.stream.Send(&remote.SeekRequest{BucketName: c.bucketName, SeekKey: seek, Prefix: c.prefix, StartSreaming: false})
	if err != nil {
		return []byte{}, nil, err
	}

	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}

	return pair.Key, pair.Value, nil
}

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remoteCursor) Next() ([]byte, []byte, error) {
	if !c.initialized {
		return c.First()
	}

	// if streaming not requested, server will send data only when remoteKV send message to bi-directional channel
	if !c.streamingRequested {
		doStream := c.prefetch > 0
		if err := c.stream.Send(&remote.SeekRequest{StartSreaming: doStream}); err != nil {
			return []byte{}, nil, err
		}
		c.streamingRequested = doStream
	}

	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.Key, pair.Value, nil
}

func (c *remoteCursor) Last() ([]byte, []byte, error) {
	panic("not implemented yet")
}

func (c *remoteCursor) closeGrpcStream() {
	if c.stream == nil {
		return
	}
	defer c.streamCancelFn() // hard cancel stream if graceful wasn't successful

	if c.streamingRequested {
		// if streaming is in progress, can't use `CloseSend` - because
		// server will not read it right not - it busy with streaming data
		// TODO: set flag 'c.streamingRequested' to false when got terminator from server (nil key or os.EOF)
		c.streamCancelFn()
	} else {
		// try graceful close stream
		err := c.stream.CloseSend()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				log.Warn("couldn't send msg CloseSend to server", "err", err)
			}
		} else {
			_, err = c.stream.Recv()
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				log.Warn("received unexpected error from server after CloseSend", "err", err)
			}
		}
	}
	c.stream = nil
	c.streamingRequested = false
}

func (c *remoteCursor) Close() {
	c.closeGrpcStream()
}

func (tx *remoteTx) CursorDupSort(bucket string) CursorDupSort {
	return &remoteCursorDupSort{remoteCursor: tx.Cursor(bucket).(*remoteCursor)}
}

func (c *remoteCursorDupSort) Prefetch(v uint) Cursor {
	c.prefetch = uint32(v)
	return c
}

//func (c *remoteCursorDupSort) initCursor() error {
//	if c.initialized {
//		return nil
//	}
//
//	if c.bucketCfg.AutoDupSortKeysConversion {
//		return fmt.Errorf("class remoteCursorDupSort not compatible with AutoDupSortKeysConversion buckets")
//	}
//
//	if c.bucketCfg.Flags&lmdb.DupSort == 0 {
//		return fmt.Errorf("class remoteCursorDupSort can be used only if bucket created with flag lmdb.DupSort")
//	}
//
//	return c.remoteCursor.initCursor()
//}

func (c *remoteCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	panic("not supported")
}

func (c *remoteCursorDupSort) SeekBothRange(key, value []byte) ([]byte, []byte, error) {
	c.closeGrpcStream() // TODO: if streaming not requested then no reason to close
	c.initialized = true

	var err error
	if c.stream == nil {
		var streamCtx context.Context
		streamCtx, c.streamCancelFn = context.WithCancel(c.ctx) // We create child context for the stream so we can cancel it to prevent leak
		c.stream, err = c.tx.db.remoteKV.Seek(streamCtx)
		if err != nil {
			return []byte{}, nil, err
		}
	}
	err = c.stream.Send(&remote.SeekRequest{BucketName: c.bucketName, SeekKey: key, SeekValue: value, Prefix: c.prefix, StartSreaming: false})
	if err != nil {
		return []byte{}, nil, err
	}

	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}

	return pair.Key, pair.Value, nil
}

func (c *remoteCursorDupSort) DeleteExact(k1, k2 []byte) error      { panic("not supported") }
func (c *remoteCursorDupSort) FirstDup() ([]byte, error)            { panic("not supported") }
func (c *remoteCursorDupSort) NextDup() ([]byte, []byte, error)     { panic("not supported") }
func (c *remoteCursorDupSort) NextNoDup() ([]byte, []byte, error)   { panic("not supported") }
func (c *remoteCursorDupSort) PrevDup() ([]byte, []byte, error)     { panic("not supported") }
func (c *remoteCursorDupSort) PrevNoDup() ([]byte, []byte, error)   { panic("not supported") }
func (c *remoteCursorDupSort) LastDup(k []byte) ([]byte, error)     { panic("not supported") }
func (c *remoteCursorDupSort) AppendDup(k []byte, v []byte) error   { panic("not supported") }
func (c *remoteCursorDupSort) PutNoDupData(key, value []byte) error { panic("not supported") }
func (c *remoteCursorDupSort) DeleteCurrentDuplicates() error       { panic("not supported") }
func (c *remoteCursorDupSort) CountDuplicates() (uint64, error)     { panic("not supported") }
func (tx *remoteTx) CursorDupFixed(bucket string) CursorDupFixed    { panic("not supported") }

func (back *RemoteBackend) AddLocal(signedTx []byte) ([]byte, error) {
	res, err := back.remoteEthBackend.Add(context.Background(), &remote.TxRequest{Signedtx: signedTx})
	if err != nil {
		return common.Hash{}.Bytes(), err
	}
	return res.Hash, nil
}

func (back *RemoteBackend) Etherbase() (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(context.Background(), &remote.EtherbaseRequest{})
	if err != nil {
		return common.Address{}, err
	}

	return common.BytesToAddress(res.Hash), nil
}

func (back *RemoteBackend) NetVersion() (uint64, error) {
	res, err := back.remoteEthBackend.NetVersion(context.Background(), &remote.NetVersionRequest{})
	if err != nil {
		return 0, err
	}

	return res.Id, nil
}
