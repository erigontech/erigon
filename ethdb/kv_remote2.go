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

type remote2Opts struct {
	DialAddress string
	inMemConn   *bufconn.Listener // for tests
	bucketsCfg  BucketConfigsFunc
}

type Remote2KV struct {
	opts     remote2Opts
	remoteKV remote.KV2Client
	remoteDB remote.DBClient
	conn     *grpc.ClientConn
	log      log.Logger
	buckets  dbutils.BucketsCfg
}

type remote2Tx struct {
	ctx                context.Context
	db                 *Remote2KV
	cursors            []*remote2Cursor
	stream             remote.KV2_TxClient
	streamCancelFn     context.CancelFunc
	streamingRequested bool
}

type remote2Cursor struct {
	initialized bool
	id          uint32
	prefetch    uint32
	ctx         context.Context
	prefix      []byte
	stream      remote.KV2_TxClient
	tx          *remote2Tx
	bucketName  string
	bucketCfg   dbutils.BucketConfigItem
}

type remote2CursorDupSort struct {
	*remote2Cursor
}

type remote2CursorDupFixed struct {
	*remote2CursorDupSort
}

func (opts remote2Opts) ReadOnly() remote2Opts {
	return opts
}

func (opts remote2Opts) Path(path string) remote2Opts {
	opts.DialAddress = path
	return opts
}

func (opts remote2Opts) WithBucketsConfig(f BucketConfigsFunc) remote2Opts {
	opts.bucketsCfg = f
	return opts
}

func (opts remote2Opts) InMem(listener *bufconn.Listener) remote2Opts {
	opts.inMemConn = listener
	return opts
}

type Remote2Backend struct {
	opts             remote2Opts
	remoteEthBackend remote.ETHBACKENDClient
	conn             *grpc.ClientConn
	log              log.Logger
}

func (opts remote2Opts) Open(certFile, keyFile, caCert string) (KV, Backend, error) {
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

	db := &Remote2KV{
		opts:     opts,
		conn:     conn,
		remoteKV: remote.NewKV2Client(conn),
		remoteDB: remote.NewDBClient(conn),
		log:      log.New("remote_db", opts.DialAddress),
		buckets:  dbutils.BucketsCfg{},
	}
	customBuckets := opts.bucketsCfg(dbutils.BucketsConfigs)
	for name, cfg := range customBuckets { // copy map to avoid changing global variable
		db.buckets[name] = cfg
	}

	eth := &Remote2Backend{
		opts:             opts,
		remoteEthBackend: remote.NewETHBACKENDClient(conn),
		conn:             conn,
		log:              log.New("remote_db", opts.DialAddress),
	}

	return db, eth, nil
}

func (opts remote2Opts) MustOpen() (KV, Backend) {
	db, txPool, err := opts.Open("", "", "")
	if err != nil {
		panic(err)
	}
	return db, txPool
}

func NewRemote2() remote2Opts {
	return remote2Opts{bucketsCfg: DefaultBucketConfigs}
}

func (db *Remote2KV) AllBuckets() dbutils.BucketsCfg {
	return db.buckets
}

// Close
// All transactions must be closed before closing the database.
func (db *Remote2KV) Close() {
	if db.conn != nil {
		if err := db.conn.Close(); err != nil {
			db.log.Warn("failed to close remote DB", "err", err)
		} else {
			db.log.Info("remote database closed")
		}
		db.conn = nil
	}
}

func (db *Remote2KV) DiskSize(ctx context.Context) (uint64, error) {
	sizeReply, err := db.remoteDB.Size(ctx, &remote.SizeRequest{})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (db *Remote2KV) Begin(ctx context.Context, parent Tx, writable bool) (Tx, error) {
	streamCtx, streamCancelFn := context.WithCancel(ctx) // We create child context for the stream so we can cancel it to prevent leak
	stream, err := db.remoteKV.Tx(streamCtx)
	if err != nil {
		streamCancelFn()
		return nil, err
	}
	return &remote2Tx{ctx: ctx, db: db, stream: stream, streamCancelFn: streamCancelFn}, nil
}

func (db *Remote2KV) View(ctx context.Context, f func(tx Tx) error) (err error) {
	tx, err := db.Begin(ctx, nil, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *Remote2KV) Update(ctx context.Context, f func(tx Tx) error) (err error) {
	return fmt.Errorf("remote db provider doesn't support .Update method")
}

func (tx *remote2Tx) Comparator(bucket string) dbutils.CmpFunc { panic("not implemented yet") }
func (tx *remote2Tx) Cmp(bucket string, a, b []byte) int       { panic("not implemented yet") }
func (tx *remote2Tx) DCmp(bucket string, a, b []byte) int      { panic("not implemented yet") }

func (tx *remote2Tx) Commit(ctx context.Context) error {
	panic("remote db is read-only")
}

func (tx *remote2Tx) Rollback() {
	for _, c := range tx.cursors {
		c.Close()
	}
	tx.closeGrpcStream()
}

func (c *remote2Cursor) Prefix(v []byte) Cursor {
	c.prefix = v
	return c
}

func (c *remote2Cursor) Prefetch(v uint) Cursor {
	c.prefetch = uint32(v)
	return c
}

func (tx *remote2Tx) BucketSize(name string) (uint64, error) {
	sizeReply, err := tx.db.remoteDB.BucketSize(tx.ctx, &remote.BucketSizeRequest{BucketName: name})
	if err != nil {
		return 0, err
	}
	return sizeReply.Size, nil
}

func (tx *remote2Tx) Get(bucket string, key []byte) (val []byte, err error) {
	c := tx.Cursor(bucket)
	defer c.Close()
	return c.SeekExact(key)
}

func (tx *remote2Tx) Has(bucket string, key []byte) (bool, error) {
	c := tx.Cursor(bucket)
	defer c.Close()
	k, _, err := c.Seek(key)
	if err != nil {
		return false, err
	}
	return bytes.Equal(key, k), nil
}

func (c *remote2Cursor) SeekExact(key []byte) (val []byte, err error) {
	if err := c.initCursor(); err != nil {
		return nil, err
	}
	return c.seekExact(key)
}

func (c *remote2Cursor) Prev() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.prev()
}

func (tx *remote2Tx) Cursor(bucket string) Cursor {
	b := tx.db.buckets[bucket]
	c := &remote2Cursor{tx: tx, ctx: tx.ctx, bucketName: bucket, bucketCfg: b, stream: tx.stream}
	tx.cursors = append(tx.cursors, c)
	return c
}

func (c *remote2Cursor) initCursor() error {
	if c.initialized {
		return nil
	}
	if err := c.stream.Send(&remote.Cursor{Op: remote.Op_OPEN, BucketName: c.bucketName}); err != nil {
		return err
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return err
	}
	c.id = msg.CursorID
	c.initialized = true
	return nil
}

func (c *remote2Cursor) Put(key []byte, value []byte) error            { panic("not supported") }
func (c *remote2Cursor) PutNoOverwrite(key []byte, value []byte) error { panic("not supported") }
func (c *remote2Cursor) PutCurrent(key, value []byte) error            { panic("not supported") }
func (c *remote2Cursor) Append(key []byte, value []byte) error         { panic("not supported") }
func (c *remote2Cursor) Delete(key []byte) error                       { panic("not supported") }
func (c *remote2Cursor) DeleteCurrent() error                          { panic("not supported") }
func (c *remote2Cursor) Count() (uint64, error)                        { panic("not supported") }
func (c *remote2Cursor) Reserve(k []byte, n int) ([]byte, error)       { panic("not supported") }

func (c *remote2Cursor) first() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remote2Cursor) next() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) nextDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) nextNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) prev() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) prevDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) prevNoDup() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_PREV_NO_DUP}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) last() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) setRange(k []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK, K: k}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) seekExact(k []byte) ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_EXACT, K: k}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remote2Cursor) getBothRange(k, v []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH, K: k, V: v}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) seekBothExact(k, v []byte) ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_SEEK_BOTH_EXACT, K: k, V: v}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) firstDup() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_FIRST_DUP}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remote2Cursor) lastDup(k []byte) ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_LAST_DUP, K: k}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remote2Cursor) getCurrent() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CURRENT}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}
func (c *remote2Cursor) multiple() ([]byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_GET_MULTIPLE}); err != nil {
		return nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return pair.V, nil
}
func (c *remote2Cursor) nextMultiple() ([]byte, []byte, error) {
	if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_NEXT_MULTIPLE}); err != nil {
		return []byte{}, nil, err
	}
	pair, err := c.stream.Recv()
	if err != nil {
		return []byte{}, nil, err
	}
	return pair.K, pair.V, nil
}

func (c *remote2Cursor) Current() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.getCurrent()
}

// Seek - doesn't start streaming (because much of code does only several .Seek calls without reading sequence of data)
// .Next() - does request streaming (if configured by user)
func (c *remote2Cursor) Seek(seek []byte) ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.setRange(seek)
}

func (c *remote2Cursor) First() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.first()
}

// Next - returns next data element from server, request streaming (if configured by user)
func (c *remote2Cursor) Next() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.next()
}

func (c *remote2Cursor) Last() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.last()
}

func (tx *remote2Tx) closeGrpcStream() {
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

func (c *remote2Cursor) Close() {
	if c.initialized {
		if err := c.stream.Send(&remote.Cursor{Cursor: c.id, Op: remote.Op_CLOSE}); err == nil {
			_, _ = c.stream.Recv()
		}
		c.initialized = false
	}
}

func (tx *remote2Tx) CursorDupSort(bucket string) CursorDupSort {
	return &remoteCursorDupSort{remoteCursor: tx.Cursor(bucket).(*remoteCursor)}
}

func (c *remote2CursorDupSort) Prefetch(v uint) Cursor {
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

func (c *remote2CursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.seekBothExact(key, value)
}

func (c *remote2CursorDupSort) SeekBothRange(key, value []byte) ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.getBothRange(key, value)
}

func (c *remote2CursorDupSort) DeleteExact(k1, k2 []byte) error      { panic("not supported") }
func (c *remote2CursorDupSort) AppendDup(k []byte, v []byte) error   { panic("not supported") }
func (c *remote2CursorDupSort) PutNoDupData(key, value []byte) error { panic("not supported") }
func (c *remote2CursorDupSort) DeleteCurrentDuplicates() error       { panic("not supported") }
func (c *remote2CursorDupSort) CountDuplicates() (uint64, error)     { panic("not supported") }

func (c *remote2CursorDupSort) FirstDup() ([]byte, error) {
	if err := c.initCursor(); err != nil {
		return nil, err
	}
	return c.firstDup()
}
func (c *remote2CursorDupSort) NextDup() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.nextDup()
}
func (c *remote2CursorDupSort) NextNoDup() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.nextNoDup()
}
func (c *remote2CursorDupSort) PrevDup() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.prevDup()
}
func (c *remote2CursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.prevNoDup()
}
func (c *remote2CursorDupSort) LastDup(k []byte) ([]byte, error) {
	if err := c.initCursor(); err != nil {
		return nil, err
	}
	return c.lastDup(k)
}

func (tx *remote2Tx) CursorDupFixed(bucket string) CursorDupFixed {
	return &remote2CursorDupFixed{remote2CursorDupSort: tx.CursorDupSort(bucket).(*remote2CursorDupSort)}
}

func (c *remote2CursorDupFixed) GetMulti() ([]byte, error) {
	if err := c.initCursor(); err != nil {
		return nil, err
	}
	return c.multiple()
}

func (c *remote2CursorDupFixed) NextMulti() ([]byte, []byte, error) {
	if err := c.initCursor(); err != nil {
		return []byte{}, nil, err
	}
	return c.nextMultiple()
}

func (c *remote2CursorDupFixed) PutMulti(key []byte, page []byte, stride int) error {
	panic("not supported")
}

func (back *Remote2Backend) AddLocal(signedTx []byte) ([]byte, error) {
	res, err := back.remoteEthBackend.Add(context.Background(), &remote.TxRequest{Signedtx: signedTx})
	if err != nil {
		return common.Hash{}.Bytes(), err
	}
	return res.Hash, nil
}

func (back *Remote2Backend) Etherbase() (common.Address, error) {
	res, err := back.remoteEthBackend.Etherbase(context.Background(), &remote.EtherbaseRequest{})
	if err != nil {
		return common.Address{}, err
	}

	return common.BytesToAddress(res.Hash), nil
}

func (back *Remote2Backend) NetVersion() (uint64, error) {
	res, err := back.remoteEthBackend.NetVersion(context.Background(), &remote.NetVersionRequest{})
	if err != nil {
		return 0, err
	}

	return res.Id, nil
}
